// Package outbox moves events from the outbox table to Kafka.
//
// Events are inserted into the outbox in the same transaction as the ledger
// entries, so an event exists exactly when its transfer committed. The relay
// publishes them and marks them published only after the broker has
// acknowledged. A crash between the two publishes the event again on
// restart: delivery is at-least-once, and consumers deduplicate by event_id.
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Message is one outbox row.
type Message struct {
	ID          int64
	EventID     uuid.UUID
	AggregateID uuid.UUID
	EventType   string
	Payload     []byte
}

// Publisher sends a batch to the broker. It must return nil only when every
// message has been acknowledged.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

const (
	// DefaultBatch is how many rows one loop publishes.
	DefaultBatch = 500
	// Retention is how long published rows are kept before pruning.
	Retention = 7 * 24 * time.Hour
)

// Relay publishes outbox rows.
type Relay struct {
	Pool      *pgxpool.Pool
	Publisher Publisher
	Batch     int
	Log       *slog.Logger
	// OnBacklog, if set, receives the number of unpublished rows after each loop.
	OnBacklog func(n int64)
}

// PublishOnce publishes one batch and returns how many rows it marked.
//
// The rows stay locked (FOR UPDATE SKIP LOCKED) until they are marked, so
// several relays can run at once without ever picking the same row. If
// publishing fails the transaction rolls back and nothing is marked.
func (r *Relay) PublishOnce(ctx context.Context) (int, error) {
	batch := r.Batch
	if batch <= 0 {
		batch = DefaultBatch
	}
	published := 0
	err := pgx.BeginFunc(ctx, r.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, event_id, aggregate_id, event_type, payload::text
			FROM outbox WHERE published_at IS NULL
			ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, batch)
		if err != nil {
			return err
		}
		var msgs []Message
		var ids []int64
		for rows.Next() {
			var m Message
			var payload string
			if err := rows.Scan(&m.ID, &m.EventID, &m.AggregateID, &m.EventType, &payload); err != nil {
				rows.Close()
				return err
			}
			m.Payload = []byte(payload)
			msgs = append(msgs, m)
			ids = append(ids, m.ID)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(msgs) == 0 {
			return nil
		}
		if err := r.Publisher.Publish(ctx, msgs); err != nil {
			return fmt.Errorf("publish %d event(s): %w", len(msgs), err)
		}
		// Only now, after the broker acknowledged every message.
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
			return err
		}
		published = len(msgs)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return published, nil
}

// Backlog returns the number of unpublished rows.
func (r *Relay) Backlog(ctx context.Context) (int64, error) {
	var n int64
	err := r.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

// Prune deletes rows that were published more than Retention ago, in batches.
func (r *Relay) Prune(ctx context.Context) (int64, error) {
	var total int64
	for {
		tag, err := r.Pool.Exec(ctx, `
			DELETE FROM outbox WHERE id IN (
				SELECT id FROM outbox
				WHERE published_at IS NOT NULL AND published_at < now() - make_interval(secs => $1)
				ORDER BY id LIMIT 1000)`, Retention.Seconds())
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < 1000 {
			return total, nil
		}
	}
}

// Run publishes until ctx is cancelled. It polls every poll interval when
// the outbox is empty, and backs off (up to 5 s) while publishing fails.
func (r *Relay) Run(ctx context.Context, poll time.Duration) {
	backoff := 100 * time.Millisecond
	lastPrune := time.Time{}
	for ctx.Err() == nil {
		n, err := r.PublishOnce(ctx)
		if r.OnBacklog != nil {
			if b, berr := r.Backlog(ctx); berr == nil {
				r.OnBacklog(b)
			}
		}
		wait := time.Duration(0)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			r.Log.Warn("publish failed; nothing was marked, will retry", "error", err, "retry_in", backoff.String())
			wait = backoff
			backoff = min(backoff*2, 5*time.Second)
		case n == 0:
			backoff = 100 * time.Millisecond
			wait = poll
		default:
			backoff = 100 * time.Millisecond
			r.Log.Debug("published", "events", n)
		}
		if time.Since(lastPrune) > time.Hour {
			lastPrune = time.Now()
			if deleted, err := r.Prune(ctx); err != nil {
				r.Log.Warn("prune failed", "error", err)
			} else if deleted > 0 {
				r.Log.Info("pruned published outbox rows", "deleted", deleted)
			}
		}
		if wait > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}
	}
}
