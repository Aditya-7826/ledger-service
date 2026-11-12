// Package consumer runs Kafka consumers that apply each event exactly once.
//
// Delivery from the outbox relay is at-least-once, so every consumer:
//   - uses a consumer group with manual offset commits;
//   - records the event_id in views.processed_events in the same database
//     transaction as its side effect, so a redelivered event is skipped;
//   - commits the Kafka offset only after that transaction committed;
//   - moves poison messages to a <topic>.dlq topic after MaxAttempts tries
//     instead of blocking the partition.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Aditya-7826/ledger-service/internal/events"
)

var (
	// ErrPoison marks a message that can never be processed (unparseable,
	// unknown schema, invalid content). It goes to the DLQ after MaxAttempts.
	ErrPoison = errors.New("poison message")
	// ErrRetry asks the runner to try the same message again later, for
	// example while a balance consumer waits for a missing sequence number.
	ErrRetry = errors.New("retry later")
)

// DLQSuffix is appended to a topic name to get its dead-letter topic.
const DLQSuffix = ".dlq"

// Handler applies one event inside tx. Returning an error rolls tx back.
type Handler interface {
	Handle(ctx context.Context, tx pgx.Tx, h events.Header, rec *kgo.Record) error
}

// Stats counts what a Runner has done since it started.
type Stats struct {
	Applied      atomic.Int64 // events whose side effect was committed
	Duplicates   atomic.Int64 // redelivered events skipped by event_id
	DeadLettered atomic.Int64 // poison messages moved to the DLQ
}

// Runner consumes one or more topics for one consumer.
type Runner struct {
	Name      string // consumer name; the dedupe key is (Name, event_id)
	EventType string // the only event type this consumer accepts
	Pool      *pgxpool.Pool
	Client    *kgo.Client
	Handler   Handler
	Log       *slog.Logger

	MaxAttempts  int           // tries before a poison message goes to the DLQ (default 3)
	RetryBackoff time.Duration // first wait between tries; doubles up to 2 s (default 100 ms)

	// OnCommitted, if set, runs after an event's transaction committed.
	OnCommitted func(h events.Header)
	// CommitOffsets commits offsets to Kafka; tests replace it to simulate
	// a crash between the database commit and the offset commit.
	CommitOffsets func(ctx context.Context, recs ...*kgo.Record) error

	Stats Stats
}

// NewClient builds a consumer-group client with auto-commit disabled. New
// groups start from the beginning of the topic.
func NewClient(brokers []string, group string, topics ...string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequiredAcks(kgo.AllISRAcks()), // for DLQ production
	)
}

// Run consumes until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) {
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = 3
	}
	if r.RetryBackoff <= 0 {
		r.RetryBackoff = 100 * time.Millisecond
	}
	commit := r.CommitOffsets
	if commit == nil {
		commit = r.Client.CommitRecords
	}
	for ctx.Err() == nil {
		fetches := r.Client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			r.Log.Warn("fetch error", "topic", topic, "partition", partition, "error", err)
		})
		var recs []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) { recs = append(recs, rec) })
		done := r.processAll(ctx, recs)
		if len(done) == 0 {
			continue
		}
		// Offsets are committed only after every database transaction above
		// committed. If this fails or the process dies here, the records are
		// delivered again and skipped as duplicates.
		if err := commit(ctx, done...); err != nil && ctx.Err() == nil {
			r.Log.Warn("offset commit failed; records will be redelivered", "error", err)
		}
	}
}

// BatchSize is the most records applied in one database transaction.
const BatchSize = 500

// processAll handles the records of one fetch, in order, and returns the
// prefix that is finished (applied, skipped or dead-lettered).
//
// Records are applied in batches of up to BatchSize per transaction: one
// commit (and one disk sync) per batch instead of per event. Each record
// still gets its own dedupe insert, so the outcome is the same as one by
// one. If anything in a batch fails (a poison message, a sequence gap, a
// database error), the batch is rolled back and its records are processed
// one at a time with the usual retries and dead-lettering.
func (r *Runner) processAll(ctx context.Context, recs []*kgo.Record) []*kgo.Record {
	done := make([]*kgo.Record, 0, len(recs))
	for start := 0; start < len(recs); start += BatchSize {
		if ctx.Err() != nil {
			return done
		}
		chunk := recs[start:min(start+BatchSize, len(recs))]
		if r.applyBatch(ctx, chunk) {
			done = append(done, chunk...)
			continue
		}
		for _, rec := range chunk {
			if err := r.process(ctx, rec); err != nil {
				return done // only when ctx was cancelled mid-record
			}
			done = append(done, rec)
		}
	}
	return done
}

// applyBatch applies every record in one transaction and reports whether it
// committed. It changes nothing (and returns false) if any record fails.
func (r *Runner) applyBatch(ctx context.Context, recs []*kgo.Record) bool {
	headers := make([]events.Header, len(recs))
	for i, rec := range recs {
		h, err := r.parse(rec)
		if err != nil {
			return false
		}
		headers[i] = h
	}
	var applied []events.Header
	var duplicates int64
	err := pgx.BeginFunc(ctx, r.Pool, func(tx pgx.Tx) error {
		applied, duplicates = applied[:0], 0
		for i, rec := range recs {
			tag, err := tx.Exec(ctx, `INSERT INTO views.processed_events (consumer, event_id) VALUES ($1, $2)
				ON CONFLICT DO NOTHING`, r.Name, headers[i].EventID)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				duplicates++ // also catches the same event twice inside this batch
				continue
			}
			if err := r.Handler.Handle(ctx, tx, headers[i], rec); err != nil {
				return err
			}
			applied = append(applied, headers[i])
		}
		return nil
	})
	if err != nil {
		return false
	}
	r.Stats.Applied.Add(int64(len(applied)))
	r.Stats.Duplicates.Add(duplicates)
	if r.OnCommitted != nil {
		for _, h := range applied {
			r.OnCommitted(h)
		}
	}
	return true
}

// process handles one record until it is applied, skipped or dead-lettered.
func (r *Runner) process(ctx context.Context, rec *kgo.Record) error {
	header, parseErr := r.parse(rec)
	attempts := 0
	backoff := r.RetryBackoff
	for {
		err := parseErr
		if err == nil {
			err = r.apply(ctx, header, rec)
		}
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch {
		case errors.Is(err, ErrPoison):
			attempts++
			if attempts >= r.MaxAttempts {
				return r.deadLetter(ctx, rec, err)
			}
		case errors.Is(err, ErrRetry):
			r.Log.Debug("waiting to retry", "reason", err.Error())
		default:
			r.Log.Warn("processing failed; will retry", "error", err, "topic", rec.Topic,
				"partition", rec.Partition, "offset", rec.Offset)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 2*time.Second)
	}
}

func (r *Runner) parse(rec *kgo.Record) (events.Header, error) {
	var h events.Header
	if err := json.Unmarshal(rec.Value, &h); err != nil {
		return h, fmt.Errorf("%w: not valid JSON: %w", ErrPoison, err)
	}
	if h.EventID == uuid.Nil {
		return h, fmt.Errorf("%w: missing event_id", ErrPoison)
	}
	if h.SchemaVersion != events.SchemaVersion {
		return h, fmt.Errorf("%w: unknown schema_version %d", ErrPoison, h.SchemaVersion)
	}
	if h.EventType != r.EventType {
		return h, fmt.Errorf("%w: unexpected event_type %q", ErrPoison, h.EventType)
	}
	return h, nil
}

// apply runs the handler and the dedupe insert in one transaction.
func (r *Runner) apply(ctx context.Context, h events.Header, rec *kgo.Record) error {
	duplicate := false
	err := pgx.BeginFunc(ctx, r.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO views.processed_events (consumer, event_id) VALUES ($1, $2)
			ON CONFLICT DO NOTHING`, r.Name, h.EventID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			duplicate = true
			return nil
		}
		return r.Handler.Handle(ctx, tx, h, rec)
	})
	if err != nil {
		return err
	}
	if duplicate {
		r.Stats.Duplicates.Add(1)
		return nil
	}
	r.Stats.Applied.Add(1)
	if r.OnCommitted != nil {
		r.OnCommitted(h)
	}
	return nil
}

// deadLetter copies the record to <topic>.dlq and lets the partition move on.
// It keeps trying until the broker accepts it: a poison message is never dropped.
func (r *Runner) deadLetter(ctx context.Context, rec *kgo.Record, cause error) error {
	dlq := &kgo.Record{
		Topic: rec.Topic + DLQSuffix,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: append(append([]kgo.RecordHeader{}, rec.Headers...),
			kgo.RecordHeader{Key: "dlq_reason", Value: []byte(cause.Error())},
			kgo.RecordHeader{Key: "dlq_consumer", Value: []byte(r.Name)},
			kgo.RecordHeader{Key: "dlq_source", Value: []byte(rec.Topic + "/" + strconv.Itoa(int(rec.Partition)) + "/" + strconv.FormatInt(rec.Offset, 10))},
		),
	}
	backoff := r.RetryBackoff
	for {
		err := r.Client.ProduceSync(ctx, dlq).FirstErr()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.Log.Warn("could not write to the DLQ; will retry", "error", err, "topic", dlq.Topic)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 2*time.Second)
	}
	r.Stats.DeadLettered.Add(1)
	r.Log.Error("poison message moved to the DLQ", "reason", cause.Error(), "dlq_topic", dlq.Topic,
		"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset)
	return nil
}
