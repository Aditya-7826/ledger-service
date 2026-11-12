package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Aditya-7826/ledger-service/internal/events"
)

// NotifyName is the consumer name of the notification builder.
const NotifyName = "notify"

// Notify turns each TransferPosted into two notification rows: the sender
// was debited, the receiver was credited.
type Notify struct{}

// Handle implements Handler.
func (Notify) Handle(ctx context.Context, tx pgx.Tx, h events.Header, rec *kgo.Record) error {
	var e events.TransferPosted
	if err := json.Unmarshal(rec.Value, &e); err != nil {
		return fmt.Errorf("%w: %v", ErrPoison, err)
	}
	if e.TransferID == uuid.Nil || e.FromAccountID == uuid.Nil || e.ToAccountID == uuid.Nil || e.AmountMinor < 1 || len(e.Currency) != 3 {
		return fmt.Errorf("%w: invalid TransferPosted content", ErrPoison)
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO views.notifications
			(event_id, transfer_id, account_id, kind, amount_minor, currency, counterparty_account_id, transfer_type, memo, occurred_at)
		VALUES ($1, $2, $3, 'DEBITED',  $5, $6, $4, $7, $8, $9),
		       ($1, $2, $4, 'CREDITED', $5, $6, $3, $7, $8, $9)
		ON CONFLICT (transfer_id, kind) DO NOTHING`,
		h.EventID, e.TransferID, e.FromAccountID, e.ToAccountID, e.AmountMinor, e.Currency, e.Type, e.Memo, h.OccurredAt)
	return err
}

// Notification is one row of views.notifications as sent to clients.
type Notification struct {
	ID                    int64     `json:"id"`
	TransferID            uuid.UUID `json:"transfer_id"`
	AccountID             uuid.UUID `json:"account_id"`
	Kind                  string    `json:"kind"`
	AmountMinor           int64     `json:"amount_minor"`
	Currency              string    `json:"currency"`
	CounterpartyAccountID uuid.UUID `json:"counterparty_account_id"`
	TransferType          string    `json:"transfer_type"`
	Memo                  string    `json:"memo"`
	OccurredAt            time.Time `json:"occurred_at"`
}

// Hub wakes up waiting SSE connections when new notifications were committed.
type Hub struct {
	mu   sync.Mutex
	subs map[chan struct{}]struct{}
}

// Wake signals every subscriber. It never blocks.
func (h *Hub) Wake() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (h *Hub) subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	if h.subs == nil {
		h.subs = map[chan struct{}]struct{}{}
	}
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *Hub) unsubscribe(ch chan struct{}) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

// Stream serves notifications as Server-Sent Events.
//
// Each event's SSE id is the notification id. A client that reconnects with
// the Last-Event-ID header (browsers send it automatically) receives
// everything after that id, so nothing is missed across a reconnect. Without
// the header the stream starts with new notifications only. A comment line
// is sent every Heartbeat so proxies and clients can tell the stream is alive.
type Stream struct {
	Pool      *pgxpool.Pool
	Hub       *Hub
	Heartbeat time.Duration
}

const streamBatch = 200

func (s *Stream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming is not supported", http.StatusInternalServerError)
		return
	}
	ctx := r.Context()
	var last int64
	raw := r.Header.Get("Last-Event-ID")
	if raw == "" {
		// A client that opens a new connection itself cannot set the header; it may pass the id as a query parameter.
		raw = r.URL.Query().Get("last_event_id")
	}
	if raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			http.Error(w, "Last-Event-ID must be a non-negative integer", http.StatusBadRequest)
			return
		}
		last = n
	} else if err := s.Pool.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) FROM views.notifications`).Scan(&last); err != nil {
		http.Error(w, "database is not reachable", http.StatusServiceUnavailable)
		return
	} else if recent, err := strconv.ParseInt(r.URL.Query().Get("recent"), 10, 64); err == nil && recent > 0 {
		// ?recent=N starts the stream with the N most recent notifications (at most 100).
		last = max(last-min(recent, 100), 0)
	}

	// Subscribe before the first query so nothing committed in between is missed.
	wake := s.Hub.subscribe()
	defer s.Hub.unsubscribe(wake)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // tell nginx not to buffer the stream
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "retry: 2000\n: connected\n\n")
	flusher.Flush()

	heartbeat := s.Heartbeat
	if heartbeat <= 0 {
		heartbeat = 15 * time.Second
	}
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()

	for {
		sent, err := s.sendAfter(ctx, w, &last)
		if err != nil {
			return // client gone or database error: the client reconnects with Last-Event-ID
		}
		if sent > 0 {
			flusher.Flush()
		}
		if sent == streamBatch {
			continue // more may be waiting
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Stream) sendAfter(ctx context.Context, w http.ResponseWriter, last *int64) (int, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, transfer_id, account_id, kind, amount_minor, currency, counterparty_account_id,
		       transfer_type, memo, occurred_at
		FROM views.notifications WHERE id > $1 ORDER BY id LIMIT $2`, *last, streamBatch)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	sent := 0
	for rows.Next() {
		var n Notification
		if err := rows.Scan(&n.ID, &n.TransferID, &n.AccountID, &n.Kind, &n.AmountMinor, &n.Currency,
			&n.CounterpartyAccountID, &n.TransferType, &n.Memo, &n.OccurredAt); err != nil {
			return sent, err
		}
		n.OccurredAt = n.OccurredAt.UTC()
		data, err := json.Marshal(n)
		if err != nil {
			return sent, err
		}
		if _, err := fmt.Fprintf(w, "id: %d\nevent: notification\ndata: %s\n\n", n.ID, data); err != nil {
			return sent, err
		}
		*last = n.ID
		sent++
	}
	return sent, rows.Err()
}
