package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Aditya-7826/ledger-service/internal/events"
)

// BalancesName is the consumer name of the balance read model.
const BalancesName = "balances"

// LedgerReader reads an account's true state from the ledger.
type LedgerReader interface {
	// AccountState sums the account's entries and returns the balance and
	// the highest account_seq.
	AccountState(ctx context.Context, account uuid.UUID) (balance, lastSeq int64, err error)
}

// Balances builds views.balance_view from EntryPosted events.
//
// An event is applied only if its account_seq is exactly last_seq + 1.
// A lower one is a duplicate and is skipped. A higher one means an event is
// missing: the handler asks to be retried, and if the gap is still there
// after GapTimeout it rebuilds the account from the ledger.
type Balances struct {
	Ledger     LedgerReader
	GapTimeout time.Duration
	Log        *slog.Logger

	Rebuilds atomic.Int64

	mu       sync.Mutex
	gapSince map[uuid.UUID]time.Time
}

// Handle implements Handler.
func (b *Balances) Handle(ctx context.Context, tx pgx.Tx, _ events.Header, rec *kgo.Record) error {
	var e events.EntryPosted
	if err := json.Unmarshal(rec.Value, &e); err != nil {
		return fmt.Errorf("%w: %w", ErrPoison, err)
	}
	if e.AccountID == uuid.Nil || e.AccountSeq < 1 || e.AmountMinor < 1 || (e.Direction != "DEBIT" && e.Direction != "CREDIT") {
		return fmt.Errorf("%w: invalid EntryPosted content", ErrPoison)
	}

	var balance, lastSeq int64
	err := tx.QueryRow(ctx, `SELECT balance_minor, last_seq FROM views.balance_view WHERE account_id = $1 FOR UPDATE`,
		e.AccountID).Scan(&balance, &lastSeq)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	switch {
	case e.AccountSeq <= lastSeq:
		// Already reflected in the view (an old or duplicated event).
		b.clearGap(e.AccountID)
		return nil

	case e.AccountSeq == lastSeq+1:
		if e.Direction == "CREDIT" {
			balance += e.AmountMinor
		} else {
			balance -= e.AmountMinor
		}
		b.clearGap(e.AccountID)
		return upsertView(ctx, tx, e.AccountID, balance, e.AccountSeq)

	default:
		waited := b.gapAge(e.AccountID)
		if waited < b.GapTimeout {
			return fmt.Errorf("%w: account %s is at seq %d but received seq %d", ErrRetry, e.AccountID, lastSeq, e.AccountSeq)
		}
		// The missing event did not show up in time: take the truth from the ledger.
		balance, seq, err := b.Ledger.AccountState(ctx, e.AccountID)
		if err != nil {
			return fmt.Errorf("rebuild account %s from the ledger: %w", e.AccountID, err)
		}
		if err := upsertView(ctx, tx, e.AccountID, balance, seq); err != nil {
			return err
		}
		b.clearGap(e.AccountID)
		b.Rebuilds.Add(1)
		b.Log.Warn("sequence gap persisted; rebuilt the account's balance view from the ledger",
			"account_id", e.AccountID, "view_seq", lastSeq, "received_seq", e.AccountSeq, "ledger_seq", seq,
			"waited", waited.Round(time.Millisecond).String())
		return nil
	}
}

func upsertView(ctx context.Context, tx pgx.Tx, account uuid.UUID, balance, seq int64) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO views.balance_view (account_id, balance_minor, last_seq) VALUES ($1, $2, $3)
		ON CONFLICT (account_id) DO UPDATE
		SET balance_minor = EXCLUDED.balance_minor, last_seq = EXCLUDED.last_seq, updated_at = now()`,
		account, balance, seq)
	return err
}

// gapAge returns how long the account has been waiting on a gap, starting the clock on first call.
func (b *Balances) gapAge(account uuid.UUID) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.gapSince == nil {
		b.gapSince = map[uuid.UUID]time.Time{}
	}
	since, ok := b.gapSince[account]
	if !ok {
		b.gapSince[account] = time.Now()
		return 0
	}
	return time.Since(since)
}

func (b *Balances) clearGap(account uuid.UUID) {
	b.mu.Lock()
	delete(b.gapSince, account)
	b.mu.Unlock()
}
