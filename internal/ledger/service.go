package ledger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Aditya-7826/ledger-service/internal/events"
	"github.com/Aditya-7826/ledger-service/internal/idempotency"
	"github.com/Aditya-7826/ledger-service/internal/store"
)

// Options configures a Service.
type Options struct {
	MaxTransferMinor int64
	IdempotencyTTL   time.Duration
}

// Service is the ledger. All writes go through PostgreSQL transactions.
type Service struct {
	pool        *pgxpool.Pool
	maxTransfer int64
	ttl         time.Duration

	sysMu sync.Mutex
	sys   map[string]uuid.UUID // currency -> SYSTEM account id
}

// NewService builds a Service on the given pool.
func NewService(pool *pgxpool.Pool, opts Options) *Service {
	return &Service{pool: pool, maxTransfer: opts.MaxTransferMinor, ttl: opts.IdempotencyTTL, sys: map[string]uuid.UUID{}}
}

var currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)

func newID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// ---------------------------------------------------------------- validation

func (s *Service) validateMoney(amount int64, currency, memo string) *Error {
	if amount <= 0 {
		return invalid("amount_minor must be greater than 0")
	}
	if amount > s.maxTransfer {
		return invalid("amount_minor must not exceed %d", s.maxTransfer)
	}
	if !currencyRe.MatchString(currency) {
		return invalid("currency must be 3 upper-case letters")
	}
	return validateText("memo", memo, MaxMemoChars)
}

func validateText(field, v string, max int) *Error {
	if utf8.RuneCountInString(v) > max {
		return invalid("%s must be at most %d characters", field, max)
	}
	if strings.ContainsRune(v, 0) {
		return invalid("%s must not contain NUL characters", field)
	}
	return nil
}

// ---------------------------------------------------------------- idempotent writes

// idempotent runs fn inside one transaction guarded by the idempotency key.
//
// A new key runs fn and stores its response. A finished key replays the
// stored response. If fn fails, the whole transaction, including the key, is
// rolled back, so the same key can be retried later.
func (s *Service) idempotent(ctx context.Context, m Meta, hash string, fn func(tx pgx.Tx) (any, error)) (Response, error) {
	var resp Response
	err := store.RunInTx(ctx, s.pool, func(tx pgx.Tx) error {
		resp = Response{}
		stored, err := idempotency.Claim(ctx, tx, m.ClientID, m.Key, hash, s.ttl)
		if err != nil {
			return err
		}
		if stored != nil {
			resp = Response{Status: stored.Status, Body: stored.Body, Replayed: true}
			return nil
		}
		out, err := fn(tx)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(out)
		if err != nil {
			return err
		}
		body, err := idempotency.Finish(ctx, tx, m.ClientID, m.Key, http.StatusCreated, raw)
		if err != nil {
			return err
		}
		resp = Response{Status: http.StatusCreated, Body: body}
		return nil
	})
	switch {
	case err == nil:
		return resp, nil
	case errors.Is(err, idempotency.ErrMismatch):
		return Response{}, errf(http.StatusUnprocessableEntity, "idempotency_key_reused",
			"idempotency key reused with a different request")
	case errors.Is(err, store.ErrBusy):
		return Response{}, errf(http.StatusServiceUnavailable, "busy", "the ledger is busy; retry with the same idempotency key")
	default:
		return Response{}, err
	}
}

// TransferInput is a validated-shape request to move money between two accounts.
type TransferInput struct {
	From        uuid.UUID
	To          uuid.UUID
	AmountMinor int64
	Currency    string
	Memo        string
}

// Transfer moves money from one account to another.
func (s *Service) Transfer(ctx context.Context, m Meta, in TransferInput) (Response, error) {
	if e := s.validateMoney(in.AmountMinor, in.Currency, in.Memo); e != nil {
		return Response{}, e
	}
	if in.From == in.To {
		return Response{}, invalid("from_account_id and to_account_id must be different")
	}
	hash := idempotency.Hash(struct {
		Op       string    `json:"op"`
		From     uuid.UUID `json:"from"`
		To       uuid.UUID `json:"to"`
		Amount   int64     `json:"amount_minor"`
		Currency string    `json:"currency"`
		Memo     string    `json:"memo"`
	}{"transfer", in.From, in.To, in.AmountMinor, in.Currency, in.Memo})

	return s.idempotent(ctx, m, hash, func(tx pgx.Tx) (any, error) {
		return s.post(ctx, tx, posting{meta: m, typ: TypeTransfer, from: in.From, to: in.To,
			amount: in.AmountMinor, currency: in.Currency, memo: in.Memo})
	})
}

// DepositInput is a request to add money to an account from the SYSTEM account.
type DepositInput struct {
	AccountID   uuid.UUID
	AmountMinor int64
	Currency    string
	Memo        string
}

// Deposit credits an account from the SYSTEM account of the same currency.
func (s *Service) Deposit(ctx context.Context, m Meta, in DepositInput) (Response, error) {
	if e := s.validateMoney(in.AmountMinor, in.Currency, in.Memo); e != nil {
		return Response{}, e
	}
	hash := idempotency.Hash(struct {
		Op       string    `json:"op"`
		Account  uuid.UUID `json:"account_id"`
		Amount   int64     `json:"amount_minor"`
		Currency string    `json:"currency"`
		Memo     string    `json:"memo"`
	}{"deposit", in.AccountID, in.AmountMinor, in.Currency, in.Memo})

	system, err := s.systemAccount(ctx, in.Currency)
	if err != nil {
		return Response{}, err
	}
	if system == in.AccountID {
		return Response{}, invalid("cannot deposit into the SYSTEM account")
	}
	return s.idempotent(ctx, m, hash, func(tx pgx.Tx) (any, error) {
		return s.post(ctx, tx, posting{meta: m, typ: TypeDeposit, from: system, to: in.AccountID,
			amount: in.AmountMinor, currency: in.Currency, memo: in.Memo})
	})
}

// Reverse creates a compensating REVERSAL transfer for an earlier transfer.
func (s *Service) Reverse(ctx context.Context, m Meta, transferID uuid.UUID, memo string) (Response, error) {
	if e := validateText("memo", memo, MaxMemoChars); e != nil {
		return Response{}, e
	}
	hash := idempotency.Hash(struct {
		Op       string    `json:"op"`
		Transfer uuid.UUID `json:"transfer_id"`
		Memo     string    `json:"memo"`
	}{"reverse", transferID, memo})

	return s.idempotent(ctx, m, hash, func(tx pgx.Tx) (any, error) {
		var orig Transfer
		err := tx.QueryRow(ctx, `
			SELECT type, from_account_id, to_account_id, amount_minor, currency
			FROM transfers WHERE id = $1`, transferID).
			Scan(&orig.Type, &orig.FromAccountID, &orig.ToAccountID, &orig.AmountMinor, &orig.Currency)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errf(http.StatusNotFound, "transfer_not_found", "transfer %s not found", transferID)
		}
		if err != nil {
			return nil, err
		}
		if orig.Type == TypeReversal {
			return nil, errf(http.StatusUnprocessableEntity, "reversal_not_reversible", "a reversal cannot itself be reversed")
		}
		if memo == "" {
			memo = "Reversal of " + transferID.String()
		}
		return s.post(ctx, tx, posting{meta: m, typ: TypeReversal, from: orig.ToAccountID, to: orig.FromAccountID,
			amount: orig.AmountMinor, currency: orig.Currency, memo: memo, reverses: &transferID})
	})
}

// ---------------------------------------------------------------- posting

type posting struct {
	meta     Meta
	typ      string
	from, to uuid.UUID
	amount   int64
	currency string
	memo     string
	reverses *uuid.UUID
}

type lockedAccount struct {
	currency, status, kind string
	balance, lastSeq       int64
}

// post writes one balanced transfer inside tx: it locks both accounts,
// re-validates under the lock, then inserts the transfer and its two entries
// and updates both balance rows.
func (s *Service) post(ctx context.Context, tx pgx.Tx, p posting) (*Transfer, error) {
	// Lock in ascending UUID order so A->B and B->A can never deadlock.
	first, second := p.from, p.to
	if bytes.Compare(first[:], second[:]) > 0 {
		first, second = second, first
	}
	accounts := map[uuid.UUID]*lockedAccount{}
	for _, id := range []uuid.UUID{first, second} {
		a := &lockedAccount{}
		err := tx.QueryRow(ctx, `SELECT currency, status, kind FROM accounts WHERE id = $1 FOR UPDATE`, id).
			Scan(&a.currency, &a.status, &a.kind)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errf(http.StatusNotFound, "account_not_found", "account %s not found", id)
		}
		if err != nil {
			return nil, err
		}
		accounts[id] = a
	}
	// Balances are read only after both locks are held, in a fresh statement,
	// so they reflect every transfer that committed before us.
	rows, err := tx.Query(ctx, `SELECT account_id, balance_minor, last_seq FROM account_balances
		WHERE account_id IN ($1, $2)`, first, second)
	if err != nil {
		return nil, err
	}
	found := 0
	for rows.Next() {
		var id uuid.UUID
		var bal, seq int64
		if err := rows.Scan(&id, &bal, &seq); err != nil {
			rows.Close()
			return nil, err
		}
		accounts[id].balance, accounts[id].lastSeq = bal, seq
		found++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if found != 2 {
		return nil, errors.New("account is missing its balance row")
	}

	from, to := accounts[p.from], accounts[p.to]
	for _, c := range []struct {
		id uuid.UUID
		a  *lockedAccount
	}{{p.from, from}, {p.to, to}} {
		switch c.a.status {
		case StatusFrozen:
			return nil, errf(http.StatusForbidden, "account_frozen", "account %s is frozen", c.id)
		case StatusClosed:
			return nil, errf(http.StatusForbidden, "account_closed", "account %s is closed", c.id)
		}
	}
	if from.currency != to.currency || from.currency != p.currency {
		return nil, errf(http.StatusUnprocessableEntity, "currency_mismatch",
			"currency mismatch: request %s, sender %s, receiver %s", p.currency, from.currency, to.currency)
	}
	if p.reverses != nil {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM transfers WHERE reverses_transfer_id = $1)`,
			*p.reverses).Scan(&exists); err != nil {
			return nil, err
		}
		if exists {
			return nil, alreadyReversed(*p.reverses)
		}
	}
	// Only SYSTEM accounts may go negative: that is how money enters the ledger.
	if from.kind != KindSystem && from.balance < p.amount {
		return nil, errf(http.StatusConflict, "insufficient_funds", "account %s has insufficient funds", p.from)
	}
	if from.balance < math.MinInt64+p.amount || to.balance > math.MaxInt64-p.amount {
		return nil, errf(http.StatusUnprocessableEntity, "amount_overflow", "the resulting balance would overflow")
	}

	t := &Transfer{
		ID: newID(), Type: p.typ, FromAccountID: p.from, ToAccountID: p.to, AmountMinor: p.amount,
		Currency: p.currency, Memo: p.memo, ReversesTransferID: p.reverses,
	}
	debit := Entry{ID: newID(), TransferID: t.ID, AccountID: p.from, Direction: Debit, AmountMinor: p.amount, AccountSeq: from.lastSeq + 1}
	credit := Entry{ID: newID(), TransferID: t.ID, AccountID: p.to, Direction: Credit, AmountMinor: p.amount, AccountSeq: to.lastSeq + 1}

	// PostgreSQL keeps microseconds; truncate so the response, the rows and the events agree.
	now := time.Now().UTC().Truncate(time.Microsecond)
	t.CreatedAt, debit.CreatedAt, credit.CreatedAt = now, now, now

	// Events go into the outbox in this same transaction: they exist exactly
	// when the transfer commits. One EntryPosted per entry, one TransferPosted.
	evDebit := entryEvent(debit, p.currency, now)
	evCredit := entryEvent(credit, p.currency, now)
	evTransfer := events.TransferPosted{
		Header: events.NewHeader(events.TypeTransferPosted, now), TransferID: t.ID, Type: t.Type,
		FromAccountID: p.from, ToAccountID: p.to, AmountMinor: p.amount, Currency: p.currency, Memo: p.memo,
		ReversesTransferID: p.reverses,
	}
	payloads := make([]string, 3)
	for i, ev := range []any{evDebit, evCredit, evTransfer} {
		raw, err := json.Marshal(ev)
		if err != nil {
			return nil, err
		}
		payloads[i] = string(raw)
	}

	b := &pgx.Batch{}
	b.Queue(`INSERT INTO transfers (id, client_id, idempotency_key, from_account_id, to_account_id, amount_minor,
			currency, memo, type, reverses_transfer_id, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		t.ID, p.meta.ClientID, p.meta.Key, p.from, p.to, p.amount, p.currency, p.memo, p.typ, p.reverses, now)
	b.Queue(`INSERT INTO entries (id, transfer_id, account_id, direction, amount_minor, account_seq, created_at)
		VALUES ($1, $2, $3, 'DEBIT', $4, $5, $9), ($6, $2, $7, 'CREDIT', $4, $8, $9)`,
		debit.ID, t.ID, debit.AccountID, p.amount, debit.AccountSeq, credit.ID, credit.AccountID, credit.AccountSeq, now)
	b.Queue(`UPDATE account_balances SET balance_minor = $2, last_seq = $3, updated_at = $4 WHERE account_id = $1`,
		p.from, from.balance-p.amount, debit.AccountSeq, now)
	b.Queue(`UPDATE account_balances SET balance_minor = $2, last_seq = $3, updated_at = $4 WHERE account_id = $1`,
		p.to, to.balance+p.amount, credit.AccountSeq, now)
	b.Queue(`INSERT INTO outbox (event_id, aggregate_id, event_type, payload, created_at) VALUES
			($1, $2, 'EntryPosted', $3::text::jsonb, $10),
			($4, $5, 'EntryPosted', $6::text::jsonb, $10),
			($7, $8, 'TransferPosted', $9::text::jsonb, $10)`,
		evDebit.EventID, debit.AccountID, payloads[0],
		evCredit.EventID, credit.AccountID, payloads[1],
		evTransfer.EventID, t.ID, payloads[2], now)

	br := tx.SendBatch(ctx, b)
	for i := 0; i < b.Len() && err == nil; i++ {
		_, err = br.Exec()
	}
	if cerr := br.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		var pg *pgconn.PgError
		if p.reverses != nil && errors.As(err, &pg) && pg.ConstraintName == "transfers_reverses_transfer_id_key" {
			return nil, alreadyReversed(*p.reverses)
		}
		return nil, err
	}
	t.Entries = []Entry{debit, credit}
	return t, nil
}

func entryEvent(e Entry, currency string, at time.Time) events.EntryPosted {
	return events.EntryPosted{
		Header: events.NewHeader(events.TypeEntryPosted, at), EntryID: e.ID, TransferID: e.TransferID,
		AccountID: e.AccountID, Direction: e.Direction, AmountMinor: e.AmountMinor, AccountSeq: e.AccountSeq,
		Currency: currency,
	}
}

func alreadyReversed(id uuid.UUID) *Error {
	return errf(http.StatusConflict, "already_reversed", "transfer %s has already been reversed", id)
}

// systemAccount returns the SYSTEM account for a currency, creating it on first use.
func (s *Service) systemAccount(ctx context.Context, currency string) (uuid.UUID, error) {
	s.sysMu.Lock()
	defer s.sysMu.Unlock()
	if id, ok := s.sys[currency]; ok {
		return id, nil
	}
	var id uuid.UUID
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO accounts (id, name, currency, kind) VALUES ($1, $2, $3, 'SYSTEM')
			ON CONFLICT (currency) WHERE kind = 'SYSTEM' DO NOTHING`, newID(), "System "+currency, currency); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT id FROM accounts WHERE kind = 'SYSTEM' AND currency = $1`, currency).Scan(&id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO account_balances (account_id) VALUES ($1) ON CONFLICT DO NOTHING`, id)
		return err
	})
	if err != nil {
		return uuid.Nil, err
	}
	s.sys[currency] = id
	return id, nil
}
