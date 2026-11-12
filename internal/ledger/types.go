// Package ledger implements the double-entry ledger: accounts, transfers,
// deposits, reversals, freezing and reconciliation.
package ledger

import (
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Account statuses, kinds, transfer types and entry directions.
const (
	StatusActive = "ACTIVE"
	StatusFrozen = "FROZEN"
	StatusClosed = "CLOSED"

	KindUser   = "USER"
	KindSystem = "SYSTEM"

	TypeTransfer = "TRANSFER"
	TypeDeposit  = "DEPOSIT"
	TypeReversal = "REVERSAL"

	Debit  = "DEBIT"
	Credit = "CREDIT"

	MaxMemoChars = 280
	MaxNameChars = 100
)

// Account is an account with its current balance. A balance is credits minus
// debits, in minor units (paise).
type Account struct {
	ID           uuid.UUID `json:"id"`
	Name         string    `json:"name"`
	Currency     string    `json:"currency"`
	Status       string    `json:"status"`
	Kind         string    `json:"kind"`
	BalanceMinor int64     `json:"balance_minor"`
	LastSeq      int64     `json:"last_seq"`
	CreatedAt    time.Time `json:"created_at"`
}

// Entry is one side of a transfer on one account.
type Entry struct {
	ID          uuid.UUID `json:"id"`
	TransferID  uuid.UUID `json:"transfer_id"`
	AccountID   uuid.UUID `json:"account_id"`
	Direction   string    `json:"direction"`
	AmountMinor int64     `json:"amount_minor"`
	AccountSeq  int64     `json:"account_seq"`
	CreatedAt   time.Time `json:"created_at"`
}

// Transfer is a movement of money with its balanced pair of entries.
type Transfer struct {
	ID                 uuid.UUID  `json:"id"`
	Type               string     `json:"type"`
	FromAccountID      uuid.UUID  `json:"from_account_id"`
	ToAccountID        uuid.UUID  `json:"to_account_id"`
	AmountMinor        int64      `json:"amount_minor"`
	Currency           string     `json:"currency"`
	Memo               string     `json:"memo"`
	ReversesTransferID *uuid.UUID `json:"reverses_transfer_id"`
	ReversedBy         *uuid.UUID `json:"reversed_by_transfer_id"`
	CreatedAt          time.Time  `json:"created_at"`
	Entries            []Entry    `json:"entries"`
}

// Error is a rejection the client can act on. Status is the HTTP status and
// Code a stable machine-readable name.
type Error struct {
	Status int
	Code   string
	Detail string
}

func (e *Error) Error() string { return e.Code + ": " + e.Detail }

func errf(status int, code, format string, args ...any) *Error {
	return &Error{Status: status, Code: code, Detail: fmt.Sprintf(format, args...)}
}

func invalid(format string, args ...any) *Error {
	return errf(http.StatusUnprocessableEntity, "validation_error", format, args...)
}

// Response is the outcome of an idempotent write: the HTTP status, the exact
// body bytes, and whether it was replayed from a stored response.
type Response struct {
	Status   int
	Body     []byte
	Replayed bool
}

// Meta identifies the caller and the idempotency key of a write.
type Meta struct {
	ClientID string
	Key      string
}
