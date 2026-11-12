package httpapi_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Aditya-7826/ledger-service/internal/store"
)

func TestRunInTxRetriesOnlyDeadlocksAndSerializationFailures(t *testing.T) {
	ctx := context.Background()

	// Fails twice with a retryable code, then succeeds.
	for _, code := range []string{"40P01", "40001"} {
		calls := 0
		err := store.RunInTx(ctx, pool, func(pgx.Tx) error {
			calls++
			if calls < 3 {
				return &pgconn.PgError{Code: code}
			}
			return nil
		})
		if err != nil || calls != 3 {
			t.Errorf("%s: err=%v calls=%d, want nil after 3 calls", code, err, calls)
		}
	}

	// Keeps failing: one try plus MaxRetries retries, then ErrBusy.
	calls := 0
	err := store.RunInTx(ctx, pool, func(pgx.Tx) error {
		calls++
		return &pgconn.PgError{Code: "40P01"}
	})
	if !errors.Is(err, store.ErrBusy) || calls != 1+store.MaxRetries {
		t.Errorf("err=%v calls=%d, want ErrBusy after %d calls", err, calls, 1+store.MaxRetries)
	}

	// Anything else (here a unique violation) is never retried.
	calls = 0
	err = store.RunInTx(ctx, pool, func(pgx.Tx) error {
		calls++
		return &pgconn.PgError{Code: "23505"}
	})
	if err == nil || errors.Is(err, store.ErrBusy) || calls != 1 {
		t.Errorf("err=%v calls=%d, want the original error after 1 call", err, calls)
	}
}
