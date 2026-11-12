// Package store owns the PostgreSQL connection pool, migrations and the
// transaction helper that retries deadlocks and serialization failures.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver used by goose
	"github.com/pressly/goose/v3"

	"github.com/Aditya-7826/ledger-service/migrations"
)

// MaxRetries is how many times a transaction is retried after a deadlock
// (40P01) or serialization failure (40001).
const MaxRetries = 3

// ErrBusy is returned when a transaction still fails after MaxRetries retries.
var ErrBusy = errors.New("transaction kept conflicting; try again")

// NewPool opens a connection pool and checks that the database answers.
func NewPool(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// Migrate applies all pending goose migrations.
func Migrate(ctx context.Context, url string) error {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return err
	}
	defer db.Close()
	goose.SetBaseFS(migrations.FS)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.UpContext(ctx, db, ".")
}

// IsRetryable reports whether err is a deadlock or serialization failure.
func IsRetryable(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && (pg.Code == "40P01" || pg.Code == "40001")
}

// RunInTx runs fn in a transaction: commit when fn returns nil, roll back
// otherwise. Deadlocks and serialization failures are retried up to
// MaxRetries times with jittered backoff; any other error is returned as is.
func RunInTx(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	for attempt := 0; ; attempt++ {
		err := pgx.BeginFunc(ctx, pool, fn)
		if err == nil || !IsRetryable(err) {
			return err
		}
		if attempt == MaxRetries {
			return fmt.Errorf("%w: %v", ErrBusy, err)
		}
		backoff := time.Duration(10<<attempt)*time.Millisecond + time.Duration(rand.Int64N(int64(10*time.Millisecond)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}
