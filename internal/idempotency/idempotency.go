// Package idempotency stores idempotency keys in PostgreSQL, in the same
// transaction as the work they protect. There is deliberately no cache in
// front: a check in one system and a write in another cannot be atomic.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrMismatch means the key was already used with a different request.
var ErrMismatch = errors.New("idempotency key reused with a different request")

var keyRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidKey reports whether s is 1-64 characters of [A-Za-z0-9_-].
// The same rule is used for client IDs.
func ValidKey(s string) bool { return keyRe.MatchString(s) }

// Hash returns the SHA-256 (hex) of the canonical JSON of v. Callers pass a
// struct of already-validated fields, so field order and formatting are fixed.
func Hash(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err) // only plain structs are passed in
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Stored is a response saved by an earlier request with the same key.
type Stored struct {
	Status int
	Body   []byte
}

// Claim takes the key inside tx.
//
// It returns (nil, nil) when this transaction now owns the key and should do
// the work, (stored, nil) when an earlier request already finished and its
// response must be replayed, and ErrMismatch when the key belongs to a
// different request.
//
// A concurrent request with the same key blocks on the primary-key index
// until the first transaction commits or rolls back.
func Claim(ctx context.Context, tx pgx.Tx, clientID, key, hash string, ttl time.Duration) (*Stored, error) {
	for attempt := 0; attempt < 3; attempt++ {
		tag, err := tx.Exec(ctx, `
			INSERT INTO idempotency_keys (client_id, key, request_hash, expires_at)
			VALUES ($1, $2, $3, now() + make_interval(secs => $4))
			ON CONFLICT (client_id, key) DO NOTHING`, clientID, key, hash, ttl.Seconds())
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() == 1 {
			return nil, nil
		}

		var (
			storedHash string
			status     *int
			body       *string
			expired    bool
		)
		err = tx.QueryRow(ctx, `
			SELECT request_hash, response_status, response_body::text, expires_at <= now()
			FROM idempotency_keys WHERE client_id = $1 AND key = $2 FOR UPDATE`, clientID, key).
			Scan(&storedHash, &status, &body, &expired)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // the cleanup job removed it between the two statements
		}
		if err != nil {
			return nil, err
		}
		if expired {
			// Past its TTL but not cleaned up yet: take it over as a new key.
			_, err = tx.Exec(ctx, `
				UPDATE idempotency_keys
				SET request_hash = $3, response_status = NULL, response_body = NULL,
				    created_at = now(), expires_at = now() + make_interval(secs => $4)
				WHERE client_id = $1 AND key = $2`, clientID, key, hash, ttl.Seconds())
			return nil, err
		}
		if storedHash != hash {
			return nil, ErrMismatch
		}
		if status == nil || body == nil {
			return nil, errors.New("idempotency key has no stored response")
		}
		return &Stored{Status: *status, Body: []byte(*body)}, nil
	}
	return nil, errors.New("could not claim idempotency key")
}

// Finish saves the response for the key and returns the body exactly as
// PostgreSQL will return it on a replay, so first response and replays are
// byte-identical.
func Finish(ctx context.Context, tx pgx.Tx, clientID, key string, status int, body []byte) ([]byte, error) {
	var out string
	err := tx.QueryRow(ctx, `
		UPDATE idempotency_keys SET response_status = $3, response_body = $4::text::jsonb
		WHERE client_id = $1 AND key = $2
		RETURNING response_body::text`, clientID, key, status, string(body)).Scan(&out)
	return []byte(out), err
}

// CleanupBatch is how many expired keys one DELETE removes.
const CleanupBatch = 1000

// Cleanup deletes expired keys in batches and returns how many were removed.
func Cleanup(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var total int64
	for {
		tag, err := pool.Exec(ctx, `
			DELETE FROM idempotency_keys WHERE (client_id, key) IN (
				SELECT client_id, key FROM idempotency_keys
				WHERE expires_at <= now() ORDER BY expires_at LIMIT $1
				FOR UPDATE SKIP LOCKED)`, CleanupBatch)
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < CleanupBatch {
			return total, nil
		}
	}
}

// RunCleanup calls Cleanup every interval until ctx is cancelled.
func RunCleanup(ctx context.Context, pool *pgxpool.Pool, interval time.Duration, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := Cleanup(ctx, pool)
			if err != nil && ctx.Err() == nil {
				log.Error("idempotency cleanup failed", "error", err)
			} else if n > 0 {
				log.Info("idempotency keys expired", "deleted", n)
			}
		}
	}
}
