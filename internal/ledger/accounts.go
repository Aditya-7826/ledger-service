package ledger

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Page is one page of a keyset-paginated list. NextCursor is nil on the last page.
type Page[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"next_cursor"`
}

const accountColumns = `a.id, a.name, a.currency, a.status, a.kind, b.balance_minor, b.last_seq, a.created_at`

func scanAccount(row pgx.Row) (*Account, error) {
	var a Account
	if err := row.Scan(&a.ID, &a.Name, &a.Currency, &a.Status, &a.Kind, &a.BalanceMinor, &a.LastSeq, &a.CreatedAt); err != nil {
		return nil, err
	}
	a.CreatedAt = a.CreatedAt.UTC()
	return &a, nil
}

func accountNotFound(id uuid.UUID) *Error {
	return errf(http.StatusNotFound, "account_not_found", "account %s not found", id)
}

// CreateAccount creates an ACTIVE USER account with a zero balance.
func (s *Service) CreateAccount(ctx context.Context, name, currency string) (*Account, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, invalid("name is required")
	}
	if e := validateText("name", name, MaxNameChars); e != nil {
		return nil, e
	}
	if !currencyRe.MatchString(currency) {
		return nil, invalid("currency must be 3 upper-case letters")
	}
	id := newID()
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO accounts (id, name, currency) VALUES ($1, $2, $3)`, id, name, currency); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO account_balances (account_id) VALUES ($1)`, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	return s.GetAccount(ctx, id)
}

// GetAccount returns an account with its balance.
func (s *Service) GetAccount(ctx context.Context, id uuid.UUID) (*Account, error) {
	a, err := scanAccount(s.pool.QueryRow(ctx, `SELECT `+accountColumns+`
		FROM accounts a JOIN account_balances b ON b.account_id = a.id WHERE a.id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, accountNotFound(id)
	}
	return a, err
}

// ListAccounts returns accounts newest first.
func (s *Service) ListAccounts(ctx context.Context, cursor *uuid.UUID, limit int) (Page[Account], error) {
	rows, err := s.pool.Query(ctx, `SELECT `+accountColumns+`
		FROM accounts a JOIN account_balances b ON b.account_id = a.id
		WHERE ($1::uuid IS NULL OR a.id < $1) ORDER BY a.id DESC LIMIT $2`, cursor, limit+1)
	if err != nil {
		return Page[Account]{}, err
	}
	defer rows.Close()
	page := Page[Account]{Items: []Account{}}
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return page, err
		}
		page.Items = append(page.Items, *a)
	}
	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		next := page.Items[limit-1].ID.String()
		page.NextCursor = &next
	}
	return page, rows.Err()
}

// SetFrozen freezes or unfreezes an account. It takes the same row lock as a
// transfer, so an in-flight transfer either commits before the freeze or
// sees FROZEN afterwards.
func (s *Service) SetFrozen(ctx context.Context, id uuid.UUID, frozen bool) (*Account, error) {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var status, kind string
		err := tx.QueryRow(ctx, `SELECT status, kind FROM accounts WHERE id = $1 FOR UPDATE`, id).Scan(&status, &kind)
		if errors.Is(err, pgx.ErrNoRows) {
			return accountNotFound(id)
		}
		if err != nil {
			return err
		}
		if kind == KindSystem {
			return errf(http.StatusConflict, "system_account", "the SYSTEM account cannot be frozen")
		}
		if status == StatusClosed {
			return errf(http.StatusConflict, "account_closed", "account %s is closed", id)
		}
		target := StatusActive
		if frozen {
			target = StatusFrozen
		}
		if status == target {
			return nil
		}
		_, err = tx.Exec(ctx, `UPDATE accounts SET status = $2 WHERE id = $1`, id, target)
		return err
	})
	if err != nil {
		return nil, err
	}
	return s.GetAccount(ctx, id)
}

// ListEntries returns an account's entries, newest (highest account_seq)
// first. beforeSeq = 0 starts from the newest.
func (s *Service) ListEntries(ctx context.Context, accountID uuid.UUID, beforeSeq int64, limit int) (Page[Entry], error) {
	page := Page[Entry]{Items: []Entry{}}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM accounts WHERE id = $1)`, accountID).Scan(&exists); err != nil {
		return page, err
	}
	if !exists {
		return page, accountNotFound(accountID)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, transfer_id, account_id, direction, amount_minor, account_seq, created_at
		FROM entries WHERE account_id = $1 AND ($2::bigint = 0 OR account_seq < $2::bigint)
		ORDER BY account_seq DESC LIMIT $3`, accountID, beforeSeq, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.TransferID, &e.AccountID, &e.Direction, &e.AmountMinor, &e.AccountSeq, &e.CreatedAt); err != nil {
			return page, err
		}
		e.CreatedAt = e.CreatedAt.UTC()
		page.Items = append(page.Items, e)
	}
	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		next := strconv.FormatInt(page.Items[limit-1].AccountSeq, 10)
		page.NextCursor = &next
	}
	return page, rows.Err()
}
