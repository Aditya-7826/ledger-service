package ledger

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const transferColumns = `t.id, t.type, t.from_account_id, t.to_account_id, t.amount_minor, t.currency, t.memo,
	t.reverses_transfer_id, (SELECT r.id FROM transfers r WHERE r.reverses_transfer_id = t.id), t.created_at`

func scanTransfer(row pgx.Row) (*Transfer, error) {
	var t Transfer
	if err := row.Scan(&t.ID, &t.Type, &t.FromAccountID, &t.ToAccountID, &t.AmountMinor, &t.Currency, &t.Memo,
		&t.ReversesTransferID, &t.ReversedBy, &t.CreatedAt); err != nil {
		return nil, err
	}
	t.CreatedAt = t.CreatedAt.UTC()
	t.Entries = []Entry{}
	return &t, nil
}

// GetTransfer returns one transfer with its entries (DEBIT first).
func (s *Service) GetTransfer(ctx context.Context, id uuid.UUID) (*Transfer, error) {
	t, err := scanTransfer(s.pool.QueryRow(ctx, `SELECT `+transferColumns+` FROM transfers t WHERE t.id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errf(http.StatusNotFound, "transfer_not_found", "transfer %s not found", id)
	}
	if err != nil {
		return nil, err
	}
	if err := s.attachEntries(ctx, []*Transfer{t}); err != nil {
		return nil, err
	}
	return t, nil
}

// ListTransfers returns transfers newest first, each with its entries.
// Transfer IDs are UUIDv7 (time-ordered), so the ID is the keyset cursor.
func (s *Service) ListTransfers(ctx context.Context, cursor *uuid.UUID, limit int) (Page[Transfer], error) {
	page := Page[Transfer]{Items: []Transfer{}}
	rows, err := s.pool.Query(ctx, `SELECT `+transferColumns+` FROM transfers t
		WHERE ($1::uuid IS NULL OR t.id < $1) ORDER BY t.id DESC LIMIT $2`, cursor, limit+1)
	if err != nil {
		return page, err
	}
	var list []*Transfer
	for rows.Next() {
		t, err := scanTransfer(rows)
		if err != nil {
			rows.Close()
			return page, err
		}
		list = append(list, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return page, err
	}
	more := len(list) > limit
	if more {
		list = list[:limit]
	}
	if err := s.attachEntries(ctx, list); err != nil {
		return page, err
	}
	for _, t := range list {
		page.Items = append(page.Items, *t)
	}
	if more {
		next := list[limit-1].ID.String()
		page.NextCursor = &next
	}
	return page, nil
}

// attachEntries loads the entries of a contiguous (by ID) run of transfers.
func (s *Service) attachEntries(ctx context.Context, list []*Transfer) error {
	if len(list) == 0 {
		return nil
	}
	byID := make(map[uuid.UUID]*Transfer, len(list))
	lo, hi := list[0].ID, list[0].ID
	for _, t := range list {
		byID[t.ID] = t
		if t.ID.String() < lo.String() {
			lo = t.ID
		}
		if t.ID.String() > hi.String() {
			hi = t.ID
		}
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, transfer_id, account_id, direction, amount_minor, account_seq, created_at
		FROM entries WHERE transfer_id BETWEEN $1 AND $2 ORDER BY transfer_id, direction DESC`, lo, hi)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.TransferID, &e.AccountID, &e.Direction, &e.AmountMinor, &e.AccountSeq, &e.CreatedAt); err != nil {
			return err
		}
		e.CreatedAt = e.CreatedAt.UTC()
		if t, ok := byID[e.TransferID]; ok {
			t.Entries = append(t.Entries, e)
		}
	}
	return rows.Err()
}
