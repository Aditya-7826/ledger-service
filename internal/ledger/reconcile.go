package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Check is one reconciliation check.
type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

// Reconciliation is the result of auditing the whole ledger.
type Reconciliation struct {
	OK        bool      `json:"ok"`
	Checks    []Check   `json:"checks"`
	CheckedAt time.Time `json:"checked_at"`
}

// Reconcile audits the ledger on one consistent snapshot:
//  1. the signed sum of all entries is zero;
//  2. every account_balances row equals the sum (and last sequence) of its entries;
//  3. every transfer has exactly one DEBIT and one CREDIT of the transfer's amount;
//  4. every account's account_seq runs 1..n without gaps.
func (s *Service) Reconcile(ctx context.Context) (*Reconciliation, error) {
	out := &Reconciliation{OK: true, CheckedAt: time.Now().UTC()}
	add := func(name string, ok bool, detail string) {
		out.Checks = append(out.Checks, Check{Name: name, OK: ok, Detail: detail})
		out.OK = out.OK && ok
	}
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var sum string
		var entries int64
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount_minor ELSE -amount_minor END), 0)::text, COUNT(*)
			FROM entries`).Scan(&sum, &entries); err != nil {
			return err
		}
		add("entries_sum_to_zero", sum == "0", fmt.Sprintf("signed sum of %d entries = %s", entries, sum))

		var bad int64
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*) FROM account_balances b
			LEFT JOIN (
				SELECT account_id,
				       SUM(CASE direction WHEN 'CREDIT' THEN amount_minor ELSE -amount_minor END) AS total,
				       MAX(account_seq) AS max_seq
				FROM entries GROUP BY account_id
			) e ON e.account_id = b.account_id
			WHERE b.balance_minor <> COALESCE(e.total, 0) OR b.last_seq <> COALESCE(e.max_seq, 0)`).Scan(&bad); err != nil {
			return err
		}
		add("balances_match_entries", bad == 0, fmt.Sprintf("%d account(s) whose stored balance differs from their entries", bad))

		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*) FROM transfers t
			LEFT JOIN (
				SELECT transfer_id,
				       COUNT(*) FILTER (WHERE direction = 'DEBIT')  AS debits,
				       COUNT(*) FILTER (WHERE direction = 'CREDIT') AS credits,
				       MIN(amount_minor) AS lo, MAX(amount_minor) AS hi
				FROM entries GROUP BY transfer_id
			) e ON e.transfer_id = t.id
			WHERE e.transfer_id IS NULL OR e.debits <> 1 OR e.credits <> 1 OR e.lo <> e.hi OR e.lo <> t.amount_minor`).Scan(&bad); err != nil {
			return err
		}
		add("transfers_balanced", bad == 0, fmt.Sprintf("%d transfer(s) without exactly one equal DEBIT and CREDIT", bad))

		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*) FROM (
				SELECT account_id FROM entries GROUP BY account_id
				HAVING COUNT(*) <> MAX(account_seq) OR MIN(account_seq) <> 1
			) gaps`).Scan(&bad); err != nil {
			return err
		}
		add("account_seq_no_gaps", bad == 0, fmt.Sprintf("%d account(s) with a gap in account_seq", bad))
		return nil
	})
	return out, err
}
