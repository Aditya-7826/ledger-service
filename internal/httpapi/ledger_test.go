package httpapi_test

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/Aditya-7826/ledger-service/internal/idempotency"
	"github.com/Aditya-7826/ledger-service/internal/ledger"
)

func TestTransferWritesBalancedEntries(t *testing.T) {
	a, b := funded(t, "alice", 10_000), mkAccount(t, "bob")

	r := transfer(t, newKey(), a, b, 2_500)
	if r.status != http.StatusCreated {
		t.Fatalf("transfer: %d %s", r.status, r.body)
	}
	var tr ledger.Transfer
	r.decode(t, &tr)
	// The same transaction wrote the events: two EntryPosted and one TransferPosted.
	if n := countRows(t, `SELECT COUNT(*) FROM outbox WHERE payload->>'transfer_id' = $1 AND published_at IS NULL`, tr.ID.String()); n != 3 {
		t.Errorf("outbox rows for the transfer = %d, want 3", n)
	}
	if tr.Type != ledger.TypeTransfer || tr.AmountMinor != 2_500 || len(tr.Entries) != 2 {
		t.Fatalf("unexpected transfer: %s", r.body)
	}
	debit, credit := tr.Entries[0], tr.Entries[1]
	if debit.Direction != ledger.Debit || debit.AccountID.String() != a || debit.AmountMinor != 2_500 || debit.AccountSeq != 2 {
		t.Errorf("debit entry wrong: %+v", debit)
	}
	if credit.Direction != ledger.Credit || credit.AccountID.String() != b || credit.AmountMinor != 2_500 || credit.AccountSeq != 1 {
		t.Errorf("credit entry wrong: %+v", credit)
	}
	if got := getAccount(t, a); got.BalanceMinor != 7_500 || got.LastSeq != 2 {
		t.Errorf("sender: %+v", got)
	}
	if got := getAccount(t, b); got.BalanceMinor != 2_500 || got.LastSeq != 1 {
		t.Errorf("receiver: %+v", got)
	}

	// GET returns the same transfer with DEBIT first.
	g := call(t, "GET", "/v1/transfers/"+tr.ID.String(), nil, nil)
	var got ledger.Transfer
	g.decode(t, &got)
	if g.status != http.StatusOK || len(got.Entries) != 2 || got.Entries[0].Direction != ledger.Debit {
		t.Errorf("get transfer: %d %s", g.status, g.body)
	}
	assertReconciled(t)
}

func TestInsufficientFundsAndSystemAccount(t *testing.T) {
	a, b := funded(t, "poor", 100), mkAccount(t, "rich")
	r := transfer(t, newKey(), a, b, 101)
	if r.status != http.StatusConflict || r.code() != "insufficient_funds" {
		t.Fatalf("want 409 insufficient_funds, got %d %s", r.status, r.body)
	}
	if getAccount(t, a).BalanceMinor != 100 {
		t.Error("balance changed after a rejected transfer")
	}
	// The SYSTEM account is the only one allowed below zero.
	if n := countRows(t, `SELECT COUNT(*) FROM account_balances b JOIN accounts a ON a.id = b.account_id
		WHERE a.kind = 'USER' AND b.balance_minor < 0`); n != 0 {
		t.Errorf("%d USER account(s) below zero", n)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM account_balances b JOIN accounts a ON a.id = b.account_id
		WHERE a.kind = 'SYSTEM' AND b.balance_minor < 0`); n != 1 {
		t.Errorf("expected the one SYSTEM account to be negative, got %d", n)
	}
}

func TestValidationTable(t *testing.T) {
	a, b := funded(t, "val-a", 1_000_000), mkAccount(t, "val-b")
	closed := mkAccount(t, "val-closed")
	if _, err := pool.Exec(context.Background(), `UPDATE accounts SET status = 'CLOSED' WHERE id = $1`, closed); err != nil {
		t.Fatal(err)
	}
	usd := call(t, "POST", "/v1/accounts", nil, map[string]string{"name": "usd", "currency": "USD"})
	var usdAcc ledger.Account
	usd.decode(t, &usdAcc)
	unknown := uuid.NewString()

	raw := func(amount string) string {
		return fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"amount_minor":%s,"currency":"INR"}`, a, b, amount)
	}
	ok := writeHeaders("")
	cases := []struct {
		name    string
		headers map[string]string
		body    any
		status  int
		code    string
	}{
		{"zero amount", ok, raw("0"), 422, "validation_error"},
		{"negative amount", ok, raw("-5"), 422, "validation_error"},
		{"float amount", ok, raw("10.5"), 400, "invalid_json"},
		{"float with zero fraction", ok, raw("10.0"), 400, "invalid_json"},
		{"exponent amount", ok, raw("1e3"), 400, "invalid_json"},
		{"string amount", ok, raw(`"100"`), 400, "invalid_json"},
		{"amount beyond int64", ok, raw("99999999999999999999"), 400, "invalid_json"},
		{"amount above configured max", ok, raw(fmt.Sprint(testMaxTransfer + 1)), 422, "validation_error"},
		{"missing amount", ok, fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"currency":"INR"}`, a, b), 422, "validation_error"},
		{"self transfer", ok, transferBody(a, a, 100), 422, "validation_error"},
		{"currency mismatch with accounts", ok, map[string]any{"from_account_id": a, "to_account_id": b, "amount_minor": 100, "currency": "USD"}, 422, "currency_mismatch"},
		{"accounts in different currencies", ok, transferBody(a, usdAcc.ID.String(), 100), 422, "currency_mismatch"},
		{"lower-case currency", ok, map[string]any{"from_account_id": a, "to_account_id": b, "amount_minor": 100, "currency": "inr"}, 422, "validation_error"},
		{"four-letter currency", ok, map[string]any{"from_account_id": a, "to_account_id": b, "amount_minor": 100, "currency": "INRX"}, 422, "validation_error"},
		{"unknown sender", ok, transferBody(unknown, b, 100), 404, "account_not_found"},
		{"unknown receiver", ok, transferBody(a, unknown, 100), 404, "account_not_found"},
		{"closed receiver", ok, transferBody(a, closed, 100), 403, "account_closed"},
		{"memo too long", ok, map[string]any{"from_account_id": a, "to_account_id": b, "amount_minor": 100, "currency": "INR", "memo": strings.Repeat("m", 281)}, 422, "validation_error"},
		{"malformed uuid", ok, transferBody("not-a-uuid", b, 100), 400, "invalid_uuid"},
		{"uuid without dashes", ok, transferBody(strings.ReplaceAll(a, "-", ""), b, 100), 400, "invalid_uuid"},
		{"missing idempotency key", map[string]string{"X-Client-Id": testClient}, transferBody(a, b, 100), 400, "invalid_idempotency_key"},
		{"idempotency key with bad characters", map[string]string{"X-Client-Id": testClient, "Idempotency-Key": "has space"}, transferBody(a, b, 100), 400, "invalid_idempotency_key"},
		{"idempotency key too long", map[string]string{"X-Client-Id": testClient, "Idempotency-Key": strings.Repeat("k", 65)}, transferBody(a, b, 100), 400, "invalid_idempotency_key"},
		{"missing client id", map[string]string{"Idempotency-Key": "k1"}, transferBody(a, b, 100), 400, "invalid_client_id"},
		{"oversized body", ok, map[string]any{"from_account_id": a, "to_account_id": b, "amount_minor": 100, "currency": "INR", "memo": strings.Repeat("x", 20_000)}, 413, "body_too_large"},
		{"unknown json field", ok, fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"amount_minor":100,"currency":"INR","admin":true}`, a, b), 400, "invalid_json"},
		{"two json objects", ok, raw("100") + raw("100"), 400, "invalid_json"},
		{"malformed json", ok, `{"from_account_id":`, 400, "invalid_json"},
		{"empty body", ok, nil, 400, "invalid_json"},
	}

	before := countRows(t, `SELECT COUNT(*) FROM transfers`)
	keysBefore := countRows(t, `SELECT COUNT(*) FROM idempotency_keys`)
	outboxBefore := countRows(t, `SELECT COUNT(*) FROM outbox`)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			headers := map[string]string{}
			for k, v := range tc.headers {
				headers[k] = v
			}
			if v, has := headers["Idempotency-Key"]; has && v == "" {
				headers["Idempotency-Key"] = newKey()
			}
			r := call(t, "POST", "/v1/transfers", headers, tc.body)
			if r.status != tc.status || r.code() != tc.code {
				t.Fatalf("want %d %s, got %d %s", tc.status, tc.code, r.status, r.body)
			}
			if ct := r.header.Get("Content-Type"); ct != "application/problem+json" {
				t.Errorf("content type = %q", ct)
			}
			if strings.Contains(string(r.body), "goroutine") || strings.Contains(string(r.body), ".go:") {
				t.Errorf("response leaks internals: %s", r.body)
			}
		})
	}
	if after := countRows(t, `SELECT COUNT(*) FROM transfers`); after != before {
		t.Errorf("rejected requests created %d transfer(s)", after-before)
	}
	if after := countRows(t, `SELECT COUNT(*) FROM idempotency_keys`); after != keysBefore {
		t.Errorf("rejected requests left %d idempotency key(s) behind", after-keysBefore)
	}
	if after := countRows(t, `SELECT COUNT(*) FROM outbox`); after != outboxBefore {
		t.Errorf("rejected requests wrote %d outbox row(s)", after-outboxBefore)
	}
}

func TestBalanceOverflowIsRejected(t *testing.T) {
	a, b := funded(t, "ovf-a", 1_000), mkAccount(t, "ovf-b")
	ctx := context.Background()
	// Put the receiver one paise away from the int64 limit, then restore it.
	if _, err := pool.Exec(ctx, `UPDATE account_balances SET balance_minor = $2 WHERE account_id = $1`, b, int64(math.MaxInt64-1)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := pool.Exec(ctx, `UPDATE account_balances SET balance_minor = 0 WHERE account_id = $1`, b); err != nil {
			t.Fatal(err)
		}
		assertReconciled(t)
	}()
	if r := transfer(t, newKey(), a, b, 2); r.status != 422 || r.code() != "amount_overflow" {
		t.Fatalf("want 422 amount_overflow, got %d %s", r.status, r.body)
	}
}

func TestReconcileDetectsTamperedBalance(t *testing.T) {
	a := funded(t, "tamper", 500)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `UPDATE account_balances SET balance_minor = 501 WHERE account_id = $1`, a); err != nil {
		t.Fatal(err)
	}
	r := call(t, "GET", "/v1/reconcile", nil, nil)
	var rec ledger.Reconciliation
	r.decode(t, &rec)
	if rec.OK {
		t.Error("reconcile passed although a balance was tampered with")
	}
	for _, c := range rec.Checks {
		if c.Name == "balances_match_entries" && c.OK {
			t.Error("balances_match_entries should fail")
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE account_balances SET balance_minor = 500 WHERE account_id = $1`, a); err != nil {
		t.Fatal(err)
	}
	assertReconciled(t)
}

func TestEntriesAreAppendOnly(t *testing.T) {
	a, b := funded(t, "ao-a", 1_000), mkAccount(t, "ao-b")
	if r := transfer(t, newKey(), a, b, 10); r.status != 201 {
		t.Fatalf("transfer: %d", r.status)
	}
	ctx := context.Background()
	before := countRows(t, `SELECT COUNT(*) FROM entries`)
	for _, sql := range []string{
		`UPDATE entries SET amount_minor = amount_minor + 1 WHERE account_id = '` + a + `'`,
		`DELETE FROM entries WHERE account_id = '` + a + `'`,
		`TRUNCATE entries`,
	} {
		if _, err := pool.Exec(ctx, sql); err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Errorf("%q: expected an append-only error, got %v", sql, err)
		}
	}
	if after := countRows(t, `SELECT COUNT(*) FROM entries`); after != before {
		t.Errorf("entries changed: %d -> %d", before, after)
	}
}

func TestReversals(t *testing.T) {
	a, b, c := funded(t, "rev-a", 1_000), mkAccount(t, "rev-b"), mkAccount(t, "rev-c")
	var tr ledger.Transfer
	transfer(t, newKey(), a, b, 400).decode(t, &tr)
	path := "/v1/transfers/" + tr.ID.String() + "/reverse"

	// Reverse once: money goes back and the reversal points at the original.
	key := newKey()
	r := call(t, "POST", path, writeHeaders(key), nil)
	if r.status != 201 {
		t.Fatalf("reverse: %d %s", r.status, r.body)
	}
	var rev ledger.Transfer
	r.decode(t, &rev)
	if rev.Type != ledger.TypeReversal || rev.ReversesTransferID == nil || *rev.ReversesTransferID != tr.ID ||
		rev.FromAccountID.String() != b || rev.ToAccountID.String() != a || rev.AmountMinor != 400 {
		t.Fatalf("unexpected reversal: %s", r.body)
	}
	if getAccount(t, a).BalanceMinor != 1_000 || getAccount(t, b).BalanceMinor != 0 {
		t.Error("balances not restored by the reversal")
	}
	// The same key replays; it does not reverse again.
	if again := call(t, "POST", path, writeHeaders(key), nil); again.status != 201 || again.header.Get("Idempotent-Replayed") != "true" {
		t.Errorf("replay of reversal: %d %s", again.status, again.body)
	}
	// A second reversal with a new key is refused.
	if r := call(t, "POST", path, writeHeaders(newKey()), nil); r.status != 409 || r.code() != "already_reversed" {
		t.Errorf("second reversal: want 409 already_reversed, got %d %s", r.status, r.body)
	}
	// A reversal cannot be reversed.
	if r := call(t, "POST", "/v1/transfers/"+rev.ID.String()+"/reverse", writeHeaders(newKey()), nil); r.status != 422 || r.code() != "reversal_not_reversible" {
		t.Errorf("reversal of reversal: want 422, got %d %s", r.status, r.body)
	}
	// The original now shows which transfer reversed it.
	var orig ledger.Transfer
	call(t, "GET", "/v1/transfers/"+tr.ID.String(), nil, nil).decode(t, &orig)
	if orig.ReversedBy == nil || *orig.ReversedBy != rev.ID {
		t.Error("original transfer does not show reversed_by_transfer_id")
	}

	// Receiver spent the money: reversal is refused with 409 and stays possible later.
	var t2 ledger.Transfer
	transfer(t, newKey(), a, b, 300).decode(t, &t2)
	if r := transfer(t, newKey(), b, c, 250); r.status != 201 {
		t.Fatalf("spend: %d", r.status)
	}
	path2 := "/v1/transfers/" + t2.ID.String() + "/reverse"
	key2 := newKey()
	if r := call(t, "POST", path2, writeHeaders(key2), nil); r.status != 409 || r.code() != "insufficient_funds" {
		t.Fatalf("reverse without funds: want 409 insufficient_funds, got %d %s", r.status, r.body)
	}
	deposit(t, b, 250)
	if r := call(t, "POST", path2, writeHeaders(key2), nil); r.status != 201 || r.header.Get("Idempotent-Replayed") != "" {
		t.Errorf("reverse after funds arrived: %d %s", r.status, r.body)
	}
	// Unknown transfer.
	if r := call(t, "POST", "/v1/transfers/"+uuid.NewString()+"/reverse", writeHeaders(newKey()), nil); r.status != 404 {
		t.Errorf("unknown transfer: %d", r.status)
	}
	assertReconciled(t)
}

func TestKeysetPagination(t *testing.T) {
	a, b := funded(t, "page-a", 1_000), mkAccount(t, "page-b")
	for i := 0; i < 7; i++ {
		if r := transfer(t, newKey(), a, b, 1); r.status != 201 {
			t.Fatalf("transfer: %d", r.status)
		}
	}
	// a has 1 deposit + 7 debits = 8 entries; walk them 3 at a time.
	var seqs []int64
	cursor := ""
	for pages := 0; pages < 10; pages++ {
		r := call(t, "GET", "/v1/accounts/"+a+"/entries?limit=3&cursor="+cursor, nil, nil)
		var page ledger.Page[ledger.Entry]
		r.decode(t, &page)
		for _, e := range page.Items {
			seqs = append(seqs, e.AccountSeq)
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	if fmt.Sprint(seqs) != "[8 7 6 5 4 3 2 1]" {
		t.Errorf("entries pages = %v", seqs)
	}

	// Transfers: newest first, no duplicates across pages.
	seen := map[uuid.UUID]bool{}
	cursor = ""
	var prev string
	for pages := 0; pages < 4; pages++ {
		r := call(t, "GET", "/v1/transfers?limit=4&cursor="+cursor, nil, nil)
		var page ledger.Page[ledger.Transfer]
		r.decode(t, &page)
		for _, tr := range page.Items {
			if seen[tr.ID] {
				t.Fatalf("transfer %s appears twice", tr.ID)
			}
			if prev != "" && tr.ID.String() >= prev {
				t.Fatalf("transfers not in descending order")
			}
			if len(tr.Entries) != 2 {
				t.Fatalf("transfer %s has %d entries", tr.ID, len(tr.Entries))
			}
			seen[tr.ID], prev = true, tr.ID.String()
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	if len(seen) < 8 {
		t.Errorf("walked only %d transfers", len(seen))
	}
	for _, bad := range []string{"/v1/transfers?limit=101", "/v1/transfers?limit=0", "/v1/transfers?cursor=x", "/v1/accounts/" + a + "/entries?cursor=-1"} {
		if r := call(t, "GET", bad, nil, nil); r.status != 400 {
			t.Errorf("%s: want 400, got %d", bad, r.status)
		}
	}
}

func TestHealthAndUnknownRoutes(t *testing.T) {
	for _, p := range []string{"/healthz", "/readyz"} {
		if r := call(t, "GET", p, nil, nil); r.status != 200 {
			t.Errorf("%s: %d", p, r.status)
		}
	}
	if r := call(t, "GET", "/nope", nil, nil); r.status != 404 || r.code() != "not_found" {
		t.Errorf("unknown route: %d %s", r.status, r.body)
	}
	if r := call(t, "DELETE", "/v1/transfers", nil, nil); r.status != 405 {
		t.Errorf("wrong method: %d", r.status)
	}
	if r := call(t, "GET", "/v1/accounts/"+uuid.NewString(), nil, nil); r.status != 404 {
		t.Errorf("unknown account: %d", r.status)
	}
}

func TestIdempotencyKeyExpiry(t *testing.T) {
	a, b := funded(t, "exp-a", 1_000), mkAccount(t, "exp-b")
	ctx := context.Background()
	expire := func(key string) {
		if _, err := pool.Exec(ctx, `UPDATE idempotency_keys SET expires_at = now() - interval '1 hour'
			WHERE client_id = $1 AND key = $2`, testClient, key); err != nil {
			t.Fatal(err)
		}
	}

	// Expired but not yet cleaned up: the key is taken over, not replayed.
	k1 := newKey()
	if r := transfer(t, k1, a, b, 10); r.status != 201 {
		t.Fatal(r.status)
	}
	expire(k1)
	if r := transfer(t, k1, a, b, 10); r.status != 201 || r.header.Get("Idempotent-Replayed") != "" {
		t.Errorf("expired key should run again: %d replayed=%q", r.status, r.header.Get("Idempotent-Replayed"))
	}

	// The cleanup job deletes expired keys; afterwards the key is new again.
	k2 := newKey()
	transfer(t, k2, a, b, 10)
	expire(k2)
	deleted, err := idempotency.Cleanup(ctx, pool)
	if err != nil || deleted < 1 {
		t.Fatalf("cleanup: deleted=%d err=%v", deleted, err)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM idempotency_keys WHERE client_id = $1 AND key = $2`, testClient, k2); n != 0 {
		t.Error("expired key was not deleted")
	}
	if n := countRows(t, `SELECT COUNT(*) FROM idempotency_keys WHERE client_id = $1 AND key = $2`, testClient, k1); n != 1 {
		t.Error("cleanup deleted a key that had not expired")
	}
	if got := getAccount(t, b).BalanceMinor; got != 30 {
		t.Errorf("receiver balance = %d, want 30", got)
	}
}

func TestStats(t *testing.T) {
	read := func() ledger.Stats {
		r := call(t, "GET", "/v1/stats", nil, nil)
		if r.status != 200 {
			t.Fatalf("stats: %d %s", r.status, r.body)
		}
		var st ledger.Stats
		r.decode(t, &st)
		return st
	}
	before := read()
	a, b := funded(t, "stats-a", 1_000), mkAccount(t, "stats-b") // 1 deposit
	if r := transfer(t, newKey(), a, b, 250); r.status != 201 {
		t.Fatal(r.status)
	}
	after := read()
	if after.Accounts-before.Accounts != 2 || after.TransfersTotal-before.TransfersTotal != 2 ||
		after.TransfersToday-before.TransfersToday != 2 || after.VolumeTodayMinor-before.VolumeTodayMinor != 1_250 ||
		after.OutboxBacklog-before.OutboxBacklog != 6 {
		t.Errorf("stats did not move as expected:\nbefore %+v\nafter  %+v", before, after)
	}
}
