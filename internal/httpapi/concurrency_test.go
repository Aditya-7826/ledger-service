package httpapi_test

import (
	"bytes"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Aditya-7826/ledger-service/internal/ledger"
)

func TestIdempotentReplayIsByteIdentical(t *testing.T) {
	a, b := funded(t, "idem-a", 1_000), mkAccount(t, "idem-b")
	key := newKey()
	first := transfer(t, key, a, b, 100)
	if first.status != 201 || first.header.Get("Idempotent-Replayed") != "" {
		t.Fatalf("first: %d replayed=%q", first.status, first.header.Get("Idempotent-Replayed"))
	}
	for i := 0; i < 3; i++ {
		again := transfer(t, key, a, b, 100)
		if again.status != 201 || again.header.Get("Idempotent-Replayed") != "true" {
			t.Fatalf("replay: %d replayed=%q", again.status, again.header.Get("Idempotent-Replayed"))
		}
		if !bytes.Equal(first.body, again.body) {
			t.Fatalf("replay body differs:\n%s\n%s", first.body, again.body)
		}
	}
	if got := getAccount(t, a).BalanceMinor; got != 900 {
		t.Errorf("sender charged more than once: balance %d", got)
	}

	// Same key, different body.
	diff := transfer(t, key, a, b, 101)
	if diff.status != 422 || diff.code() != "idempotency_key_reused" {
		t.Errorf("different body: want 422 idempotency_key_reused, got %d %s", diff.status, diff.body)
	}
	// The same key under another client is a different key.
	other := call(t, "POST", "/v1/transfers", map[string]string{"Idempotency-Key": key, "X-Client-Id": "other-client"}, transferBody(a, b, 100))
	if other.status != 201 || other.header.Get("Idempotent-Replayed") != "" {
		t.Errorf("other client: %d", other.status)
	}
}

func TestTwentyConcurrentIdenticalRequestsMakeOneTransfer(t *testing.T) {
	a, b := funded(t, "dup-a", 1_000), mkAccount(t, "dup-b")
	key := newKey()
	const n = 20
	results := make([]result, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = transfer(t, key, a, b, 50)
		}()
	}
	close(start)
	wg.Wait()

	fresh := 0
	for _, r := range results {
		if r.status != 201 {
			t.Fatalf("status %d %s", r.status, r.body)
		}
		if r.header.Get("Idempotent-Replayed") == "" {
			fresh++
		}
		if !bytes.Equal(r.body, results[0].body) {
			t.Fatal("responses differ")
		}
	}
	if fresh != 1 {
		t.Errorf("%d responses were not replays, want exactly 1", fresh)
	}
	if got := countRows(t, `SELECT COUNT(*) FROM transfers WHERE client_id = $1 AND idempotency_key = $2`, testClient, key); got != 1 {
		t.Errorf("%d transfers for one key", got)
	}
	if got := getAccount(t, a).BalanceMinor; got != 950 {
		t.Errorf("sender balance %d, want 950", got)
	}
}

func TestRejectedKeySucceedsOnRetryAfterDeposit(t *testing.T) {
	a, b := mkAccount(t, "retry-a"), mkAccount(t, "retry-b")
	key := newKey()
	if r := transfer(t, key, a, b, 500); r.status != 409 || r.code() != "insufficient_funds" {
		t.Fatalf("want 409, got %d %s", r.status, r.body)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM idempotency_keys WHERE client_id = $1 AND key = $2`, testClient, key); n != 0 {
		t.Fatal("a rejected request kept its idempotency key")
	}
	deposit(t, a, 500)
	r := transfer(t, key, a, b, 500)
	if r.status != 201 || r.header.Get("Idempotent-Replayed") != "" {
		t.Fatalf("retry after deposit: %d %s", r.status, r.body)
	}
	if getAccount(t, b).BalanceMinor != 500 {
		t.Error("receiver not credited")
	}
}

// A->B and B->A at the same time is the classic deadlock. Locks are taken in
// ascending UUID order, so none may reach the client.
func TestOpposingTransfersNeverDeadlock(t *testing.T) {
	a, b := funded(t, "dl-a", 100_000), funded(t, "dl-b", 100_000)
	const rounds, workers = 1000, 8
	var next atomic.Int64
	var failures atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1)
				if i > 2*rounds {
					return
				}
				from, to := a, b
				if i%2 == 0 {
					from, to = b, a
				}
				if r := transfer(t, newKey(), from, to, 1); r.status != 201 {
					failures.Add(1)
					t.Errorf("transfer %d: %d %s", i, r.status, r.body)
				}
			}
		}()
	}
	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("%d of %d transfers failed", failures.Load(), 2*rounds)
	}
	if x, y := getAccount(t, a), getAccount(t, b); x.BalanceMinor != 100_000 || y.BalanceMinor != 100_000 {
		t.Errorf("balances %d / %d, want 100000 each", x.BalanceMinor, y.BalanceMinor)
	}
	assertReconciled(t)
}

// 50 goroutines, random transfers among 20 accounts. Afterwards the ledger
// must reconcile, no balance may be negative and no money may have appeared
// or vanished.
func TestConcurrentRandomTransfersKeepInvariants(t *testing.T) {
	const accounts, workers, initial = 20, 50, 100_000
	duration := 15 * time.Second
	if testing.Short() {
		duration = 3 * time.Second
	}
	ids := make([]string, accounts)
	for i := range ids {
		ids[i] = funded(t, "inv", initial)
	}
	deadline := time.Now().Add(duration)
	var ok, rejected atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				from := rand.IntN(accounts)
				to := (from + 1 + rand.IntN(accounts-1)) % accounts
				// Large amounts on purpose so that some transfers run out of funds.
				r := transfer(t, newKey(), ids[from], ids[to], 1+rand.Int64N(60_000))
				switch {
				case r.status == 201:
					ok.Add(1)
				case r.status == 409 && r.code() == "insufficient_funds":
					rejected.Add(1)
				default:
					t.Errorf("unexpected response: %d %s", r.status, r.body)
					return
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("%d transfers committed, %d rejected for insufficient funds in %s", ok.Load(), rejected.Load(), duration)
	if ok.Load() == 0 || rejected.Load() == 0 {
		t.Errorf("workload did not exercise both paths: ok=%d rejected=%d", ok.Load(), rejected.Load())
	}

	var total int64
	for _, id := range ids {
		acc := getAccount(t, id)
		if acc.BalanceMinor < 0 {
			t.Errorf("account %s is below zero: %d", id, acc.BalanceMinor)
		}
		total += acc.BalanceMinor
	}
	if total != accounts*initial {
		t.Errorf("money not conserved: total %d, want %d", total, accounts*initial)
	}
	if n := countRows(t, `SELECT COUNT(*) FROM account_balances b JOIN accounts a ON a.id = b.account_id
		WHERE a.kind = 'USER' AND b.balance_minor < 0`); n != 0 {
		t.Errorf("%d USER account(s) below zero", n)
	}
	assertReconciled(t)
}

// Once the freeze call has returned, no further entry may appear on the
// account, and every transfer that starts afterwards must get 403.
func TestFreezeRace(t *testing.T) {
	x, y := funded(t, "fr-x", 1_000_000), mkAccount(t, "fr-y")
	var frozen atomic.Bool
	var before, after403, afterOther atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				startedAfterFreeze := frozen.Load()
				r := transfer(t, newKey(), x, y, 1)
				switch {
				case !startedAfterFreeze:
					before.Add(1)
					if r.status != 201 && r.status != 403 {
						t.Errorf("before freeze: %d %s", r.status, r.body)
					}
				case r.status == 403 && r.code() == "account_frozen":
					after403.Add(1)
				default:
					afterOther.Add(1)
					t.Errorf("transfer started after the freeze returned got %d %s", r.status, r.body)
				}
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	if r := call(t, "POST", "/v1/accounts/"+x+"/freeze", nil, nil); r.status != 200 {
		t.Fatalf("freeze: %d %s", r.status, r.body)
	}
	frozen.Store(true)
	atFreeze := getAccount(t, x)
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	final := getAccount(t, x)
	if final.Status != ledger.StatusFrozen {
		t.Fatalf("status = %s", final.Status)
	}
	if final.LastSeq != atFreeze.LastSeq || final.BalanceMinor != atFreeze.BalanceMinor {
		t.Errorf("entries were written after the freeze: seq %d -> %d", atFreeze.LastSeq, final.LastSeq)
	}
	if before.Load() == 0 || after403.Load() == 0 || afterOther.Load() != 0 {
		t.Errorf("before=%d after403=%d afterOther=%d", before.Load(), after403.Load(), afterOther.Load())
	}
	// Frozen accounts cannot receive either; unfreezing restores both directions.
	if r := transfer(t, newKey(), y, x, 1); r.status != 403 {
		t.Errorf("credit to frozen account: %d", r.status)
	}
	if r := call(t, "POST", "/v1/accounts/"+x+"/freeze", nil, nil); r.status != http.StatusOK {
		t.Errorf("freezing twice should be a no-op: %d", r.status)
	}
	if r := call(t, "POST", "/v1/accounts/"+x+"/unfreeze", nil, nil); r.status != 200 {
		t.Fatalf("unfreeze: %d", r.status)
	}
	if r := transfer(t, newKey(), x, y, 1); r.status != 201 {
		t.Errorf("transfer after unfreeze: %d %s", r.status, r.body)
	}
	assertReconciled(t)
}
