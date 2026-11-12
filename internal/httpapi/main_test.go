package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Aditya-7826/ledger-service/internal/httpapi"
	"github.com/Aditya-7826/ledger-service/internal/ledger"
	"github.com/Aditya-7826/ledger-service/internal/store"
	"github.com/Aditya-7826/ledger-service/internal/testutil"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// One PostgreSQL container and one API server are shared by every test in
// this package (8 GB RAM rule).
var (
	pool    *pgxpool.Pool
	baseURL string
	client  *http.Client
)

const (
	testMaxTransfer = int64(1_000_000_000)
	testClient      = "test-client"
)

func TestMain(m *testing.M) { os.Exit(runMain(m)) }

func runMain(m *testing.M) int {
	ctx := context.Background()
	url, stop, err := testutil.StartPostgres(ctx)
	defer stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres container:", err)
		return 1
	}
	if err := store.Migrate(ctx, url); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}
	if pool, err = store.NewPool(ctx, url, 30); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer pool.Close()

	svc := ledger.NewService(pool, ledger.Options{MaxTransferMinor: testMaxTransfer, IdempotencyTTL: 24 * time.Hour})
	srv := httptest.NewServer(httpapi.NewRouter(svc, pool, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer srv.Close()
	baseURL = srv.URL
	client = &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{MaxIdleConns: 200, MaxIdleConnsPerHost: 200},
	}
	return m.Run()
}

// ---------------------------------------------------------------- helpers

type result struct {
	status int
	header http.Header
	body   []byte
}

func (r result) code() string {
	var p struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(r.body, &p)
	return p.Code
}

func (r result) decode(t testing.TB, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
}

// call sends a request. body may be nil, a string (sent as is) or any value
// (sent as JSON). It is safe to use from goroutines: failures are reported
// with t.Errorf and a zero result.
func call(t testing.TB, method, path string, headers map[string]string, body any) result {
	t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			t.Errorf("marshal: %v", err)
			return result{}
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, baseURL+path, rd)
	if err != nil {
		t.Errorf("new request: %v", err)
		return result{}
	}
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Errorf("%s %s: %v", method, path, err)
		return result{}
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	return result{status: res.StatusCode, header: res.Header, body: raw}
}

func writeHeaders(key string) map[string]string {
	return map[string]string{"Idempotency-Key": key, "X-Client-Id": testClient}
}

func newKey() string { return uuid.NewString() }

func mkAccount(t testing.TB, name string) string {
	t.Helper()
	r := call(t, "POST", "/v1/accounts", nil, map[string]string{"name": name, "currency": "INR"})
	if r.status != http.StatusCreated {
		t.Fatalf("create account: %d %s", r.status, r.body)
	}
	var a ledger.Account
	r.decode(t, &a)
	return a.ID.String()
}

func deposit(t testing.TB, account string, amount int64) {
	t.Helper()
	r := call(t, "POST", "/v1/deposits", writeHeaders(newKey()),
		map[string]any{"account_id": account, "amount_minor": amount, "currency": "INR"})
	if r.status != http.StatusCreated {
		t.Fatalf("deposit: %d %s", r.status, r.body)
	}
}

// funded creates an account holding amount.
func funded(t testing.TB, name string, amount int64) string {
	t.Helper()
	id := mkAccount(t, name)
	for amount > 0 {
		part := min(amount, testMaxTransfer)
		deposit(t, id, part)
		amount -= part
	}
	return id
}

func transferBody(from, to string, amount int64) map[string]any {
	return map[string]any{"from_account_id": from, "to_account_id": to, "amount_minor": amount, "currency": "INR"}
}

func transfer(t testing.TB, key, from, to string, amount int64) result {
	t.Helper()
	return call(t, "POST", "/v1/transfers", writeHeaders(key), transferBody(from, to, amount))
}

func getAccount(t testing.TB, id string) ledger.Account {
	t.Helper()
	r := call(t, "GET", "/v1/accounts/"+id, nil, nil)
	if r.status != http.StatusOK {
		t.Fatalf("get account: %d %s", r.status, r.body)
	}
	var a ledger.Account
	r.decode(t, &a)
	return a
}

func assertReconciled(t testing.TB) {
	t.Helper()
	r := call(t, "GET", "/v1/reconcile", nil, nil)
	var rec ledger.Reconciliation
	r.decode(t, &rec)
	if r.status != http.StatusOK || !rec.OK {
		t.Fatalf("reconcile failed: %s", r.body)
	}
	if len(rec.Checks) != 4 {
		t.Fatalf("expected 4 reconcile checks, got %d", len(rec.Checks))
	}
}

func countRows(t testing.TB, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}
