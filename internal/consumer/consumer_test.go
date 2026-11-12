package consumer_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Aditya-7826/ledger-service/internal/consumer"
	"github.com/Aditya-7826/ledger-service/internal/events"
	"github.com/Aditya-7826/ledger-service/internal/httpapi"
	"github.com/Aditya-7826/ledger-service/internal/ledger"
	"github.com/Aditya-7826/ledger-service/internal/outbox"
	"github.com/Aditya-7826/ledger-service/internal/store"
	"github.com/Aditya-7826/ledger-service/internal/testutil"
)

// One PostgreSQL and one Redpanda container are shared by this package.
var (
	pool      *pgxpool.Pool
	svc       *ledger.Service
	broker    string
	ledgerAPI *consumer.HTTPLedger
	quiet     = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func TestMain(m *testing.M) { os.Exit(runMain(m)) }

func runMain(m *testing.M) int {
	ctx := context.Background()
	fail := func(what string, err error) int {
		fmt.Fprintln(os.Stderr, what+":", err)
		return 1
	}
	url, stopPG, err := testutil.StartPostgres(ctx)
	defer stopPG()
	if err != nil {
		return fail("start postgres", err)
	}
	var stopRP func()
	broker, stopRP, err = testutil.StartRedpanda(ctx)
	defer stopRP()
	if err != nil {
		return fail("start redpanda", err)
	}
	if err := store.Migrate(ctx, url); err != nil {
		return fail("migrate", err)
	}
	if pool, err = store.NewPool(ctx, url, 20); err != nil {
		return fail("pool", err)
	}
	defer pool.Close()
	if err := events.EnsureTopics(ctx, []string{broker}, events.TopicEntries, events.TopicTransfers,
		events.TopicEntries+consumer.DLQSuffix, events.TopicTransfers+consumer.DLQSuffix); err != nil {
		return fail("topics", err)
	}
	svc = ledger.NewService(pool, ledger.Options{MaxTransferMinor: 1_000_000_000, IdempotencyTTL: time.Hour})
	api := httptest.NewServer(httpapi.NewRouter(svc, pool, quiet))
	defer api.Close()
	ledgerAPI = &consumer.HTTPLedger{BaseURL: api.URL}
	return m.Run()
}

// ---------------------------------------------------------------- helpers

func meta() ledger.Meta { return ledger.Meta{ClientID: "consumer-test", Key: uuid.NewString()} }

func newAccount(t *testing.T, deposit int64) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	a, err := svc.CreateAccount(ctx, "acct", "INR")
	if err != nil {
		t.Fatal(err)
	}
	if deposit > 0 {
		if _, err := svc.Deposit(ctx, meta(), ledger.DepositInput{AccountID: a.ID, AmountMinor: deposit, Currency: "INR"}); err != nil {
			t.Fatal(err)
		}
	}
	return a.ID
}

func transfer(t *testing.T, from, to uuid.UUID, amount int64) {
	t.Helper()
	if _, err := svc.Transfer(context.Background(), meta(), ledger.TransferInput{From: from, To: to, AmountMinor: amount, Currency: "INR"}); err != nil {
		t.Fatal(err)
	}
}

func kafkaPublisher(t *testing.T) *outbox.KafkaPublisher {
	t.Helper()
	pub, err := outbox.NewKafkaPublisher([]string{broker})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	return pub
}

// relayAll publishes the whole outbox backlog to the real topics.
func relayAll(t *testing.T, pub outbox.Publisher) {
	t.Helper()
	r := &outbox.Relay{Pool: pool, Publisher: pub, Log: quiet}
	for {
		n, err := r.PublishOnce(context.Background())
		if err != nil {
			t.Fatalf("relay: %v", err)
		}
		if n == 0 {
			return
		}
	}
}

// crashAfterPublish publishes for real, then reports a failure once: the
// relay "died" before marking, so the same rows are published again.
type crashAfterPublish struct {
	real    outbox.Publisher
	crashed bool
}

func (c *crashAfterPublish) Publish(ctx context.Context, msgs []outbox.Message) error {
	if err := c.real.Publish(ctx, msgs); err != nil {
		return err
	}
	if !c.crashed {
		c.crashed = true
		return errors.New("relay crashed before marking")
	}
	return nil
}

// startRunner runs a consumer in the background until the test ends (or stop is called).
func startRunner(t *testing.T, r *consumer.Runner, group string, topic string) (stop func()) {
	t.Helper()
	client, err := consumer.NewClient([]string{broker}, group, topic)
	if err != nil {
		t.Fatal(err)
	}
	r.Pool, r.Client = pool, client
	if r.Log == nil {
		r.Log = quiet
	}
	if r.RetryBackoff == 0 {
		r.RetryBackoff = 20 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Run(ctx)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			<-done
			client.Close()
		})
	}
	t.Cleanup(stop)
	return stop
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, what)
}

func count(t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// viewMismatches counts ledger accounts whose balance view differs from the ledger.
func viewMismatches(t *testing.T) int64 {
	t.Helper()
	return count(t, `
		SELECT COUNT(*) FROM account_balances b
		LEFT JOIN views.balance_view v ON v.account_id = b.account_id
		WHERE b.last_seq > 0
		  AND (v.account_id IS NULL OR v.balance_minor <> b.balance_minor OR v.last_seq <> b.last_seq)`)
}

func view(t *testing.T, account uuid.UUID) (balance, seq int64) {
	t.Helper()
	err := pool.QueryRow(context.Background(), `SELECT balance_minor, last_seq FROM views.balance_view WHERE account_id = $1`,
		account).Scan(&balance, &seq)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
	return balance, seq
}

// entryEvents returns the account's EntryPosted payloads in account_seq order.
func entryEvents(t *testing.T, account uuid.UUID) [][]byte {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT payload::text FROM outbox
		WHERE aggregate_id = $1 AND event_type = 'EntryPosted' ORDER BY (payload->>'account_seq')::bigint`, account)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, []byte(s))
	}
	return out
}

func newTopic(t *testing.T) string {
	t.Helper()
	topic := "test." + uuid.NewString()
	if err := events.EnsureTopics(context.Background(), []string{broker}, topic, topic+consumer.DLQSuffix); err != nil {
		t.Fatal(err)
	}
	return topic
}

func produce(t *testing.T, topic string, key uuid.UUID, values ...[]byte) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for _, v := range values {
		if err := cl.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Key: []byte(key.String()), Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

func balancesRunner(name string, h *consumer.Balances) *consumer.Runner {
	if h.Log == nil {
		h.Log = quiet
	}
	if h.Ledger == nil {
		h.Ledger = ledgerAPI
	}
	return &consumer.Runner{Name: name, EventType: events.TypeEntryPosted, Handler: h}
}

// ---------------------------------------------------------------- tests

// A random concurrent workload, delivered with duplicates (the relay
// "crashes" after publishing its first batch). Each event must be applied
// once: the balance view ends up equal to the ledger, and every transfer has
// exactly two notifications.
func TestRandomWorkloadWithDuplicateDeliveryMatchesLedger(t *testing.T) {
	const accounts, workers, perWorker = 8, 4, 75
	ids := make([]uuid.UUID, accounts)
	for i := range ids {
		ids[i] = newAccount(t, 50_000)
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				from := rand.IntN(accounts)
				to := (from + 1 + rand.IntN(accounts-1)) % accounts
				_, err := svc.Transfer(context.Background(), meta(), ledger.TransferInput{
					From: ids[from], To: ids[to], AmountMinor: 1 + rand.Int64N(30_000), Currency: "INR"})
				var le *ledger.Error
				if err != nil && !(errors.As(err, &le) && le.Code == "insufficient_funds") {
					t.Errorf("transfer: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	crashing := &crashAfterPublish{real: kafkaPublisher(t)}
	if _, err := (&outbox.Relay{Pool: pool, Publisher: crashing, Log: quiet}).PublishOnce(context.Background()); err == nil {
		t.Fatal("expected the simulated relay crash")
	}
	relayAll(t, crashing) // the first batch goes out a second time

	balances := balancesRunner(consumer.BalancesName, &consumer.Balances{GapTimeout: time.Minute})
	notify := &consumer.Runner{Name: consumer.NotifyName, EventType: events.TypeTransferPosted, Handler: consumer.Notify{}}
	startRunner(t, balances, "g-"+uuid.NewString(), events.TopicEntries)
	startRunner(t, notify, "g-"+uuid.NewString(), events.TopicTransfers)

	transfers := count(t, `SELECT COUNT(*) FROM transfers`)
	eventually(t, 60*time.Second, "balance view to equal the ledger", func() bool { return viewMismatches(t) == 0 })
	eventually(t, 60*time.Second, "two notifications per transfer", func() bool {
		return count(t, `SELECT COUNT(*) FROM views.notifications`) == 2*transfers
	})

	if balances.Stats.Duplicates.Load()+notify.Stats.Duplicates.Load() == 0 {
		t.Error("the duplicated batch was not seen as duplicates")
	}
	if n := count(t, `SELECT COUNT(*) FROM (SELECT transfer_id FROM views.notifications GROUP BY transfer_id HAVING COUNT(*) <> 2) x`); n != 0 {
		t.Errorf("%d transfer(s) without exactly two notifications", n)
	}
	rec, err := svc.Reconcile(context.Background())
	if err != nil || !rec.OK {
		t.Errorf("ledger does not reconcile: %+v %v", rec, err)
	}
	t.Logf("%d transfers; balances applied=%d duplicates=%d; notify applied=%d duplicates=%d", transfers,
		balances.Stats.Applied.Load(), balances.Stats.Duplicates.Load(), notify.Stats.Applied.Load(), notify.Stats.Duplicates.Load())
}

// The consumer commits to the database but its Kafka offset commit never
// happens (as if it crashed in between). The next consumer in the same group
// receives everything again and must apply nothing a second time.
func TestCrashBetweenDatabaseCommitAndOffsetCommit(t *testing.T) {
	a, b := newAccount(t, 10_000), newAccount(t, 0)
	for i := 0; i < 20; i++ {
		transfer(t, a, b, 100)
	}
	relayAll(t, kafkaPublisher(t))
	group := "g-" + uuid.NewString()

	first := balancesRunner(consumer.BalancesName, &consumer.Balances{GapTimeout: time.Minute})
	first.CommitOffsets = func(context.Context, ...*kgo.Record) error { return errors.New("crashed before the offset commit") }
	stop := startRunner(t, first, group, events.TopicEntries)
	eventually(t, 60*time.Second, "first consumer to apply the events", func() bool { return viewMismatches(t) == 0 })
	stop()
	seen := first.Stats.Applied.Load() + first.Stats.Duplicates.Load()
	if first.Stats.Applied.Load() < 42 { // 1 deposit + 20 transfers = 21 transfers, 2 entries each
		t.Fatalf("first consumer applied only %d events", first.Stats.Applied.Load())
	}
	balanceBefore, seqBefore := view(t, a)

	second := balancesRunner(consumer.BalancesName, &consumer.Balances{GapTimeout: time.Minute})
	startRunner(t, second, group, events.TopicEntries)
	eventually(t, 60*time.Second, "second consumer to receive everything again", func() bool {
		return second.Stats.Duplicates.Load() >= seen
	})
	if second.Stats.Applied.Load() != 0 {
		t.Errorf("second consumer applied %d events a second time", second.Stats.Applied.Load())
	}
	if bal, seq := view(t, a); bal != balanceBefore || seq != seqBefore || bal != 8_000 || seq != 21 {
		t.Errorf("view changed on redelivery: balance %d seq %d", bal, seq)
	}
	if n := viewMismatches(t); n != 0 {
		t.Errorf("%d account(s) differ from the ledger", n)
	}
}

// Sequence handling at the handler level: in order applies, a gap asks for a
// retry, a late event then applies, and old events are skipped.
func TestOutOfOrderAndDuplicateSequences(t *testing.T) {
	account := uuid.New()
	h := &consumer.Balances{GapTimeout: time.Minute, Log: quiet, Ledger: ledgerAPI}
	apply := func(seq int64, direction string, amount int64) error {
		ev := events.EntryPosted{Header: events.NewHeader(events.TypeEntryPosted, time.Now()), EntryID: uuid.New(),
			TransferID: uuid.New(), AccountID: account, Direction: direction, AmountMinor: amount, AccountSeq: seq, Currency: "INR"}
		raw, _ := json.Marshal(ev)
		return pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
			return h.Handle(context.Background(), tx, ev.Header, &kgo.Record{Value: raw})
		})
	}
	expect := func(step string, wantBal, wantSeq int64) {
		t.Helper()
		if bal, seq := view(t, account); bal != wantBal || seq != wantSeq {
			t.Fatalf("%s: view = (%d, seq %d), want (%d, seq %d)", step, bal, seq, wantBal, wantSeq)
		}
	}
	if err := apply(1, "CREDIT", 1000); err != nil {
		t.Fatal(err)
	}
	expect("seq 1", 1000, 1)
	if err := apply(3, "DEBIT", 50); !errors.Is(err, consumer.ErrRetry) {
		t.Fatalf("seq 3 before seq 2: want ErrRetry, got %v", err)
	}
	expect("after the gap", 1000, 1)
	if err := apply(2, "DEBIT", 100); err != nil {
		t.Fatal(err)
	}
	expect("seq 2 arrived late", 900, 2)
	if err := apply(3, "DEBIT", 50); err != nil {
		t.Fatal(err)
	}
	expect("seq 3 retried", 850, 3)
	if err := apply(2, "DEBIT", 100); err != nil {
		t.Fatal(err)
	}
	expect("old seq 2 again", 850, 3)

	// Invalid content is poison, not a retry.
	bad, _ := json.Marshal(events.EntryPosted{AccountID: account, Direction: "SIDEWAYS", AmountMinor: 1, AccountSeq: 4})
	err := pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
		return h.Handle(context.Background(), tx, events.Header{}, &kgo.Record{Value: bad})
	})
	if !errors.Is(err, consumer.ErrPoison) {
		t.Errorf("invalid direction: want ErrPoison, got %v", err)
	}
}

// An event never arrives (seq 2 is missing). After GapTimeout the consumer
// rebuilds the account from the ledger's entries endpoint and logs it.
func TestPersistentGapRebuildsFromLedger(t *testing.T) {
	x, y := newAccount(t, 1_000), newAccount(t, 0)
	transfer(t, x, y, 100)
	transfer(t, x, y, 50)
	evs := entryEvents(t, x)
	if len(evs) != 3 {
		t.Fatalf("expected 3 entry events, got %d", len(evs))
	}
	topic := newTopic(t)
	produce(t, topic, x, evs[0], evs[2]) // seq 1 and seq 3; seq 2 is lost

	var logs bytes.Buffer
	h := &consumer.Balances{GapTimeout: 500 * time.Millisecond, Log: slog.New(slog.NewTextHandler(&logs, nil))}
	r := balancesRunner("balances-gap-"+uuid.NewString(), h)
	stop := startRunner(t, r, "g-"+uuid.NewString(), topic)

	eventually(t, 30*time.Second, "the view to be rebuilt from the ledger", func() bool {
		bal, seq := view(t, x)
		return bal == 850 && seq == 3
	})
	stop()
	if h.Rebuilds.Load() != 1 {
		t.Errorf("rebuilds = %d, want 1", h.Rebuilds.Load())
	}
	if !strings.Contains(logs.String(), "rebuilt the account's balance view from the ledger") {
		t.Errorf("the rebuild was not logged: %s", logs.String())
	}

	// The view endpoint reports the lag against the ledger.
	mux := http.NewServeMux()
	mux.Handle("GET /v1/views/balances/{id}", consumer.BalanceViewHandler(pool, ledgerAPI))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	get := func(id string) (int, consumer.BalanceView) {
		res, err := http.Get(srv.URL + "/v1/views/balances/" + id)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var v consumer.BalanceView
		_ = json.NewDecoder(res.Body).Decode(&v)
		return res.StatusCode, v
	}
	if status, v := get(x.String()); status != 200 || v.BalanceMinor != 850 || v.LastSeq != 3 || v.LedgerLastSeq != 3 || v.LagSeq != 0 {
		t.Errorf("caught-up view: %d %+v", status, v)
	}
	// y's events were never delivered to this consumer: the view lags by 2 entries.
	if status, v := get(y.String()); status != 200 || v.LastSeq != 0 || v.LedgerLastSeq != 2 || v.LagSeq != 2 || v.UpdatedAt != nil {
		t.Errorf("lagging view: %d %+v", status, v)
	}
	if status, _ := get(uuid.NewString()); status != 404 {
		t.Errorf("unknown account: %d", status)
	}
	if status, _ := get("nope"); status != 400 {
		t.Errorf("bad id: %d", status)
	}
}

// Four poison messages sit in front of two valid events in the same
// partition. Each goes to the DLQ after three attempts, and the valid events
// behind them are still applied.
func TestPoisonMessagesGoToDLQAndPartitionKeepsFlowing(t *testing.T) {
	y, z := newAccount(t, 700), newAccount(t, 0)
	transfer(t, y, z, 200)
	evs := entryEvents(t, y)
	topic := newTopic(t)

	unknownSchema, _ := json.Marshal(map[string]any{"event_id": uuid.NewString(), "event_type": events.TypeEntryPosted, "schema_version": 99})
	wrongType, _ := json.Marshal(events.TransferPosted{Header: events.NewHeader(events.TypeTransferPosted, time.Now())})
	badContent, _ := json.Marshal(events.EntryPosted{Header: events.NewHeader(events.TypeEntryPosted, time.Now()), AccountID: y, Direction: "DEBIT", AmountMinor: 5, AccountSeq: 0})
	produce(t, topic, y, []byte("this is not json"), unknownSchema, wrongType, badContent, evs[0], evs[1])

	var logs bytes.Buffer
	r := balancesRunner("balances-poison-"+uuid.NewString(), &consumer.Balances{GapTimeout: time.Minute})
	r.Log = slog.New(slog.NewTextHandler(&logs, nil))
	stop := startRunner(t, r, "g-"+uuid.NewString(), topic)
	eventually(t, 30*time.Second, "valid events behind the poison to be applied", func() bool {
		bal, seq := view(t, y)
		return bal == 500 && seq == 2
	})
	stop()
	if got := r.Stats.DeadLettered.Load(); got != 4 {
		t.Errorf("dead-lettered = %d, want 4", got)
	}
	if got := strings.Count(logs.String(), "poison message moved to the DLQ"); got != 4 {
		t.Errorf("DLQ moves logged %d times, want 4", got)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics(topic+consumer.DLQSuffix),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var dlq []*kgo.Record
	deadline := time.Now().Add(20 * time.Second)
	for len(dlq) < 4 && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		cl.PollFetches(ctx).EachRecord(func(rec *kgo.Record) { dlq = append(dlq, rec) })
		cancel()
	}
	if len(dlq) != 4 {
		t.Fatalf("DLQ holds %d records, want 4", len(dlq))
	}
	if string(dlq[0].Value) != "this is not json" {
		t.Errorf("first DLQ record = %q", dlq[0].Value)
	}
	for _, rec := range dlq {
		reason := ""
		for _, h := range rec.Headers {
			if h.Key == "dlq_reason" {
				reason = string(h.Value)
			}
		}
		if !strings.Contains(reason, "poison message") {
			t.Errorf("DLQ record without a reason header: %q", reason)
		}
	}
}

// ---------------------------------------------------------------- SSE

type sseEvent struct {
	id      string
	event   string
	data    string
	comment string
}

// openStream connects to the SSE endpoint and delivers parsed events on a channel.
func openStream(t *testing.T, url, lastEventID string) (<-chan sseEvent, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if res.StatusCode != 200 || !strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		cancel()
		t.Fatalf("stream: status %d content-type %q", res.StatusCode, res.Header.Get("Content-Type"))
	}
	ch := make(chan sseEvent, 100)
	go func() {
		defer close(ch)
		defer res.Body.Close()
		sc := bufio.NewScanner(res.Body)
		var cur sseEvent
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if cur != (sseEvent{}) {
					ch <- cur
				}
				cur = sseEvent{}
			case strings.HasPrefix(line, ":"):
				cur.comment = strings.TrimSpace(line[1:])
			case strings.HasPrefix(line, "id: "):
				cur.id = line[4:]
			case strings.HasPrefix(line, "event: "):
				cur.event = line[7:]
			case strings.HasPrefix(line, "data: "):
				cur.data = line[6:]
			}
		}
	}()
	return ch, cancel
}

// nextNotifications reads until n notification events arrived, also reporting whether a heartbeat was seen.
func nextNotifications(t *testing.T, ch <-chan sseEvent, n int) (out []consumer.Notification, ids []string, heartbeat bool) {
	t.Helper()
	timeout := time.After(30 * time.Second)
	for len(out) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("stream closed after %d of %d notifications", len(out), n)
			}
			if ev.comment == "heartbeat" {
				heartbeat = true
			}
			if ev.event != "notification" {
				continue
			}
			var note consumer.Notification
			if err := json.Unmarshal([]byte(ev.data), &note); err != nil {
				t.Fatalf("bad notification data %q: %v", ev.data, err)
			}
			if ev.id != fmt.Sprint(note.ID) {
				t.Errorf("SSE id %q does not match notification id %d", ev.id, note.ID)
			}
			out, ids = append(out, note), append(ids, ev.id)
		case <-timeout:
			t.Fatalf("timed out with %d of %d notifications", len(out), n)
		}
	}
	return out, ids, heartbeat
}

func TestNotificationStreamResumesWithLastEventID(t *testing.T) {
	hub := &consumer.Hub{}
	srv := httptest.NewServer(&consumer.Stream{Pool: pool, Hub: hub, Heartbeat: 100 * time.Millisecond})
	defer srv.Close()
	notify := &consumer.Runner{Name: consumer.NotifyName, EventType: events.TypeTransferPosted, Handler: consumer.Notify{},
		OnCommitted: func(events.Header) { hub.Wake() }}
	startRunner(t, notify, "g-"+uuid.NewString(), events.TopicTransfers)
	pub := kafkaPublisher(t)

	a, b := newAccount(t, 5_000), newAccount(t, 0)
	relayAll(t, pub)
	// Let the consumer catch up on everything published so far.
	eventually(t, 60*time.Second, "notify consumer to catch up", func() bool {
		return count(t, `SELECT COUNT(*) FROM views.notifications`) == 2*count(t, `SELECT COUNT(*) FROM transfers`)
	})

	// A fresh connection gets only what happens from now on.
	stream, closeStream := openStream(t, srv.URL, "")
	if first := <-stream; first.comment != "connected" {
		t.Fatalf("first frame = %+v", first)
	}
	transfer(t, a, b, 1234)
	relayAll(t, pub)
	notes, ids, _ := nextNotifications(t, stream, 2)
	if notes[0].Kind != "DEBITED" || notes[0].AccountID != a || notes[0].CounterpartyAccountID != b || notes[0].AmountMinor != 1234 ||
		notes[1].Kind != "CREDITED" || notes[1].AccountID != b || notes[1].CounterpartyAccountID != a || notes[0].TransferID != notes[1].TransferID {
		t.Fatalf("unexpected notifications: %+v", notes)
	}
	// An idle stream sends heartbeat comments.
	sawHeartbeat := false
	timeout := time.After(5 * time.Second)
	for !sawHeartbeat {
		select {
		case ev := <-stream:
			sawHeartbeat = ev.comment == "heartbeat"
		case <-timeout:
			t.Fatal("no heartbeat on an idle stream")
		}
	}
	closeStream()

	// While disconnected, two more notifications are created ...
	transfer(t, b, a, 34)
	relayAll(t, pub)
	eventually(t, 30*time.Second, "missed notifications to be stored", func() bool {
		return count(t, `SELECT COUNT(*) FROM views.notifications WHERE id > $1`, ids[1]) == 2
	})
	// ... and a reconnect with Last-Event-ID delivers exactly those, first.
	resumed, closeResumed := openStream(t, srv.URL, ids[1])
	defer closeResumed()
	missed, _, _ := nextNotifications(t, resumed, 2)
	if missed[0].Kind != "DEBITED" || missed[0].AccountID != b || missed[0].AmountMinor != 34 || missed[1].Kind != "CREDITED" ||
		missed[0].ID <= notes[1].ID {
		t.Errorf("resume delivered the wrong notifications: %+v", missed)
	}

	// Last-Event-ID: 0 replays from the very first notification.
	replay, closeReplay := openStream(t, srv.URL, "0")
	defer closeReplay()
	if first, _, _ := nextNotifications(t, replay, 1); first[0].ID != 1 {
		t.Errorf("replay from 0 started at id %d", first[0].ID)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("Last-Event-ID", "abc")
	if res, err := http.DefaultClient.Do(req); err != nil || res.StatusCode != 400 {
		t.Errorf("bad Last-Event-ID: %v %v", res, err)
	}
}
