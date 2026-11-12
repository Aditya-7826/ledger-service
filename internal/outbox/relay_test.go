package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Aditya-7826/ledger-service/internal/events"
	"github.com/Aditya-7826/ledger-service/internal/ledger"
	"github.com/Aditya-7826/ledger-service/internal/outbox"
	"github.com/Aditya-7826/ledger-service/internal/store"
	"github.com/Aditya-7826/ledger-service/internal/testutil"
)

// One PostgreSQL and one Redpanda container are shared by this package.
var (
	pool   *pgxpool.Pool
	svc    *ledger.Service
	broker string
	quiet  = slog.New(slog.NewTextHandler(io.Discard, nil))
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
	if pool, err = store.NewPool(ctx, url, 10); err != nil {
		return fail("pool", err)
	}
	defer pool.Close()
	if err := events.EnsureTopics(ctx, []string{broker}, events.TopicEntries, events.TopicTransfers); err != nil {
		return fail("topics", err)
	}
	svc = ledger.NewService(pool, ledger.Options{MaxTransferMinor: 1_000_000_000, IdempotencyTTL: time.Hour})
	return m.Run()
}

// ---------------------------------------------------------------- helpers

// workload commits one deposit and n transfers between two new accounts and
// returns the IDs of the outbox events they produced (3 per transfer).
func workload(t *testing.T, n int) map[uuid.UUID]bool {
	t.Helper()
	ctx := context.Background()
	// Start from an empty backlog so each test sees only its own events.
	if _, err := pool.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE published_at IS NULL`); err != nil {
		t.Fatal(err)
	}
	var maxID int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) FROM outbox`).Scan(&maxID); err != nil {
		t.Fatal(err)
	}
	a, err := svc.CreateAccount(ctx, "a", "INR")
	if err != nil {
		t.Fatal(err)
	}
	b, err := svc.CreateAccount(ctx, "b", "INR")
	if err != nil {
		t.Fatal(err)
	}
	meta := func() ledger.Meta { return ledger.Meta{ClientID: "relay-test", Key: uuid.NewString()} }
	if _, err := svc.Deposit(ctx, meta(), ledger.DepositInput{AccountID: a.ID, AmountMinor: 1_000_000, Currency: "INR"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := svc.Transfer(ctx, meta(), ledger.TransferInput{From: a.ID, To: b.ID, AmountMinor: 1, Currency: "INR"}); err != nil {
			t.Fatal(err)
		}
	}
	ids := map[uuid.UUID]bool{}
	rows, err := pool.Query(ctx, `SELECT event_id FROM outbox WHERE id > $1`, maxID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids[id] = true
	}
	if want := 3 * (n + 1); len(ids) != want {
		t.Fatalf("workload produced %d outbox rows, want %d", len(ids), want)
	}
	return ids
}

func backlog(t *testing.T) int64 {
	t.Helper()
	n, err := (&outbox.Relay{Pool: pool}).Backlog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func drain(t *testing.T, r *outbox.Relay) int {
	t.Helper()
	total := 0
	for {
		n, err := r.PublishOnce(context.Background())
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
		if n == 0 {
			return total
		}
		total += n
	}
}

func kafkaRelay(t *testing.T) *outbox.Relay {
	t.Helper()
	pub, err := outbox.NewKafkaPublisher([]string{broker})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	return &outbox.Relay{Pool: pool, Publisher: pub, Log: quiet}
}

// consume reads both topics from the beginning and returns the records
// whose event_id is in want, grouped by event ID. It stops once every wanted
// ID has been seen at least minEach times, or after a timeout.
func consume(t *testing.T, want map[uuid.UUID]bool, minEach int) map[uuid.UUID][]*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(events.TopicEntries, events.TopicTransfers),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	got := map[uuid.UUID][]*kgo.Record{}
	complete := func() bool {
		for id := range want {
			if len(got[id]) < minEach {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(30 * time.Second)
	for !complete() && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		fetches := cl.PollFetches(ctx)
		cancel()
		fetches.EachRecord(func(rec *kgo.Record) {
			var h events.Header
			if err := json.Unmarshal(rec.Value, &h); err != nil {
				t.Errorf("unparseable record: %s", rec.Value)
				return
			}
			if want[h.EventID] {
				got[h.EventID] = append(got[h.EventID], rec)
			}
		})
	}
	// One more short poll to catch any unexpected extra copies.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	cl.PollFetches(ctx).EachRecord(func(rec *kgo.Record) {
		var h events.Header
		if json.Unmarshal(rec.Value, &h) == nil && want[h.EventID] {
			got[h.EventID] = append(got[h.EventID], rec)
		}
	})
	cancel()
	return got
}

type failingPublisher struct{ calls int }

func (f *failingPublisher) Publish(context.Context, []outbox.Message) error {
	f.calls++
	return errors.New("broker unreachable")
}

// crashAfterPublish really publishes, then fails: the relay "dies" after the
// broker acknowledged but before the rows were marked.
type crashAfterPublish struct{ real outbox.Publisher }

func (c crashAfterPublish) Publish(ctx context.Context, msgs []outbox.Message) error {
	if err := c.real.Publish(ctx, msgs); err != nil {
		return err
	}
	return errors.New("relay crashed before marking")
}

// ---------------------------------------------------------------- tests

func TestTopicsHaveThreePartitions(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	topics, err := kadm.NewClient(cl).ListTopics(context.Background(), events.TopicEntries, events.TopicTransfers)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{events.TopicEntries, events.TopicTransfers} {
		if got := len(topics[name].Partitions); got != events.TopicPartitions {
			t.Errorf("%s has %d partitions, want %d", name, got, events.TopicPartitions)
		}
	}
	// Creating them again is not an error.
	if err := events.EnsureTopics(context.Background(), []string{broker}, events.TopicEntries, events.TopicTransfers); err != nil {
		t.Errorf("EnsureTopics twice: %v", err)
	}
}

// Relay dies before publishing: nothing is marked, and the restarted relay
// publishes every event exactly once.
func TestCrashBeforePublishIsRetriedAfterRestart(t *testing.T) {
	want := workload(t, 10)

	failing := &failingPublisher{}
	if n, err := (&outbox.Relay{Pool: pool, Publisher: failing, Log: quiet}).PublishOnce(context.Background()); err == nil || n != 0 {
		t.Fatalf("expected a publish error, got n=%d err=%v", n, err)
	}
	if got := backlog(t); got != int64(len(want)) {
		t.Fatalf("backlog after failed publish = %d, want %d (nothing may be marked)", got, len(want))
	}

	if n := drain(t, kafkaRelay(t)); n != len(want) {
		t.Fatalf("restarted relay published %d, want %d", n, len(want))
	}
	if got := backlog(t); got != 0 {
		t.Fatalf("backlog after restart = %d", got)
	}

	got := consume(t, want, 1)
	lastSeq := map[string]int64{}     // account -> last account_seq seen, per partition order
	partitionOf := map[string]int32{} // account -> partition
	lastOffset := map[string]int64{}  // account -> offset of its previous event
	var entryRecords []*kgo.Record
	for id := range want {
		recs := got[id]
		if len(recs) != 1 {
			t.Fatalf("event %s delivered %d times, want 1", id, len(recs))
		}
		rec := recs[0]
		var h events.Header
		_ = json.Unmarshal(rec.Value, &h)
		if h.SchemaVersion != events.SchemaVersion || h.OccurredAt.IsZero() || h.EventType == "" {
			t.Errorf("incomplete header: %s", rec.Value)
		}
		if topic, _ := events.Topic(h.EventType); topic != rec.Topic {
			t.Errorf("%s published to %s", h.EventType, rec.Topic)
		}
		if h.EventType == events.TypeEntryPosted {
			entryRecords = append(entryRecords, rec)
		}
	}
	// Per account: one partition, and account_seq rises with the offset.
	bySeq := map[string]map[int64]*kgo.Record{}
	for _, rec := range entryRecords {
		var e events.EntryPosted
		_ = json.Unmarshal(rec.Value, &e)
		acct := e.AccountID.String()
		if string(rec.Key) != acct {
			t.Errorf("record key %q is not the account id %q", rec.Key, acct)
		}
		if p, seen := partitionOf[acct]; seen && p != rec.Partition {
			t.Errorf("account %s spread over partitions %d and %d", acct, p, rec.Partition)
		}
		partitionOf[acct] = rec.Partition
		if bySeq[acct] == nil {
			bySeq[acct] = map[int64]*kgo.Record{}
		}
		bySeq[acct][e.AccountSeq] = rec
	}
	for acct, recs := range bySeq {
		for seq := int64(1); seq <= int64(len(recs)); seq++ {
			rec, ok := recs[seq]
			if !ok {
				t.Errorf("account %s is missing account_seq %d", acct, seq)
				continue
			}
			if seq > 1 && rec.Offset <= lastOffset[acct] {
				t.Errorf("account %s: seq %d has offset %d, not after %d", acct, seq, rec.Offset, lastOffset[acct])
			}
			lastOffset[acct], lastSeq[acct] = rec.Offset, seq
		}
	}
	if len(bySeq) == 0 {
		t.Error("no EntryPosted events were checked")
	}
}

// Relay dies after the broker acknowledged but before marking: the event is
// published a second time. This is why delivery is at-least-once and why
// consumers must deduplicate by event_id (asserted in the consumer tests).
func TestCrashAfterPublishDeliversTwice(t *testing.T) {
	want := workload(t, 5)
	real := kafkaRelay(t)

	crashing := &outbox.Relay{Pool: pool, Publisher: crashAfterPublish{real.Publisher}, Log: quiet}
	if _, err := crashing.PublishOnce(context.Background()); err == nil {
		t.Fatal("expected the simulated crash")
	}
	if got := backlog(t); got != int64(len(want)) {
		t.Fatalf("backlog after crash = %d, want %d (rows must not be marked)", got, len(want))
	}
	if n := drain(t, real); n != len(want) {
		t.Fatalf("restarted relay published %d, want %d", n, len(want))
	}
	got := consume(t, want, 2)
	for id := range want {
		if len(got[id]) != 2 {
			t.Errorf("event %s delivered %d times, want exactly 2", id, len(got[id]))
		}
	}
}

// recorder notes which relay published which row and how often.
type recorder struct {
	mu   sync.Mutex
	seen map[int64]int
	by   map[string]int
}

type recordingPublisher struct {
	rec  *recorder
	name string
}

func (p recordingPublisher) Publish(_ context.Context, msgs []outbox.Message) error {
	time.Sleep(5 * time.Millisecond) // hold the row locks for a moment, like a real broker round trip
	p.rec.mu.Lock()
	defer p.rec.mu.Unlock()
	for _, m := range msgs {
		p.rec.seen[m.ID]++
	}
	p.rec.by[p.name] += len(msgs)
	return nil
}

func TestTwoRelaysNeverPublishTheSameRow(t *testing.T) {
	want := workload(t, 199) // 600 outbox rows
	rec := &recorder{seen: map[int64]int{}, by: map[string]int{}}
	var wg sync.WaitGroup
	for _, name := range []string{"relay-1", "relay-2"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := &outbox.Relay{Pool: pool, Publisher: recordingPublisher{rec, name}, Batch: 20, Log: quiet}
			idle := 0
			for idle < 3 {
				n, err := r.PublishOnce(context.Background())
				if err != nil {
					t.Errorf("%s: %v", name, err)
					return
				}
				if n == 0 {
					idle++
					time.Sleep(10 * time.Millisecond)
				} else {
					idle = 0
				}
			}
		}()
	}
	wg.Wait()

	if len(rec.seen) != len(want) {
		t.Errorf("published %d distinct rows, want %d", len(rec.seen), len(want))
	}
	for id, n := range rec.seen {
		if n != 1 {
			t.Errorf("outbox row %d was published %d times", id, n)
		}
	}
	if rec.by["relay-1"] == 0 || rec.by["relay-2"] == 0 {
		t.Errorf("both relays should have done work: %v", rec.by)
	}
	t.Logf("rows published per relay: %v", rec.by)
	if got := backlog(t); got != 0 {
		t.Errorf("backlog = %d", got)
	}
}

func TestPruneDeletesOnlyOldPublishedRows(t *testing.T) {
	workload(t, 2) // 9 rows, unpublished
	ctx := context.Background()
	// Mark 4 as published 8 days ago and 2 as published just now; 3 stay unpublished.
	if _, err := pool.Exec(ctx, `UPDATE outbox SET published_at = now() - interval '8 days'
		WHERE id IN (SELECT id FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 4)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox SET published_at = now()
		WHERE id IN (SELECT id FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 2)`); err != nil {
		t.Fatal(err)
	}
	var before int64
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&before)
	deleted, err := (&outbox.Relay{Pool: pool, Log: quiet}).Prune(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 4 {
		t.Errorf("pruned %d rows, want 4", deleted)
	}
	if got := backlog(t); got != 3 {
		t.Errorf("unpublished rows after prune = %d, want 3", got)
	}
	var after int64
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&after)
	if before-after != 4 {
		t.Errorf("outbox shrank by %d, want 4", before-after)
	}
}
