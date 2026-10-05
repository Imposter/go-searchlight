package replica

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/store/postgres"
	"github.com/Imposter/go-searchlight/internal/store/storetest"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// waitAppliedUnwoken waits for the tailer to apply seq without waking it: only the
// store's notifications (or its post-reconnect poll) can.
func waitAppliedUnwoken(t *testing.T, tl *Tailer, seq int64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for tl.Applied() < seq {
		if time.Now().After(deadline) {
			t.Fatalf("applied %d, want %d: no notification woke the tailer", tl.Applied(), seq)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestNotificationsWakeTheTailer: on Postgres a copy whose poll interval is an hour
// still applies other nodes' writes at once, through LISTEN/NOTIFY; when the
// listening connection is killed, the hub reconnects and polls, so neither a write
// made while it was down nor one made after is missed.
func TestNotificationsWakeTheTailer(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		if d.dialect != "postgres" {
			t.Skip("no notifications on " + d.dialect)
		}
		ctx := context.Background()
		writer := d.open(t)
		createIndex(t, writer, "n", testMapping)
		id := ShardID{Index: "n", Shard: 0}
		reader := sdkmetric.NewManualReader()
		opts := testOptions()
		opts.PollInterval, opts.WatchedPollInterval = time.Hour, time.Hour
		opts.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
		copyStore := d.open(t)
		hub := NewHub(copyStore, HubOptions{Logger: quietLogger, Meter: opts.Meter, RetryBase: 5 * time.Millisecond})
		if hub == nil {
			t.Fatal("a postgres store has no hub")
		}
		hubCtx, stopHub := context.WithCancel(context.Background())
		hubDone := make(chan error, 1)
		go func() { hubDone <- hub.Run(hubCtx) }()
		defer func() {
			stopHub()
			<-hubDone
		}()
		opts.Hub = hub
		c := newCopy(t, copyStore, id, opts)
		c.start()
		waitWatching := func() {
			deadline := time.Now().Add(15 * time.Second)
			for c.tailer.State() != StateTailing || counterSum(t, reader, telemetry.MetricReplicaRecoveries, "", "") == 0 {
				if time.Now().After(deadline) {
					t.Fatal("the copy never started tailing")
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
		waitWatching()

		head := mustApply(t, writer, upsert("n", 0, "a", docBody("a", 1)))
		waitAppliedUnwoken(t, c.tailer, head)

		// Kill the listening connection, write, and expect the write to arrive
		// after the hub reconnects (by the poll at ready, or a notification).
		u, err := url.Parse(d.url)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := postgres.Config(u)
		if err != nil {
			t.Fatal(err)
		}
		admin := stdlib.OpenDB(*cfg)
		defer admin.Close()
		var killed int
		deadline := time.Now().Add(15 * time.Second)
		for killed == 0 {
			if err := admin.QueryRowContext(ctx, `SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
WHERE application_name = $1 AND query LIKE 'LISTEN%' AND pid <> pg_backend_pid()`, d.appName).Scan(&killed); err != nil {
				t.Fatal(err)
			}
			if killed == 0 && time.Now().After(deadline) {
				t.Fatal("no listening connection to kill")
			}
			time.Sleep(10 * time.Millisecond)
		}
		head = mustApply(t, writer, upsert("n", 0, "b", docBody("b", 2)))
		waitAppliedUnwoken(t, c.tailer, head)
		deadline = time.Now().Add(15 * time.Second)
		for counterSum(t, reader, telemetry.MetricReplicaWatchReconnects, "", "") == 0 {
			if time.Now().After(deadline) {
				t.Fatal("the hub never counted a reconnect")
			}
			time.Sleep(10 * time.Millisecond)
		}
		head = mustApply(t, writer, upsert("n", 0, "c", docBody("c", 3)))
		waitAppliedUnwoken(t, c.tailer, head)
		if err := c.stop(); err != nil {
			t.Fatal(err)
		}
	})
}

// fakeWatcher is a store.Watcher whose subscription the test controls.
type fakeWatcher struct {
	store.Store
	sessions chan *fakeSession
}

type fakeSession struct {
	ready  func()
	notify func(store.Notification)
	fail   chan error
}

func (w *fakeWatcher) Watch(ctx context.Context, ready func(), fn func(store.Notification)) error {
	s := &fakeSession{ready: ready, notify: fn, fail: make(chan error, 1)}
	select {
	case w.sessions <- s:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-s.fail:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestHub covers the hub on every platform with a fake subscription: a notification
// wakes the tailers of its shard only, ready and a failed subscription wake every
// tailer, and the subscription is restarted.
func TestHub(t *testing.T) {
	sqlite := &db{dialect: "sqlite", url: storetest.SQLiteURL(t.TempDir() + "/hub.db")}
	w := &fakeWatcher{Store: sqlite.open(t), sessions: make(chan *fakeSession, 1)}
	reader := sdkmetric.NewManualReader()
	h := NewHub(w, HubOptions{Logger: quietLogger, RetryBase: time.Millisecond, Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("t")})
	a := &Tailer{id: ShardID{Index: "i", Shard: 0}, wake: make(chan struct{}, 1)}
	b := &Tailer{id: ShardID{Index: "i", Shard: 1}, wake: make(chan struct{}, 1)}
	unsubA, unsubB := h.subscribe(a), h.subscribe(b)
	defer unsubB()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	woken := func(tl *Tailer) bool {
		select {
		case <-tl.wake:
			return true
		default:
			return false
		}
	}
	s := <-w.sessions
	if h.Watching() {
		t.Fatal("watching before ready")
	}
	s.ready()
	if !h.Watching() || !woken(a) || !woken(b) {
		t.Fatal("ready did not wake every tailer")
	}
	s.notify(store.Notification{Shard: a.id, Seq: 5})
	if !woken(a) || woken(b) {
		t.Fatal("a notification woke the wrong tailers")
	}
	a.applied.Store(5)
	s.notify(store.Notification{Shard: a.id, Seq: 5})
	if woken(a) {
		t.Fatal("a notification of an applied seq woke the tailer")
	}
	s.fail <- errors.New("connection reset")
	s = <-w.sessions // restarted
	if h.Watching() || !woken(a) || !woken(b) {
		t.Fatal("a failed subscription did not hand the tailers back to polling")
	}
	if n := counterSum(t, reader, telemetry.MetricReplicaWatchReconnects, "", ""); n != 1 {
		t.Fatalf("%d reconnects counted", n)
	}
	unsubA()
	s.ready()
	if woken(a) {
		t.Fatal("an unsubscribed tailer was woken")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if NewHub(sqlite.open(t), HubOptions{}) != nil {
		t.Fatal("a sqlite store has a hub")
	}
}
