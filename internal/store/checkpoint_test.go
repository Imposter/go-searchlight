package store

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/store/sqlite"
)

// TestCheckpointGateAfterBurst (N3): after a burst grows SQLite's write-ahead log far
// past the checkpoint threshold, the checkpointer copies it back and a truncating
// checkpoint empties the file, and small writes afterwards pass ticks with no
// checkpoint (and no fsync): the gate counts the frames pending, not the file's size.
func TestCheckpointGateAfterBurst(t *testing.T) {
	st := sqliteHarness(t).open(t)
	s, ok := st.(*sqlStore)
	if !ok {
		t.Fatalf("%T is not the SQL store", st)
	}
	ctx := context.Background()
	if _, err := st.Indexes().Create(ctx, IndexMeta{Name: "wal", Mapping: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	body := []byte(fmt.Sprintf(`{"pad":%q}`, strings.Repeat("x", 4000)))
	burst := make([]Change, 6000)
	for i := range burst {
		burst[i] = Change{Index: "wal", Kind: KindUpsert, ID: fmt.Sprintf("b%d", i), Payload: body}
	}
	if _, _, err := st.Apply(ctx, burst); err != nil {
		t.Fatal(err)
	}
	if size := s.walSize(); size < 4*sqlite.JournalSizeLimit {
		t.Fatalf("the burst left a %d-byte log: the test proves nothing", size)
	}
	small := func(n int) {
		for i := range n {
			if _, _, err := st.Apply(ctx, []Change{{Index: "wal", Kind: KindUpsert, ID: fmt.Sprintf("s%d", i), Payload: []byte(`{"n":1}`)}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	deadline := time.Now().Add(time.Minute)
	for {
		small(1)
		pending, err := sqlite.PendingLog(s.dbPath)
		if err != nil {
			t.Fatal(err)
		}
		if pending < sqlite.JournalSizeLimit && s.walSize() <= sqlite.JournalSizeLimit {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the log was not copied back and cut down: %d bytes pending, file %d bytes", pending, s.walSize())
		}
		time.Sleep(100 * time.Millisecond)
	}
	before := s.checkpoints.Load()
	for range 10 {
		small(5)
		time.Sleep(sqlite.CheckpointEvery / 2)
	}
	if got := s.checkpoints.Load() - before; got != 0 {
		t.Fatalf("%d checkpoints for a few small writes after the burst: the gate is judging the file's size", got)
	}
}

// TestPinnedLogWarns: a read transaction held open pins the write-ahead log, so a
// checkpoint cannot copy it back while it grows; past LogWarnBytes the store warns at
// the checkpointer's next tick, which the store's fake clock gives it.
func TestPinnedLogWarns(t *testing.T) {
	logs := &captureHandler{}
	clk := clock.NewFake(time.Now())
	st := sqliteHarness(t).open(t, WithLogger(slog.New(logs)), WithClock(clk))
	s, ok := st.(*sqlStore)
	if !ok {
		t.Fatalf("%T is not the SQL store", st)
	}
	s.logWarn.Store(sqlite.JournalSizeLimit)
	ctx := context.Background()
	if _, err := st.Indexes().Create(ctx, IndexMeta{Name: "pin", Mapping: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	tx, err := s.r.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(tx)
	var n int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sl_indexes").Scan(&n); err != nil {
		t.Fatal(err)
	}
	body := []byte(fmt.Sprintf(`{"pad":%q}`, strings.Repeat("x", 4000)))
	for b := range 3 {
		batch := make([]Change, 1000)
		for i := range batch {
			batch[i] = Change{Index: "pin", Kind: KindUpsert, ID: fmt.Sprintf("b%d-%d", b, i), Payload: body}
		}
		if _, _, err := st.Apply(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	if logs.has("the write-ahead log keeps growing") {
		t.Fatal("warned before the checkpointer ticked")
	}
	clk.Advance(sqlite.CheckpointEvery)
	wait, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for ; !logs.has("the write-ahead log keeps growing"); runtime.Gosched() {
		if wait.Err() != nil {
			t.Fatal("no warning about the pinned log at the checkpointer's tick")
		}
	}
}

// captureHandler keeps the messages of the records logged at warn and above.
type captureHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (h *captureHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func (h *captureHandler) has(prefix string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.msgs {
		if strings.HasPrefix(m, prefix) {
			return true
		}
	}
	return false
}
