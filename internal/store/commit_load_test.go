package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Imposter/go-searchlight/internal/store/postgres"
)

// commitLoadPool is the documents a load rewrites, loaded before the measuring starts.
const commitLoadPool = 50_000

// commitLoad is one case of BenchmarkCommitLoad.
type commitLoad struct {
	nodes, writers, shards, bulk int
	// noNotify drops pg_notify from the transaction; asyncCommit sets the session's
	// synchronous_commit to off, which takes the WAL flush out of COMMIT. direct calls the
	// store's Apply with no group committer, so each bulk is its own transaction.
	noNotify, asyncCommit, direct bool
}

func (c commitLoad) String() string {
	notify, flush := "on", "sync"
	if c.noNotify {
		notify = "off"
	}
	if c.asyncCommit {
		flush = "async"
	}
	gc := "group"
	if c.direct {
		gc = "direct"
	}
	return fmt.Sprintf("nodes=%d/writers=%d/shards=%d/bulk=%d/notify=%s/flush=%s/commit=%s", c.nodes, c.writers, c.shards, c.bulk, notify, flush, gc)
}

// commitLoads are BenchmarkCommitLoad's cases: the matrix at one node, then the variants
// that take one cost out, at the bulk and shard counts that show it.
func commitLoads() []commitLoad {
	var out []commitLoad
	for _, shards := range []int{1, 8, 64} {
		for _, writers := range []int{1, 4, 16} {
			for _, bulk := range []int{500, 4096} {
				out = append(out, commitLoad{nodes: 1, writers: writers, shards: shards, bulk: bulk})
			}
		}
	}
	for _, bulk := range []int{1, 500, 4096} {
		for _, shards := range []int{1, 64} {
			out = append(out,
				commitLoad{nodes: 1, writers: 1, shards: shards, bulk: bulk, noNotify: true},
				commitLoad{nodes: 1, writers: 1, shards: shards, bulk: bulk, asyncCommit: true},
				commitLoad{nodes: 3, writers: 4, shards: shards, bulk: bulk},
				commitLoad{nodes: 3, writers: 4, shards: shards, bulk: bulk, noNotify: true},
			)
		}
	}
	for _, writers := range []int{1, 16, 64} {
		for _, shards := range []int{1, 64} {
			for _, bulk := range []int{1, 64} {
				out = append(out, commitLoad{nodes: 1, writers: writers, shards: shards, bulk: bulk, direct: true})
			}
		}
	}
	return out
}

// BenchmarkCommitLoad measures the commit path alone on Postgres: writers per node send
// bulks of rewrites of existing documents through a group committer each, with no node,
// tailers or HTTP in the way, and it times every statement of every transaction from the
// client, through a pgx tracer, so nothing in the store changes:
//
//	go test ./internal/store -run '^$' -bench 'CommitLoad/nodes=1/writers=4/shards=64' -benchtime 150x
//
// One iteration is 1,000 changes, whatever the bulk size. The metrics are each phase's mean milliseconds per
// transaction: begin, lock (the counter's SELECT FOR UPDATE, so its queue when several
// commit at once), index (reading sl_indexes), write (the statement of every row),
// commit, and notify where a notification runs apart from the transaction. wal_sync_ms
// is the server's mean flush, and wal_syncs/s how many it ran, from pg_stat_wal
// (track_wal_io_timing must be on); waits, in the log, are backends by wait event from
// pg_stat_activity, sampled every 50 ms.
func BenchmarkCommitLoad(b *testing.B) {
	base := os.Getenv(envPG)
	if base == "" {
		b.Skip(envPG + " is not set")
	}
	for _, c := range commitLoads() {
		b.Run(c.String(), func(b *testing.B) { runCommitLoad(b, base, c) })
	}
}

func runCommitLoad(b *testing.B, base string, c commitLoad) {
	h := postgresHarness(b, base)
	if c.asyncCommit {
		h.url += "&synchronous_commit=off"
	}
	tr := &phaseTracer{}
	gcs := make([]Applier, c.nodes)
	var st Store
	for i := range gcs {
		s := h.open(b)
		tracePhases(b, s, h.url, tr, c.noNotify)
		s.CompressBodies(true)
		if st == nil {
			st = s
			meta := IndexMeta{Name: "bench", Settings: fmt.Appendf(nil, `{"shards": %d}`, c.shards)}
			if _, err := st.Indexes().Create(context.Background(), meta); err != nil {
				b.Fatal(err)
			}
		}
		gcs[i] = s
		if !c.direct {
			gc := NewGroupCommitter(s, GroupCommitOptions{})
			b.Cleanup(func() { _ = gc.Close() })
			gcs[i] = gc
		}
	}
	bodies := make([]string, 1024)
	for i := range bodies {
		bodies[i] = benchBody(i)
	}
	ids := make([]string, commitLoadPool)
	for i := range ids {
		ids[i] = fmt.Sprintf("d%d", i)
	}
	var next atomic.Int64
	bulkAt := func(n int) []Change {
		start := next.Add(int64(n)) - int64(n)
		batch := make([]Change, n)
		for j := range batch {
			k := int(start+int64(j)) % commitLoadPool
			batch[j] = upsert("bench", k%c.shards, ids[k], bodies[(k+int(start))%len(bodies)])
		}
		return batch
	}
	ctx := context.Background()
	for i := 0; i < commitLoadPool; i += 4096 {
		if _, _, err := gcs[0].Apply(ctx, bulkAt(4096)); err != nil {
			b.Fatal(err)
		}
	}

	admin, err := sql.Open("pgx", h.url)
	if err != nil {
		b.Fatal(err)
	}
	defer admin.Close()
	stopWaits := sampleWaits(admin)
	walBefore := walStats(admin)
	tr.reset()
	bulks := max(1, int64(b.N)*1000/int64(c.bulk))
	var taken atomic.Int64
	var failed atomic.Pointer[error]
	b.ResetTimer()
	began := time.Now()
	var wg sync.WaitGroup
	for _, gc := range gcs {
		for range c.writers {
			wg.Go(func() {
				for taken.Add(1) <= bulks {
					if _, _, err := gc.Apply(ctx, bulkAt(c.bulk)); err != nil {
						failed.CompareAndSwap(nil, &err)
						return
					}
				}
			})
		}
	}
	wg.Wait()
	elapsed := time.Since(began)
	b.StopTimer()
	if e := failed.Load(); e != nil {
		b.Fatal(*e)
	}
	wal := walStats(admin)
	waits := stopWaits()
	report := tr.report()
	commits := report["commit"].n
	b.ReportMetric(float64(bulks)*float64(c.bulk)/elapsed.Seconds(), "changes/s")
	b.ReportMetric(float64(commits)/elapsed.Seconds(), "commits/s")
	b.ReportMetric(float64(bulks)*float64(c.bulk)/float64(max(commits, 1)), "changes/commit")
	for _, name := range []string{"begin", "lock", "index", "write", "commit", "notify"} {
		if p, ok := report[name]; ok {
			b.ReportMetric(p.meanMS(), name+"_ms")
		}
	}
	b.ReportMetric(report["lock"].quantileMS(0.95), "lock_p95_ms")
	b.ReportMetric(report["commit"].quantileMS(0.95), "commit_p95_ms")
	if n := wal["wal_sync"] - walBefore["wal_sync"]; n > 0 {
		b.ReportMetric((wal["wal_sync_time"]-walBefore["wal_sync_time"])/n, "wal_sync_ms")
		b.ReportMetric(n/elapsed.Seconds(), "wal_syncs/s")
	}
	if n := wal["wal_write"] - walBefore["wal_write"]; n > 0 {
		b.ReportMetric((wal["wal_write_time"]-walBefore["wal_write_time"])/n, "wal_write_ms")
	}
	b.ReportMetric((wal["wal_bytes"]-walBefore["wal_bytes"])/elapsed.Seconds()/(1<<20), "wal_MB/s")
	b.ReportMetric((wal["wal_bytes"]-walBefore["wal_bytes"])/float64(bulks*int64(c.bulk)), "wal_bytes/change")
	b.Logf("waits: %s", waits)
}

// tracePhases gives s a write pool of its own whose every statement tr times. noNotify
// makes s a store of a dialect that cannot listen, so its transactions notify nobody.
func tracePhases(b *testing.B, s Store, storeURL string, tr *phaseTracer, noNotify bool) {
	b.Helper()
	u, err := url.Parse(storeURL)
	if err != nil {
		b.Fatal(err)
	}
	cfg, err := postgres.Config(u)
	if err != nil {
		b.Fatal(err)
	}
	cfg.Tracer = tr
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(16)
	b.Cleanup(func() { _ = db.Close() })
	e := engine(s)
	e.w = &gatedDB{DB: db}
	if noNotify {
		d := *e.d
		d.Listen = nil
		e.d = &d
	}
}

// phaseTracer times statements by the part of a commit they are.
type phaseTracer struct {
	mu     sync.Mutex
	phases map[string]*phaseTimes
}

type phaseTimes struct {
	n int
	d []time.Duration
}

func (p *phaseTimes) meanMS() float64 {
	if p.n == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range p.d {
		sum += d
	}
	return sum.Seconds() * 1000 / float64(p.n)
}

func (p *phaseTimes) quantileMS(q float64) float64 {
	if p.n == 0 {
		return 0
	}
	sorted := slices.Sorted(slices.Values(p.d))
	return sorted[min(len(sorted)-1, int(q*float64(len(sorted))))].Seconds() * 1000
}

func (t *phaseTracer) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.phases = nil
}

func (t *phaseTracer) report() map[string]*phaseTimes {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := map[string]*phaseTimes{"lock": {}, "commit": {}}
	for k, v := range t.phases {
		out[k] = v
	}
	return out
}

type phaseStart struct {
	phase string
	at    time.Time
}

type phaseKey struct{}

// phaseOf names the part of a commit a statement is, or "" for one outside it.
func phaseOf(q string) string {
	switch {
	case strings.Contains(q, "FOR UPDATE"):
		return "lock"
	case strings.Contains(q, "WITH changes AS"):
		return "write"
	case strings.Contains(q, "FROM sl_indexes"):
		return "index"
	case strings.Contains(q, "pg_notify"):
		return "notify"
	}
	switch strings.ToLower(strings.TrimSpace(q)) {
	case "commit":
		return "commit"
	default:
		if strings.HasPrefix(strings.ToLower(q), "begin") {
			return "begin"
		}
	}
	return ""
}

// TraceQueryStart implements [pgx.QueryTracer].
func (t *phaseTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, phaseKey{}, phaseStart{phase: phaseOf(data.SQL), at: time.Now()})
}

// TraceQueryEnd implements [pgx.QueryTracer].
func (t *phaseTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	st, ok := ctx.Value(phaseKey{}).(phaseStart)
	if !ok || st.phase == "" {
		return
	}
	d := time.Since(st.at)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.phases == nil {
		t.phases = map[string]*phaseTimes{}
	}
	p := t.phases[st.phase]
	if p == nil {
		p = &phaseTimes{}
		t.phases[st.phase] = p
	}
	p.n++
	p.d = append(p.d, d)
}

// walStats reads pg_stat_wal's numeric columns; empty where it cannot.
func walStats(db *sql.DB) map[string]float64 {
	var raw []byte
	out := map[string]float64{}
	if err := db.QueryRow("SELECT to_jsonb(s) FROM pg_stat_wal s").Scan(&raw); err != nil {
		return out
	}
	var row map[string]any
	if err := json.Unmarshal(raw, &row); err != nil {
		return out
	}
	for k, v := range row {
		if f, ok := v.(float64); ok {
			out[k] = f
		}
	}
	return out
}

// sampleWaits samples pg_stat_activity until the function it returns is called, which
// gives each wait event's mean number of backends, largest first.
func sampleWaits(db *sql.DB) func() string {
	const q = `SELECT backend_type || '/' || COALESCE(wait_event_type, 'CPU') || '/' || COALESCE(wait_event, ''), count(*)
FROM pg_stat_activity WHERE pid <> pg_backend_pid()
  AND ((datname = current_database() AND state IN ('active', 'idle in transaction')) OR backend_type = 'walwriter')
GROUP BY 1`
	stop, done := make(chan struct{}), make(chan struct{})
	counts := map[string]int{}
	samples := 0
	go func() {
		defer close(done)
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
			}
			rows, err := db.Query(q)
			if err != nil {
				continue
			}
			for rows.Next() {
				var k string
				var n int
				if rows.Scan(&k, &n) == nil {
					counts[k] += n
				}
			}
			_ = rows.Close()
			samples++
		}
	}()
	return func() string {
		close(stop)
		<-done
		keys := make([]string, 0, len(counts))
		for k := range counts {
			keys = append(keys, k)
		}
		slices.SortFunc(keys, func(a, b string) int { return counts[b] - counts[a] })
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s=%.2f", k, float64(counts[k])/float64(max(samples, 1))))
		}
		return strings.Join(parts, " ")
	}
}
