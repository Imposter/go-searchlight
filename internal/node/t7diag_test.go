package node_test

// T7 write-to-visible harness (test-only, opt-in): a single SQLite node, 1 shard,
// T7_PRELOAD documents, writes at a random refresh phase. It attributes each write's
// latency to the stages of the path from the spans the node, the group committer, the
// replica tailer and the shard already emit, and reports the refresh duration and the
// refresh period (start to start).
//
//	SEARCHLIGHT_T7DIAG=1 T7_INTERVAL=1s T7_N=200 T7_MODES=random-waitfor \
//	  go test ./internal/node -run TestT7Diag -v -timeout 30m
//
// Modes:
//   - random-waitfor: sleep a uniform random time in [0, 2 x interval), then write with
//     refresh=wait_for;
//   - random-waitfor-load: the same, while two goroutines write and fsync 256 KiB files
//     in the node's data directory without pause (disk contention);
//   - stream-waitfor: the same, under a steady background stream of 50 writes/s.

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
)

type t7Span struct {
	name       string
	start, end time.Time
	ints       map[string]int64
	seqOnly    bool
}

type t7Rec struct {
	on    atomic.Bool
	mu    sync.Mutex
	spans []t7Span
}

var t7Names = map[string]bool{
	"store.group_commit": true, "replica.apply": true, "shard.refresh": true, "shard.flush": true, "shard.merge": true,
}

func (r *t7Rec) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (r *t7Rec) Shutdown(context.Context) error                  { return nil }
func (r *t7Rec) ForceFlush(context.Context) error                { return nil }
func (r *t7Rec) OnEnd(s sdktrace.ReadOnlySpan) {
	if !r.on.Load() || !t7Names[s.Name()] {
		return
	}
	sp := t7Span{name: s.Name(), start: s.StartTime(), end: s.EndTime(), ints: map[string]int64{}}
	for _, kv := range s.Attributes() {
		switch kv.Value.Type().String() {
		case "INT64":
			sp.ints[string(kv.Key)] = kv.Value.AsInt64()
		case "BOOL":
			if kv.Key == "seq_only" {
				sp.seqOnly = kv.Value.AsBool()
			}
		}
	}
	r.mu.Lock()
	r.spans = append(r.spans, sp)
	r.mu.Unlock()
}

func (r *t7Rec) take() []t7Span {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.spans
	r.spans = nil
	return out
}

type t7Sample struct {
	commit, wake, apply, tick, refresh, total time.Duration
	ok                                        bool
}

func t7Env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func t7Doc(i int) json.RawMessage {
	cats := []string{"tools", "garden", "kitchen", "toys", "books", "audio", "video", "sport"}
	words := []string{"red", "blue", "steel", "oak", "compact", "pro", "max", "mini", "smart", "eco", "ultra", "classic"}
	r := rand.New(rand.NewPCG(uint64(i), 7))
	title := words[r.IntN(len(words))] + " " + words[r.IntN(len(words))] + " " + cats[r.IntN(len(cats))]
	desc := strings.Repeat(words[r.IntN(len(words))]+" ", 8+r.IntN(16))
	return json.RawMessage(fmt.Sprintf(`{"title":%q,"category":%q,"price":%d,"stock":%d,"rating":%.1f,"desc":%q,"tags":[%q,%q]}`,
		title, cats[r.IntN(len(cats))], 100+r.IntN(100000), r.IntN(500), 1+4*r.Float64(), desc,
		words[r.IntN(len(words))], words[r.IntN(len(words))]))
}

// t7FsyncLoad writes and fsyncs 256 KiB files in dir until stop is set.
func t7FsyncLoad(t *testing.T, dir string, worker int, stop *atomic.Bool) {
	buf := make([]byte, 256<<10)
	path := filepath.Join(dir, fmt.Sprintf("t7load-%d.tmp", worker))
	defer os.Remove(path)
	for !stop.Load() {
		f, err := os.Create(path)
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = f.Write(buf)
		_ = f.Sync()
		_ = f.Close()
	}
}

func TestT7Diag(t *testing.T) {
	if os.Getenv("SEARCHLIGHT_T7DIAG") == "" {
		t.Skip("set SEARCHLIGHT_T7DIAG=1 to run the T7 harness")
	}
	interval, err := time.ParseDuration(t7Env("T7_INTERVAL", "1s"))
	if err != nil {
		t.Fatal(err)
	}
	samples, _ := strconv.Atoi(t7Env("T7_N", "200"))
	preload, _ := strconv.Atoi(t7Env("T7_PRELOAD", "50000"))
	modes := strings.Split(t7Env("T7_MODES", "random-waitfor,random-waitfor-load,stream-waitfor"), ",")

	cfg := testConfig(t)
	cfg.RefreshInterval = interval
	cfg.ChangelogPollInterval = 500 * time.Millisecond
	cfg.MergeThreads = max(1, runtime.GOMAXPROCS(0)/4)
	st := openStore(t, cfg)
	rec := &t7Rec{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	n := open(t, cfg, st, func(o *node.Options) { o.Tracer = tp.Tracer("t7diag") })
	bg := context.Background()
	if _, err := n.CreateIndex(bg, "t7", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}

	var last int64
	for lo := 0; lo < preload; lo += 1000 {
		ops := make([]api.WriteOp, 0, 1000)
		for i := lo; i < min(lo+1000, preload); i++ {
			ops = append(ops, api.WriteOp{Kind: api.OpUpsert, ID: fmt.Sprintf("p%07d", i), Body: t7Doc(i)})
		}
		for {
			res, err := n.Write(bg, "t7", ops, api.WriteOptions{})
			if err != nil && strings.Contains(err.Error(), "retry") {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			last = res.Seq
			break
		}
	}
	wctx, cancel := context.WithTimeout(bg, 5*time.Minute)
	if _, err := n.Search(wctx, "t7", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: last}); err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(3 * interval)
	t.Logf("preloaded %d docs; refresh_interval %s", preload, interval)

	for _, mode := range modes {
		rec.take()
		rec.on.Store(true)
		var stop atomic.Bool
		var bgWG sync.WaitGroup
		if strings.HasPrefix(mode, "stream") {
			bgWG.Add(1)
			go func() {
				defer bgWG.Done()
				for i := 0; !stop.Load(); i++ {
					_, _ = n.Write(bg, "t7", []api.WriteOp{{Kind: api.OpUpsert, ID: fmt.Sprintf("s-%s-%07d", mode, i), Body: t7Doc(i)}}, api.WriteOptions{})
					time.Sleep(20 * time.Millisecond)
				}
			}()
		}
		if strings.HasSuffix(mode, "-load") {
			for w := range 2 {
				bgWG.Add(1)
				go func() {
					defer bgWG.Done()
					t7FsyncLoad(t, cfg.DataDir, w, &stop)
				}()
			}
		}
		type raw struct {
			seq      int64
			t0, seen time.Time
		}
		var raws []raw
		rng := rand.New(rand.NewPCG(42, uint64(len(mode))))
		for i := range samples {
			time.Sleep(time.Duration(rng.Int64N(int64(2 * interval))))
			id := fmt.Sprintf("v-%s-%06d", mode, i)
			op := []api.WriteOp{{Kind: api.OpUpsert, ID: id, Body: t7Doc(1_000_000 + i)}}
			ctx, cancel := context.WithTimeout(bg, 30*time.Second)
			t0 := time.Now()
			res, err := n.Write(ctx, "t7", op, api.WriteOptions{Refresh: api.RefreshWaitFor})
			seen := time.Now()
			cancel()
			if err != nil || res.TimedOut {
				t.Fatalf("%s: wait_for write: %v", mode, err)
			}
			raws = append(raws, raw{res.Seq, t0, seen})
		}
		stop.Store(true)
		bgWG.Wait()
		time.Sleep(50 * time.Millisecond)
		rec.on.Store(false)
		spans := rec.take()

		var gcs, applies, refreshes, flushes, merges []t7Span
		for _, s := range spans {
			switch s.name {
			case "store.group_commit":
				gcs = append(gcs, s)
			case "replica.apply":
				applies = append(applies, s)
			case "shard.refresh":
				if !s.seqOnly {
					refreshes = append(refreshes, s)
				}
			case "shard.flush":
				flushes = append(flushes, s)
			case "shard.merge":
				merges = append(merges, s)
			}
		}
		byStart := func(a, b t7Span) int { return a.start.Compare(b.start) }
		slices.SortFunc(refreshes, byStart)
		slices.SortFunc(applies, byStart)

		res := make([]t7Sample, 0, len(raws))
		for _, r := range raws {
			s := t7Sample{total: r.seen.Sub(r.t0)}
			var ap, rf *t7Span
			for i := range applies {
				if applies[i].ints["first_seq"] <= r.seq && r.seq <= applies[i].ints["last_seq"] {
					ap = &applies[i]
					break
				}
			}
			for i := range refreshes {
				if refreshes[i].ints["seq"] >= r.seq {
					rf = &refreshes[i]
					break
				}
			}
			if ap == nil || rf == nil {
				res = append(res, s)
				continue
			}
			commitEnd := r.t0
			for i := range gcs {
				if !gcs[i].end.Before(r.t0) && !gcs[i].end.After(ap.start) && gcs[i].end.After(commitEnd) {
					commitEnd = gcs[i].end
				}
			}
			s.commit = commitEnd.Sub(r.t0)
			s.wake = ap.start.Sub(commitEnd)
			s.apply = ap.end.Sub(ap.start)
			s.tick = rf.start.Sub(ap.end)
			s.refresh = rf.end.Sub(rf.start)
			s.ok = true
			res = append(res, s)
		}
		t7Report(t, mode, interval, res)
		var durs, periods, flushDurs []time.Duration
		for i := range refreshes {
			durs = append(durs, refreshes[i].end.Sub(refreshes[i].start))
			if i > 0 {
				periods = append(periods, refreshes[i].start.Sub(refreshes[i-1].start))
			}
		}
		for _, f := range flushes {
			flushDurs = append(flushDurs, f.end.Sub(f.start))
		}
		t.Logf("%s: %d refreshes, duration p50 %s p99 %s max %s; period start->start p50 %s p99 %s max %s; %d flushes, duration p50 %s p99 %s",
			mode, len(refreshes), t7Pct(durs, 50), t7Pct(durs, 99), t7Pct(durs, 100),
			t7Pct(periods, 50), t7Pct(periods, 99), t7Pct(periods, 100),
			len(flushes), t7Pct(flushDurs, 50), t7Pct(flushDurs, 99))
		t.Logf("%s: refreshes over 30ms %d, overlapping a flush %d, a merge %d; group commits over 100ms %d, overlapping a flush %d, a merge %d",
			mode, t7Slow(refreshes, 30*time.Millisecond, nil), t7Slow(refreshes, 30*time.Millisecond, flushes), t7Slow(refreshes, 30*time.Millisecond, merges),
			t7Slow(gcs, 100*time.Millisecond, nil), t7Slow(gcs, 100*time.Millisecond, flushes), t7Slow(gcs, 100*time.Millisecond, merges))
	}
}

// t7Slow counts the spans longer than over, and with others, those overlapping one of
// them.
func t7Slow(spans []t7Span, over time.Duration, others []t7Span) int {
	n := 0
	for _, s := range spans {
		if s.end.Sub(s.start) <= over {
			continue
		}
		if others == nil {
			n++
			continue
		}
		for _, o := range others {
			if o.start.Before(s.end) && o.end.After(s.start) {
				n++
				break
			}
		}
	}
	return n
}

func t7Pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)
	return s[int(p/100*float64(len(s)-1))].Round(100 * time.Microsecond)
}

func t7Report(t *testing.T, mode string, interval time.Duration, res []t7Sample) {
	t.Helper()
	cols := []struct {
		name string
		get  func(*t7Sample) time.Duration
	}{
		{"commit", func(s *t7Sample) time.Duration { return s.commit }},
		{"wake", func(s *t7Sample) time.Duration { return s.wake }},
		{"apply", func(s *t7Sample) time.Duration { return s.apply }},
		{"tick wait", func(s *t7Sample) time.Duration { return s.tick }},
		{"refresh", func(s *t7Sample) time.Duration { return s.refresh }},
		{"TOTAL", func(s *t7Sample) time.Duration { return s.total }},
	}
	ok := 0
	for i := range res {
		if res[i].ok {
			ok++
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\n== %s (interval %s, n=%d, attributed %d)\n", mode, interval, len(res), ok)
	fmt.Fprintf(&b, "%-10s %10s %10s %10s %10s\n", "stage", "p50", "p90", "p99", "max")
	for _, c := range cols {
		var d []time.Duration
		for i := range res {
			if res[i].ok || c.name == "TOTAL" {
				d = append(d, c.get(&res[i]))
			}
		}
		fmt.Fprintf(&b, "%-10s %10s %10s %10s %10s\n", c.name, t7Pct(d, 50), t7Pct(d, 90), t7Pct(d, 99), t7Pct(d, 100))
	}
	t.Log(b.String())
}
