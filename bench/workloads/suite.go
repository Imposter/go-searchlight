package workloads

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Imposter/go-searchlight/bench/datasets"
	"github.com/Imposter/go-searchlight/bench/es"
	"github.com/Imposter/go-searchlight/bench/report"
	"github.com/Imposter/go-searchlight/internal/search"
)

// Config is a benchmark run.
type Config struct {
	Label string
	// DataFile is the product NDJSON written by datasets; Docs limits how many of its
	// lines are loaded (0: all). Seed is the seed it was generated with, used to
	// regenerate documents for writes and percolation.
	DataFile string
	Docs     int64
	Seed     uint64
	// SearchFile holds saved searches; SearchSets are the set sizes percolated
	// against, each a prefix of the file.
	SearchFile string
	SearchSets []int

	Index, PercIndex string
	Shards           int

	BulkBatch, BulkConcurrency int
	// Search is how the search workloads run.
	Search   RunOptions
	Variants int
	// PageDepth is how deep paging walks go; PageWalks how many are measured.
	PageDepth, PageWalks int

	PercolateBatch, PercolateConcurrency int
	// PercolateIterations batches and PercolateSingle single-document requests are
	// measured per saved-search set.
	PercolateIterations, PercolateSingle int
	BulkPercolateIterations              int

	// VisibleIterations measures refresh_visible and refresh_wait_for, each at
	// Concurrency 1 (queueing would change what the latency means). Its default,
	// 1000, is minSamples(0.99): the fewest a trusted p99 needs. At the ~1.1 s per
	// iteration measured in bench/testdata/smoke-report.md (300 iterations took
	// about 5m25s and 5m28s there), 1000 costs roughly 18 minutes per direction,
	// ~36 minutes for both -- comfortably inside bench.yml's 180-minute job timeout
	// alongside everything else, which the same run finished in under a minute.
	VisibleIterations int

	MixedDuration                          time.Duration
	MixedReaders, MixedWriters, MixedBatch int

	CrossCheckVariants int
	// Restarters restart an engine, by name (target T9); RestartIterations restarts are
	// timed.
	Restarters        map[string]Restarter
	RestartIterations int
	// Recoverers measure a new replica of an engine from zero to serving, by name
	// (target T8); RecoveryIterations recoveries are timed.
	Recoverers         map[string]Recoverer
	RecoveryIterations int

	// LoadOnly stops after the load and footprint.
	LoadOnly bool
	// StopOnMismatch skips every timing workload once the cross-check (run right
	// after the load) finds an unresolved mismatch: comparative numbers from engines
	// that disagree on answers are not worth the time to collect.
	StopOnMismatch bool
	// Only and Skip filter workloads by name or group (empty Only: all).
	Only, Skip []string
	Log        io.Writer
}

func (c *Config) defaults() {
	set := func(p *int, v int) {
		if *p <= 0 {
			*p = v
		}
	}
	set(&c.Shards, 1)
	set(&c.BulkBatch, 1000)
	set(&c.BulkConcurrency, 4)
	set(&c.Search.Warmup, 100)
	set(&c.Search.Iterations, 1000)
	set(&c.Search.Concurrency, 1)
	set(&c.Variants, 64)
	set(&c.PageDepth, 10_000)
	set(&c.PageWalks, 5)
	set(&c.PercolateBatch, 100)
	set(&c.PercolateConcurrency, 4)
	set(&c.PercolateIterations, 50)
	set(&c.PercolateSingle, 1000)
	set(&c.BulkPercolateIterations, 20)
	set(&c.VisibleIterations, 1000)
	set(&c.MixedReaders, 4)
	set(&c.MixedWriters, 1)
	set(&c.MixedBatch, 500)
	set(&c.CrossCheckVariants, 4)
	set(&c.RestartIterations, 3)
	set(&c.RecoveryIterations, 3)
	if c.MixedDuration <= 0 {
		c.MixedDuration = 30 * time.Second
	}
	if c.Index == "" {
		c.Index = "products"
	}
	if c.PercIndex == "" {
		c.PercIndex = "saved-searches"
	}
	if c.Log == nil {
		c.Log = io.Discard
	}
	slices.Sort(c.SearchSets)
}

func (c *Config) wants(name, group string) bool {
	match := func(list []string) bool {
		for _, w := range list {
			if w == name || w == group || strings.HasPrefix(name, w+"_") {
				return true
			}
		}
		return false
	}
	if match(c.Skip) {
		return false
	}
	return len(c.Only) == 0 || match(c.Only)
}

// suite is one run in progress.
type suite struct {
	cfg     Config
	engines []Engine
	run     *report.Run
	loaded  int64
	// now and sleep stand in for time.Now and sleepJitter in the visibility
	// workload's measured write (writeJitter's delay, and the write-to-visible
	// timing around it): nil in production, where they default to the real clock
	// and a real wait. A test can inject both to make "the delay is excluded from
	// the recorded latency" an exact, deterministic check instead of a wall-clock
	// comparison against a noisy, shared CI runner's scheduling.
	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

// clock and sleeper return s.now and s.sleep, defaulting to the real ones.
func (s *suite) clock() func() time.Time {
	if s.now != nil {
		return s.now
	}
	return time.Now
}

func (s *suite) sleeper() func(context.Context, time.Duration) error {
	if s.sleep != nil {
		return s.sleep
	}
	return sleepJitter
}

func (s *suite) logf(format string, args ...any) {
	fmt.Fprintf(s.cfg.Log, "%s "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
}

// RunSuite runs every workload (spec section 14) on each engine, one engine at a time,
// and returns the run's results. Engines are compared only if they answer the
// cross-check identically.
func RunSuite(ctx context.Context, cfg Config, engines []Engine) (*report.Run, error) {
	cfg.defaults()
	s := &suite{cfg: cfg, engines: engines, run: &report.Run{
		Schema: report.SchemaVersion, Label: cfg.Label, StartedAt: time.Now().UTC(),
		Env: report.CaptureEnvironment(), Caveats: es.Caveats,
		Options: report.Options{
			Warmup: cfg.Search.Warmup, Iterations: cfg.Search.Iterations, Concurrency: cfg.Search.Concurrency, Rate: cfg.Search.Rate,
			Variants: cfg.Variants, BulkBatch: cfg.BulkBatch, BulkConcurrency: cfg.BulkConcurrency,
			PercolateBatch: cfg.PercolateBatch, PercolateConcurrency: cfg.PercolateConcurrency, PageDepth: cfg.PageDepth,
			MixedSeconds: cfg.MixedDuration.Seconds(),
		},
	}}
	for _, e := range engines {
		if err := e.Ready(ctx); err != nil {
			return nil, fmt.Errorf("%s is not ready: %w", e.Name(), err)
		}
		info, err := e.Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		s.run.Engines = append(s.run.Engines, info)
		s.logf("%s %s at %s", info.Name, info.Version, info.URL)
	}
	if err := s.dataset(); err != nil {
		return nil, err
	}
	var sampler *footprintSampler
	var stopSampler func()
	if !cfg.LoadOnly {
		sampler, stopSampler = s.startFootprintSampler(ctx)
	}
	steps := []func(context.Context) error{s.load, s.crossCheckSearches, s.searches, s.visibility, s.mixed, s.percolation, s.restart, s.recovery}
	if cfg.LoadOnly {
		steps = steps[:1]
	}
	const crossCheckStep = 1 // s.crossCheckSearches, above
	var stepErr error
	for i, step := range steps {
		if err := step(ctx); err != nil {
			stepErr = err
			break
		}
		if i == crossCheckStep && cfg.StopOnMismatch && s.run.CrossCheck.Failed() {
			s.run.Notes = append(s.run.Notes, "Stopped after the cross-check found an unresolved mismatch (--stop-on-mismatch): "+
				"further timing workloads would compare answers that are already known to disagree.")
			break
		}
	}
	if stopSampler != nil {
		stopSampler() // blocks until the last sample is folded in
		s.mergeFootprintPeak(sampler)
	}
	s.run.FinishedAt = time.Now().UTC()
	if stepErr != nil {
		return s.run, stepErr
	}
	s.run.Targets = report.Evaluate(s.run)
	return s.run, nil
}

func (s *suite) dataset() error {
	st, err := os.Stat(s.cfg.DataFile)
	if err != nil {
		return fmt.Errorf("the dataset: %w (generate it with slbench gen)", err)
	}
	s.run.Dataset = report.Dataset{
		Seed: s.cfg.Seed, File: s.cfg.DataFile, FileBytes: st.Size(), Fields: len(datasets.Products),
		SearchSets: s.cfg.SearchSets, SearchFile: s.cfg.SearchFile, Shards: s.cfg.Shards,
	}
	return nil
}

func (s *suite) result(name, group, desc string, eng Engine, o RunOptions, m *Measurement) report.Result {
	r := report.Result{
		Workload: name, Group: group, Engine: eng.Name(), Description: desc,
		Concurrency: o.Concurrency, Rate: o.Rate, Warmup: o.Warmup, Iterations: o.Iterations,
		Ops: m.Ops, Docs: m.Docs, Errors: m.Errors, ElapsedSec: m.Elapsed.Seconds(),
		OpsPerSec: m.Throughput(), DocsPerSec: m.DocsPerSec(),
	}
	if m.Hist.Count() > 0 {
		sm := m.Hist.Summary()
		r.Latency = &report.Latency{Count: sm.Count, Min: sm.Min, Mean: sm.Mean, P50: sm.P50, P90: sm.P90, P99: sm.P99, P999: sm.P999, Max: sm.Max}
	}
	if m.FirstErr != nil {
		r.Error = m.FirstErr.Error()
		if len(r.Error) > 500 {
			r.Error = r.Error[:500] + "…"
		}
	}
	return r
}

func (s *suite) add(r report.Result) {
	lat := ""
	if r.Latency != nil {
		lat = fmt.Sprintf(" p50 %s p99 %s", report.FormatMicros(r.Latency.P50), report.FormatMicros(r.Latency.P99))
	}
	s.logf("  %-28s %-13s %8.1f ops/s%s errors %d %s", r.Workload, r.Engine, r.OpsPerSec, lat, r.Errors, r.Error)
	s.run.Results = append(s.run.Results, r)
}

// load bulk-loads the dataset into each engine in turn (bulk_index), waits until all
// of it is searchable, and measures the footprint.
func (s *suite) load(ctx context.Context) error {
	for _, eng := range s.engines {
		s.logf("loading %s into %s", s.cfg.DataFile, eng.Name())
		if err := eng.CreateIndex(ctx, s.cfg.Index, datasets.Products, s.cfg.Shards); err != nil {
			return fmt.Errorf("%s: creating %s: %w", eng.Name(), s.cfg.Index, err)
		}
		m, n, err := s.bulkLoad(ctx, eng)
		if err != nil {
			return fmt.Errorf("%s: loading: %w", eng.Name(), err)
		}
		if n == 0 {
			return errors.New("the dataset is empty")
		}
		s.loaded = n
		o := RunOptions{Concurrency: s.cfg.BulkConcurrency, Iterations: int(m.Ops)}
		res := s.result("bulk_index", report.GroupIndexing, fmt.Sprintf("%d documents, %d per _bulk", n, s.cfg.BulkBatch), eng, o, m)
		res.Values = map[string]float64{"batch": float64(s.cfg.BulkBatch)}
		s.add(res)
		if err := s.waitSearchable(ctx, eng, n); err != nil {
			return err
		}
		r, err := eng.Resources(ctx, s.cfg.Index)
		if err != nil {
			return fmt.Errorf("%s: resources: %w", eng.Name(), err)
		}
		fp := report.Result{
			Workload: "footprint", Group: report.GroupFootprint, Engine: eng.Name(), Ops: 1,
			Description: "disk: " + r.DiskSource + "; RSS: " + r.RSSSource + " (right after the load)",
			Values: map[string]float64{
				"disk_bytes": float64(r.DiskBytes), "rss_bytes": float64(r.RSSBytes),
				"disk_per_million": float64(r.DiskBytes) / float64(n) * 1e6,
				"rss_per_million":  float64(r.RSSBytes) / float64(n) * 1e6,
			},
		}
		s.logf("  footprint %s: disk %s, RSS %s", eng.Name(), report.FormatBytes(float64(r.DiskBytes)), report.FormatBytes(float64(r.RSSBytes)))
		s.run.Results = append(s.run.Results, fp)
	}
	s.run.Dataset.Docs = s.loaded
	if s.loaded > 0 {
		s.run.Dataset.AvgDocBytes = float64(s.run.Dataset.FileBytes) / float64(s.loaded)
	}
	return nil
}

// footprintSampleInterval is how often the sampler re-measures disk and RSS while
// the suite runs. A var, not a const, so a test can shorten it.
var footprintSampleInterval = 1500 * time.Millisecond

// footprintSampler periodically re-measures every engine's disk and RSS while the
// suite runs, keeping a running maximum: a measurement taken once after the load
// and once at the end misses mid-run spikes (a merge, the percolator load, the
// mixed workload), which is exactly the footprint spec section 1's T6 cares about.
type footprintSampler struct {
	mu   sync.Mutex
	peak map[string]report.Resources // by engine name
}

func newFootprintSampler() *footprintSampler {
	return &footprintSampler{peak: map[string]report.Resources{}}
}

// sample measures every engine once and folds it into the running maximum.
// Best-effort: a measurement error for one engine does not affect the others or
// stop future samples.
func (f *footprintSampler) sample(ctx context.Context, engines []Engine, index string) {
	for _, eng := range engines {
		r, err := eng.Resources(ctx, index)
		if err != nil {
			continue
		}
		f.mu.Lock()
		p := f.peak[eng.Name()]
		if r.DiskBytes > p.DiskBytes {
			p.DiskBytes, p.DiskSource = r.DiskBytes, r.DiskSource
		}
		if r.RSSBytes > p.RSSBytes {
			p.RSSBytes, p.RSSSource = r.RSSBytes, r.RSSSource
		}
		f.peak[eng.Name()] = p
		f.mu.Unlock()
	}
}

func (f *footprintSampler) peakOf(name string) report.Resources {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peak[name]
}

// startFootprintSampler samples every engine's disk and RSS every
// footprintSampleInterval until the returned stop function is called; stop blocks
// until the sampling goroutine has actually exited, so the caller can safely read
// the sampler's peaks right after it returns.
func (s *suite) startFootprintSampler(ctx context.Context) (sampler *footprintSampler, stop func()) {
	sampler = newFootprintSampler()
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		t := time.NewTicker(footprintSampleInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-t.C:
				sampler.sample(ctx, s.engines, s.cfg.Index)
			}
		}
	}()
	return sampler, func() {
		close(done)
		<-stopped
	}
}

// mergeFootprintPeak folds sampler's running maximum into each engine's "footprint"
// result, replacing the load-time snapshot's numbers wherever the sampler saw
// something larger while the rest of the suite ran.
func (s *suite) mergeFootprintPeak(sampler *footprintSampler) {
	for i := range s.run.Results {
		fp := &s.run.Results[i]
		if fp.Workload != "footprint" {
			continue
		}
		p := sampler.peakOf(fp.Engine)
		peaked := false
		if disk := float64(p.DiskBytes); disk > fp.Values["disk_bytes"] {
			fp.Values["disk_bytes"], fp.Values["disk_per_million"] = disk, disk/float64(s.loaded)*1e6
			peaked = true
		}
		if rss := float64(p.RSSBytes); rss > fp.Values["rss_bytes"] {
			fp.Values["rss_bytes"], fp.Values["rss_per_million"] = rss, rss/float64(s.loaded)*1e6
			peaked = true
		}
		if peaked {
			fp.Description = fmt.Sprintf("disk: %s; RSS: %s (peak across the run, sampled every %s)", p.DiskSource, p.RSSSource, footprintSampleInterval)
		}
		s.logf("  footprint %s (peak): disk %s, RSS %s", fp.Engine,
			report.FormatBytes(fp.Values["disk_bytes"]), report.FormatBytes(fp.Values["rss_bytes"]))
	}
}

func (s *suite) bulkLoad(ctx context.Context, eng Engine) (*Measurement, int64, error) {
	f, err := os.Open(s.cfg.DataFile)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	batches := make(chan []Doc, 2*s.cfg.BulkConcurrency)
	m := &Measurement{Hist: NewHistogram()}
	var mu sync.Mutex
	var wg sync.WaitGroup
	t0 := time.Now()
	for range s.cfg.BulkConcurrency {
		wg.Go(func() {
			for b := range batches {
				st := nanotime()
				err := eng.Bulk(ctx, s.cfg.Index, b, "")
				lat := time.Duration(nanotime() - st)
				mu.Lock()
				if err != nil {
					m.Errors++
					if m.FirstErr == nil {
						m.FirstErr = err
						// Logged as soon as it happens, and not just returned at the end: once
						// cancel wakes the reader loop below, its own ctx.Err() ("context
						// canceled") would otherwise be the only error anyone sees, hiding the
						// real cause (a 429, a 503, a timeout, ...).
						s.logf("  %s: bulk failed, stopping the load: %v", eng.Name(), err)
						cancel()
					}
				} else {
					m.Hist.RecordDuration(lat)
					m.Ops++
					m.Docs += int64(len(b))
				}
				mu.Unlock()
			}
		})
	}
	var n int64
	batch := make([]Doc, 0, s.cfg.BulkBatch)
	errStop := errors.New("stop")
	readErr := datasets.ReadLines(f, func(line []byte) error {
		if s.cfg.Docs > 0 && n >= s.cfg.Docs {
			return errStop
		}
		id, doc, err := datasets.SplitProductLine(line)
		if err != nil {
			return err
		}
		batch = append(batch, Doc{ID: id, Body: slices.Clone(doc)})
		n++
		if len(batch) == s.cfg.BulkBatch {
			select {
			case batches <- batch:
			case <-ctx.Done():
				return ctx.Err()
			}
			batch = make([]Doc, 0, s.cfg.BulkBatch)
			if n%200_000 == 0 {
				s.logf("  %s: %d documents sent", eng.Name(), n)
			}
		}
		return nil
	})
	if len(batch) > 0 && ctx.Err() == nil {
		batches <- batch
	}
	close(batches)
	wg.Wait()
	m.Elapsed = time.Since(t0)
	// m.FirstErr (a worker's real failure) wins over readErr: once a worker's error
	// calls cancel, the reader above gets ctx.Err() ("context canceled") from its own
	// batches<- select, which would otherwise mask the actual cause.
	switch {
	case m.FirstErr != nil:
		return m, n, m.FirstErr
	case readErr != nil && !errors.Is(readErr, errStop):
		return m, n, readErr
	default:
		return m, n, nil
	}
}

func (s *suite) waitSearchable(ctx context.Context, eng Engine, want int64) error {
	deadline := time.Now().Add(5 * time.Minute)
	for {
		if err := eng.Refresh(ctx, s.cfg.Index); err != nil {
			return fmt.Errorf("%s: refresh: %w", eng.Name(), err)
		}
		n, err := eng.Count(ctx, s.cfg.Index)
		if err != nil {
			return fmt.Errorf("%s: count: %w", eng.Name(), err)
		}
		if n == want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: %d documents searchable, want %d", eng.Name(), n, want)
		}
		time.Sleep(time.Second)
	}
}

func (s *suite) both() (Engine, Engine) {
	var sl, el Engine
	for _, e := range s.engines {
		switch e.Name() {
		case report.Searchlight:
			sl = e
		case report.Elasticsearch:
			el = e
		}
	}
	return sl, el
}

// crossCheckSearches compares both engines' answers to a sample of every search
// workload's requests and to the coverage checks.
func (s *suite) crossCheckSearches(ctx context.Context) error {
	sl, el := s.both()
	if sl == nil || el == nil {
		s.run.CrossCheck.Skipped = "needs both engines"
		return nil
	}
	s.logf("cross-checking searches")
	s.run.CrossCheck.Ran = true
	var docBody map[string]any
	if err := json.Unmarshal(datasets.AppendProduct(nil, s.cfg.Seed, min(5, s.loaded-1)), &docBody); err != nil {
		return err
	}
	specs := append(SearchSpecs(s.cfg.Seed, s.cfg.Variants, s.cfg.PageDepth), CoverageChecks(docBody)...)
	for _, spec := range specs {
		for v, raw := range spec.Bodies[:min(len(spec.Bodies), max(s.cfg.CrossCheckVariants, 1))] {
			name := fmt.Sprintf("%s#%d", spec.Name, v)
			checked := exactBody(raw, spec.PageDepth > 0)
			pages := 1
			if spec.PageDepth > 0 {
				pages = 3
			}
			problems, tolerated, err := s.compareSearch(ctx, sl, el, checked, pages)
			if err != nil {
				problems, tolerated = []string{err.Error()}, false
			}
			s.recordCheck(name, string(checked), problems, tolerated)
		}
	}
	cc := s.run.CrossCheck
	s.logf("  %d checked, %d identical, %d tolerated, %d mismatched", cc.Checked, cc.Matched, cc.Tolerated, cc.Checked-cc.Matched-cc.Tolerated)
	return nil
}

func (s *suite) recordCheck(name, req string, problems []string, tolerated bool) {
	cc := &s.run.CrossCheck
	cc.Checked++
	switch {
	case len(problems) == 0:
		cc.Matched++
	case tolerated:
		cc.Tolerated++
		cc.Mismatches = append(cc.Mismatches, report.Mismatch{Name: name, Request: req, Problems: problems, Tolerated: true})
	default:
		cc.Mismatches = append(cc.Mismatches, report.Mismatch{Name: name, Request: req, Problems: problems})
		s.logf("  MISMATCH %s: %s", name, strings.Join(problems, "; "))
	}
}

// exactBody adapts a workload body for the cross-check: exact totals, and at most 100
// hits (a paging body keeps its size).
func exactBody(raw []byte, paging bool) []byte {
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return raw
	}
	m["track_total"] = true
	if size, ok := m["size"].(float64); ok && size > 100 && !paging {
		m["size"] = 100
	}
	b, err := json.Marshal(m)
	if err != nil {
		return raw
	}
	return b
}

func (s *suite) compareSearch(ctx context.Context, sl, el Engine, raw []byte, pages int) ([]string, bool, error) {
	req, ps := search.ParseRequest(raw)
	if len(ps) > 0 {
		return nil, false, &es.InvalidError{Problems: ps}
	}
	sp, err := sl.Prepare(raw)
	if err != nil {
		return nil, false, err
	}
	ep, err := el.Prepare(raw)
	if err != nil {
		return nil, false, err
	}
	var slAfter, esAfter []any
	var problems []string
	tolerated := true
	for p := range pages {
		sr, err := sl.Search(ctx, s.cfg.Index, sp, slAfter)
		if err != nil {
			return nil, false, fmt.Errorf("searchlight: %w", err)
		}
		er, err := el.Search(ctx, s.cfg.Index, ep, esAfter)
		if err != nil {
			return nil, false, fmt.Errorf("elasticsearch: %w", err)
		}
		ps, tol := diffSearch(req, sr, er)
		for _, pr := range ps {
			problems = append(problems, fmt.Sprintf("page %d: %s", p+1, pr))
		}
		tolerated = tolerated && (len(ps) == 0 || tol)
		if sr.Next == nil || er.Next == nil {
			break
		}
		slAfter, esAfter = sr.Next, er.Next
	}
	return problems, tolerated && len(problems) > 0, nil
}

// searches runs every search workload on each engine.
func (s *suite) searches(ctx context.Context) error {
	for _, spec := range SearchSpecs(s.cfg.Seed, s.cfg.Variants, s.cfg.PageDepth) {
		if !s.cfg.wants(spec.Name, spec.Group) {
			continue
		}
		for _, eng := range s.engines {
			res, err := s.searchWorkload(ctx, eng, spec)
			if err != nil {
				return err
			}
			s.add(res)
		}
	}
	return nil
}

func (s *suite) searchWorkload(ctx context.Context, eng Engine, spec SearchSpec) (report.Result, error) {
	prepared := make([]Prepared, len(spec.Bodies))
	for i, b := range spec.Bodies {
		p, err := eng.Prepare(b)
		if err != nil {
			return report.Result{}, fmt.Errorf("%s: %s: %w", eng.Name(), spec.Name, err)
		}
		prepared[i] = p
	}
	o := s.cfg.Search
	if spec.PageDepth == 0 {
		op := func(ctx context.Context, i int) (int, time.Duration, error) {
			_, err := eng.Search(ctx, s.cfg.Index, prepared[i%len(prepared)], nil)
			return 1, 0, err
		}
		return s.result(spec.Name, spec.Group, spec.Description, eng, o, Run(ctx, o, op)), nil
	}
	// Paging: sequential walks; each iteration is one page.
	const pageSize = 100
	perWalk := max(spec.PageDepth/pageSize, 1)
	var cursor []any
	walk := -1
	op := func(ctx context.Context, i int) (int, time.Duration, error) {
		w, p := i/perWalk, i%perWalk
		if p == 0 || w != walk || cursor == nil {
			walk, cursor = w, nil
		}
		res, err := eng.Search(ctx, s.cfg.Index, prepared[w%len(prepared)], cursor)
		if err != nil {
			cursor = nil
			return 0, 0, err
		}
		cursor = res.Next
		return 1, 0, nil
	}
	po := RunOptions{Warmup: perWalk, Iterations: perWalk * s.cfg.PageWalks, Concurrency: 1}
	return s.result(spec.Name, spec.Group, spec.Description, eng, po, Run(ctx, po, op)), nil
}

// idQuery is a search for one document by id.
func idQuery(id string) []byte {
	return body(q{"query": cond("_id", "eq", id), "size": 0, "track_total": true})
}

// defaultRefreshInterval is read-to-visible's assumed refresh interval when an
// engine's reported Config has no "refresh_interval" (Searchlight's own default,
// config.Default().RefreshInterval, and also Elasticsearch's).
const defaultRefreshInterval = time.Second

// refreshInterval is eng's refresh interval, from the EngineInfo.Config it reported
// to RunSuite ("refresh_interval", a time.ParseDuration string), or
// defaultRefreshInterval when it did not report one or reported one that does not
// parse to a positive duration.
func (s *suite) refreshInterval(eng Engine) time.Duration {
	for _, info := range s.run.Engines {
		if info.Name != eng.Name() {
			continue
		}
		if v, ok := info.Config["refresh_interval"]; ok {
			if d, err := time.ParseDuration(v); err == nil && d > 0 {
				return d
			}
		}
	}
	return defaultRefreshInterval
}

// writeJitter is the delay a measured visibility write sleeps before it starts,
// standing in for a real client's write landing at a random point in the refresh
// cycle rather than, as a closed loop otherwise does, right after the previous
// write's own refresh. It is uniform on [0, interval), and deterministic in
// (seed, workload, i): the same call gives every engine the identical delay at the
// same iteration, so the comparison stays apples to apples, and a run is
// reproducible.
func writeJitter(seed uint64, workload string, i int, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	h := fnv.New64a()
	fmt.Fprintf(h, "%s:%d", workload, i)
	r := rand.New(rand.NewPCG(seed, h.Sum64())) //nolint:gosec // reproducible benchmark jitter, not secrets
	return time.Duration(r.Int64N(int64(interval)))
}

// sleepJitter sleeps d, unless ctx ends first.
func sleepJitter(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// visibility measures write-to-visible latency (refresh_visible) and refresh=wait_for.
// Each measured write sleeps a random, unmeasured delay first (writeJitter): a real
// client does not write in lockstep right after the engine's previous refresh, which
// a closed loop at concurrency 1 would otherwise do every iteration.
func (s *suite) visibility(ctx context.Context) error {
	for _, w := range []string{"refresh_visible", "refresh_wait_for"} {
		if !s.cfg.wants(w, report.GroupVisibility) {
			continue
		}
		for _, eng := range s.engines {
			o := RunOptions{Warmup: min(2, s.cfg.VisibleIterations), Iterations: s.cfg.VisibleIterations, Concurrency: 1}
			interval := s.refreshInterval(eng)
			now, sleep := s.clock(), s.sleeper()
			op := func(ctx context.Context, i int) (int, time.Duration, error) {
				id := fmt.Sprintf("v%s-%06d", strings.TrimPrefix(w, "refresh_"), i)
				p, err := eng.Prepare(idQuery(id))
				if err != nil {
					return 0, 0, err
				}
				doc := Doc{ID: id, Body: datasets.AppendProduct(nil, s.cfg.Seed, s.loaded+1_000_000+int64(i))}
				if err := sleep(ctx, writeJitter(s.cfg.Seed, w, i, interval)); err != nil {
					return 0, 0, err
				}
				if w == "refresh_wait_for" {
					// Only the write is timed: refresh=wait_for's latency is the write's
					// own, and the confirming search is an untimed correctness check
					// (it fails the iteration if the write was not visible on return,
					// but its own latency is not part of what T7 measures). The jitter
					// above ran before t0, so it is never part of timed either.
					t0 := now()
					bulkErr := eng.Bulk(ctx, s.cfg.Index, []Doc{doc}, "wait_for")
					timed := now().Sub(t0)
					if bulkErr != nil {
						return 0, 0, bulkErr
					}
					res, err := eng.Search(ctx, s.cfg.Index, p, nil)
					if err != nil {
						return 0, 0, err
					}
					if res.Total != 1 {
						return 0, 0, fmt.Errorf("%s: written with refresh=wait_for but not visible on return", id)
					}
					return 1, timed, nil
				}
				t0 := now()
				if err := eng.Bulk(ctx, s.cfg.Index, []Doc{doc}, ""); err != nil {
					return 0, 0, err
				}
				deadline := time.Now().Add(10 * time.Second)
				for {
					res, err := eng.Search(ctx, s.cfg.Index, p, nil)
					if err != nil {
						return 0, 0, err
					}
					if res.Total == 1 {
						return 1, now().Sub(t0), nil
					}
					if time.Now().After(deadline) {
						return 0, 0, fmt.Errorf("%s not visible after 10 s", id)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			desc := "write without refresh, poll every 10 ms until searchable (latency from the write's start, after a random pre-write delay of up to one refresh interval, unmeasured)"
			if w == "refresh_wait_for" {
				desc = "write with refresh=wait_for (the write's latency); visible on return (also after the same random pre-write delay, unmeasured)"
			}
			s.add(s.result(w, report.GroupVisibility, desc, eng, o, Run(ctx, o, op)))
		}
	}
	return nil
}

// mixed runs readers and writers concurrently for a fixed time.
func (s *suite) mixed(ctx context.Context) error {
	if !s.cfg.wants("mixed", report.GroupMixed) {
		return nil
	}
	var reads [][]byte
	for _, spec := range SearchSpecs(s.cfg.Seed+7, 16, s.cfg.PageDepth) {
		switch spec.Name {
		case "filter_bool", "filter_term", "filter_contains", "sorted_recent_top10", "agg_terms":
			reads = append(reads, spec.Bodies...)
		}
	}
	for _, eng := range s.engines {
		prepared := make([]Prepared, len(reads))
		for i, b := range reads {
			p, err := eng.Prepare(b)
			if err != nil {
				return err
			}
			prepared[i] = p
		}
		ro := RunOptions{Concurrency: s.cfg.MixedReaders, Duration: s.cfg.MixedDuration}
		wo := RunOptions{Concurrency: s.cfg.MixedWriters, Duration: s.cfg.MixedDuration}
		var rm, wm *Measurement
		var wg sync.WaitGroup
		wg.Go(func() {
			rm = Run(ctx, ro, func(ctx context.Context, i int) (int, time.Duration, error) {
				_, err := eng.Search(ctx, s.cfg.Index, prepared[i%len(prepared)], nil)
				return 1, 0, err
			})
		})
		wg.Go(func() {
			wm = Run(ctx, wo, func(ctx context.Context, i int) (int, time.Duration, error) {
				docs := make([]Doc, s.cfg.MixedBatch)
				for k := range docs {
					j := (int64(i)*int64(s.cfg.MixedBatch) + int64(k)) % s.loaded
					docs[k] = Doc{ID: datasets.ProductID(j), Body: datasets.AppendProduct(nil, s.cfg.Seed+1, j)}
				}
				return len(docs), 0, eng.Bulk(ctx, s.cfg.Index, docs, "")
			})
		})
		wg.Wait()
		s.add(s.result("mixed_read", report.GroupMixed, fmt.Sprintf("%d readers (filter, sorted, terms) beside %d bulk writers", s.cfg.MixedReaders, s.cfg.MixedWriters), eng, ro, rm))
		s.add(s.result("mixed_write", report.GroupMixed, fmt.Sprintf("%d writers updating %d documents per _bulk beside the readers", s.cfg.MixedWriters, s.cfg.MixedBatch), eng, wo, wm))
	}
	return nil
}

// readSearches reads the first n saved searches.
func (s *suite) readSearches(n int) ([]datasets.SavedSearch, error) {
	f, err := os.Open(s.cfg.SearchFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := make([]datasets.SavedSearch, 0, n)
	errStop := errors.New("stop")
	err = datasets.ReadLines(f, func(line []byte) error {
		if len(out) >= n {
			return errStop
		}
		var ss datasets.SavedSearch
		if err := json.Unmarshal(line, &ss); err != nil {
			return err
		}
		out = append(out, ss)
		return nil
	})
	if err != nil && !errors.Is(err, errStop) {
		return nil, err
	}
	if len(out) < n {
		return nil, fmt.Errorf("%s holds %d saved searches, fewer than %d", s.cfg.SearchFile, len(out), n)
	}
	return out, nil
}

// percolation loads each saved-search set in turn (a prefix of the file, so each set
// adds to the last) and measures batch and single-document percolation against it,
// then _bulk?percolate at the largest set.
func (s *suite) percolation(ctx context.Context) error {
	if len(s.cfg.SearchSets) == 0 || s.cfg.SearchFile == "" || !s.cfg.wants("percolate", report.GroupPercolate) {
		return nil
	}
	largest := s.cfg.SearchSets[len(s.cfg.SearchSets)-1]
	qs, err := s.readSearches(largest)
	if err != nil {
		return err
	}
	pool := make([]json.RawMessage, 4096)
	for i := range pool {
		pool[i] = datasets.AppendProduct(nil, s.cfg.Seed, s.loaded+int64(i))
	}
	for _, eng := range s.engines {
		if err := eng.CreatePercolatorIndex(ctx, s.cfg.PercIndex, datasets.Products, s.cfg.Shards); err != nil {
			return fmt.Errorf("%s: creating %s: %w", eng.Name(), s.cfg.PercIndex, err)
		}
	}
	sl, el := s.both()
	prev := 0
	for _, n := range s.cfg.SearchSets {
		for _, eng := range s.engines {
			s.logf("%s: storing saved searches %d..%d", eng.Name(), prev, n)
			t0 := time.Now()
			if err := eng.PutQueries(ctx, s.cfg.PercIndex, qs[prev:n]); err != nil {
				return fmt.Errorf("%s: saved searches: %w", eng.Name(), err)
			}
			took := time.Since(t0)
			s.run.Results = append(s.run.Results, report.Result{
				Workload: fmt.Sprintf("percolator_load_%d", n), Group: report.GroupPercolate, Engine: eng.Name(),
				Description: fmt.Sprintf("store saved searches %d..%d", prev, n), Ops: int64(n - prev), Docs: int64(n - prev),
				ElapsedSec: took.Seconds(), OpsPerSec: float64(n-prev) / took.Seconds(),
			})
		}
		prev = n
		if sl != nil && el != nil {
			s.crossCheckPercolate(ctx, sl, el, n, pool[:min(len(pool), 300)])
		}
		batch := s.cfg.PercolateBatch
		for _, eng := range s.engines {
			name := fmt.Sprintf("percolate_batch_%d", n)
			if s.cfg.wants(name, report.GroupPercolate) {
				o := RunOptions{Warmup: max(s.cfg.PercolateIterations/10, 2), Iterations: s.cfg.PercolateIterations, Concurrency: s.cfg.PercolateConcurrency}
				op := func(ctx context.Context, i int) (int, time.Duration, error) {
					start := (i * batch) % len(pool)
					docs := pool[start:min(start+batch, len(pool))]
					_, err := eng.Percolate(ctx, s.cfg.PercIndex, docs)
					return len(docs), 0, err
				}
				s.add(s.result(name, report.GroupPercolate, fmt.Sprintf("%d documents per request against %d saved searches", batch, n), eng, o, Run(ctx, o, op)))
			}
			name = fmt.Sprintf("percolate_single_%d", n)
			if s.cfg.wants(name, report.GroupPercolate) {
				o := RunOptions{Warmup: max(s.cfg.PercolateSingle/10, 5), Iterations: s.cfg.PercolateSingle, Concurrency: 1}
				op := func(ctx context.Context, i int) (int, time.Duration, error) {
					_, err := eng.Percolate(ctx, s.cfg.PercIndex, pool[i%len(pool):i%len(pool)+1])
					return 1, 0, err
				}
				s.add(s.result(name, report.GroupPercolate, fmt.Sprintf("one document per request against %d saved searches (per-document latency)", n), eng, o, Run(ctx, o, op)))
			}
		}
	}
	if !s.cfg.wants("bulk_percolate", report.GroupPercolate) {
		return nil
	}
	for _, eng := range s.engines {
		const bp = 500
		o := RunOptions{Warmup: 2, Iterations: s.cfg.BulkPercolateIterations, Concurrency: 1}
		op := func(ctx context.Context, i int) (int, time.Duration, error) {
			docs := make([]Doc, bp)
			for k := range docs {
				j := (i*bp + k) % len(pool)
				docs[k] = Doc{ID: fmt.Sprintf("bp%09d", i*bp+k), Body: pool[j]}
			}
			_, err := eng.BulkPercolate(ctx, s.cfg.PercIndex, docs)
			return bp, 0, err
		}
		desc := fmt.Sprintf("%d documents written and percolated per request against %d saved searches (Elasticsearch: _bulk then percolate)", bp, largest)
		s.add(s.result("bulk_percolate", report.GroupPercolate, desc, eng, o, Run(ctx, o, op)))
	}
	return nil
}

func (s *suite) crossCheckPercolate(ctx context.Context, sl, el Engine, n int, docs []json.RawMessage) {
	const chunk = 50
	var problems []string
	matched := 0
	for start := 0; start < len(docs); start += chunk {
		part := docs[start:min(start+chunk, len(docs))]
		a, err := sl.Percolate(ctx, s.cfg.PercIndex, part)
		if err != nil {
			problems = append(problems, "searchlight: "+err.Error())
			break
		}
		b, err := el.Percolate(ctx, s.cfg.PercIndex, part)
		if err != nil {
			problems = append(problems, "elasticsearch: "+err.Error())
			break
		}
		for i := range a {
			matched += len(a[i])
		}
		for _, p := range diffPercolate(a, b) {
			problems = append(problems, fmt.Sprintf("batch at %d: %s", start, p))
		}
	}
	s.logf("  percolation cross-check at %d saved searches: %d documents, %d matches", n, len(docs), matched)
	s.recordCheck(fmt.Sprintf("percolate_%d", n), fmt.Sprintf("%d generated documents", len(docs)), problems, false)
}

// Restarter restarts an engine (target T9): Restart returns once the engine was
// stopped and started again, before it necessarily serves; the suite times it until a
// count of the index is whole again.
type Restarter struct {
	// Describe says how, for the report.
	Describe string
	Restart  func(ctx context.Context) error
}

// CommandRestarter restarts an engine with a shell command (sh -c; cmd /C on Windows).
func CommandRestarter(cmdline string, log io.Writer) Restarter {
	return Restarter{Describe: "restart command: " + cmdline, Restart: func(ctx context.Context) error {
		var cmd *exec.Cmd
		if runtime.GOOS == "windows" {
			cmd = exec.CommandContext(ctx, "cmd", "/C", cmdline)
		} else {
			cmd = exec.CommandContext(ctx, "sh", "-c", cmdline)
		}
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("restart command: %w", err)
		}
		return nil
	}}
}

// restart times each engine's restart until it serves the full index again.
func (s *suite) restart(ctx context.Context) error {
	if !s.cfg.wants("restart", report.GroupRestart) {
		return nil
	}
	for _, eng := range s.engines {
		rs, ok := s.cfg.Restarters[eng.Name()]
		if !ok || rs.Restart == nil {
			continue
		}
		want, err := eng.Count(ctx, s.cfg.Index)
		if err != nil {
			return err
		}
		o := RunOptions{Iterations: s.cfg.RestartIterations, Concurrency: 1}
		op := func(ctx context.Context, _ int) (int, time.Duration, error) {
			if err := rs.Restart(ctx); err != nil {
				return 0, 0, err
			}
			rctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
			defer cancel()
			return 1, 0, waitReady(rctx, func(ctx context.Context) error {
				n, err := eng.Count(ctx, s.cfg.Index)
				if err != nil {
					return err
				}
				if n != want {
					return fmt.Errorf("%d of %d documents", n, want)
				}
				return nil
			})
		}
		res := s.result("restart", report.GroupRestart, rs.Describe+", until the full count is served", eng, o, Run(ctx, o, op))
		res.Values = map[string]float64{"docs": float64(want)}
		s.add(res)
	}
	return nil
}

// Recoverer runs an engine's new-replica recovery (target T8) on a cluster of its own.
// The suite loads the dataset into Source, the cluster's first node, then each Recover
// starts a node holding nothing, returns how long it took from its start until its own
// copies serve all want documents of index, and removes it again.
type Recoverer interface {
	Source() Engine
	Recover(ctx context.Context, index string, want int64) (time.Duration, error)
}

// recovery loads the dataset into each recoverer's source and times new replicas.
func (s *suite) recovery(ctx context.Context) error {
	if !s.cfg.wants("recovery", report.GroupRecovery) {
		return nil
	}
	for _, eng := range s.engines {
		rec := s.cfg.Recoverers[eng.Name()]
		if rec == nil {
			continue
		}
		src := rec.Source()
		s.logf("loading %s into %s's recovery source", s.cfg.DataFile, eng.Name())
		if err := src.CreateIndex(ctx, s.cfg.Index, datasets.Products, s.cfg.Shards); err != nil {
			return fmt.Errorf("%s: recovery source: creating %s: %w", eng.Name(), s.cfg.Index, err)
		}
		_, n, err := s.bulkLoad(ctx, src)
		if err != nil {
			return fmt.Errorf("%s: recovery source: loading: %w", eng.Name(), err)
		}
		if err := s.waitSearchable(ctx, src, n); err != nil {
			return err
		}
		o := RunOptions{Iterations: s.cfg.RecoveryIterations, Concurrency: 1}
		op := func(ctx context.Context, _ int) (int, time.Duration, error) {
			d, err := rec.Recover(ctx, s.cfg.Index, n)
			return 1, d, err
		}
		desc := fmt.Sprintf("a node with an empty data directory joins a cluster holding %d documents in %d shards, timed from its start until its own copies serve them all", n, s.cfg.Shards)
		res := s.result("recovery", report.GroupRecovery, desc, eng, o, Run(ctx, o, op))
		res.Values = map[string]float64{"docs": float64(n)}
		s.add(res)
	}
	return nil
}
