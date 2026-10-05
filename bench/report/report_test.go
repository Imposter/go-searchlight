package report

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/bench/es"
)

var update = flag.Bool("update", false, "rewrite testdata/report.md")

func lat(p50, p99 float64) *Latency {
	return &Latency{Count: 1000, Min: p50 / 2, Mean: p50 * 1.1, P50: p50, P90: (p50 + p99) / 2, P99: p99, P999: p99 * 1.5, Max: p99 * 2}
}

func res(workload, group, engine string, ops, docs float64, l *Latency) Result {
	return Result{
		Workload: workload, Group: group, Engine: engine, Concurrency: 1, Ops: 1000, Docs: int64(docs / ops * 1000),
		ElapsedSec: 1000 / ops, OpsPerSec: ops, DocsPerSec: docs, Latency: l,
	}
}

// sampleRun is a run with both engines where Searchlight wins some targets and loses
// others.
func sampleRun() *Run {
	t0 := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	r := &Run{
		Schema: SchemaVersion, Label: "sample", StartedAt: t0, FinishedAt: t0.Add(42 * time.Minute),
		Env: Environment{OS: "linux", Arch: "amd64", Kernel: "6.8.0", CPU: "Test CPU @ 3.0GHz", CPUs: 4, MemTotal: 16 << 30, GoVersion: "go1.25.6", CI: "GitHub Actions"},
		Engines: []EngineInfo{
			{Name: Searchlight, URL: "http://127.0.0.1:8780", Version: "bench", Config: map[string]string{"store": "SQLite"}},
			{Name: Elasticsearch, URL: "http://127.0.0.1:9200", Version: "8.15.3", Config: map[string]string{"heap_max": "4.0 GiB"}},
		},
		Dataset:    Dataset{Docs: 1_000_000, Seed: 1, File: "products-1m.ndjson", FileBytes: 870 << 20, Fields: 18, SearchSets: []int{1000, 10_000, 100_000}, Shards: 1, AvgDocBytes: 912},
		Options:    Options{Warmup: 100, Iterations: 1000, Concurrency: 1, Variants: 64, BulkBatch: 1000, BulkConcurrency: 4, PercolateBatch: 100, PercolateConcurrency: 4, PageDepth: 10_000, MixedSeconds: 30},
		CrossCheck: CrossCheck{Ran: true, Checked: 120, Matched: 119, Tolerated: 1, Mismatches: []Mismatch{{Name: "agg_terms#2", Request: `{"size":0}`, Problems: []string{"aggs.brands bucket 9: a=10 vs b=10"}, Tolerated: true}}},
		Caveats:    es.Caveats[:2],
		Notes:      []string{"A note."},
	}
	add := func(rs ...Result) { r.Results = append(r.Results, rs...) }
	add(res("bulk_index", GroupIndexing, Searchlight, 50, 50_000, lat(80_000, 200_000)), res("bulk_index", GroupIndexing, Elasticsearch, 30, 30_000, lat(130_000, 300_000)))
	add(res("filter_term", GroupFilter, Searchlight, 2000, 2000, lat(400, 900)), res("filter_term", GroupFilter, Elasticsearch, 1000, 1000, lat(800, 2000)))
	add(res("filter_contains", GroupFilter, Searchlight, 900, 900, lat(1100, 3000)), res("filter_contains", GroupFilter, Elasticsearch, 1000, 1000, lat(1000, 2500)))
	add(res("sorted_price_top100", GroupSorted, Searchlight, 500, 500, lat(1500, 4000)), res("sorted_price_top100", GroupSorted, Elasticsearch, 300, 300, lat(2500, 6000)))
	add(res("agg_terms", GroupAggs, Searchlight, 300, 300, lat(3000, 6000)), res("agg_terms", GroupAggs, Elasticsearch, 200, 200, lat(5000, 9000)))
	add(res("refresh_visible", GroupVisibility, Searchlight, 1, 1, lat(600_000, 1_050_000)), res("refresh_wait_for", GroupVisibility, Searchlight, 1, 1, lat(500_000, 1_000_000)))
	add(res("refresh_visible", GroupVisibility, Elasticsearch, 1, 1, lat(550_000, 1_020_000)), res("refresh_wait_for", GroupVisibility, Elasticsearch, 1, 1, lat(500_000, 990_000)))
	for _, n := range []string{"10000", "100000"} {
		add(res("percolate_batch_"+n, GroupPercolate, Searchlight, 400, 40_000, lat(10_000, 20_000)), res("percolate_batch_"+n, GroupPercolate, Elasticsearch, 20, 2000, lat(200_000, 400_000)))
		add(res("percolate_single_"+n, GroupPercolate, Searchlight, 4000, 4000, lat(250, 700)), res("percolate_single_"+n, GroupPercolate, Elasticsearch, 100, 100, lat(9000, 20_000)))
	}
	add(res("mixed_read", GroupMixed, Searchlight, 3000, 3000, lat(1000, 4000)), res("mixed_read", GroupMixed, Elasticsearch, 2000, 2000, lat(1500, 6000)))
	add(Result{Workload: "footprint", Group: GroupFootprint, Engine: Searchlight, Ops: 1, Description: "du", Values: map[string]float64{"disk_bytes": 900 << 20, "disk_per_million": 900 << 20, "rss_bytes": 700 << 20, "rss_per_million": 700 << 20}})
	add(Result{Workload: "footprint", Group: GroupFootprint, Engine: Elasticsearch, Ops: 1, Description: "_stats", Values: map[string]float64{"disk_bytes": 1200 << 20, "disk_per_million": 1200 << 20, "rss_bytes": 600 << 20, "rss_per_million": 600 << 20}})
	return r
}

func byID(checks []TargetCheck) map[string]TargetCheck {
	m := map[string]TargetCheck{}
	for i := range checks {
		m[checks[i].ID] = checks[i]
	}
	return m
}

func TestEvaluateTargets(t *testing.T) {
	got := byID(Evaluate(sampleRun()))
	want := map[string]string{
		"T1":  Pass,        // 50k vs 30k docs/s
		"T2":  Fail,        // filter_contains is behind
		"T3":  Pass,        // sorted ahead
		"T4":  Pass,        // aggs ahead
		"T5":  Pass,        // 20× throughput, p99 700 µs at 100k
		"T6":  Fail,        // RSS above Elasticsearch's
		"T7":  Pass,        // p99 1.05 s
		"T8":  NotMeasured, // cluster
		"T9":  NotMeasured, // no restart command
		"T10": NotMeasured,
		"T11": NotMeasured,
	}
	for id, status := range want {
		if got[id].Status != status {
			t.Errorf("%s: %s (%s), want %s", id, got[id].Status, got[id].Detail, status)
		}
	}
	if !strings.Contains(got["T2"].Detail, "filter_contains") {
		t.Errorf("T2 does not name the losing workload: %s", got["T2"].Detail)
	}
}

func TestEvaluateWithoutBaselineOrWithMismatch(t *testing.T) {
	r := sampleRun()
	var sl []Result
	for _, res := range r.Results {
		if res.Engine == Searchlight {
			sl = append(sl, res)
		}
	}
	r.Results, r.Engines = sl, r.Engines[:1]
	got := byID(Evaluate(r))
	for _, id := range []string{"T1", "T2", "T3", "T4", "T6"} {
		if got[id].Status != NoBaseline {
			t.Errorf("%s without Elasticsearch: %s, want %s", id, got[id].Status, NoBaseline)
		}
	}
	if got["T7"].Status != Pass {
		t.Errorf("T7 is absolute and should still pass: %s", got["T7"].Status)
	}
	// T2's own under-sampled p99 renders as "n/a (n=...)" via formatQuantile, the
	// same as a single workload's table row -- cosmetic consistency with T5 and T7,
	// not just a status: a NO BASELINE target still shouldn't print a number it
	// can't back up.
	for i := range r.Results {
		if r.Results[i].Workload == "filter_term" {
			r.Results[i].Latency.Count = 300
		}
	}
	if sl := byID(Evaluate(r))["T2"].Searchlight; !strings.Contains(sl, "n/a (n=300)") {
		t.Errorf("T2 without a baseline and an under-sampled workload: Searchlight=%q, want it to contain %q", sl, "n/a (n=300)")
	}

	r = sampleRun()
	r.CrossCheck.Mismatches = append(r.CrossCheck.Mismatches, Mismatch{Name: "x", Problems: []string{"total 1 vs 2"}})
	got = byID(Evaluate(r))
	if got["T1"].Status != Invalid || got["T2"].Status != Invalid || got["T7"].Status != Pass {
		t.Errorf("after a mismatch: T1 %s, T2 %s, T7 %s", got["T1"].Status, got["T2"].Status, got["T7"].Status)
	}

	r = sampleRun()
	for i := range r.Results {
		if r.Results[i].Workload == "percolate_single_100000" && r.Results[i].Engine == Searchlight {
			r.Results[i].Latency.P99 = 1500
		}
	}
	if s := byID(Evaluate(r))["T5"].Status; s != Fail {
		t.Errorf("a 1.5 ms p99 at 100k: T5 %s, want FAIL", s)
	}
}

// TestEvaluateInsufficientSamples checks that a target gated on a percentile with too
// few recorded values never reports PASS or FAIL: it reports INSUFFICIENT SAMPLES
// instead, and that status is never shown as a win or a loss.
func TestEvaluateInsufficientSamples(t *testing.T) {
	// agg_terms is T4's only workload in sampleRun, so an under-sampled p99 there
	// cannot be outvoted by another, sufficiently-sampled workload failing instead.
	r := sampleRun()
	for i := range r.Results {
		if r.Results[i].Workload == "agg_terms" {
			r.Results[i].Latency.Count = 300 // below minSamples(0.99) = 1000
		}
	}
	got := byID(Evaluate(r))
	if got["T4"].Status != InsufficientSamples {
		t.Errorf("T4 with an under-sampled p99: %s, want %s", got["T4"].Status, InsufficientSamples)
	}
	if !strings.Contains(got["T4"].Detail, "agg_terms") || !strings.Contains(got["T4"].Detail, "insufficient samples") {
		t.Errorf("T4's detail does not explain the shortfall: %s", got["T4"].Detail)
	}

	r = sampleRun()
	for i := range r.Results {
		if r.Results[i].Workload == "refresh_visible" && r.Results[i].Engine == Searchlight {
			r.Results[i].Latency.Count = 300
		}
	}
	got = byID(Evaluate(r))
	if got["T7"].Status != InsufficientSamples {
		t.Errorf("T7 with an under-sampled refresh_visible p99: %s, want %s", got["T7"].Status, InsufficientSamples)
	}

	r = sampleRun()
	for i := range r.Results {
		if r.Results[i].Workload == "percolate_single_100000" && r.Results[i].Engine == Searchlight {
			r.Results[i].Latency.Count = 300
			r.Results[i].Latency.P99 = 1500 // would otherwise fail the < 1 ms target
		}
	}
	if s := byID(Evaluate(r))["T5"].Status; s != InsufficientSamples {
		t.Errorf("T5 with an under-sampled percolate_single_100000 p99: %s, want %s", s, InsufficientSamples)
	}
}

func TestMinSamplesAndFormatQuantile(t *testing.T) {
	for q, want := range map[float64]int64{0.50: 20, 0.90: 100, 0.99: 1000, 0.999: 10000} {
		if got := minSamples(q); got != want {
			t.Errorf("minSamples(%v) = %d, want %d", q, got, want)
		}
	}
	if got := formatQuantile(900, 1000, 0.99); got != "900 µs" {
		t.Errorf("sufficient: %q", got)
	}
	if got := formatQuantile(900, 300, 0.99); got != "n/a (n=300)" {
		t.Errorf("insufficient: %q", got)
	}
}

func TestRenderGolden(t *testing.T) {
	var b bytes.Buffer
	if err := Render(&b, sampleRun()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("testdata", "report.md")
	if *update {
		if err := os.WriteFile(path, b.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run go test ./bench/report -update)", err)
	}
	if !bytes.Equal(bytes.ReplaceAll(want, []byte("\r\n"), []byte("\n")), b.Bytes()) {
		t.Errorf("the rendered report differs from testdata/report.md:\n%s", b.String())
	}
	for _, s := range []string{"## Spec section 1 targets", "| T5 |", "**PASS**", "**FAIL**", "## Correctness cross-check", "## Translation caveats", "p99.9"} {
		if !strings.Contains(b.String(), s) {
			t.Errorf("the report lacks %q", s)
		}
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	r := sampleRun()
	r.Targets = Evaluate(r)
	path := filepath.Join(t.TempDir(), "run.json")
	if err := r.Save(path); err != nil {
		t.Fatal(err)
	}
	back, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(back.Results) != len(r.Results) || back.Find("filter_term", Searchlight).Latency.P99 != 900 || len(back.Targets) != 11 {
		t.Fatalf("round trip lost data: %+v", back.Find("filter_term", Searchlight))
	}
	if err := os.WriteFile(path, []byte(`{"schema": 99}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("a future schema was accepted")
	}
}

func TestFormatters(t *testing.T) {
	for in, want := range map[float64]string{0: "0", 0.5: "0.5 µs", 12.34: "12.3 µs", 999: "999 µs", 1500: "1.5 ms", 25_000: "25 ms", 2_500_000: "2.5 s"} {
		if got := FormatMicros(in); got != want {
			t.Errorf("FormatMicros(%v) = %q, want %q", in, got, want)
		}
	}
	if got := FormatBytes(1536); got != "1.5 KiB" {
		t.Errorf("FormatBytes(1536) = %q", got)
	}
	if got := FormatRate(12_345); got != "12.3k" {
		t.Errorf("FormatRate(12345) = %q", got)
	}
	if got := cell("a|b\nc"); got != `a\|b c` {
		t.Errorf("cell = %q", got)
	}
}

// TestRecoveryAndRestartTargets: T8 is measured by the recovery workload (against
// Elasticsearch's when it ran, else without a baseline, saying when the dataset is
// below the target's 10M documents), and T9 by the restart workload's slowest restart.
func TestRecoveryAndRestartTargets(t *testing.T) {
	r := sampleRun()
	recovery := res("recovery", GroupRecovery, Searchlight, 0.1, 0.1, lat(8e6, 9e6))
	recovery.Values = map[string]float64{"docs": 1e6}
	restart := res("restart", GroupRestart, Searchlight, 0.5, 0.5, lat(1e6, 2e6))
	r.Results = append(r.Results, recovery, restart)
	got := byID(Evaluate(r))
	if c := got["T8"]; c.Status != NoBaseline || !strings.Contains(c.Detail, "1000000 documents") {
		t.Errorf("T8 with Searchlight only: %s (%s)", c.Status, c.Detail)
	}
	if c := got["T9"]; c.Status != Pass {
		t.Errorf("T9: %s (%s)", c.Status, c.Detail)
	}

	r.Results = append(r.Results, res("recovery", GroupRecovery, Elasticsearch, 0.2, 0.2, lat(4e6, 5e6)))
	if c := byID(Evaluate(r))["T8"]; c.Status != Fail {
		t.Errorf("T8 slower than Elasticsearch: %s (%s)", c.Status, c.Detail)
	}
	r.Results[len(r.Results)-1].Latency = lat(10e6, 20e6)
	if c := byID(Evaluate(r))["T8"]; c.Status != Pass {
		t.Errorf("T8 within Elasticsearch: %s (%s)", c.Status, c.Detail)
	}

	r.Results[len(r.Results)-2].Latency = lat(6e6, 12e6)
	if c := byID(Evaluate(r))["T9"]; c.Status != Fail {
		t.Errorf("T9 with a 24 s restart: %s (%s)", c.Status, c.Detail)
	}
}
