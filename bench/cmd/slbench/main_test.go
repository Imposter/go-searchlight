package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Imposter/go-searchlight/bench/report"
	"github.com/Imposter/go-searchlight/internal/slproc"
	"github.com/Imposter/go-searchlight/internal/store/postgres"
)

func slbench(t *testing.T, stdin string, args ...string) (int, string) {
	t.Helper()
	var out, errs bytes.Buffer
	code := run(context.Background(), args, strings.NewReader(stdin), &out, &errs)
	return code, out.String() + errs.String()
}

// TestEndToEndBinary generates a tiny dataset, builds the searchlight binary, runs
// every workload against it as its own process (restarts included), and renders the
// report: the harness works end to end. With SEARCHLIGHT_TEST_PG_URL set, the recovery
// workload runs too, on a schema of its own.
func TestEndToEndBinary(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary and runs every workload")
	}
	dir := t.TempDir()
	bin, err := slproc.Build(t.Context(), dir, "bench-test")
	if err != nil {
		t.Fatal(err)
	}
	if code, out := slbench(t, "", "gen", "-out", dir, "-docs", "1500", "-searches", "60,120", "-workers", "2"); code != 0 {
		t.Fatalf("gen: %d\n%s", code, out)
	}
	data := ProductsFile(dir, 1500)
	results := filepath.Join(dir, "results.json")
	md := filepath.Join(dir, "report.md")
	args := []string{
		"run", "-data", data, "-searches", SearchesFile(dir, 120), "-sets", "60,120",
		"-sl-bin", bin, "-sl-dir", filepath.Join(dir, "sl"), "-warmup", "2", "-iterations", "5", "-variants", "3",
		"-page-depth", "300", "-page-walks", "1", "-bulk-batch", "500", "-percolate-batch", "20", "-percolate-iterations", "2",
		"-percolate-single", "3", "-bulk-percolate-iterations", "1", "-visible-iterations", "1", "-mixed", "300ms",
		"-restart-iterations", "1", "-recovery-iterations", "1", "-shards", "2",
		"-out", results, "-md", md, "-label", "test",
	}
	recovery := false
	if base := os.Getenv("SEARCHLIGHT_TEST_PG_URL"); base != "" {
		args = append(args, "-recovery-store-url", pgSchema(t, base))
		recovery = true
	}
	code, out := slbench(t, "", args...)
	if code != 0 {
		t.Fatalf("run: %d\n%s", code, out)
	}
	r, err := report.Load(results)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"bulk_index", "footprint", "filter_term", "filter_contains", "sorted_price_top100", "paging_search_after_300",
		"agg_terms", "agg_cardinality", "refresh_visible", "refresh_wait_for", "mixed_read", "mixed_write",
		"percolate_batch_60", "percolate_single_120", "bulk_percolate", "restart",
	}
	if recovery {
		want = append(want, "recovery")
	}
	for _, w := range want {
		res := r.Find(w, report.Searchlight)
		if res == nil {
			t.Errorf("no %s result", w)
			continue
		}
		if w != "footprint" && !res.OK() {
			t.Errorf("%s: errors %d %s", w, res.Errors, res.Error)
		}
	}
	if r.Dataset.Docs != 1500 || len(r.Targets) != 11 {
		t.Errorf("dataset %+v, %d targets", r.Dataset, len(r.Targets))
	}
	if fp := r.Find("footprint", report.Searchlight); fp == nil || fp.Values["disk_bytes"] <= 0 {
		t.Errorf("footprint %+v", fp)
	}
	for _, tc := range r.Targets {
		measured := tc.ID == "T9" || (tc.ID == "T8" && recovery)
		if measured && (tc.Status == report.NotMeasured || tc.Status == report.Invalid) {
			t.Errorf("%s: %s (%s)", tc.ID, tc.Status, tc.Detail)
		}
	}
	b, err := os.ReadFile(md)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "Searchlight only") || !strings.Contains(string(b), "| T1 |") {
		t.Errorf("the report lacks its summary:\n%s", b)
	}

	// report re-renders the JSON.
	again := filepath.Join(dir, "again.md")
	if code, out := slbench(t, "", "report", "-in", results, "-out", again); code != 0 {
		t.Fatalf("report: %d\n%s", code, out)
	}
}

// pgSchema creates a schema for the test on the Postgres at base and returns a URL
// whose search_path is it.
func pgSchema(t *testing.T, base string) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := postgres.Config(u)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = admin.Close() })
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	schema := "sl_bench_" + hex.EncodeToString(b)
	if _, err := admin.ExecContext(t.Context(), "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

func TestTranslateCommand(t *testing.T) {
	code, out := slbench(t, `{"field": "title", "op": "contains", "value": "Drill"}`, "translate", "-query")
	if code != 0 || !strings.Contains(out, `"title.tri": "drill"`) {
		t.Fatalf("%d\n%s", code, out)
	}
	code, out = slbench(t, `{"query": {"field": "brand", "op": "eq", "value": "Acme"}, "sort": ["price"], "size": 5}`, "translate")
	if code != 0 || !strings.Contains(out, `"sl_id"`) {
		t.Fatalf("%d\n%s", code, out)
	}
	if code, out := slbench(t, "", "translate", "-mapping"); code != 0 || !strings.Contains(out, "percolator") {
		t.Fatalf("%d\n%s", code, out)
	}
}

func TestUsageAndErrors(t *testing.T) {
	if code, _ := slbench(t, ""); code != 2 {
		t.Errorf("no command: exit %d", code)
	}
	if code, _ := slbench(t, "", "bogus"); code != 2 {
		t.Errorf("unknown command: exit %d", code)
	}
	if code, out := slbench(t, "", "run", "-data", filepath.Join(t.TempDir(), "none.ndjson"), "-es-url", "http://127.0.0.1:1"); code != 1 {
		t.Errorf("a missing dataset: exit %d\n%s", code, out)
	}
	if got, err := parseInts("1k, 2m,30"); err != nil || len(got) != 3 || got[0] != 1000 || got[1] != 2_000_000 || got[2] != 30 {
		t.Errorf("parseInts: %v %v", got, err)
	}
	if countName(10_000_000) != "10m" || countName(50_000) != "50k" || countName(1500) != "1500" {
		t.Error("countName")
	}
}
