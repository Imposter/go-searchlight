package report

import (
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strings"
	"time"
)

// FormatMicros writes a duration given in microseconds with three significant digits.
func FormatMicros(us float64) string {
	switch {
	case us <= 0:
		return "0"
	case us < 1000:
		return sig(us) + " µs"
	case us < 1e6:
		return sig(us/1e3) + " ms"
	default:
		return sig(us/1e6) + " s"
	}
}

// sig formats v with three significant digits, without trailing zeros.
func sig(v float64) string {
	switch {
	case v >= 100:
		return fmt.Sprintf("%.0f", v)
	case v >= 10:
		return strings.TrimSuffix(fmt.Sprintf("%.1f", v), ".0")
	default:
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", v), "0"), ".")
	}
}

// FormatRate writes a per-second rate compactly (12.3k).
func FormatRate(v float64) string {
	switch {
	case v >= 1e6:
		return sig(v/1e6) + "M"
	case v >= 1e4:
		return sig(v/1e3) + "k"
	default:
		return sig(v)
	}
}

// minSamples is the fewest recorded values needed to trust quantile q with reasonable
// confidence, roughly 1/(1-q)×10: p50 needs 20, p90 needs 100, p99 needs 1,000, p999
// needs 10,000. Below it, the quantile is noise, not a measurement.
func minSamples(q float64) int64 {
	if q >= 1 {
		return math.MaxInt64
	}
	// A small epsilon absorbs float64 rounding (1-0.9 is 0.09999999999999998, not
	// 0.1), so a q meant to land on a whole number does, rather than ceiling up one.
	return int64(math.Ceil(10/(1-q) - 1e-9))
}

// sufficient reports whether l has enough samples to trust its p99 — the quantile
// every spec section 1 target gated on latency compares.
func sufficient(l *Latency) bool {
	return l != nil && l.Count >= minSamples(0.99)
}

// formatQuantile formats a quantile value, or "n/a (n=…)" when count is too few
// samples to trust it (see minSamples): an under-sampled percentile is never shown as
// a number, so it cannot be mistaken for one.
func formatQuantile(value float64, count int64, q float64) string {
	if count < minSamples(q) {
		return fmt.Sprintf("n/a (n=%d)", count)
	}
	return FormatMicros(value)
}

// FormatBytes writes a byte count in binary units.
func FormatBytes(v float64) string {
	if v <= 0 {
		return "—"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	i := 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	return sig(v) + " " + units[i]
}

// cell escapes a markdown table cell.
func cell(s string) string {
	return strings.NewReplacer("|", `\|`, "\n", " ").Replace(s)
}

func table(w io.Writer, header []string, rows [][]string) {
	fmt.Fprintf(w, "| %s |\n", strings.Join(header, " | "))
	seps := make([]string, len(header))
	for i := range seps {
		seps[i] = "---"
	}
	fmt.Fprintf(w, "| %s |\n", strings.Join(seps, " | "))
	for _, r := range rows {
		cs := make([]string, len(r))
		for i, c := range r {
			cs[i] = cell(c)
		}
		fmt.Fprintf(w, "| %s |\n", strings.Join(cs, " | "))
	}
	fmt.Fprintln(w)
}

// groupOrder is the order the report presents groups in, with their titles.
var groupOrder = []struct{ group, title, about string }{
	{GroupIndexing, "Bulk indexing", "The whole dataset loaded through `_bulk` at a fixed concurrency; each batch is acknowledged once durable. Latency is per batch."},
	{GroupVisibility, "Refresh to searchable", "Each measured write first sleeps a random, unmeasured delay uniform on [0, refresh interval) — seeded and identical for both engines at the same iteration — so writes land at a random point in the refresh cycle instead of, as a closed loop otherwise would, right after the previous one's own refresh. `refresh_visible`: one document written without refresh, then polled (every 10 ms) until a search finds it; latency runs from the write's start (after its delay). `refresh_wait_for`: the write's own latency with `refresh=wait_for` (also after its delay), then an immediate search must find it."},
	{GroupFilter, "Filter and boolean search", "Each workload cycles through query variants with Zipfian values; size 10, bodies returned."},
	{GroupSorted, "Sorted and paged search", "Top-k with a sort, and `search_after` walks to the stated depth (latency per page)."},
	{GroupAggs, "Aggregations", "Size 0; Elasticsearch's request cache is off; filters vary per iteration."},
	{GroupPercolate, "Percolation", "Saved-search sets are prefixes of one generated stream. `percolate_batch_N`: batches of documents at a fixed concurrency (docs/s, latency per batch). `percolate_single_N`: one document per request (per-document latency: the p99 cell is \"end-to-end [server p99]\" when the engine reports its own server time -- analyze, match and encode, never the network or the client's decode, like Elasticsearch's own `took` -- and target T5's < 1 ms clause is judged on that server p99, not the end-to-end number; \"(no server timing)\" means an engine has not reported one yet, so end-to-end is all there is to judge). `bulk_percolate`: Searchlight's `_bulk?percolate=true` against Elasticsearch's `_bulk` then percolate."},
	{GroupMixed, "Concurrent mixed read/write", "Readers cycle through filter, sorted and aggregation searches while writers update existing documents in bulk, for a fixed time."},
	{GroupRestart, "Restart to serving", "The engine stopped gracefully and started again (Searchlight: the node slbench runs; Elasticsearch: the operator's restart command), timed until it answers a count with the full total."},
	{GroupRecovery, "New replica from zero to serving", "Searchlight: a node with an empty data directory joins a cluster of its own that already holds the dataset, timed from its start until its own copies serve every document (peer recovery, then the changelog replayed). Elasticsearch: on the same index and data the rest of the run already loaded, the second node's replica is dropped and restored (number_of_replicas 0 then 1), timed until the cluster is green again. The two are not quite the same clock: Elasticsearch's timing starts on an already-running second node (recovery alone), while Searchlight's includes a fresh process start, so Elasticsearch's figure is a lower bound on a true apples-to-apples comparison."},
}

// Render writes the run as markdown.
func Render(w io.Writer, r *Run) error {
	if len(r.Targets) == 0 {
		r.Targets = Evaluate(r)
	}
	title := "Searchlight vs Elasticsearch benchmark"
	if r.Label != "" {
		title += ": " + r.Label
	}
	fmt.Fprintf(w, "# %s\n\n", title)
	fmt.Fprintf(w, "Generated by `slbench report` from a run started %s and finished %s (%s). ",
		r.StartedAt.UTC().Format(time.RFC3339), r.FinishedAt.UTC().Format(time.RFC3339), r.FinishedAt.Sub(r.StartedAt).Round(time.Second))
	fmt.Fprintf(w, "The JSON artifact of the same run holds every number below; compare runs with it, not with this page.\n\n")
	if !r.Has(Elasticsearch) {
		fmt.Fprintf(w, "> **Searchlight only.** Elasticsearch was not part of this run, so comparative targets read %s. It proves the harness, not the claim.\n\n", NoBaseline)
	}
	if r.CrossCheck.Failed() {
		fmt.Fprintf(w, "> **The cross-check failed:** the engines returned different answers for some requests, so comparative results are %s until that is fixed.\n\n", Invalid)
	}

	fmt.Fprintf(w, "## Spec section 1 targets\n\n")
	rows := make([][]string, 0, len(r.Targets))
	for i := range r.Targets {
		t := &r.Targets[i]
		rows = append(rows, []string{t.ID, t.Area, t.Target, t.Searchlight, t.Elasticsearch, "**" + t.Status + "**", t.Detail})
	}
	table(w, []string{"#", "Area", "Target", "Searchlight", "Elasticsearch", "Status", "Detail"}, rows)

	renderEnvironment(w, r)
	renderEngines(w, r)
	renderDataset(w, r)
	renderMethod(w, r)
	renderCrossCheck(w, r)
	renderFootprint(w, r)
	for _, g := range groupOrder {
		renderGroup(w, r, g.group, g.title, g.about)
	}
	renderCaveats(w, r)
	if len(r.Notes) > 0 {
		fmt.Fprintf(w, "## Notes\n\n")
		for _, n := range r.Notes {
			fmt.Fprintf(w, "- %s\n", n)
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintf(w, "## Reproduce\n\n")
	fmt.Fprintf(w, "On a Linux host with Docker: `make build`, `go run ./bench/cmd/slbench gen`, start Elasticsearch with `docker compose -f bench/docker-compose.es.yml up -d`, then `slbench run --sl-bin bin/searchlight` (it runs the binary itself; `--recovery-store-url` adds the recovery workload on Postgres) and `slbench report` (`slbench help` lists every flag). `.github/workflows/bench.yml` runs exactly these steps.\n")
	return nil
}

func renderEnvironment(w io.Writer, r *Run) {
	e := r.Env
	fmt.Fprintf(w, "## Environment\n\n")
	rows := [][]string{
		{"OS / arch", e.OS + " / " + e.Arch},
		{"Kernel", orDash(e.Kernel)},
		{"CPU", fmt.Sprintf("%s (%d logical CPUs)", orDash(e.CPU), e.CPUs)},
		{"Memory", FormatBytes(float64(e.MemTotal))},
		{"Go", e.GoVersion},
	}
	if e.CI != "" {
		rows = append(rows, []string{"CI", e.CI})
	}
	if e.Commit != "" {
		rows = append(rows, []string{"Commit", e.Commit})
	}
	table(w, []string{"", ""}, rows)
	fmt.Fprintf(w, "Both engines ran on this machine, one workload at a time (never both engines at once).\n\n")
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

func renderEngines(w io.Writer, r *Run) {
	fmt.Fprintf(w, "## Engines\n\n")
	for _, e := range r.Engines {
		fmt.Fprintf(w, "**%s** %s at `%s`\n\n", e.Name, orDash(e.Version), e.URL)
		keys := make([]string, 0, len(e.Config))
		for k := range e.Config {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		rows := make([][]string, 0, len(keys))
		for _, k := range keys {
			rows = append(rows, []string{k, e.Config[k]})
		}
		if len(rows) > 0 {
			table(w, []string{"setting", "value"}, rows)
		}
	}
}

func renderDataset(w io.Writer, r *Run) {
	d := r.Dataset
	fmt.Fprintf(w, "## Dataset\n\n")
	sets := make([]string, len(d.SearchSets))
	for i, n := range d.SearchSets {
		sets[i] = shortCount(n)
	}
	table(w, []string{"", ""}, [][]string{
		{"Documents", fmt.Sprintf("%d scrape-bot-shaped product listings, %d fields", d.Docs, d.Fields)},
		{"Seed", fmt.Sprint(d.Seed)},
		{"File", fmt.Sprintf("`%s` (%s, %.0f bytes per document)", d.File, FormatBytes(float64(d.FileBytes)), d.AvgDocBytes)},
		{"Saved-search sets", orDash(strings.Join(sets, ", "))},
		{"Shards", fmt.Sprint(d.Shards)},
	})
	fmt.Fprintf(w, "Fields: title and description (text, about 60 Zipfian words), brand (2,000 values), category (300), tags (keyword list from 1,000, 1–6 per listing), source (20), condition, sku, url, price (log-normal), list_price, rating, reviews, stock (numbers), in_stock and on_sale (bools), first_seen and updated_at (dates). Values are Zipfian; optional fields are missing from some listings.\n\n")
}

func renderMethod(w io.Writer, r *Run) {
	o := r.Options
	fmt.Fprintf(w, "## Method\n\n")
	loop := fmt.Sprintf("closed loop at concurrency %d", o.Concurrency)
	if o.Rate > 0 {
		loop = fmt.Sprintf("open loop at %.0f requests/s (latency from each request's scheduled start, so queueing is charged)", o.Rate)
	}
	fmt.Fprintf(w, "- Each search workload runs %d warmup then %d measured requests, %s, cycling through %d query variants. Latencies are client-side wall time per request, recorded in an HDR-style histogram (0.1%% precision).\n", o.Warmup, o.Iterations, loop, o.Variants)
	if o.Repeats > 1 {
		fmt.Fprintf(w, "- Filter, sorted/paging, aggregation and percolate-single workloads each repeat %d times (independent warmup and measured requests every time), merged into one histogram; a table's \"p99 (± run-to-run)\" is the coefficient of variation across the repeats' own p99s, not a confidence interval.\n", o.Repeats)
	}
	fmt.Fprintf(w, "- Bulk loads send %d documents per request at concurrency %d. Percolation batches hold %d documents at concurrency %d. Deep paging walks to depth %d.\n", o.BulkBatch, o.BulkConcurrency, o.PercolateBatch, o.PercolateConcurrency, o.PageDepth)
	if o.MixedSeconds > 0 {
		fmt.Fprintf(w, "- The mixed workload runs for %.0f s.\n", o.MixedSeconds)
	}
	fmt.Fprintf(w, "- Every query is written once in Searchlight's DSL and translated to Elasticsearch's by `bench/es` (see the caveats); both engines receive the same sequence.\n\n")
}

func renderCrossCheck(w io.Writer, r *Run) {
	c := r.CrossCheck
	fmt.Fprintf(w, "## Correctness cross-check\n\n")
	switch {
	case !c.Ran:
		fmt.Fprintf(w, "Not run: %s\n\n", orDash(c.Skipped))
		return
	case c.Failed():
		fmt.Fprintf(w, "**FAILED.** ")
	default:
		fmt.Fprintf(w, "**Passed.** ")
	}
	fmt.Fprintf(w, "%d requests compared (hit ids in order, totals, aggregations, percolation matches): %d identical, %d different within a documented caveat, %d different.\n\n",
		c.Checked, c.Matched, c.Tolerated, c.Checked-c.Matched-c.Tolerated)
	if len(c.Mismatches) > 0 {
		rows := make([][]string, 0, len(c.Mismatches))
		for _, m := range c.Mismatches {
			kind := "mismatch"
			if m.Tolerated {
				kind = "tolerated"
			}
			probs := m.Problems
			if len(probs) > 5 {
				probs = append(slices.Clone(probs[:5]), fmt.Sprintf("… %d more", len(m.Problems)-5))
			}
			req := m.Request
			if len(req) > 300 {
				req = req[:300] + "…"
			}
			rows = append(rows, []string{m.Name, kind, "`" + req + "`", strings.Join(probs, "; ")})
		}
		table(w, []string{"request", "kind", "body", "problems"}, rows)
	}
}

func renderFootprint(w io.Writer, r *Run) {
	var rows [][]string
	for _, eng := range []string{Searchlight, Elasticsearch} {
		res := r.Find("footprint", eng)
		if res == nil {
			continue
		}
		rows = append(rows, []string{
			eng,
			FormatBytes(res.Values["disk_bytes"]), FormatBytes(res.Values["disk_per_million"]),
			FormatBytes(res.Values["rss_bytes"]), FormatBytes(res.Values["rss_per_million"]),
			res.Description,
		})
	}
	if len(rows) == 0 {
		return
	}
	fmt.Fprintf(w, "## Disk and memory\n\nThe running maximum of samples taken every 1.5 s across the whole run, so a mid-run spike (a merge, the percolator load, the mixed workload) is not missed by measuring only right after the load. "+
		"\"Disk\" is the node-local index alone (target T6, ruled 2026-10-05); RSS is read the same way for both engines where possible (the source column says how for each row, so the methods can be checked against each other).\n\n")
	table(w, []string{"engine", "disk", "disk per 1M docs", "RSS", "RSS per 1M docs", "how"}, rows)
	renderStore(w, r)
	renderSectionBreakdown(w, r)
}

// renderStore renders the durable store's size informationally (T6 ruling,
// 2026-10-05): shared by every replica, not a per-node cost, so never part of the
// disk column above or of T6's pass/fail.
func renderStore(w io.Writer, r *Run) {
	var rows [][]string
	for _, eng := range []string{Searchlight, Elasticsearch} {
		res := r.Find("footprint", eng)
		if res == nil || res.Values["store_bytes"] <= 0 {
			continue
		}
		rows = append(rows, []string{eng, FormatBytes(res.Values["store_bytes"]), FormatBytes(res.Values["store_per_million"])})
	}
	if len(rows) == 0 {
		return
	}
	fmt.Fprintf(w, "Durable store, informational only (not part of T6, and not the disk column above): the SQL store every replica shares, not a per-node cost. See issue #52 for shrinking it.\n\n")
	table(w, []string{"engine", "store", "store per 1M docs"}, rows)
}

// sectionBreakdownOrder is the section-breakdown column order (largest, typically,
// to smallest in a real segment): Searchlight's term dictionary and postings
// together, doc values, points, presence bitmaps, stored fields, the id dictionary
// and the field directory meta, and anything else either engine reports that this
// list does not name.
var sectionBreakdownOrder = []string{"terms", "docvalues", "points", "presence", "stored", "ids", "meta", "other"}

// renderSectionBreakdown renders target T6's per-section disk breakdown (bytes per
// Searchlight segment section, next to Elasticsearch's _disk_usage API), from the
// "footprint" result's "section_<name>_bytes" values, measured right after the
// load (not sampled at the run's peak, unlike the totals table above it).
func renderSectionBreakdown(w io.Writer, r *Run) {
	type row struct {
		engine string
		sizes  map[string]int64
	}
	var byEngine []row
	names := map[string]bool{}
	for _, eng := range []string{Searchlight, Elasticsearch} {
		res := r.Find("footprint", eng)
		if res == nil {
			continue
		}
		sizes := map[string]int64{}
		for k, v := range res.Values {
			if name, ok := strings.CutPrefix(k, "section_"); ok {
				name = strings.TrimSuffix(name, "_bytes")
				sizes[name] = int64(v)
				names[name] = true
			}
		}
		if len(sizes) > 0 {
			byEngine = append(byEngine, row{eng, sizes})
		}
	}
	if len(byEngine) == 0 {
		return
	}
	cols := make([]string, 0, len(sectionBreakdownOrder))
	for _, n := range sectionBreakdownOrder {
		if names[n] {
			cols = append(cols, n)
			delete(names, n)
		}
	}
	extra := make([]string, 0, len(names))
	for n := range names {
		extra = append(extra, n)
	}
	sort.Strings(extra)
	cols = append(cols, extra...)
	header := append([]string{"engine"}, cols...)
	rows := make([][]string, len(byEngine))
	for i, eng := range byEngine {
		row := make([]string, len(cols)+1)
		row[0] = eng.engine
		for j, c := range cols {
			row[j+1] = FormatBytes(float64(eng.sizes[c]))
		}
		rows[i] = row
	}
	fmt.Fprintf(w, "Disk by section, right after the load (not the peak above): Searchlight's segment sections (`internal/segment`, spec section 6), Elasticsearch's `_disk_usage` API aggregated to sit beside them (its norms, term vectors and knn vectors fold into \"other\"; it has no analog of \"ids\" or \"meta\", which live inside its inverted index and stored fields already).\n\n")
	table(w, header, rows)
}

func renderGroup(w io.Writer, r *Run, group, title, about string) {
	names := r.workloadsIn(group)
	if len(names) == 0 {
		return
	}
	fmt.Fprintf(w, "## %s\n\n%s\n\n", title, about)
	rows := make([][]string, 0, 2*len(names))
	for _, name := range names {
		s, e := r.Find(name, Searchlight), r.Find(name, Elasticsearch)
		for _, res := range []*Result{s, e} {
			if res == nil {
				continue
			}
			row := []string{name, res.Engine, conc(res), FormatRate(res.OpsPerSec), docsRate(res)}
			if l := res.Latency; l != nil {
				p99 := formatQuantile(l.P99, l.Count, 0.99)
				if cv := res.Values["p99_cv_pct"]; cv > 0 {
					p99 = fmt.Sprintf("%s (±%.1f%%)", p99, cv)
				}
				if sc := res.Values["server_count"]; sc > 0 {
					p99 = fmt.Sprintf("%s [server %s]", p99, formatQuantile(res.Values["server_p99_us"], int64(sc), 0.99))
				}
				row = append(row, formatQuantile(l.P50, l.Count, 0.50), p99, formatQuantile(l.P999, l.Count, 0.999), FormatMicros(l.Max))
			} else {
				row = append(row, "—", "—", "—", "—")
			}
			errs := fmt.Sprint(res.Errors)
			if res.Error != "" {
				errs += " (" + res.Error + ")"
			}
			row = append(row, errs, ratio(res, s, e))
			rows = append(rows, row)
		}
	}
	table(w, []string{"workload", "engine", "load", "ops/s", "docs/s", "p50", "p99 (± run-to-run)", "p99.9", "max", "errors", "SL vs ES"}, rows)
}

func conc(res *Result) string {
	if res.Rate > 0 {
		return fmt.Sprintf("%.0f/s", res.Rate)
	}
	if res.Concurrency > 0 {
		return fmt.Sprintf("c=%d", res.Concurrency)
	}
	return "—"
}

func docsRate(res *Result) string {
	if res.DocsPerSec <= 0 || res.Docs == res.Ops {
		return "—"
	}
	return FormatRate(res.DocsPerSec)
}

// ratio is shown on the Searchlight row: its p50 latency over Elasticsearch's (lower
// is better) or, without latency, its rate over Elasticsearch's (higher is better).
func ratio(res, s, e *Result) string {
	if res != s || s == nil || e == nil || !s.OK() || !e.OK() {
		return ""
	}
	if s.Latency != nil && e.Latency != nil && e.Latency.P50 > 0 && s.Group != GroupIndexing && s.Group != GroupPercolate {
		return fmt.Sprintf("p50 %.2f×", s.Latency.P50/e.Latency.P50)
	}
	if es := perSec(e); es > 0 {
		v := perSec(s) / es
		if !math.IsInf(v, 0) {
			return fmt.Sprintf("%.2f× rate", v)
		}
	}
	return ""
}

func renderCaveats(w io.Writer, r *Run) {
	if len(r.Caveats) == 0 {
		return
	}
	fmt.Fprintf(w, "## Translation caveats\n\nEvery place where the engines' semantics differ, or where the Elasticsearch side had a choice of structure, and what the benchmark does about it.\n\n")
	rows := make([][]string, 0, len(r.Caveats))
	for _, c := range r.Caveats {
		rows = append(rows, []string{c.Area, c.Difference, c.Choice, c.Benchmark})
	}
	table(w, []string{"area", "difference", "choice", "effect on the benchmark"}, rows)
}
