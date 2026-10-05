package report

import (
	"fmt"
	"slices"
	"strings"
)

// Target statuses.
const (
	Pass        = "PASS"
	Fail        = "FAIL"
	NoBaseline  = "NO BASELINE"  // Searchlight measured, Elasticsearch not (a smoke run)
	NotMeasured = "NOT MEASURED" // the run has no workload for it
	Invalid     = "INVALID"      // the engines disagree on answers, or a workload errored
	// InsufficientSamples marks a target whose only evidence is a percentile with too
	// few recorded values to trust (see minSamples): it is never reported PASS or FAIL.
	InsufficientSamples = "INSUFFICIENT SAMPLES"
)

// Workload groups: each maps to a spec section 1 target.
const (
	GroupIndexing   = "indexing"
	GroupVisibility = "visibility"
	GroupFilter     = "filter"
	GroupSorted     = "sorted"
	GroupAggs       = "aggs"
	GroupPercolate  = "percolate"
	GroupMixed      = "mixed"
	GroupFootprint  = "footprint"
	GroupRestart    = "restart"
	GroupRecovery   = "recovery"
)

// Thresholds of the absolute targets.
const (
	// PercolateSpeedup is how many times Elasticsearch's percolation throughput
	// Searchlight must reach.
	PercolateSpeedup = 10.0
	// PercolateP99Micros is the per-document p99 bound at 100k saved queries.
	PercolateP99Micros = 1000.0
	// VisibleP99Micros bounds write-to-visible at the default 1 s refresh: the
	// interval plus 100 ms for the poll's granularity and the requests.
	VisibleP99Micros = 1_100_000.0
	// RestartSeconds is "seconds, not proportional to index size".
	RestartSeconds = 10.0
)

// TargetCheck is one spec section 1 target, evaluated.
type TargetCheck struct {
	ID            string   `json:"id"`
	Area          string   `json:"area"`
	Target        string   `json:"target"`
	Workloads     []string `json:"workloads"`
	Searchlight   string   `json:"searchlight"`
	Elasticsearch string   `json:"elasticsearch"`
	Status        string   `json:"status"`
	Detail        string   `json:"detail"`
}

// worst orders statuses: FAIL and INVALID dominate, then insufficient samples, then
// the unmeasured, then PASS.
func worst(a, b string) string {
	rank := map[string]int{Pass: 0, NoBaseline: 1, NotMeasured: 2, InsufficientSamples: 3, Fail: 4, Invalid: 5}
	if rank[b] > rank[a] {
		return b
	}
	return a
}

// workloadsIn returns the run's workload names in a group, in first-seen order.
func (r *Run) workloadsIn(group string) []string {
	var out []string
	for i := range r.Results {
		res := &r.Results[i]
		if res.Group == group && !slices.Contains(out, res.Workload) {
			out = append(out, res.Workload)
		}
	}
	return out
}

// Evaluate checks every spec section 1 target against the run's results.
func Evaluate(r *Run) []TargetCheck {
	checks := []TargetCheck{
		r.higherIsBetter("T1", "Bulk indexing throughput (docs/s per node)", "≥ Elasticsearch, at equal durability (acknowledged = durable)", GroupIndexing, 1),
		r.latency("T2", "Filter / boolean search latency, p50 and p99", "≤ Elasticsearch", GroupFilter),
		r.latency("T3", "Sorted and paged search, p50 and p99", "≤ Elasticsearch", GroupSorted),
		r.latency("T4", "Aggregation latency (terms, range, histogram, stats)", "≤ Elasticsearch", GroupAggs),
		r.percolation(),
		r.footprint(),
		r.visibility(),
		r.recovery(),
		r.restart(),
		{
			ID: "T10", Area: "Replica failure", Target: "no lost acknowledged write, no client-visible error with ≥ 2 replicas",
			Status: NotMeasured, Detail: "A correctness property under failure, checked by the chaos suite (test/chaos, .github/workflows/chaos.yml), not a performance workload.",
		},
		{
			ID: "T11", Area: "Scale", Target: "larger than RAM per node (memory-mapped segments), many nodes through shards",
			Status: NotMeasured, Detail: "Needs a dataset larger than the host's RAM (slbench gen --docs 10000000 on a small host) and the cluster for many nodes.",
		},
	}
	if r.CrossCheck.Failed() {
		for i := range checks {
			if checks[i].Status == Pass || checks[i].Status == Fail || checks[i].Status == InsufficientSamples {
				if checks[i].ID == "T7" || checks[i].ID == "T9" {
					continue // absolute targets do not compare answers
				}
				checks[i].Status = Invalid
				checks[i].Detail = "The engines returned different answers (see the cross-check); a comparison of different answers is meaningless. " + checks[i].Detail
			}
		}
	}
	return checks
}

func (r *Run) higherIsBetter(id, area, target, group string, factor float64) TargetCheck {
	c := TargetCheck{ID: id, Area: area, Target: target, Workloads: r.workloadsIn(group)}
	if len(c.Workloads) == 0 {
		c.Status, c.Detail = NotMeasured, "No "+group+" workload ran."
		return c
	}
	c.Status = Pass
	var sl, es, notes []string
	for _, w := range c.Workloads {
		s, e := r.Find(w, Searchlight), r.Find(w, Elasticsearch)
		st, note := compareHigher(w, s, e, factor)
		c.Status = worst(c.Status, st)
		if note != "" {
			notes = append(notes, note)
		}
		sl = append(sl, rate(s))
		es = append(es, rate(e))
	}
	c.Searchlight, c.Elasticsearch, c.Detail = strings.Join(sl, "; "), strings.Join(es, "; "), strings.Join(notes, " ")
	return c
}

// rate is a result's headline rate.
func rate(res *Result) string {
	if res == nil {
		return "—"
	}
	if !res.OK() {
		return "error"
	}
	if res.DocsPerSec > 0 && res.Docs != res.Ops {
		return FormatRate(res.DocsPerSec) + " docs/s"
	}
	return FormatRate(res.OpsPerSec) + " ops/s"
}

func perSec(res *Result) float64 {
	if res.DocsPerSec > 0 {
		return res.DocsPerSec
	}
	return res.OpsPerSec
}

func compareHigher(w string, s, e *Result, factor float64) (string, string) {
	switch {
	case s == nil:
		return NotMeasured, w + ": Searchlight not measured."
	case !s.OK():
		return Invalid, fmt.Sprintf("%s: Searchlight errored (%d errors %s).", w, s.Errors, s.Error)
	case e == nil:
		return NoBaseline, ""
	case !e.OK():
		return Invalid, fmt.Sprintf("%s: Elasticsearch errored (%d errors %s).", w, e.Errors, e.Error)
	}
	ratio := perSec(s) / perSec(e)
	if ratio >= factor {
		return Pass, fmt.Sprintf("%s: %.2f× Elasticsearch.", w, ratio)
	}
	return Fail, fmt.Sprintf("%s: %.2f× Elasticsearch (needs %.0f×).", w, ratio, factor)
}

func (r *Run) latency(id, area, target, group string) TargetCheck {
	c := TargetCheck{ID: id, Area: area, Target: target, Workloads: r.workloadsIn(group)}
	if len(c.Workloads) == 0 {
		c.Status, c.Detail = NotMeasured, "No "+group+" workload ran."
		return c
	}
	c.Status = Pass
	won, compared := 0, 0
	var losses []string
	var slP50, slP99, esP50, esP99 []float64
	var slCount, esCount []int64
	for _, w := range c.Workloads {
		s, e := r.Find(w, Searchlight), r.Find(w, Elasticsearch)
		switch {
		case s == nil:
			c.Status = worst(c.Status, NotMeasured)
			continue
		case !s.OK() || s.Latency == nil:
			c.Status = worst(c.Status, Invalid)
			losses = append(losses, w+": Searchlight errored")
			continue
		}
		slP50, slP99, slCount = append(slP50, s.Latency.P50), append(slP99, s.Latency.P99), append(slCount, s.Latency.Count)
		switch {
		case e == nil:
			c.Status = worst(c.Status, NoBaseline)
			continue
		case !e.OK() || e.Latency == nil:
			c.Status = worst(c.Status, Invalid)
			losses = append(losses, w+": Elasticsearch errored")
			continue
		}
		esP50, esP99, esCount = append(esP50, e.Latency.P50), append(esP99, e.Latency.P99), append(esCount, e.Latency.Count)
		if !sufficient(s.Latency) || !sufficient(e.Latency) {
			c.Status = worst(c.Status, InsufficientSamples)
			losses = append(losses, fmt.Sprintf("%s: insufficient samples for p99 (searchlight n=%d, elasticsearch n=%d; need ≥%d)",
				w, s.Latency.Count, e.Latency.Count, minSamples(0.99)))
			continue
		}
		compared++
		if s.Latency.P50 <= e.Latency.P50 && s.Latency.P99 <= e.Latency.P99 {
			won++
			continue
		}
		c.Status = worst(c.Status, Fail)
		losses = append(losses, fmt.Sprintf("%s (p50 %s vs %s, p99 %s vs %s)", w,
			FormatMicros(s.Latency.P50), FormatMicros(e.Latency.P50), FormatMicros(s.Latency.P99), FormatMicros(e.Latency.P99)))
	}
	c.Searchlight = spread(slP50, slP99, slCount)
	c.Elasticsearch = spread(esP50, esP99, esCount)
	if compared > 0 {
		c.Detail = fmt.Sprintf("%d of %d workloads at or under Elasticsearch on both p50 and p99.", won, compared)
	}
	if len(losses) > 0 {
		c.Detail += " Behind: " + strings.Join(losses, "; ") + "."
	}
	return c
}

// spread summarizes p50s and p99s across workloads as ranges. An end of a range
// that doesn't have enough samples to trust renders as formatQuantile does for a
// single workload's table row ("n/a (n=…)"), not as a number.
func spread(p50, p99 []float64, counts []int64) string {
	if len(p50) == 0 {
		return "—"
	}
	return "p50 " + quantileSpread(p50, counts, 0.50) + ", p99 " + quantileSpread(p99, counts, 0.99)
}

// quantileSpread formats one quantile's range across workloads: the lowest and
// highest value, each checked against its own workload's sample count.
func quantileSpread(v []float64, counts []int64, q float64) string {
	loI, hiI := 0, 0
	for i, x := range v {
		if x < v[loI] {
			loI = i
		}
		if x > v[hiI] {
			hiI = i
		}
	}
	lo := formatQuantile(v[loI], counts[loI], q)
	if loI == hiI {
		return lo
	}
	return lo + "–" + formatQuantile(v[hiI], counts[hiI], q)
}

func (r *Run) percolation() TargetCheck {
	c := TargetCheck{
		ID: "T5", Area: "Percolation throughput (docs/s against 10k / 100k saved queries)",
		Target:    fmt.Sprintf("≥ %.0f× Elasticsearch's percolator, with p99 per document < 1 ms at 100k queries", PercolateSpeedup),
		Workloads: r.workloadsIn(GroupPercolate),
	}
	c.Status = Pass
	var sl, es, notes []string
	for _, n := range []int{10_000, 100_000} {
		w := fmt.Sprintf("percolate_batch_%d", n)
		s, e := r.Find(w, Searchlight), r.Find(w, Elasticsearch)
		st, note := compareHigher(w, s, e, PercolateSpeedup)
		if s == nil {
			note = fmt.Sprintf("%s not run (the run's largest saved-search set is smaller).", w)
		}
		c.Status = worst(c.Status, st)
		if note != "" {
			notes = append(notes, note)
		}
		sl = append(sl, fmt.Sprintf("%s: %s", shortCount(n), rate(s)))
		es = append(es, fmt.Sprintf("%s: %s", shortCount(n), rate(e)))
	}
	single := r.Find("percolate_single_100000", Searchlight)
	switch {
	case single == nil:
		c.Status = worst(c.Status, NotMeasured)
		notes = append(notes, "percolate_single_100000 not run, so the 1 ms p99 is unchecked.")
	case !single.OK():
		c.Status = worst(c.Status, Invalid)
	default:
		p99, count, isServer, has := percolateP99(single)
		label := "end-to-end (no server timing)"
		if isServer {
			label = "server"
		}
		var endToEnd string
		if single.Latency != nil {
			endToEnd = fmt.Sprintf(" (end-to-end p50 %s, p99 %s)", FormatMicros(single.Latency.P50), formatQuantile(single.Latency.P99, single.Latency.Count, 0.99))
		}
		switch {
		case !has:
			c.Status = worst(c.Status, Invalid)
		case count < minSamples(0.99):
			c.Status = worst(c.Status, InsufficientSamples)
			sl = append(sl, fmt.Sprintf("p99/doc @100k: n/a (n=%d, %s)%s", count, label, endToEnd))
			notes = append(notes, fmt.Sprintf("percolate_single_100000 has %d %s samples; p99 needs ≥%d to judge the < 1 ms target.", count, label, minSamples(0.99)))
		default:
			row := fmt.Sprintf("p99/doc @100k: %s (%s)", FormatMicros(p99), label)
			if isServer {
				row += endToEnd
			}
			sl = append(sl, row)
			if p99 >= PercolateP99Micros {
				c.Status = worst(c.Status, Fail)
				notes = append(notes, fmt.Sprintf("p99 per document at 100k is %s (%s; needs < 1 ms).", FormatMicros(p99), label))
			}
		}
	}
	if e := r.Find("percolate_single_100000", Elasticsearch); e != nil {
		if ep99, _, eIsServer, eHas := percolateP99(e); eHas {
			elabel := "end-to-end (no server timing)"
			if eIsServer {
				elabel = `server, Elasticsearch's own "took"`
			}
			es = append(es, fmt.Sprintf("p99/doc @100k: %s (%s)", FormatMicros(ep99), elabel))
		}
	}
	c.Searchlight, c.Elasticsearch, c.Detail = strings.Join(sl, "; "), strings.Join(es, "; "), strings.Join(notes, " ")
	return c
}

// percolateP99 returns a percolate_single result's judged p99 (operator decision,
// 2026-10-05: the engine's own server time -- analyze, match and encode, like
// Elasticsearch's "took" -- when it reported enough samples of it; the harness's
// own end-to-end measurement otherwise, clearly distinguished by server), its
// sample count, and whether a judgment was possible at all (ok).
func percolateP99(res *Result) (p99 float64, count int64, server, ok bool) {
	if res == nil {
		return 0, 0, false, false
	}
	if sc := int64(res.Values["server_count"]); sc > 0 {
		return res.Values["server_p99_us"], sc, true, true
	}
	if res.Latency != nil {
		return res.Latency.P99, res.Latency.Count, false, true
	}
	return 0, 0, false, false
}

func shortCount(n int) string {
	if n >= 1000 && n%1000 == 0 {
		return fmt.Sprintf("%dk", n/1000)
	}
	return fmt.Sprint(n)
}

func (r *Run) footprint() TargetCheck {
	c := TargetCheck{
		ID: "T6", Area: "Index size on disk and resident memory per million documents", Target: "≤ Elasticsearch",
		Workloads: []string{"footprint"},
	}
	s, e := r.Find("footprint", Searchlight), r.Find("footprint", Elasticsearch)
	if s == nil {
		c.Status, c.Detail = NotMeasured, "No footprint measured."
		return c
	}
	desc := func(res *Result) string {
		if res == nil {
			return "—"
		}
		return fmt.Sprintf("disk %s/M, RSS %s/M", FormatBytes(res.Values["disk_per_million"]), FormatBytes(res.Values["rss_per_million"]))
	}
	c.Searchlight, c.Elasticsearch = desc(s), desc(e)
	if e == nil {
		c.Status = NoBaseline
		return c
	}
	c.Status = Pass
	var notes []string
	for _, k := range []string{"disk_per_million", "rss_per_million"} {
		sv, ev := s.Values[k], e.Values[k]
		if sv <= 0 || ev <= 0 {
			c.Status = worst(c.Status, NotMeasured)
			notes = append(notes, k+" not measured on both engines.")
			continue
		}
		if sv > ev {
			c.Status = worst(c.Status, Fail)
			notes = append(notes, fmt.Sprintf("%s: %.2f× Elasticsearch.", k, sv/ev))
		} else {
			notes = append(notes, fmt.Sprintf("%s: %.2f× Elasticsearch.", k, sv/ev))
		}
	}
	c.Detail = strings.Join(notes, " ")
	return c
}

// visibility checks T7: write-to-visible latency and refresh=wait_for. Elasticsearch's
// wait_for waits for the next scheduled refresh rather than forcing one, so its bound
// is the same as plain write-to-visible (p99 at or under the 1 s refresh interval,
// plus slack), not some much smaller number — both halves are gated the same way, and
// T7 fails if either does.
func (r *Run) visibility() TargetCheck {
	c := TargetCheck{
		ID: "T7", Area: "Write-to-visible latency", Target: "≤ 1 s by default (the refresh interval), for both plain writes and refresh=wait_for",
		Workloads: []string{"refresh_visible", "refresh_wait_for"},
	}
	vis, wf := r.Find("refresh_visible", Searchlight), r.Find("refresh_wait_for", Searchlight)
	if vis == nil || wf == nil {
		c.Status, c.Detail = NotMeasured, "The visibility workloads did not run."
		return c
	}
	desc := func(v, w *Result) string {
		if v == nil || v.Latency == nil || w == nil || w.Latency == nil {
			return "—"
		}
		return fmt.Sprintf("visible p50 %s p99 %s; wait_for p50 %s p99 %s",
			FormatMicros(v.Latency.P50), formatQuantile(v.Latency.P99, v.Latency.Count, 0.99),
			FormatMicros(w.Latency.P50), formatQuantile(w.Latency.P99, w.Latency.Count, 0.99))
	}
	c.Searchlight = desc(vis, wf)
	c.Elasticsearch = desc(r.Find("refresh_visible", Elasticsearch), r.Find("refresh_wait_for", Elasticsearch))
	if !vis.OK() || !wf.OK() || vis.Latency == nil || wf.Latency == nil {
		c.Status, c.Detail = Invalid, "A visibility workload errored (a wait_for write not visible at once counts as an error)."
		return c
	}
	visSuff, wfSuff := sufficient(vis.Latency), sufficient(wf.Latency)
	if !visSuff || !wfSuff {
		var short []string
		if !visSuff {
			short = append(short, fmt.Sprintf("refresh_visible (n=%d)", vis.Latency.Count))
		}
		if !wfSuff {
			short = append(short, fmt.Sprintf("refresh_wait_for (n=%d)", wf.Latency.Count))
		}
		c.Status = InsufficientSamples
		c.Detail = fmt.Sprintf("p99 needs ≥%d samples: %s.", minSamples(0.99), strings.Join(short, ", "))
		return c
	}
	visFail, wfFail := vis.Latency.P99 > VisibleP99Micros, wf.Latency.P99 > VisibleP99Micros
	switch {
	case visFail && wfFail:
		c.Status = Fail
		c.Detail = fmt.Sprintf("p99 write-to-visible %s and p99 wait_for %s both exceed 1 s + 100 ms.",
			FormatMicros(vis.Latency.P99), FormatMicros(wf.Latency.P99))
	case visFail:
		c.Status = Fail
		c.Detail = fmt.Sprintf("p99 write-to-visible %s exceeds 1 s + 100 ms (wait_for p99 %s is within it).",
			FormatMicros(vis.Latency.P99), FormatMicros(wf.Latency.P99))
	case wfFail:
		c.Status = Fail
		c.Detail = fmt.Sprintf("p99 wait_for %s exceeds 1 s + 100 ms (write-to-visible p99 %s is within it).",
			FormatMicros(wf.Latency.P99), FormatMicros(vis.Latency.P99))
	default:
		c.Status, c.Detail = Pass, "Absolute target (p99 ≤ 1.1 s: the 1 s interval plus poll granularity) for both plain writes and refresh=wait_for."
	}
	return c
}

func (r *Run) recovery() TargetCheck {
	c := TargetCheck{
		ID: "T8", Area: "New replica from zero to serving (10M docs)", Target: "≤ Elasticsearch peer recovery",
		Workloads: []string{"recovery"},
	}
	s, e := r.Find("recovery", Searchlight), r.Find("recovery", Elasticsearch)
	if s == nil {
		c.Status, c.Detail = NotMeasured, "No recovery workload ran: it needs a Postgres or MySQL store for a second Searchlight node (slbench run --sl-bin with --recovery-store-url)."
		return c
	}
	secs := func(res *Result) string {
		if res == nil || res.Latency == nil {
			return "—"
		}
		return FormatMicros(res.Latency.Max)
	}
	c.Searchlight, c.Elasticsearch = secs(s), secs(e)
	scale := ""
	if docs := s.Values["docs"]; docs > 0 && docs < 10e6 {
		scale = fmt.Sprintf(" Measured at %.0f documents, not the target's 10M.", docs)
	}
	switch {
	case !s.OK() || s.Latency == nil:
		c.Status, c.Detail = Invalid, "The recovery workload errored."
	case e == nil:
		c.Status, c.Detail = NoBaseline, "Elasticsearch's peer recovery was not run: pass --es-recovery (needs the second node bench/docker-compose.es.yml starts)."+scale
	case !e.OK() || e.Latency == nil:
		c.Status, c.Detail = Invalid, "Elasticsearch's recovery workload errored."
	case s.Latency.Max > e.Latency.Max:
		c.Status, c.Detail = Fail, "Searchlight's slowest recovery is slower than Elasticsearch's."+scale
	default:
		c.Status, c.Detail = Pass, "Searchlight's slowest recovery is within Elasticsearch's."+scale
	}
	return c
}

func (r *Run) restart() TargetCheck {
	c := TargetCheck{
		ID: "T9", Area: "Node restart to serving", Target: "seconds, not proportional to index size (segments are reopened, not rebuilt)",
		Workloads: []string{"restart"},
	}
	s := r.Find("restart", Searchlight)
	if s == nil {
		c.Status, c.Detail = NotMeasured, "No restart ran: slbench restarts a node it runs itself (--sl-bin), or one --sl-restart-cmd restarts."
		return c
	}
	secs := func(res *Result) string {
		if res == nil || res.Latency == nil {
			return "—"
		}
		return FormatMicros(res.Latency.Max)
	}
	c.Searchlight, c.Elasticsearch = secs(s), secs(r.Find("restart", Elasticsearch))
	switch {
	case !s.OK() || s.Latency == nil:
		c.Status, c.Detail = Invalid, "The restart workload errored."
	case s.Latency.Max/1e6 > RestartSeconds:
		c.Status, c.Detail = Fail, fmt.Sprintf("Slowest restart %s exceeds %.0f s.", FormatMicros(s.Latency.Max), RestartSeconds)
	default:
		c.Status, c.Detail = Pass, fmt.Sprintf("Absolute target: every restart under %.0f s.", RestartSeconds)
	}
	return c
}
