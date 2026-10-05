// Package report holds a benchmark run's results (the JSON artifact that runs are
// compared by), evaluates them against the spec section 1 targets, and renders the
// markdown report (docs/benchmarks.md).
package report

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/Imposter/go-searchlight/bench/es"
)

// SchemaVersion is the JSON artifact's version; bump it on an incompatible change.
const SchemaVersion = 1

// Engine names.
const (
	Searchlight   = "searchlight"
	Elasticsearch = "elasticsearch"
)

// Run is one benchmark run.
type Run struct {
	Schema     int           `json:"schema"`
	Label      string        `json:"label,omitempty"`
	StartedAt  time.Time     `json:"started_at"`
	FinishedAt time.Time     `json:"finished_at"`
	Env        Environment   `json:"environment"`
	Engines    []EngineInfo  `json:"engines"`
	Dataset    Dataset       `json:"dataset"`
	Options    Options       `json:"options"`
	Results    []Result      `json:"results"`
	CrossCheck CrossCheck    `json:"cross_check"`
	Caveats    []es.Caveat   `json:"caveats"`
	Notes      []string      `json:"notes,omitempty"`
	Targets    []TargetCheck `json:"targets"`
}

// Environment is the machine and toolchain.
type Environment struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	Kernel    string `json:"kernel,omitempty"`
	CPU       string `json:"cpu"`
	CPUs      int    `json:"cpus"`
	MemTotal  int64  `json:"mem_total_bytes"`
	GoVersion string `json:"go_version"`
	Hostname  string `json:"hostname,omitempty"`
	CI        string `json:"ci,omitempty"`
	Commit    string `json:"commit,omitempty"`
}

// EngineInfo describes an engine under test.
type EngineInfo struct {
	Name    string            `json:"name"`
	URL     string            `json:"url"`
	Version string            `json:"version"`
	Config  map[string]string `json:"config"`
}

// Resources is an engine's footprint.
type Resources struct {
	DiskBytes  int64  `json:"disk_bytes"`
	DiskSource string `json:"disk_source"`
	RSSBytes   int64  `json:"rss_bytes"`
	RSSSource  string `json:"rss_source"`
}

// Dataset is the data a run loaded.
type Dataset struct {
	Docs        int64   `json:"docs"`
	Seed        uint64  `json:"seed"`
	File        string  `json:"file"`
	FileBytes   int64   `json:"file_bytes"`
	Fields      int     `json:"fields"`
	SearchSets  []int   `json:"saved_search_sets"`
	SearchFile  string  `json:"saved_search_file,omitempty"`
	Shards      int     `json:"shards"`
	AvgDocBytes float64 `json:"avg_doc_bytes"`
}

// Options are the run's knobs.
type Options struct {
	Warmup               int     `json:"warmup"`
	Iterations           int     `json:"iterations"`
	Concurrency          int     `json:"concurrency"`
	Rate                 float64 `json:"rate,omitempty"`
	Variants             int     `json:"variants"`
	BulkBatch            int     `json:"bulk_batch"`
	BulkConcurrency      int     `json:"bulk_concurrency"`
	PercolateBatch       int     `json:"percolate_batch"`
	PercolateConcurrency int     `json:"percolate_concurrency"`
	PageDepth            int     `json:"page_depth"`
	MixedSeconds         float64 `json:"mixed_seconds"`
	// Repeats is how many independent times each latency-gated workload (filter,
	// sorted/paging, aggregations, percolate-single) ran, merged into one histogram;
	// 1 means no repeat.
	Repeats int `json:"repeats,omitempty"`
}

// Latency is a latency distribution in microseconds.
type Latency struct {
	Count int64   `json:"count"`
	Min   float64 `json:"min_us"`
	Mean  float64 `json:"mean_us"`
	P50   float64 `json:"p50_us"`
	P90   float64 `json:"p90_us"`
	P99   float64 `json:"p99_us"`
	P999  float64 `json:"p999_us"`
	Max   float64 `json:"max_us"`
}

// Result is one workload's measurement on one engine.
type Result struct {
	Workload    string             `json:"workload"`
	Group       string             `json:"group"`
	Engine      string             `json:"engine"`
	Description string             `json:"description,omitempty"`
	Concurrency int                `json:"concurrency,omitempty"`
	Rate        float64            `json:"rate,omitempty"`
	Warmup      int                `json:"warmup,omitempty"`
	Iterations  int                `json:"iterations,omitempty"`
	Ops         int64              `json:"ops"`
	Docs        int64              `json:"docs,omitempty"`
	Errors      int64              `json:"errors"`
	ElapsedSec  float64            `json:"elapsed_s"`
	OpsPerSec   float64            `json:"ops_per_s"`
	DocsPerSec  float64            `json:"docs_per_s,omitempty"`
	Latency     *Latency           `json:"latency,omitempty"`
	Values      map[string]float64 `json:"values,omitempty"`
	Error       string             `json:"error,omitempty"`
}

// OK reports whether the result is usable: it ran without errors.
func (r *Result) OK() bool { return r.Error == "" && r.Errors == 0 && r.Ops > 0 }

// CrossCheck is the correctness comparison of the two engines' answers.
type CrossCheck struct {
	Ran        bool       `json:"ran"`
	Checked    int        `json:"checked"`
	Matched    int        `json:"matched"`
	Tolerated  int        `json:"tolerated"`
	Mismatches []Mismatch `json:"mismatches,omitempty"`
	Skipped    string     `json:"skipped,omitempty"`
}

// Mismatch is one request the engines answered differently.
type Mismatch struct {
	Name     string   `json:"name"`
	Request  string   `json:"request"`
	Problems []string `json:"problems"`
	// Tolerated marks a difference inside a documented caveat.
	Tolerated bool `json:"tolerated,omitempty"`
}

// Failed reports whether the cross-check found a difference outside the caveats.
func (c *CrossCheck) Failed() bool {
	for _, m := range c.Mismatches {
		if !m.Tolerated {
			return true
		}
	}
	return false
}

// Find returns the result of a workload on an engine.
func (r *Run) Find(workload, engine string) *Result {
	for i := range r.Results {
		if r.Results[i].Workload == workload && r.Results[i].Engine == engine {
			return &r.Results[i]
		}
	}
	return nil
}

// Has reports whether the run measured engine.
func (r *Run) Has(engine string) bool {
	for _, e := range r.Engines {
		if e.Name == engine {
			return true
		}
	}
	return false
}

// Save writes r as indented JSON to path.
func (r *Run) Save(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644) //nolint:gosec // a report, not a secret
}

// Load reads a run saved by Save.
func Load(path string) (*Run, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r Run
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("report: %s: %w", path, err)
	}
	if r.Schema != SchemaVersion {
		return nil, fmt.Errorf("report: %s has schema %d, want %d", path, r.Schema, SchemaVersion)
	}
	return &r, nil
}
