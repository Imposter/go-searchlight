package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Imposter/go-searchlight/bench/datasets"
	"github.com/Imposter/go-searchlight/bench/es"
	"github.com/Imposter/go-searchlight/bench/report"
	"github.com/Imposter/go-searchlight/bench/workloads"
)

// errMismatch is a run whose engines answered differently.
var errMismatch = errors.New("the cross-check found different answers; comparative results are invalid")

// keyValues parses "k=v,k=v".
func keyValues(s string) map[string]string {
	out := map[string]string{}
	for kv := range strings.SplitSeq(s, ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(kv), "="); ok && k != "" {
			out[k] = v
		}
	}
	return out
}

func splitList(s string) []string {
	var out []string
	for f := range strings.SplitSeq(s, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

func cmdRun(ctx context.Context, args []string, stdout, stderr io.Writer, loadOnly bool) error {
	name := "run"
	if loadOnly {
		name = "load"
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	data := fs.String("data", "", "product dataset written by slbench gen (required)")
	docs := fs.Int64("docs", 0, "load only the first N documents of the dataset (0: all)")
	searches := fs.String("searches", "", "saved-search file (the largest set; smaller sets are its prefixes)")
	sets := fs.String("sets", "1k,10k,100k", "saved-search set sizes to percolate against")

	slBin := fs.String("sl-bin", "", "run this searchlight binary for the run, on SQLite with a generated tokens file under --sl-dir (and restart it for the restart workload)")
	slDir := fs.String("sl-dir", "sl-bench", "with --sl-bin: the node's files (data, database, tokens, searchlight.log)")
	slSettings := fs.String("sl-settings", "", "with --sl-bin: more node settings, name=value,... (default log_level=warn,shutdown_grace=0s)")
	recoveryStore := fs.String("recovery-store-url", "", "with --sl-bin: a Postgres or MySQL store_url for the recovery workload (target T8), run on a cluster of its own")
	recoveryIter := fs.Int("recovery-iterations", 3, "measured recoveries (new replicas from zero to serving)")
	slURL := fs.String("sl-url", "", "the API URL of a Searchlight node run elsewhere (instead of --sl-bin)")
	slToken := fs.String("sl-token", "", "with --sl-url: the bearer token")
	slDisk := fs.String("sl-disk", "", "with --sl-url: comma-separated directories holding the node's data and database, for disk usage")
	slPID := fs.Int("sl-pid", 0, "with --sl-url: the node's process id on this host (Linux: RSS from /proc; else /metrics)")
	slConfig := fs.String("sl-config", "", "k=v,... describing Searchlight's configuration for the report")
	esURL := fs.String("es-url", "", "Elasticsearch URL")
	esPID := fs.Int("es-pid", 0, "the Elasticsearch JVM's process id on this host (Linux: RSS from /proc)")
	esConfig := fs.String("es-config", "", "k=v,... describing Elasticsearch's configuration for the report")

	shards := fs.Int("shards", 1, "shards per index on both engines")
	warmup := fs.Int("warmup", 100, "warmup requests per search workload")
	iterations := fs.Int("iterations", 1000, "measured requests per search workload")
	concurrency := fs.Int("concurrency", 1, "requests in flight (closed loop), or the most in flight under --rate")
	rate := fs.Float64("rate", 0, "open-loop requests per second for search workloads (0: closed loop)")
	variants := fs.Int("variants", 64, "query variants per search workload")
	pageDepth := fs.Int("page-depth", 10_000, "deep paging depth in hits")
	pageWalks := fs.Int("page-walks", 5, "measured deep paging walks")
	bulkBatch := fs.Int("bulk-batch", 1000, "documents per _bulk while loading")
	bulkConc := fs.Int("bulk-concurrency", 4, "concurrent _bulk requests while loading")
	percBatch := fs.Int("percolate-batch", 100, "documents per percolate request")
	percConc := fs.Int("percolate-concurrency", 4, "concurrent percolate batch requests")
	percIter := fs.Int("percolate-iterations", 50, "measured percolate batches per saved-search set")
	percSingle := fs.Int("percolate-single", 300, "measured single-document percolations per saved-search set")
	bulkPercIter := fs.Int("bulk-percolate-iterations", 20, "measured _bulk?percolate requests")
	visible := fs.Int("visible-iterations", 1000, "measured write-to-visible iterations per direction (visible and wait_for), at concurrency 1 so queueing never shows up as latency; 1000 is minSamples(0.99), the fewest a trusted p99 needs")
	mixed := fs.Duration("mixed", 30*time.Second, "how long the mixed read/write workload runs")
	ccVariants := fs.Int("crosscheck-variants", 4, "variants per workload the cross-check compares")
	only := fs.String("only", "", "comma-separated workloads or groups to run (default all)")
	skip := fs.String("skip", "", "comma-separated workloads or groups to skip")
	slRestart := fs.String("sl-restart-cmd", "", "with --sl-url: a shell command that restarts Searchlight (enables the restart workload)")
	esRestart := fs.String("es-restart-cmd", "", "shell command that restarts Elasticsearch")
	restartIter := fs.Int("restart-iterations", 3, "measured restarts per engine")

	out := fs.String("out", "bench-results.json", "write the run's JSON here")
	md := fs.String("md", "", "also render the markdown report here")
	label := fs.String("label", "", "a label for the report's title")
	allowMismatch := fs.Bool("allow-mismatch", false, "exit 0 even when the cross-check fails")
	stopOnMismatch := fs.Bool("stop-on-mismatch", false, "skip every timing workload once the cross-check finds an unresolved mismatch (comparative numbers from engines that disagree are not worth collecting)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *data == "" {
		return errors.New("--data is required (slbench gen writes one)")
	}
	meta, err := readMeta(*data)
	if err != nil {
		return err
	}
	setSizes, err := parseInts(*sets)
	if err != nil {
		return fmt.Errorf("--sets: %w", err)
	}
	if *searches == "" {
		setSizes = nil
	}

	restarters := map[string]workloads.Restarter{}
	recoverers := map[string]workloads.Recoverer{}
	slCfg := keyValues(*slConfig)
	slPIDFunc := func() int { return *slPID }
	slDiskPaths := splitList(*slDisk)
	if *slBin != "" {
		if *slURL != "" {
			return errors.New("give --sl-bin or --sl-url, not both")
		}
		settings := append([]string{"log_level=warn", "shutdown_grace=0s"}, splitList(*slSettings)...)
		node, stop, err := runNode(ctx, nodeSpec{bin: *slBin, dir: *slDir, nodeID: "bench", settings: settings})
		if err != nil {
			return err
		}
		defer stop(stderr)
		*slURL, *slToken = node.URL, node.Token
		slPIDFunc, slDiskPaths = node.PID, node.DiskPaths()
		restarters[report.Searchlight] = workloads.Restarter{
			Describe: "the node stopped gracefully and its binary started again",
			Restart:  func(ctx context.Context) error { return node.Restart(ctx, nodeStopTimeout) },
		}
		if _, ok := slCfg["store"]; !ok {
			slCfg["store"] = "SQLite (synchronous=FULL, WAL), its own process"
		}
		for _, kv := range settings {
			if k, v, ok := strings.Cut(kv, "="); ok {
				slCfg[k] = v
			}
		}
		if *recoveryStore != "" {
			rec, err := newRecoverer(ctx, nodeSpec{bin: *slBin, dir: filepath.Join(*slDir, "recovery"), store: *recoveryStore, settings: settings}, stdout)
			if err != nil {
				return err
			}
			defer rec.close(stderr)
			recoverers[report.Searchlight] = rec
		}
	} else if *recoveryStore != "" {
		return errors.New("--recovery-store-url needs --sl-bin: slbench starts the recovery cluster's nodes itself")
	}
	if *slRestart != "" {
		restarters[report.Searchlight] = workloads.CommandRestarter(*slRestart, stdout)
	}
	if *esRestart != "" {
		restarters[report.Elasticsearch] = workloads.CommandRestarter(*esRestart, stdout)
	}

	var engines []workloads.Engine
	if *slURL != "" {
		engines = append(engines, workloads.NewSearchlight(workloads.SearchlightOptions{
			URL: *slURL, Token: *slToken, DiskPaths: slDiskPaths, PID: slPIDFunc, Config: slCfg, Log: stdout,
		}))
	}
	if *esURL != "" {
		engines = append(engines, workloads.NewElasticsearch(workloads.ElasticsearchOptions{
			URL: *esURL, PID: *esPID, Config: keyValues(*esConfig), Fields: datasets.Products, Log: stdout,
		}))
	}
	if len(engines) == 0 {
		return errors.New("no engine: give --sl-bin, --sl-url or --es-url")
	}

	cfg := workloads.Config{
		Label: *label, DataFile: *data, Docs: *docs, Seed: meta.Seed,
		SearchFile: *searches, SearchSets: setSizes, Shards: *shards,
		BulkBatch: *bulkBatch, BulkConcurrency: *bulkConc,
		Search:   workloads.RunOptions{Warmup: *warmup, Iterations: *iterations, Concurrency: *concurrency, Rate: *rate},
		Variants: *variants, PageDepth: *pageDepth, PageWalks: *pageWalks,
		PercolateBatch: *percBatch, PercolateConcurrency: *percConc, PercolateIterations: *percIter, PercolateSingle: *percSingle,
		BulkPercolateIterations: *bulkPercIter, VisibleIterations: *visible, MixedDuration: *mixed,
		CrossCheckVariants: *ccVariants,
		Restarters:         restarters, RestartIterations: *restartIter,
		Recoverers: recoverers, RecoveryIterations: *recoveryIter,
		Only: splitList(*only), Skip: splitList(*skip),
		LoadOnly: loadOnly, StopOnMismatch: *stopOnMismatch, Log: stdout,
	}
	run, runErr := workloads.RunSuite(ctx, cfg, engines)
	if run == nil {
		return runErr
	}
	if runErr != nil {
		run.Notes = append(run.Notes, "The run stopped early: "+runErr.Error())
	}
	if len(run.Targets) == 0 {
		run.Targets = report.Evaluate(run)
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o750); err != nil {
		return err
	}
	if err := run.Save(*out); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "results: %s\n", *out)
	if *md != "" {
		if err := writeMarkdown(run, *md); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "report: %s\n", *md)
	}
	for i := range run.Targets {
		t := &run.Targets[i]
		fmt.Fprintf(stdout, "  %-4s %-13s %s\n", t.ID, t.Status, t.Area)
	}
	if runErr != nil {
		return runErr
	}
	if run.CrossCheck.Failed() && !*allowMismatch {
		return errMismatch
	}
	return nil
}

func writeMarkdown(run *report.Run, path string) error {
	var b bytes.Buffer
	if err := report.Render(&b, run); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	return os.WriteFile(path, b.Bytes(), 0o644) //nolint:gosec // a report
}

func cmdReport(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	in := fs.String("in", "bench-results.json", "a run's JSON written by slbench run")
	out := fs.String("out", "docs/benchmarks.md", "the markdown report to write ('-' for stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	run, err := report.Load(*in)
	if err != nil {
		return err
	}
	run.Targets = report.Evaluate(run)
	if *out == "-" {
		return report.Render(stdout, run)
	}
	if err := writeMarkdown(run, *out); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "report: %s\n", *out)
	return nil
}

func cmdTranslate(args []string, stdin io.Reader, stdout io.Writer) error {
	fs := flag.NewFlagSet("translate", flag.ContinueOnError)
	asQuery := fs.Bool("query", false, "stdin is a query node, not a search body")
	mapping := fs.Bool("mapping", false, "print the Elasticsearch index and percolator mappings instead")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var v any
	tr := es.NewTranslator(datasets.Products)
	switch {
	case *mapping:
		v = map[string]any{"documents": es.IndexBody(datasets.Products, 1), "percolator": es.PercolatorIndexBody(datasets.Products, 1)}
	default:
		raw, err := io.ReadAll(stdin)
		if err != nil {
			return err
		}
		if *asQuery {
			v, err = tr.ParseQuery(raw)
		} else {
			v, err = tr.Search(raw)
		}
		if err != nil {
			return err
		}
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
