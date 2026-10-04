package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Imposter/go-searchlight/bench/datasets"
)

// Meta is a dataset file's sidecar (FILE.meta.json): what generated it.
type Meta struct {
	Kind string `json:"kind"`
	Docs int64  `json:"docs"`
	Seed uint64 `json:"seed"`
}

func metaPath(file string) string { return file + ".meta.json" }

func readMeta(file string) (Meta, error) {
	var m Meta
	b, err := os.ReadFile(metaPath(file))
	if err != nil {
		return m, fmt.Errorf("%w (a dataset written by slbench gen has one)", err)
	}
	err = json.Unmarshal(b, &m)
	return m, err
}

// ProductsFile and SearchesFile name the generated files in a directory.
func ProductsFile(dir string, docs int64) string {
	return filepath.Join(dir, "products-"+countName(docs)+".ndjson")
}

// SearchesFile names a saved-search set's file.
func SearchesFile(dir string, n int) string {
	return filepath.Join(dir, "searches-"+countName(int64(n))+".ndjson")
}

func countName(n int64) string {
	switch {
	case n >= 1_000_000 && n%1_000_000 == 0:
		return strconv.FormatInt(n/1_000_000, 10) + "m"
	case n >= 1000 && n%1000 == 0:
		return strconv.FormatInt(n/1000, 10) + "k"
	default:
		return strconv.FormatInt(n, 10)
	}
}

func parseInts(s string) ([]int, error) {
	var out []int
	for f := range strings.SplitSeq(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		mult := 1
		switch {
		case strings.HasSuffix(f, "k"):
			mult, f = 1000, strings.TrimSuffix(f, "k")
		case strings.HasSuffix(f, "m"):
			mult, f = 1_000_000, strings.TrimSuffix(f, "m")
		}
		n, err := strconv.Atoi(f)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("%q is not a positive count", f)
		}
		out = append(out, n*mult)
	}
	return out, nil
}

func cmdGen(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("gen", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("out", "bench-data", "directory to write the dataset into")
	docsFlag := fs.String("docs", "1m", "product listings to generate (1m, 10m, 50k, ...)")
	searches := fs.String("searches", "1k,10k,100k", "saved-search set sizes (each file is a prefix of the largest)")
	seed := fs.Uint64("seed", 1, "generator seed: the same seed writes the same bytes")
	workers := fs.Int("workers", 0, "generator goroutines (0: GOMAXPROCS)")
	force := fs.Bool("force", false, "rewrite files that already exist with the same parameters")
	if err := fs.Parse(args); err != nil {
		return err
	}
	docs, err := parseInts(*docsFlag)
	if err != nil || len(docs) != 1 {
		return fmt.Errorf("--docs: one count, like 1m: %w", err)
	}
	sets, err := parseInts(*searches)
	if err != nil {
		return fmt.Errorf("--searches: %w", err)
	}
	if err := os.MkdirAll(*dir, 0o750); err != nil {
		return err
	}
	n := int64(docs[0])
	if err := genFile(ctx, ProductsFile(*dir, n), Meta{Kind: "products", Docs: n, Seed: *seed}, datasets.ProductLines(*seed), *workers, *force, stdout); err != nil {
		return err
	}
	for _, k := range sets {
		meta := Meta{Kind: "searches", Docs: int64(k), Seed: *seed}
		if err := genFile(ctx, SearchesFile(*dir, k), meta, datasets.SearchLines(*seed), *workers, *force, stdout); err != nil {
			return err
		}
	}
	return nil
}

// genFile writes n lines to path through a temporary file, unless path already holds
// the same dataset (its sidecar matches).
func genFile(ctx context.Context, path string, meta Meta, lines datasets.LineFunc, workers int, force bool, stdout io.Writer) error {
	if have, err := readMeta(path); err == nil && have == meta && !force {
		if _, err := os.Stat(path); err == nil {
			fmt.Fprintf(stdout, "%s: up to date\n", path)
			return nil
		}
	}
	t0 := time.Now()
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 4<<20)
	err = datasets.Write(ctx, w, lines, 0, meta.Docs, workers)
	if err == nil {
		err = w.Flush()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := os.WriteFile(metaPath(path), b, 0o644); err != nil { //nolint:gosec // not a secret
		return err
	}
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "%s: %d %s, %.1f MiB in %s\n", path, meta.Docs, meta.Kind, float64(st.Size())/(1<<20), time.Since(t0).Round(time.Millisecond))
	return nil
}
