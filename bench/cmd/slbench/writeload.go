package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Imposter/go-searchlight/internal/slproc"
)

// cmdWriteload measures a sustained _bulk write load and the size-0 reads served beside
// it, on a node of a small CPU budget (GOMAXPROCS of its own): the workload of a scraper
// mirroring listings with nested variants, each bulk percolated against a few saved
// searches with their matches and inner hits.
//
//	slbench writeload --sl-bin bin/searchlight --dir /tmp/wl --docs 150k --duration 2m
//
// It preloads --docs listings, then for --duration sends --bulk-document bulks of
// listings picked at random, each one changed (a new snapshot) with --changed
// probability and sent as stored otherwise, while --readers goroutines run counts and
// facet searches. A bulk that outlives --client-timeout is abandoned and sent again,
// as a client with that timeout would. It reports the write throughput, the bulk and
// read latencies, and the noops, retries and timeouts.
func cmdWriteload(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("writeload", flag.ContinueOnError)
	fs.SetOutput(stderr)
	bin := fs.String("sl-bin", "", "searchlight binary to start (or --sl-url)")
	slURL := fs.String("sl-url", "", "a running node's URL (with --sl-token) instead of --sl-bin")
	slToken := fs.String("sl-token", "", "the running node's write token")
	dir := fs.String("dir", "writeload", "directory for the node's files (with --sl-bin)")
	storeURL := fs.String("store-url", "", "the node's store_url (empty: SQLite in --dir)")
	procs := fs.Int("gomaxprocs", 1, "GOMAXPROCS of the node (with --sl-bin)")
	docsFlag := fs.String("docs", "150k", "listings preloaded")
	bulk := fs.Int("bulk", 500, "documents per measured bulk")
	duration := fs.Duration("duration", 60*time.Second, "how long the measured load runs")
	writers := fs.Int("writers", 1, "concurrent bulk writers")
	changed := fs.Float64("changed", 0.5, "share of a bulk's documents changed (the rest are sent as stored)")
	readers := fs.Int("readers", 2, "concurrent readers (counts and facet searches)")
	readPause := fs.Duration("read-pause", 100*time.Millisecond, "pause between one reader's reads")
	readParams := fs.String("read-params", "", "query string added to every read, such as max_staleness=5s")
	clientTimeout := fs.Duration("client-timeout", 10*time.Second, "a bulk abandoned and sent again past this (0: wait)")
	percolate := fs.Bool("percolate", true, "bulks percolate with matches and inner hits")
	settings := fs.String("settings", "", "further node settings, comma-separated name=value")
	seed := fs.Uint64("seed", 1, "random seed")
	out := fs.String("json", "", "write the result as JSON to this file too")
	profile := fs.String("cpuprofile", "", "write a CPU profile of the node during the measured load to this file (with --sl-bin)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ns, err := parseInts(*docsFlag)
	if err != nil || len(ns) != 1 {
		return fmt.Errorf("--docs: one count, like 150k: %w", err)
	}
	n := ns[0]

	base, token, admin := *slURL, *slToken, ""
	if base == "" {
		if *bin == "" {
			return errors.New("give --sl-bin or --sl-url")
		}
		var extra []string
		if *settings != "" {
			extra = strings.Split(*settings, ",")
		}
		abs, err := filepath.Abs(*dir)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(abs, 0o750); err != nil {
			return err
		}
		logFile, err := os.OpenFile(filepath.Join(abs, "searchlight.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		defer logFile.Close()
		node, err := slproc.Launch(ctx, slproc.Options{
			Bin: *bin, Dir: abs, StoreURL: *storeURL, Settings: append([]string{"log_level=warn", "pprof=true"}, extra...), Log: logFile,
			Env: []string{fmt.Sprintf("GOMAXPROCS=%d", *procs)},
		})
		if err == nil {
			err = node.WaitReady(ctx)
		}
		if err != nil {
			if node != nil {
				_ = node.Kill()
			}
			return fmt.Errorf("starting searchlight: %w", err)
		}
		defer func() { _ = node.Stop(nodeStopTimeout) }()
		base, token, admin = node.URL, node.Token, node.AdminURL
	}
	c := &wlClient{base: strings.TrimRight(base, "/"), token: token, http: &http.Client{}}

	fmt.Fprintf(stdout, "writeload: preloading %d listings\n", n)
	if err := c.setup(ctx, n); err != nil {
		return err
	}
	if *profile != "" && admin != "" {
		secs := max(1, int(duration.Seconds())-2)
		go func() {
			if err := fetchProfile(ctx, fmt.Sprintf("%s/debug/pprof/profile?seconds=%d", admin, secs), *profile); err != nil {
				fmt.Fprintln(stderr, "writeload: cpu profile:", err)
			}
		}()
	}
	res, err := c.run(ctx, wlRun{
		n: n, bulk: *bulk, duration: *duration, writers: *writers, changed: *changed, readers: *readers,
		readPause: *readPause, readParams: *readParams, clientTimeout: *clientTimeout, percolate: *percolate, seed: *seed,
	})
	if err != nil {
		return err
	}
	res.print(stdout)
	if *out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		return os.WriteFile(*out, b, 0o600)
	}
	return nil
}

// fetchProfile saves the profile url serves to path.
func fetchProfile(ctx context.Context, url, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

type wlClient struct {
	base, token string
	http        *http.Client
}

// do sends one request under ctx and returns its status and body.
func (c *wlClient) do(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

func (c *wlClient) must(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	status, b, err := c.do(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	if status/100 != 2 {
		return nil, fmt.Errorf("%s %s: %d %s", method, path, status, b)
	}
	return b, nil
}

const wlIndex = "listings"

// wlMapping is a scrape-bot shaped index: listing fields and a nested field of variants.
const wlMapping = `{"mapping": {"fields": {
  "source": "keyword", "key": "keyword", "title": "text", "url": "keyword", "gone": "bool",
  "brand": "keyword", "category": "keyword", "price": "number",
  "_links": "keyword_list", "_first_seen": "date", "_changed_at": "date", "_snapshot": "number",
  "_attrs": "keyword_list", "_vattrs": "keyword_list", "_derived": "keyword_list", "_digest": "keyword",
  "_v": {"type": "nested", "fields": {"_key": "text", "size": "keyword", "color": "keyword", "price": "number", "in_stock": "bool"}}}},
  "settings": {"shards": 1, "refresh_interval": "1s"}}`

// wlQueries are the saved searches every bulk percolates against.
var wlQueries = []string{
	`{"query": {"all": [{"field": "brand", "op": "eq", "value": "acme"}, {"field": "price", "op": "lt", "value": 5000}]}}`,
	`{"query": {"nested": {"path": "_v", "query": {"all": [{"field": "_v.size", "op": "eq", "value": "l"}, {"field": "_v.in_stock", "op": "eq", "value": true}, {"field": "_v.price", "op": "lt", "value": 15000}]}, "inner_hits": {"size": 5}}}}`,
	`{"query": {"field": "title", "op": "words_all", "value": ["oak", "chair"]}}`,
	`{"query": {"all": [{"field": "category", "op": "eq", "value": "tools"}, {"field": "gone", "op": "eq", "value": false}]}}`,
	`{"query": {"nested": {"path": "_v", "query": {"field": "_v.color", "op": "eq", "value": "red"}, "inner_hits": {"name": "red"}}}}`,
	`{"query": {"any": [{"field": "brand", "op": "eq", "value": "globex"}, {"field": "_attrs", "op": "has_any", "value": ["s.material", "s.finish"]}]}}`,
}

var (
	wlWords      = []string{"oak", "chair", "steel", "table", "lamp", "drill", "saw", "jacket", "helmet", "glove", "boot", "tent", "cable", "brass", "walnut", "pine", "desk", "stool", "kit", "set"}
	wlBrands     = []string{"acme", "globex", "initech", "umbrella", "hooli", "stark", "wayne", "tyrell"}
	wlCategories = []string{"tools", "furniture", "apparel", "outdoor", "lighting", "electrical"}
	wlSizes      = []string{"xs", "s", "m", "l", "xl"}
	wlColors     = []string{"red", "blue", "black", "white", "green", "grey"}
	wlAttrs      = []string{"s.material", "s.finish", "s.weight", "s.width", "s.height", "s.depth", "s.warranty", "s.origin"}
)

// wlDoc is listing i at snapshot rev, the same bytes for the same pair.
func wlDoc(i, rev int) []byte {
	r := rand.New(rand.NewPCG(uint64(i), 7)) //nolint:gosec // a benchmark's data
	title := make([]string, 6)
	for k := range title {
		title[k] = wlWords[r.IntN(len(wlWords))]
	}
	attrs := make([]string, 0, 4)
	for _, a := range wlAttrs {
		if r.IntN(2) == 0 {
			attrs = append(attrs, a)
		}
	}
	price := 500 + r.IntN(30000) + rev%7*10
	doc := map[string]any{
		"source": "shop", "key": fmt.Sprintf("k%d", i), "title": strings.Join(title, " "),
		"url":  fmt.Sprintf("https://shop.example.com/p/%d", i),
		"gone": r.IntN(20) == 0, "brand": wlBrands[r.IntN(len(wlBrands))], "category": wlCategories[r.IntN(len(wlCategories))],
		"price": price, "_links": []string{fmt.Sprintf("shop/%d", i), fmt.Sprintf("sku-%d", i*7)},
		"_first_seen": 1_700_000_000_000 + int64(i)*1000, "_changed_at": 1_760_000_000_000 + int64(rev)*60_000,
		"_snapshot": rev, "_attrs": attrs, "_vattrs": []string{"shop:73697a65", "shop:636f6c6f72"},
		"_derived": []string{"_first_seen", "_changed_at", "_snapshot", "_attrs", "_vattrs", "_v"},
	}
	variants := make([]map[string]any, 1+r.IntN(8))
	for k := range variants {
		variants[k] = map[string]any{
			"_key": fmt.Sprintf("SKU-%d-%d", i, k), "size": wlSizes[r.IntN(len(wlSizes))], "color": wlColors[r.IntN(len(wlColors))],
			"price": price + r.IntN(2000) + rev%5, "in_stock": (r.IntN(3)+rev)%3 != 0,
		}
	}
	doc["_v"] = variants
	b, _ := json.Marshal(doc)
	sum := sha256.Sum256(b)
	doc["_digest"] = hex.EncodeToString(sum[:8])
	b, _ = json.Marshal(doc)
	return b
}

// setup creates the index and its saved searches, and preloads n listings at rev 0.
func (c *wlClient) setup(ctx context.Context, n int) error {
	if _, err := c.must(ctx, http.MethodPut, "/indexes/"+wlIndex, []byte(wlMapping)); err != nil {
		return err
	}
	var b bytes.Buffer
	for k, q := range wlQueries {
		fmt.Fprintf(&b, "{\"query_upsert\": {\"id\": \"q%d\"}}\n%s\n", k, q)
	}
	if _, err := c.must(ctx, http.MethodPost, "/indexes/"+wlIndex+"/_bulk", b.Bytes()); err != nil {
		return err
	}
	const chunk = 1000
	var seq atomic.Int64
	var next atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Go(func() {
			for {
				start := int(next.Add(chunk) - chunk)
				if start >= n {
					return
				}
				var b bytes.Buffer
				for i := start; i < min(start+chunk, n); i++ {
					fmt.Fprintf(&b, "{\"upsert\": {\"id\": \"d%d\"}}\n%s\n", i, wlDoc(i, 0))
				}
				body, err := c.must(ctx, http.MethodPost, "/indexes/"+wlIndex+"/_bulk", b.Bytes())
				if err != nil {
					errs <- err
					return
				}
				var r struct{ Seq int64 }
				_ = json.Unmarshal(body, &r)
				for {
					cur := seq.Load()
					if r.Seq <= cur || seq.CompareAndSwap(cur, r.Seq) {
						break
					}
				}
			}
		})
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}
	_, err := c.must(ctx, http.MethodPost, fmt.Sprintf("/indexes/%s/_count?wait_for_seq=%d", wlIndex, seq.Load()), []byte(`{}`))
	return err
}

type wlRun struct {
	n, bulk, writers, readers int
	duration, readPause       time.Duration
	clientTimeout             time.Duration
	changed                   float64
	readParams                string
	percolate                 bool
	seed                      uint64
}

// wlResult is a run's numbers.
type wlResult struct {
	Seconds      float64 `json:"seconds"`
	Docs         int64   `json:"docs"`
	DocsPerSec   float64 `json:"docs_per_sec"`
	Bulks        int     `json:"bulks"`
	BulkP50MS    float64 `json:"bulk_p50_ms"`
	BulkP95MS    float64 `json:"bulk_p95_ms"`
	BulkMaxMS    float64 `json:"bulk_max_ms"`
	Retries      int64   `json:"retries"`
	Noops        int64   `json:"noops"`
	Reads        int     `json:"reads"`
	ReadP50MS    float64 `json:"read_p50_ms"`
	ReadP95MS    float64 `json:"read_p95_ms"`
	ReadP99MS    float64 `json:"read_p99_ms"`
	ReadMaxMS    float64 `json:"read_max_ms"`
	ReadErrors   int64   `json:"read_errors"`
	ReadTimeouts int64   `json:"read_timeouts"`
}

func (r *wlResult) print(w io.Writer) {
	fmt.Fprintf(w, "writes: %d docs in %.1f s = %.0f docs/s; %d bulks, p50 %.0f ms, p95 %.0f ms, max %.0f ms; %d retried after the client timeout; %d noops\n",
		r.Docs, r.Seconds, r.DocsPerSec, r.Bulks, r.BulkP50MS, r.BulkP95MS, r.BulkMaxMS, r.Retries, r.Noops)
	fmt.Fprintf(w, "reads:  %d, p50 %.1f ms, p95 %.1f ms, p99 %.1f ms, max %.0f ms; %d errors, %d over 10 s\n",
		r.Reads, r.ReadP50MS, r.ReadP95MS, r.ReadP99MS, r.ReadMaxMS, r.ReadErrors, r.ReadTimeouts)
}

// readTimeout is a reader's client timeout, as a dashboard's would be.
const readTimeout = 10 * time.Second

// run sends the measured load and returns its numbers.
func (c *wlClient) run(ctx context.Context, o wlRun) (*wlResult, error) {
	revs := make([]atomic.Int64, o.n)
	stop := make(chan struct{})
	var mu sync.Mutex
	var bulkLat, readLat []time.Duration
	var docs, retries, noops, readErrs, readTimeouts atomic.Int64
	path := "/indexes/" + wlIndex + "/_bulk"
	if o.percolate {
		path += "?percolate=true&percolate_matches=true&percolate_inner_hits=true"
	}
	var wg sync.WaitGroup
	errs := make(chan error, o.writers)
	for w := range o.writers {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(o.seed, uint64(w))) //nolint:gosec // a benchmark's choice of documents
			for {
				select {
				case <-stop:
					return
				default:
				}
				var b bytes.Buffer
				picked := map[int]bool{}
				for len(picked) < o.bulk {
					i := r.IntN(o.n)
					if picked[i] {
						continue
					}
					picked[i] = true
					rev := revs[i].Load()
					if r.Float64() < o.changed {
						rev = revs[i].Add(1)
					}
					fmt.Fprintf(&b, "{\"upsert\": {\"id\": \"d%d\"}}\n%s\n", i, wlDoc(i, int(rev)))
				}
				start := time.Now()
				var body []byte
				for {
					rctx, cancel := ctx, context.CancelFunc(func() {})
					if o.clientTimeout > 0 {
						rctx, cancel = context.WithTimeout(ctx, o.clientTimeout)
					}
					status, got, err := c.do(rctx, http.MethodPost, path, b.Bytes())
					cancel()
					if err != nil && rctx.Err() != nil && ctx.Err() == nil {
						retries.Add(1)
						continue
					}
					if err != nil {
						errs <- err
						return
					}
					if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
						retries.Add(1)
						time.Sleep(100 * time.Millisecond)
						continue
					}
					if status != http.StatusOK {
						errs <- fmt.Errorf("bulk: %d %s", status, got)
						return
					}
					body = got
					break
				}
				d := time.Since(start)
				var res struct {
					Items []struct {
						Result string `json:"result"`
					} `json:"items"`
				}
				_ = json.Unmarshal(body, &res)
				for _, it := range res.Items {
					if it.Result == "noop" {
						noops.Add(1)
					}
				}
				docs.Add(int64(o.bulk))
				mu.Lock()
				bulkLat = append(bulkLat, d)
				mu.Unlock()
			}
		})
	}
	reads := []struct{ path, body string }{
		{"/indexes/" + wlIndex + "/_count", `{"query": {"field": "brand", "op": "eq", "value": "acme"}}`},
		{"/indexes/" + wlIndex + "/_search", `{"size": 0, "query": {"field": "category", "op": "eq", "value": "tools"}, "aggs": {
			"brands": {"terms": {"field": "brand"}},
			"sizes": {"terms": {"field": "_v.size", "where": {"field": "_v.in_stock", "op": "eq", "value": true}}},
			"price": {"stats": {"field": "price"}}}}`},
		{"/indexes/" + wlIndex + "/_count", `{"query": {"nested": {"path": "_v", "query": {"all": [{"field": "_v.size", "op": "eq", "value": "m"}, {"field": "_v.in_stock", "op": "eq", "value": true}]}}}}`},
	}
	for k := range o.readers {
		wg.Go(func() {
			for i := k; ; i++ {
				select {
				case <-stop:
					return
				case <-time.After(o.readPause):
				}
				rd := reads[i%len(reads)]
				p := rd.path
				if o.readParams != "" {
					p += "?" + o.readParams
				}
				rctx, cancel := context.WithTimeout(ctx, readTimeout)
				start := time.Now()
				status, _, err := c.do(rctx, http.MethodPost, p, []byte(rd.body))
				d := time.Since(start)
				timedOut := rctx.Err() != nil
				cancel()
				switch {
				case timedOut:
					readTimeouts.Add(1)
					d = readTimeout
				case err != nil || status != http.StatusOK:
					readErrs.Add(1)
					continue
				}
				mu.Lock()
				readLat = append(readLat, d)
				mu.Unlock()
			}
		})
	}
	began := time.Now()
	select {
	case <-time.After(o.duration):
	case err := <-errs:
		close(stop)
		wg.Wait()
		return nil, err
	case <-ctx.Done():
	}
	close(stop)
	wg.Wait()
	select {
	case err := <-errs:
		return nil, err
	default:
	}
	secs := time.Since(began).Seconds()
	res := &wlResult{
		Seconds: secs, Docs: docs.Load(), DocsPerSec: float64(docs.Load()) / secs, Bulks: len(bulkLat),
		Retries: retries.Load(), Noops: noops.Load(), Reads: len(readLat), ReadErrors: readErrs.Load(), ReadTimeouts: readTimeouts.Load(),
	}
	res.BulkP50MS, res.BulkP95MS, _, res.BulkMaxMS = percentiles(bulkLat)
	res.ReadP50MS, res.ReadP95MS, res.ReadP99MS, res.ReadMaxMS = percentiles(readLat)
	return res, nil
}

// percentiles are d's p50, p95, p99 and max in milliseconds.
func percentiles(d []time.Duration) (p50, p95, p99, top float64) {
	if len(d) == 0 {
		return 0, 0, 0, 0
	}
	slices.Sort(d)
	at := func(q float64) float64 {
		return float64(d[min(len(d)-1, int(q*float64(len(d))))].Microseconds()) / 1000
	}
	return at(0.50), at(0.95), at(0.99), float64(d[len(d)-1].Microseconds()) / 1000
}
