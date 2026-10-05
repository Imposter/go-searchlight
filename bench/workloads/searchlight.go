package workloads

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Imposter/go-searchlight/bench/datasets"
	"github.com/Imposter/go-searchlight/bench/report"
)

// SearchlightOptions configure a [Searchlight] engine.
type SearchlightOptions struct {
	URL, Token string
	// DiskPaths are the directories holding the node's data (data_dir and, for
	// SQLite, the database file's directory), summed for the disk footprint.
	DiskPaths []string
	// PID, when set, returns the node's process id now (a restart changes it): on
	// Linux the resident set is read from /proc; otherwise, or when it returns 0, from
	// the node's /metrics (process_resident_memory_bytes).
	PID func() int
	// Config describes the node's configuration for the report (store, durability,
	// refresh interval, ...).
	Config map[string]string
	// QueryWriters is how many saved queries are PUT at once (default 32).
	QueryWriters int
	// Log receives a line for every 429/503 retry and every request that ultimately
	// fails (nil discards them; the suite passes its own log so they land next to
	// "loading ... into searchlight" instead of vanishing behind a bare
	// "context canceled").
	Log io.Writer
}

// Searchlight is the Searchlight engine, over its HTTP API.
type Searchlight struct {
	c    *client
	opts SearchlightOptions
	// seq is the newest seq any write answered, per index: Refresh waits for it.
	mu  sync.Mutex
	seq map[string]int64
}

// NewSearchlight returns the engine at opts.URL.
func NewSearchlight(opts SearchlightOptions) *Searchlight {
	if opts.QueryWriters <= 0 {
		opts.QueryWriters = 32
	}
	return &Searchlight{c: newClient(opts.URL, opts.Token, opts.Log), opts: opts, seq: map[string]int64{}}
}

// Name implements Engine.
func (*Searchlight) Name() string { return "searchlight" }

func (s *Searchlight) noteSeq(index string, seq int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq > s.seq[index] {
		s.seq[index] = seq
	}
}

func (s *Searchlight) lastSeq(index string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq[index]
}

func ipath(index string, rest ...string) string {
	p := "/indexes/" + url.PathEscape(index)
	for _, r := range rest {
		p += "/" + r
	}
	return p
}

// Info implements Engine.
func (s *Searchlight) Info(ctx context.Context) (report.EngineInfo, error) {
	info := report.EngineInfo{Name: s.Name(), URL: s.opts.URL, Config: map[string]string{}}
	for k, v := range s.opts.Config {
		info.Config[k] = v
	}
	var nodes struct {
		Nodes []struct {
			ID, Version string
			Self        bool
		} `json:"nodes"`
	}
	var raw json.RawMessage
	if err := s.c.json(ctx, http.MethodGet, "/_cluster/nodes", nil, &raw); err != nil {
		return info, err
	}
	// The node list is {"nodes": [...]} or a bare list, depending on the coordinator.
	if json.Unmarshal(raw, &nodes) != nil || len(nodes.Nodes) == 0 {
		_ = json.Unmarshal(raw, &nodes.Nodes)
	}
	for _, n := range nodes.Nodes {
		if n.Self || info.Version == "" {
			info.Version = n.Version
		}
	}
	info.Config["nodes"] = strconv.Itoa(len(nodes.Nodes))
	return info, nil
}

// Ready implements Engine.
func (s *Searchlight) Ready(ctx context.Context) error {
	return waitReady(ctx, func(ctx context.Context) error {
		_, err := s.c.do(ctx, http.MethodGet, "/readyz", "", nil)
		return err
	})
}

// CreateIndex implements Engine.
func (s *Searchlight) CreateIndex(ctx context.Context, index string, fields []datasets.Field, shards int) error {
	if err := s.DeleteIndex(ctx, index); err != nil {
		return err
	}
	body := datasets.SearchlightMapping(fields, shards, "1s")
	// An index being dropped may still hold its name for a moment.
	deadline := time.Now().Add(time.Minute)
	for {
		err := s.c.json(ctx, http.MethodPut, ipath(index), body, nil)
		var st *StatusError
		if err == nil || !errors.As(err, &st) || st.Status != http.StatusConflict || time.Now().After(deadline) {
			s.mu.Lock()
			delete(s.seq, index)
			s.mu.Unlock()
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// CreatePercolatorIndex implements Engine: saved queries live on an index with the
// documents' mapping.
func (s *Searchlight) CreatePercolatorIndex(ctx context.Context, index string, fields []datasets.Field, shards int) error {
	return s.CreateIndex(ctx, index, fields, shards)
}

// DeleteIndex implements Engine.
func (s *Searchlight) DeleteIndex(ctx context.Context, index string) error {
	_, err := s.c.do(ctx, http.MethodDelete, ipath(index), "", nil, http.StatusOK, http.StatusAccepted, http.StatusNoContent, http.StatusNotFound)
	return err
}

type slBulkResponse struct {
	Seq    int64 `json:"seq"`
	Errors bool  `json:"errors"`
	Items  []struct {
		ID      string          `json:"id"`
		Status  int             `json:"status"`
		Queries []string        `json:"queries"`
		Error   json.RawMessage `json:"error"`
	} `json:"items"`
	Percolated *bool `json:"percolated"`
}

func slBulkBody(docs []Doc) []byte {
	var b bytes.Buffer
	b.Grow(len(docs) * 1024)
	for _, d := range docs {
		b.WriteString(`{"upsert":{"id":`)
		id, _ := json.Marshal(d.ID)
		b.Write(id)
		b.WriteString("}}\n")
		b.Write(d.Body)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

func (s *Searchlight) bulk(ctx context.Context, index string, docs []Doc, query string) (*slBulkResponse, error) {
	body, err := s.c.do(ctx, http.MethodPost, ipath(index, "_bulk")+query, "application/x-ndjson", slBulkBody(docs))
	if err != nil {
		return nil, err
	}
	var res slBulkResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("searchlight bulk: %w", err)
	}
	s.noteSeq(index, res.Seq)
	if res.Errors {
		for _, it := range res.Items {
			if len(it.Error) > 0 {
				return &res, fmt.Errorf("searchlight bulk: %s: HTTP %d: %s", it.ID, it.Status, it.Error)
			}
		}
		return &res, errors.New("searchlight bulk: errors")
	}
	return &res, nil
}

// Bulk implements Engine.
func (s *Searchlight) Bulk(ctx context.Context, index string, docs []Doc, refresh string) error {
	q := ""
	if refresh != "" {
		q = "?refresh=" + refresh
	}
	_, err := s.bulk(ctx, index, docs, q)
	return err
}

// BulkPercolate implements Engine with one _bulk?percolate=true request.
func (s *Searchlight) BulkPercolate(ctx context.Context, index string, docs []Doc) ([][]string, error) {
	res, err := s.bulk(ctx, index, docs, "?percolate=true")
	if err != nil {
		return nil, err
	}
	if res.Percolated != nil && !*res.Percolated {
		return nil, errors.New("searchlight bulk: the percolation did not finish")
	}
	out := make([][]string, len(res.Items))
	for i, it := range res.Items {
		out[i] = it.Queries
	}
	return out, nil
}

// Refresh implements Engine: a count that waits for the newest seq written.
func (s *Searchlight) Refresh(ctx context.Context, index string) error {
	_, err := s.c.do(ctx, http.MethodPost, ipath(index, "_count")+"?wait_for_seq="+strconv.FormatInt(s.lastSeq(index), 10), "application/json", []byte(`{}`))
	return err
}

// Count implements Engine.
func (s *Searchlight) Count(ctx context.Context, index string) (int64, error) {
	var res struct {
		Count int64 `json:"count"`
	}
	err := s.c.json(ctx, http.MethodPost, ipath(index, "_count"), []byte(`{}`), &res)
	return res.Count, err
}

// slPrepared is a search body, decoded once so a cursor can be set.
type slPrepared struct {
	raw  []byte
	keys map[string]json.RawMessage
}

func (*slPrepared) isPrepared() {}

// Prepare implements Engine: Searchlight takes the body as it is.
func (*Searchlight) Prepare(search []byte) (Prepared, error) {
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(search, &keys); err != nil {
		return nil, fmt.Errorf("searchlight: a search body: %w", err)
	}
	return &slPrepared{raw: search, keys: keys}, nil
}

type slSearchResponse struct {
	Total struct {
		Value    int64  `json:"value"`
		Relation string `json:"relation"`
	} `json:"total"`
	Hits []struct {
		ID string `json:"id"`
	} `json:"hits"`
	Next []any           `json:"next"`
	Aggs json.RawMessage `json:"aggs"`
}

// Search implements Engine.
func (s *Searchlight) Search(ctx context.Context, index string, p Prepared, after []any) (SearchResult, error) {
	sp, ok := p.(*slPrepared)
	if !ok {
		return SearchResult{}, fmt.Errorf("searchlight: a search prepared by another engine (%T)", p)
	}
	body := sp.raw
	if after != nil {
		keys := make(map[string]json.RawMessage, len(sp.keys)+1)
		for k, v := range sp.keys {
			keys[k] = v
		}
		a, err := json.Marshal(after)
		if err != nil {
			return SearchResult{}, err
		}
		keys["search_after"] = a
		if body, err = json.Marshal(keys); err != nil {
			return SearchResult{}, err
		}
	}
	b, err := s.c.do(ctx, http.MethodPost, ipath(index, "_search"), "application/json", body)
	if err != nil {
		return SearchResult{}, err
	}
	var res slSearchResponse
	if err := json.Unmarshal(b, &res); err != nil {
		return SearchResult{}, fmt.Errorf("searchlight search: %w", err)
	}
	out := SearchResult{Total: res.Total.Value, Relation: res.Total.Relation, Next: res.Next, Aggs: res.Aggs}
	out.IDs = make([]string, len(res.Hits))
	for i, h := range res.Hits {
		out.IDs[i] = h.ID
	}
	return out, nil
}

// PutQueries implements Engine: one PUT per saved query, QueryWriters at once (the API
// has no bulk endpoint for saved queries).
func (s *Searchlight) PutQueries(ctx context.Context, index string, qs []datasets.SavedSearch) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Int64
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	for range s.opts.QueryWriters {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(qs) || ctx.Err() != nil {
					return
				}
				var res struct {
					Seq int64 `json:"seq"`
				}
				body := map[string]any{"query": qs[i].Query, "meta": qs[i].Meta}
				if err := s.c.json(ctx, http.MethodPut, ipath(index, "queries", url.PathEscape(qs[i].ID)), body, &res); err != nil {
					once.Do(func() { first = fmt.Errorf("saved query %s: %w", qs[i].ID, err); cancel() })
					return
				}
				s.noteSeq(index, res.Seq)
			}
		})
	}
	wg.Wait()
	if first != nil {
		return first
	}
	return s.Refresh(ctx, index)
}

// Percolate implements Engine.
func (s *Searchlight) Percolate(ctx context.Context, index string, docs []json.RawMessage) ([][]string, error) {
	var res struct {
		Results []struct {
			Queries []string `json:"queries"`
		} `json:"results"`
	}
	if err := s.c.json(ctx, http.MethodPost, ipath(index, "_percolate"), map[string]any{"docs": docs}, &res); err != nil {
		return nil, err
	}
	if len(res.Results) != len(docs) {
		return nil, fmt.Errorf("searchlight percolate: %d results for %d documents", len(res.Results), len(docs))
	}
	out := make([][]string, len(docs))
	for i, r := range res.Results {
		out[i] = r.Queries
	}
	return out, nil
}

// Resources implements Engine.
func (s *Searchlight) Resources(ctx context.Context, _ string) (report.Resources, error) {
	var r report.Resources
	if len(s.opts.DiskPaths) > 0 {
		n, err := pathsSize(s.opts.DiskPaths)
		if err != nil {
			return r, err
		}
		r.DiskBytes, r.DiskSource = n, "du "+strings.Join(s.opts.DiskPaths, " + ")
	} else {
		r.DiskSource = "not measured (no --sl-disk paths)"
	}
	if s.opts.PID != nil {
		if pid := s.opts.PID(); pid > 0 {
			if rss := procRSS(pid); rss > 0 {
				r.RSSBytes, r.RSSSource = rss, "/proc/<pid>/status VmRSS"
				return r, nil
			}
		}
	}
	body, err := s.c.do(ctx, http.MethodGet, "/metrics", "", nil)
	if err != nil {
		r.RSSSource = "not measured: " + err.Error()
		return r, nil //nolint:nilerr // best-effort footprint: the failure is recorded in RSSSource, not fatal
	}
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(sc.Text(), "process_resident_memory_bytes "); ok {
			if f, err := strconv.ParseFloat(strings.TrimSpace(rest), 64); err == nil {
				r.RSSBytes, r.RSSSource = int64(f), "/metrics process_resident_memory_bytes"
			}
		}
	}
	if r.RSSBytes == 0 {
		r.RSSSource = "not measured (no process_resident_memory_bytes in /metrics)"
	}
	return r, nil
}

// dirSize sums the sizes of the regular files under root (or root itself).
func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil // removed while walking (a merged segment)
				}
				return err
			}
			total += info.Size()
		}
		return nil
	})
	return total, err
}
