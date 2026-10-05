package workloads

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/Imposter/go-searchlight/bench/datasets"
	"github.com/Imposter/go-searchlight/bench/es"
	"github.com/Imposter/go-searchlight/bench/report"
)

// ElasticsearchOptions configure an [Elasticsearch] engine.
type ElasticsearchOptions struct {
	URL string
	// Fields is the mapping the translator reads (the documents' fields).
	Fields []datasets.Field
	// PID, when set on Linux, reads the resident set of the Elasticsearch JVM from
	// /proc (docker inspect -f '{{.State.Pid}}' gives it); otherwise it is the
	// container's cgroup memory as the node reports it, or else its JVM heap and
	// non-heap committed.
	PID int
	// Config describes the node's configuration for the report.
	Config map[string]string
	// QueryBatch is how many saved queries go in one _bulk (default 1000).
	QueryBatch int
	// Log receives a line for every 429/503 retry and every request that
	// ultimately fails (nil discards them).
	Log io.Writer
}

// Elasticsearch is the Elasticsearch 8.x engine, over its REST API, with requests
// translated by package es.
type Elasticsearch struct {
	c    *client
	opts ElasticsearchOptions
	tr   *es.Translator
}

// NewElasticsearch returns the engine at opts.URL.
func NewElasticsearch(opts ElasticsearchOptions) *Elasticsearch {
	if opts.QueryBatch <= 0 {
		opts.QueryBatch = 1000
	}
	if opts.Fields == nil {
		opts.Fields = datasets.Products
	}
	return &Elasticsearch{c: newClient(opts.URL, "", opts.Log), opts: opts, tr: es.NewTranslator(opts.Fields)}
}

// Name implements Engine.
func (*Elasticsearch) Name() string { return "elasticsearch" }

// Info implements Engine.
func (e *Elasticsearch) Info(ctx context.Context) (report.EngineInfo, error) {
	info := report.EngineInfo{Name: e.Name(), URL: e.opts.URL, Config: map[string]string{}}
	for k, v := range e.opts.Config {
		info.Config[k] = v
	}
	var root struct {
		Version struct {
			Number      string `json:"number"`
			BuildFlavor string `json:"build_flavor"`
			Lucene      string `json:"lucene_version"`
		} `json:"version"`
	}
	if err := e.c.json(ctx, http.MethodGet, "/", nil, &root); err != nil {
		return info, err
	}
	info.Version = root.Version.Number
	info.Config["lucene"] = root.Version.Lucene
	var nodes struct {
		Nodes map[string]struct {
			JVM struct {
				Version string `json:"version"`
				Mem     struct {
					HeapMax int64 `json:"heap_max_in_bytes"`
				} `json:"mem"`
			} `json:"jvm"`
			OS struct {
				AvailableProcessors int `json:"available_processors"`
			} `json:"os"`
		} `json:"nodes"`
	}
	if err := e.c.json(ctx, http.MethodGet, "/_nodes/jvm,os", nil, &nodes); err == nil {
		for _, n := range nodes.Nodes {
			info.Config["jvm"] = n.JVM.Version
			info.Config["heap_max"] = formatBytes(n.JVM.Mem.HeapMax)
			info.Config["processors"] = strconv.Itoa(n.OS.AvailableProcessors)
		}
		info.Config["nodes"] = strconv.Itoa(len(nodes.Nodes))
	}
	info.Config["durability"] = "index.translog.durability=request (fsync per request)"
	info.Config["refresh_interval"] = "1s"
	info.Config["request_cache"] = "off per request (request_cache=false)"
	return info, nil
}

// Ready implements Engine.
func (e *Elasticsearch) Ready(ctx context.Context) error {
	return waitReady(ctx, func(ctx context.Context) error {
		_, err := e.c.do(ctx, http.MethodGet, "/_cluster/health?wait_for_status=yellow&timeout=5s", "", nil)
		return err
	})
}

func epath(index, rest string) string { return "/" + url.PathEscape(index) + rest }

func (e *Elasticsearch) create(ctx context.Context, index string, body any) error {
	if err := e.DeleteIndex(ctx, index); err != nil {
		return err
	}
	if err := e.c.json(ctx, http.MethodPut, epath(index, ""), body, nil); err != nil {
		return err
	}
	_, err := e.c.do(ctx, http.MethodGet, "/_cluster/health/"+url.PathEscape(index)+"?wait_for_status=green&timeout=60s", "", nil)
	return err
}

// CreateIndex implements Engine.
func (e *Elasticsearch) CreateIndex(ctx context.Context, index string, fields []datasets.Field, shards int) error {
	return e.create(ctx, index, es.IndexBody(fields, shards))
}

// CreatePercolatorIndex implements Engine.
func (e *Elasticsearch) CreatePercolatorIndex(ctx context.Context, index string, fields []datasets.Field, shards int) error {
	return e.create(ctx, index, es.PercolatorIndexBody(fields, shards))
}

// DeleteIndex implements Engine.
func (e *Elasticsearch) DeleteIndex(ctx context.Context, index string) error {
	_, err := e.c.do(ctx, http.MethodDelete, epath(index, ""), "", nil, http.StatusOK, http.StatusNotFound)
	return err
}

// withID inserts the sl_id field (the sort tie-breaker) into a document object.
func withID(body json.RawMessage, id string) []byte {
	b := bytes.TrimSpace(body)
	idJSON, _ := json.Marshal(id)
	out := make([]byte, 0, len(b)+len(idJSON)+16)
	out = append(out, `{"`+es.IDField+`":`...)
	out = append(out, idJSON...)
	rest := bytes.TrimSpace(b[1:])
	if len(rest) > 0 && rest[0] != '}' {
		out = append(out, ',')
	}
	return append(out, rest...)
}

type esBulkResponse struct {
	Errors bool `json:"errors"`
	Items  []map[string]struct {
		ID     string          `json:"_id"`
		Status int             `json:"status"`
		Error  json.RawMessage `json:"error"`
	} `json:"items"`
}

func (e *Elasticsearch) bulkBody(ctx context.Context, index string, body []byte, refresh string) error {
	q := ""
	if refresh != "" {
		q = "?refresh=" + refresh
	}
	b, err := e.c.do(ctx, http.MethodPost, epath(index, "/_bulk")+q, "application/x-ndjson", body)
	if err != nil {
		return err
	}
	var res esBulkResponse
	if err := json.Unmarshal(b, &res); err != nil {
		return fmt.Errorf("elasticsearch bulk: %w", err)
	}
	if res.Errors {
		for _, it := range res.Items {
			for _, r := range it {
				if len(r.Error) > 0 {
					return fmt.Errorf("elasticsearch bulk: %s: HTTP %d: %s", r.ID, r.Status, r.Error)
				}
			}
		}
		return errors.New("elasticsearch bulk: errors")
	}
	return nil
}

// Bulk implements Engine.
func (e *Elasticsearch) Bulk(ctx context.Context, index string, docs []Doc, refresh string) error {
	var b bytes.Buffer
	b.Grow(len(docs) * 1100)
	for _, d := range docs {
		id, _ := json.Marshal(d.ID)
		b.WriteString(`{"index":{"_id":`)
		b.Write(id)
		b.WriteString("}}\n")
		b.Write(withID(d.Body, d.ID))
		b.WriteByte('\n')
	}
	return e.bulkBody(ctx, index, b.Bytes(), refresh)
}

// BulkPercolate implements Engine: Elasticsearch has no write-and-percolate request,
// so it is a _bulk followed by a percolation of the same documents.
func (e *Elasticsearch) BulkPercolate(ctx context.Context, index string, docs []Doc) ([][]string, error) {
	if err := e.Bulk(ctx, index, docs, ""); err != nil {
		return nil, err
	}
	bodies := make([]json.RawMessage, len(docs))
	for i, d := range docs {
		bodies[i] = d.Body
	}
	return e.Percolate(ctx, index, bodies)
}

// Refresh implements Engine.
func (e *Elasticsearch) Refresh(ctx context.Context, index string) error {
	_, err := e.c.do(ctx, http.MethodPost, epath(index, "/_refresh"), "", nil)
	return err
}

// Count implements Engine.
func (e *Elasticsearch) Count(ctx context.Context, index string) (int64, error) {
	var res struct {
		Count int64 `json:"count"`
	}
	err := e.c.json(ctx, http.MethodGet, epath(index, "/_count"), nil, &res)
	return res.Count, err
}

type esPrepared struct {
	body map[string]any
	raw  []byte
}

func (*esPrepared) isPrepared() {}

// Prepare implements Engine: the Searchlight body translated to Elasticsearch's DSL.
func (e *Elasticsearch) Prepare(search []byte) (Prepared, error) {
	body, err := e.tr.Search(search)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &esPrepared{body: body, raw: raw}, nil
}

type esSearchResponse struct {
	Hits struct {
		Total *struct {
			Value    int64  `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []struct {
			ID     string              `json:"_id"`
			Sort   []any               `json:"sort"`
			Fields map[string][]string `json:"fields"`
		} `json:"hits"`
	} `json:"hits"`
	Aggs json.RawMessage `json:"aggregations"`
}

// Search implements Engine. Every search skips the shard request cache.
func (e *Elasticsearch) Search(ctx context.Context, index string, p Prepared, after []any) (SearchResult, error) {
	ep, ok := p.(*esPrepared)
	if !ok {
		return SearchResult{}, fmt.Errorf("elasticsearch: a search prepared by another engine (%T)", p)
	}
	raw := ep.raw
	if after != nil {
		body := make(map[string]any, len(ep.body)+1)
		for k, v := range ep.body {
			body[k] = v
		}
		body["search_after"] = after
		var err error
		if raw, err = json.Marshal(body); err != nil {
			return SearchResult{}, err
		}
	}
	b, err := e.c.do(ctx, http.MethodPost, epath(index, "/_search?request_cache=false"), "application/json", raw)
	if err != nil {
		return SearchResult{}, err
	}
	var res esSearchResponse
	if err := json.Unmarshal(b, &res); err != nil {
		return SearchResult{}, fmt.Errorf("elasticsearch search: %w", err)
	}
	out := SearchResult{Aggs: res.Aggs}
	if res.Hits.Total != nil {
		out.Total, out.Relation = res.Hits.Total.Value, res.Hits.Total.Relation
	}
	out.IDs = make([]string, len(res.Hits.Hits))
	for i, h := range res.Hits.Hits {
		out.IDs[i] = h.ID
	}
	size, _ := ep.body["size"].(int)
	if n := len(res.Hits.Hits); n > 0 && n >= size {
		out.Next = res.Hits.Hits[n-1].Sort
	}
	return out, nil
}

// PutQueries implements Engine: _bulk batches into the percolator index, then a refresh.
func (e *Elasticsearch) PutQueries(ctx context.Context, index string, qs []datasets.SavedSearch) error {
	var b bytes.Buffer
	for start := 0; start < len(qs); start += e.opts.QueryBatch {
		b.Reset()
		for _, q := range qs[start:min(start+e.opts.QueryBatch, len(qs))] {
			raw, err := json.Marshal(q.Query)
			if err != nil {
				return err
			}
			meta, err := json.Marshal(q.Meta)
			if err != nil {
				return err
			}
			doc, err := e.tr.PercolatorDoc(q.ID, raw, meta)
			if err != nil {
				return err
			}
			body, err := json.Marshal(doc)
			if err != nil {
				return err
			}
			id, _ := json.Marshal(q.ID)
			b.WriteString(`{"index":{"_id":`)
			b.Write(id)
			b.WriteString("}}\n")
			b.Write(body)
			b.WriteByte('\n')
		}
		if err := e.bulkBody(ctx, index, b.Bytes(), ""); err != nil {
			return err
		}
	}
	return e.Refresh(ctx, index)
}

// percolatePage is the most matching queries one percolate request returns.
const percolatePage = 10_000

// Percolate implements Engine: one percolate query for the batch, paged by query id
// when more queries match than one page holds.
func (e *Elasticsearch) Percolate(ctx context.Context, index string, docs []json.RawMessage) ([][]string, error) {
	out := make([][]string, len(docs))
	body := es.PercolateBody(docs, percolatePage)
	var after []any
	for {
		if after != nil {
			body["sort"] = []any{map[string]any{es.QueryIDField: "asc"}}
			body["search_after"] = after
		}
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		b, err := e.c.do(ctx, http.MethodPost, epath(index, "/_search?request_cache=false"), "application/json", raw)
		if err != nil {
			return nil, err
		}
		var res struct {
			Hits struct {
				Total struct {
					Value int64 `json:"value"`
				} `json:"total"`
				Hits []struct {
					ID     string `json:"_id"`
					Sort   []any  `json:"sort"`
					Fields struct {
						Slots []int `json:"_percolator_document_slot"`
					} `json:"fields"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if err := json.Unmarshal(b, &res); err != nil {
			return nil, fmt.Errorf("elasticsearch percolate: %w", err)
		}
		hits := res.Hits.Hits
		if after == nil && int64(len(hits)) < res.Hits.Total.Value {
			// More matches than a page: start over in qid order to page through them.
			out = make([][]string, len(docs))
			after = []any{""}
			continue
		}
		for _, h := range hits {
			slots := h.Fields.Slots
			if len(slots) == 0 && len(docs) == 1 {
				slots = []int{0}
			}
			for _, s := range slots {
				if s >= 0 && s < len(out) {
					out[s] = append(out[s], h.ID)
				}
			}
		}
		if after == nil || len(hits) < percolatePage {
			return out, nil
		}
		after = hits[len(hits)-1].Sort
	}
}

// Resources implements Engine. Both disk and RSS are the single node Searchlight's
// own equivalent footprint is: primaries only (never replicas' bytes, so a transient
// replica -- bench/docker-compose.es.yml's second node, --es-recovery -- never
// inflates T6's numbers), and _local (this connected node, es1, not the whole
// cluster: with a second node, /_nodes/stats answers for both, and picking one of a
// map's entries would be an arbitrary, non-deterministic choice of which).
func (e *Elasticsearch) Resources(ctx context.Context, index string) (report.Resources, error) {
	var r report.Resources
	var stats struct {
		Indices map[string]struct {
			Primaries struct {
				Store struct {
					Size int64 `json:"size_in_bytes"`
				} `json:"store"`
			} `json:"primaries"`
		} `json:"indices"`
	}
	if err := e.c.json(ctx, http.MethodGet, epath(index, "/_stats/store"), nil, &stats); err != nil {
		return r, err
	}
	for _, s := range stats.Indices {
		r.DiskBytes += s.Primaries.Store.Size
	}
	r.DiskSource = "_stats/store primaries.store.size_in_bytes (this node's own copy; index files, translog excluded)"
	if rss := procRSS(e.opts.PID); rss > 0 {
		r.RSSBytes, r.RSSSource = rss, fmt.Sprintf("/proc/%d/status VmRSS (the JVM)", e.opts.PID)
		return r, nil
	}
	var ns struct {
		Nodes map[string]struct {
			OS struct {
				Cgroup struct {
					Memory struct {
						Usage string `json:"usage_in_bytes"`
					} `json:"memory"`
				} `json:"cgroup"`
			} `json:"os"`
			JVM struct {
				Mem struct {
					HeapCommitted    int64 `json:"heap_committed_in_bytes"`
					NonHeapCommitted int64 `json:"non_heap_committed_in_bytes"`
				} `json:"mem"`
			} `json:"jvm"`
		} `json:"nodes"`
	}
	if err := e.c.json(ctx, http.MethodGet, "/_nodes/_local/stats/os,jvm", nil, &ns); err != nil {
		r.RSSSource = "not measured: " + err.Error()
		return r, nil //nolint:nilerr // best-effort footprint: the failure is recorded in RSSSource, not fatal
	}
	for _, n := range ns.Nodes {
		if v, err := strconv.ParseInt(n.OS.Cgroup.Memory.Usage, 10, 64); err == nil && v > 0 {
			r.RSSBytes, r.RSSSource = v, "_nodes/_local/stats os.cgroup.memory.usage_in_bytes (this node's container, includes page cache)"
			continue
		}
		r.RSSBytes = n.JVM.Mem.HeapCommitted + n.JVM.Mem.NonHeapCommitted
		r.RSSSource = "_nodes/_local/stats JVM heap + non-heap committed (a lower bound of this node's RSS)"
	}
	return r, nil
}

// SectionSizes implements SectionSizer using Elasticsearch's _disk_usage API
// (run_expensive_tasks=true: without it, a cached estimate can be absent and the
// call returns nothing useful), aggregated to sit beside Searchlight's segment
// sections: "inverted_index" as "terms" (the term dictionary and postings together,
// as Searchlight's own "terms" section holds them), "doc_values" as "docvalues",
// "points" as "points", "stored_fields" as "stored", and norms, term vectors and
// knn vectors folded into "other" (structures Searchlight's format has no
// equivalent of). There is no analog of Searchlight's "ids" or "meta" sections:
// Elasticsearch's _id lives inside its inverted index and stored fields already
// counted above.
func (e *Elasticsearch) SectionSizes(ctx context.Context, index string) (map[string]int64, error) {
	// Only "inverted_index" nests (total_in_bytes inside it); stored_fields,
	// doc_values, points, norms, term_vectors and knn_vectors are each a flat
	// "<name>_in_bytes" sibling of a human-readable "<name>" string, not an object
	// of their own. Confirmed against a real Elasticsearch 8.15.3 response (a
	// first guess at "stored_fields" nesting the same way failed to decode it).
	var res map[string]struct {
		AllFields struct {
			InvertedIndex struct {
				TotalInBytes int64 `json:"total_in_bytes"`
			} `json:"inverted_index"`
			StoredFieldsInBytes int64 `json:"stored_fields_in_bytes"`
			DocValuesInBytes    int64 `json:"doc_values_in_bytes"`
			PointsInBytes       int64 `json:"points_in_bytes"`
			NormsInBytes        int64 `json:"norms_in_bytes"`
			TermVectorsInBytes  int64 `json:"term_vectors_in_bytes"`
			KnnVectorsInBytes   int64 `json:"knn_vectors_in_bytes"`
		} `json:"all_fields"`
	}
	if err := e.c.json(ctx, http.MethodPost, epath(index, "/_disk_usage")+"?run_expensive_tasks=true", nil, &res); err != nil {
		return nil, err
	}
	idx, ok := res[index]
	if !ok {
		return nil, fmt.Errorf("elasticsearch _disk_usage: no %q in the response", index)
	}
	af := idx.AllFields
	return map[string]int64{
		"terms":     af.InvertedIndex.TotalInBytes,
		"stored":    af.StoredFieldsInBytes,
		"docvalues": af.DocValuesInBytes,
		"points":    af.PointsInBytes,
		"other":     af.NormsInBytes + af.TermVectorsInBytes + af.KnnVectorsInBytes,
	}, nil
}

// setReplicas sets index's number_of_replicas.
func (e *Elasticsearch) setReplicas(ctx context.Context, index string, n int) error {
	body := map[string]any{"index": map[string]any{"number_of_replicas": n}}
	return e.c.json(ctx, http.MethodPut, epath(index, "/_settings"), body, nil)
}

// waitGreen polls index's cluster health until it reports green or timeout passes.
func (e *Elasticsearch) waitGreen(ctx context.Context, index string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	path := "/_cluster/health/" + url.PathEscape(index) + "?wait_for_status=green&timeout=5s"
	for {
		if _, err := e.c.do(ctx, http.MethodGet, path, "", nil); err == nil {
			return nil
		} else if time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ElasticsearchRecoverer is [Recoverer] for Elasticsearch's own peer recovery
// (target T8): bench/docker-compose.es.yml always starts a second node, so the
// baseline is measured on the same index and data [RunSuite]'s load step already
// put in place (Source returns nil: there is no separate cluster to load), by
// dropping the replica (number_of_replicas to 0, so the second node holds nothing
// of the index) and then restoring it (back to 1, which can only be satisfied by a
// full peer recovery onto whichever node does not already hold a copy), timed until
// the cluster reports green again.
type ElasticsearchRecoverer struct {
	eng *Elasticsearch
}

// NewElasticsearchRecoverer returns a [Recoverer] measuring eng's own cluster's peer
// recovery.
func NewElasticsearchRecoverer(eng *Elasticsearch) *ElasticsearchRecoverer {
	return &ElasticsearchRecoverer{eng: eng}
}

// Source implements Recoverer: nil, because recovery runs on the index and data the
// rest of the suite already loaded, not a separate source cluster.
func (*ElasticsearchRecoverer) Source() Engine { return nil }

// Recover implements Recoverer.
func (r *ElasticsearchRecoverer) Recover(ctx context.Context, index string, _ int64) (time.Duration, error) {
	e := r.eng
	if err := e.setReplicas(ctx, index, 0); err != nil {
		return 0, fmt.Errorf("elasticsearch recovery: dropping the replica: %w", err)
	}
	if err := e.waitGreen(ctx, index, 2*time.Minute); err != nil {
		return 0, fmt.Errorf("elasticsearch recovery: waiting for the drop: %w", err)
	}
	start := time.Now()
	if err := e.setReplicas(ctx, index, 1); err != nil {
		return 0, fmt.Errorf("elasticsearch recovery: adding the replica: %w", err)
	}
	if err := e.waitGreen(ctx, index, time.Hour); err != nil {
		return 0, fmt.Errorf("elasticsearch recovery: waiting for green: %w", err)
	}
	return time.Since(start), nil
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
