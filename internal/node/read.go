package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/percolate"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// waitSeq waits, under ctx's deadline, until every copy of idx has every change up
// to seq searchable. It wakes the tailers first, so a copy with no change of its own
// up to seq advances to it at once rather than at its next poll.
func (n *Single) waitSeq(ctx context.Context, idx *index, seq int64) error {
	if seq <= 0 {
		return nil
	}
	if err := n.checkHead(ctx, seq); err != nil {
		return err
	}
	for _, c := range idx.copies {
		c.tailer.Wake()
	}
	for _, c := range idx.copies {
		if err := c.notServing(); err != nil {
			return err
		}
		if err := c.waitRefreshed(ctx, seq); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				var at int64
				if sh := c.shard(); sh != nil {
					at = sh.RefreshedSeq()
				}
				return &api.Error{
					Status: http.StatusGatewayTimeout, Code: api.CodeTimeout, Err: err,
					Detail: fmt.Sprintf("seq %d was not searchable on shard %d before the deadline (it is at %d)", seq, c.id.Shard, at),
				}
			}
			if h := c.halted.Load(); h != nil {
				return api.Unavailable(*h, "shard %d of index %q has halted", c.id.Shard, idx.name)
			}
			return err
		}
	}
	return nil
}

// checkHead refuses (400) a wait_for_seq past the newest committed seq, which no
// copy would ever reach: the node's head is raised from the store's first.
func (n *Single) checkHead(ctx context.Context, seq int64) error {
	if seq <= n.head.Load() {
		return nil
	}
	if h, _, err := n.st.HeadSeq(ctx); err == nil {
		n.noteHead(h)
	}
	if head := n.head.Load(); seq > head {
		return api.InvalidAt("params.wait_for_seq", "wait_for_seq %d is past the newest committed seq, %d", seq, head)
	}
	return nil
}

// waitQueries waits until every saved query committed through this node is
// searchable on the copy that holds it, so a percolation sees the queries saved just
// before it. Only copies whose saved queries changed wait, and only until their next
// refresh; after that it is free.
func (n *Single) waitQueries(ctx context.Context, idx *index) error {
	for _, c := range idx.copies {
		q := c.querySeq.Load()
		if q == 0 {
			continue
		}
		sh := c.shard()
		if sh == nil {
			return api.Unavailable(shard.ErrClosed, "shard %d is unavailable", c.id.Shard)
		}
		if sh.RefreshedSeq() >= q {
			continue
		}
		c.tailer.Wake()
		if err := c.waitRefreshed(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// acquire holds the current generation of every copy of idx, by shard, until release.
func (n *Single) acquire(idx *index) ([]*shard.Generation, func(), error) {
	gens := make([]*shard.Generation, 0, len(idx.copies))
	release := func() {
		for _, g := range gens {
			g.Release()
		}
	}
	for _, c := range idx.copies {
		g, err := c.acquire()
		if err != nil {
			release()
			if idx.dropped.Load() {
				return nil, nil, indexNotFound(idx.name)
			}
			return nil, nil, err
		}
		gens = append(gens, g)
	}
	return gens, release, nil
}

// acquireTries bounds acquire's retries when the copy's shard is swapped under it.
const acquireTries = 4

// acquire holds the copy's current generation, checking that the copy serves both
// before and after: a tailer marks a copy recovering and then swaps in the shard it
// rebuilds, so a reader that read the state before the mark and the shard after the
// swap would hold an empty shard. After the Acquire the copy must still serve, from
// the very shard acquired; otherwise the generation is released and it tries again.
func (c *copyState) acquire() (*shard.Generation, error) {
	for range acquireTries {
		if err := c.notServing(); err != nil {
			return nil, err
		}
		sh := c.shard()
		if sh == nil {
			return nil, api.Unavailable(shard.ErrClosed, "shard %d of index %q is closed", c.id.Shard, c.id.Index)
		}
		g := sh.Acquire()
		if g == nil {
			if c.shard() != sh {
				continue // swapped: the old shard closed under the reader
			}
			return nil, api.Unavailable(shard.ErrClosed, "shard %d of index %q is closed", c.id.Shard, c.id.Index)
		}
		if c.notServing() == nil && c.shard() == sh {
			return g, nil
		}
		g.Release()
	}
	if err := c.notServing(); err != nil {
		return nil, err
	}
	return nil, api.Unavailable(shard.ErrClosed, "shard %d of index %q is being swapped; retry", c.id.Shard, c.id.Index)
}

// waitRefreshed waits until the copy has seq searchable, following a swap: the shard
// a rebuild replaces closes, and the wait goes on on its replacement.
func (c *copyState) waitRefreshed(ctx context.Context, seq int64) error {
	for {
		sh := c.shard()
		if sh == nil {
			return api.Unavailable(shard.ErrClosed, "shard %d is unavailable", c.id.Shard)
		}
		err := sh.WaitRefreshed(ctx, seq)
		if errors.Is(err, shard.ErrClosed) {
			if next := c.shard(); next != nil && next != sh {
				continue
			}
		}
		return err
	}
}

// readSpan starts a read's span.
func (n *Single) readSpan(ctx context.Context, name, index string) (context.Context, trace.Span) {
	return n.tr.Start(ctx, name, trace.WithAttributes(attribute.String(telemetry.KeyIndex, index)))
}

func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed")
	}
	span.End()
}

// Search implements [api.Coordinator]: a query-then-fetch over every shard, each
// shard's generation held from its query to its fetch.
func (n *Single) Search(ctx context.Context, name string, r *search.Request, opts api.ReadOptions) (res *api.SearchResult, err error) {
	ctx, span := n.readSpan(ctx, "node.search", name)
	defer func() { endSpan(span, err) }()
	idx, err := n.lookup(name)
	if err != nil {
		return nil, err
	}
	if problems := query.Validate(r.Query, idx.meta.Load().mapping); len(problems) > 0 {
		return nil, api.Invalid("the query is invalid", problems...)
	}
	if err := n.waitSeq(ctx, idx, opts.WaitForSeq); err != nil {
		return nil, err
	}
	gens, release, err := n.acquire(idx)
	if err != nil {
		return nil, err
	}
	defer release()
	r.Index = name
	stale := n.indexStale(idx)
	resp, err := searchGenerations(ctx, gens, r)
	if err != nil {
		return nil, err
	}
	return &api.SearchResult{Response: resp, Stale: stale}, nil
}

// searchGenerations searches gens, one per shard: in one pass with bodies when there
// is one shard; otherwise the query phase per shard (NoBodies), the reduce, then the
// fetch of the winning hits' bodies from the generations that found them.
func searchGenerations(ctx context.Context, gens []*shard.Generation, r *search.Request) (*search.Response, error) {
	if len(gens) == 1 {
		sr, err := search.ExecuteShard(ctx, gens[0], r)
		if err != nil {
			return nil, err
		}
		return search.ReduceContext(ctx, []*search.ShardResult{sr}, r), nil
	}
	q := *r
	q.NoBodies = true
	results := make([]*search.ShardResult, len(gens))
	errs := make([]error, len(gens))
	var wg sync.WaitGroup
	for i, g := range gens {
		wg.Go(func() { results[i], errs[i] = search.ExecuteShard(ctx, g, &q) })
	}
	wg.Wait()
	if err := firstError(errs); err != nil {
		return nil, err
	}
	resp := search.ReduceContext(ctx, results, &q)
	if len(resp.Hits) == 0 {
		return resp, nil
	}
	// Each segment id names one shard's segment: group the hits by the shard whose
	// generation holds their segment.
	owner := map[string]int{}
	for s, g := range gens {
		for k := range g.Segments {
			owner[g.Segments[k].ID] = s
		}
	}
	byShard := map[int][]int{}
	for i := range resp.Hits {
		h := &resp.Hits[i]
		if h.Ref == nil {
			return nil, fmt.Errorf("node: hit %q has no ref to fetch", h.ID)
		}
		s, ok := owner[h.Ref.Segment]
		if !ok {
			return nil, fmt.Errorf("%w: hit %q", search.ErrStaleHit, h.ID)
		}
		byShard[s] = append(byShard[s], i)
	}
	ferrs := make([]error, len(gens))
	for _, s := range slices.Sorted(maps.Keys(byShard)) {
		wg.Go(func() {
			pos := byShard[s]
			hits := make([]search.Hit, len(pos))
			for k, i := range pos {
				hits[k] = resp.Hits[i]
			}
			if err := search.FetchShard(ctx, gens[s], hits, r.Fields); err != nil {
				ferrs[s] = err
				return
			}
			for k, i := range pos {
				resp.Hits[i].Body = hits[k].Body
			}
		})
	}
	wg.Wait()
	if err := firstError(ferrs); err != nil {
		return nil, err
	}
	return resp, nil
}

func firstError(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Percolate implements [api.Coordinator]. Given documents are analyzed under the
// index's mapping (their new fields are not added to it: no saved query can name a
// field the mapping lacks); stored ones are read from this node's copies.
func (n *Single) Percolate(ctx context.Context, name string, req *api.PercolateRequest, opts api.ReadOptions) (res *api.PercolateResponse, err error) {
	ctx, span := n.readSpan(ctx, "node.percolate", name)
	defer func() { endSpan(span, err) }()
	idx, err := n.lookup(name)
	if err != nil {
		return nil, err
	}
	m := idx.meta.Load().mapping
	docs := make([]schema.Doc, 0, len(req.Docs)+len(req.IDs))
	out := make([]api.PercolateResult, 0, len(req.Docs)+len(req.IDs))
	for i, body := range req.Docs {
		doc, _, err := schema.Analyze(m, "_percolate_"+strconv.Itoa(i), body)
		if err != nil {
			return nil, docProblem(err, "docs."+strconv.Itoa(i))
		}
		docs = append(docs, doc)
		out = append(out, api.PercolateResult{Found: true})
	}
	if err := n.waitSeq(ctx, idx, opts.WaitForSeq); err != nil {
		return nil, err
	}
	if err := n.waitQueries(ctx, idx); err != nil {
		return nil, err
	}
	stale := n.indexStale(idx)
	// Stored documents are read from this node's copies (as searches are), not
	// realtime from the database as GET is: pass wait_for_seq to see a write.
	gens, release, err := n.acquire(idx)
	if err != nil {
		return nil, err
	}
	stored := make([]int, 0, len(req.IDs)) // out positions of found stored docs
	for i, id := range req.IDs {
		loc := "ids." + strconv.Itoa(i)
		if err := schema.ValidateID(id); err != nil {
			release()
			return nil, docProblem(err, loc)
		}
		res := api.PercolateResult{ID: id}
		body, ok, err := gens[ShardFor(id, len(gens))].Get(id)
		if err != nil {
			release()
			return nil, err
		}
		if ok {
			doc, _, err := schema.Analyze(m, id, body)
			if err != nil {
				release()
				return nil, docProblem(err, loc)
			}
			res.Found = true
			docs = append(docs, doc)
			stored = append(stored, len(out))
		}
		out = append(out, res)
	}
	release()
	matches, err := n.percolateDocs(ctx, idx, docs)
	if err != nil {
		return nil, err
	}
	for i := range req.Docs {
		out[i].Queries = matches[i]
	}
	for k, p := range stored {
		out[p].Queries = matches[len(req.Docs)+k]
	}
	return &api.PercolateResponse{Results: out, Stale: stale}, nil
}

// docProblem places a document's analysis error at loc.
func docProblem(err error, loc string) error {
	var ve *schema.ValidationError
	if errors.As(err, &ve) {
		if ve.Field != "" {
			loc += "." + ve.Field
		}
		return api.InvalidAt(loc, "%s", ve.Message)
	}
	return api.InvalidAt(loc, "%v", err)
}

// Fields implements [api.Coordinator]: the fields are the mapping's; a keyword_list
// field's top entries come from a terms aggregation over every document.
func (n *Single) Fields(ctx context.Context, name string, entries int, opts api.ReadOptions) (cat *api.FieldCatalog, err error) {
	ctx, span := n.readSpan(ctx, "node.fields", name)
	defer func() { endSpan(span, err) }()
	idx, err := n.lookup(name)
	if err != nil {
		return nil, err
	}
	m := idx.meta.Load().mapping
	cat = &api.FieldCatalog{Dynamic: m.Dynamic, Fields: make([]api.FieldInfo, 0, len(m.Fields))}
	aggs := map[string]search.Agg{}
	for _, f := range slices.Sorted(maps.Keys(m.Fields)) {
		cat.Fields = append(cat.Fields, api.FieldInfo{Name: f, Type: m.Fields[f]})
		if entries > 0 && m.Fields[f] == schema.KeywordList {
			aggs[f] = search.Agg{Type: search.AggTerms, Field: f, Size: entries}
		}
	}
	if len(aggs) == 0 {
		return cat, nil
	}
	if err := n.waitSeq(ctx, idx, opts.WaitForSeq); err != nil {
		return nil, err
	}
	gens, release, err := n.acquire(idx)
	if err != nil {
		return nil, err
	}
	defer release()
	resp, err := searchGenerations(ctx, gens, &search.Request{Query: &query.All{}, Aggs: aggs, TrackTotal: search.TrackTotalNone, Index: name})
	if err != nil {
		return nil, err
	}
	for i := range cat.Fields {
		res := resp.Aggs[cat.Fields[i].Name]
		if res == nil {
			continue
		}
		cat.Fields[i].Entries = make([]api.EntryCount, 0, len(res.Buckets))
		for _, b := range res.Buckets {
			cat.Fields[i].Entries = append(cat.Fields[i].Entries, api.EntryCount{Value: fmt.Sprint(b.Key), Count: b.DocCount})
		}
	}
	return cat, nil
}

// GetDocument implements [api.Coordinator]: read from the store, so realtime. While
// the database cannot be reached, or when it does not answer within max_lag, it is
// read from this node's copy instead, marked stale.
func (n *Single) GetDocument(ctx context.Context, name, id string) (*api.Document, error) {
	idx, err := n.lookup(name)
	if err != nil {
		return nil, err
	}
	if n.stale() {
		return n.staleDocument(idx, id, api.Unavailable(store.ErrClosed, "the database cannot be reached"))
	}
	rctx, cancel := context.WithTimeout(ctx, n.recordTimeout())
	defer cancel()
	r, err := n.getRecord(rctx, idx, store.RecordDocument, id)
	if isUnreachable(err) || (errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil) {
		return n.staleDocument(idx, id, err)
	}
	if err != nil {
		return nil, err
	}
	return &api.Document{ID: id, Seq: r.Seq, Body: r.Body}, nil
}

// GetQuery implements [api.Coordinator]: read from the store, so realtime; from this
// node's copy, marked stale, while the database cannot be reached.
func (n *Single) GetQuery(ctx context.Context, name, id string) (*api.SavedQuery, error) {
	idx, err := n.lookup(name)
	if err != nil {
		return nil, err
	}
	if n.stale() {
		return n.staleQuery(idx, id, api.Unavailable(store.ErrClosed, "the database cannot be reached"))
	}
	rctx, cancel := context.WithTimeout(ctx, n.recordTimeout())
	defer cancel()
	r, err := n.getRecord(rctx, idx, store.RecordQuery, id)
	if isUnreachable(err) || (errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil) {
		return n.staleQuery(idx, id, err)
	}
	if err != nil {
		return nil, err
	}
	return &api.SavedQuery{ID: id, Seq: r.Seq, Query: r.Body, Meta: r.Meta}, nil
}

// recordTimeout bounds a realtime read of one record: past it (a database that hangs
// rather than fails) the read falls back to this node's copy, marked stale.
func (n *Single) recordTimeout() time.Duration {
	if n.cfg.MaxLag > 0 {
		return n.cfg.MaxLag
	}
	return 2 * time.Second
}

// isUnreachable reports whether a record read failed because the database is
// unreachable (a 503), rather than for the request's own reasons.
func isUnreachable(err error) bool {
	var ae *api.Error
	return errors.As(err, &ae) && ae.Status == http.StatusServiceUnavailable
}

// staleDocument reads a document from this node's copy.
func (n *Single) staleDocument(idx *index, id string, cause error) (*api.Document, error) {
	gens, release, err := n.acquire(idx)
	if err != nil {
		return nil, cause
	}
	defer release()
	body, ok, err := gens[ShardFor(id, len(gens))].Get(id)
	if err != nil {
		return nil, cause
	}
	if !ok {
		e := api.NotFound(api.CodeDocumentNotFound, "document %q is not in this node's copy of index %q (the database cannot be reached)", id, idx.name)
		e.Extra = map[string]any{"stale": true}
		return nil, e
	}
	return &api.Document{ID: id, Body: body, Stale: true}, nil
}

// staleQuery reads a saved query from this node's copy.
func (n *Single) staleQuery(idx *index, id string, cause error) (*api.SavedQuery, error) {
	gens, release, err := n.acquire(idx)
	if err != nil {
		return nil, cause
	}
	defer release()
	g := gens[ShardFor(id, len(gens))]
	seg, ord, ok := g.LookupQuery(id)
	if !ok {
		e := api.NotFound(api.CodeQueryNotFound, "saved query %q is not in this node's copy of index %q (the database cannot be reached)", id, idx.name)
		e.Extra = map[string]any{"stale": true}
		return nil, e
	}
	sq, err := g.QuerySegments[seg].Segment.Query(ord)
	if err != nil {
		return nil, cause
	}
	q, err := percolate.EncodeQuery(sq.Query)
	if err != nil {
		return nil, cause
	}
	return &api.SavedQuery{ID: id, Seq: sq.Seq, Query: q, Meta: sq.Meta, Stale: true}, nil
}

func (n *Single) getRecord(ctx context.Context, idx *index, kind store.RecordKind, id string) (store.Record, error) {
	if err := schema.ValidateID(id); err != nil {
		return store.Record{}, docProblem(err, "id")
	}
	r, err := n.records.GetRecord(ctx, kind, store.ShardID{Index: idx.name, Shard: ShardFor(id, len(idx.copies))}, id)
	switch {
	case err == nil, errors.Is(err, store.ErrNotFound):
		n.noteDB(nil)
	case store.IsTransient(err) && ctx.Err() == nil:
		n.noteDB(err)
	}
	if errors.Is(err, store.ErrNotFound) {
		if kind == store.RecordQuery {
			return r, api.NotFound(api.CodeQueryNotFound, "saved query %q does not exist in index %q", id, idx.name)
		}
		return r, api.NotFound(api.CodeDocumentNotFound, "document %q does not exist in index %q", id, idx.name)
	}
	return r, storeError(err, idx.name)
}

// ListQueries implements [api.Coordinator]: read from the store, so realtime.
func (n *Single) ListQueries(ctx context.Context, name, after string, size int) ([]*api.SavedQuery, error) {
	if _, err := n.lookup(name); err != nil {
		return nil, err
	}
	recs, err := n.records.ListQueries(ctx, name, after, size)
	if err != nil {
		return nil, storeError(err, name)
	}
	out := make([]*api.SavedQuery, len(recs))
	for i := range recs {
		out[i] = &api.SavedQuery{ID: recs[i].ID, Seq: recs[i].Seq, Query: recs[i].Body, Meta: recs[i].Meta}
	}
	return out, nil
}
