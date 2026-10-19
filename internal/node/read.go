package node

import (
	"context"
	"encoding/json"
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
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// checkHead refuses (400) a wait_for_seq past the newest committed seq, which no
// copy would ever reach: the node's head is raised from the store's first. While the
// store cannot be asked, a seq past this node's head may well be committed (by another
// node): it is unavailable (503, retryable), not invalid.
func (n *Single) checkHead(ctx context.Context, seq int64) error {
	if seq <= n.head.Load() {
		return nil
	}
	h, _, err := n.st.HeadSeq(ctx)
	if err != nil {
		n.noteDB(err)
		return api.Unavailable(err, "wait_for_seq %d is past the newest seq this node knows, %d, and the database cannot be asked for its newest", seq, n.head.Load())
	}
	n.noteHead(h)
	if head := n.head.Load(); seq > head {
		return api.InvalidAt("params.wait_for_seq", "wait_for_seq %d is past the newest committed seq, %d", seq, head)
	}
	return nil
}

// acquireTries bounds acquire's retries when the copy's shard is swapped under it.
const acquireTries = 4

// acquire holds the copy's current generation, checking that the copy serves both
// before and after: a tailer marks a copy recovering and then swaps in the shard it
// rebuilds, so a reader that read the state before the mark and the shard after the
// swap would hold an empty shard. After the Acquire the copy must still serve, from
// the very shard acquired; otherwise the generation is released and it tries again, as
// it does when the shard closed under it because a rebuild swapped it.
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
				continue
			}
			return nil, api.Unavailable(shard.ErrClosed, "shard %d of index %q is closed", c.id.Shard, c.id.Index)
		}
		if c.notServing() == nil && c.shard() == sh {
			if c.spec.Served != nil {
				c.spec.Served()
			}
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

// validQuery checks r's queries (its query, post_filter and aggregation filters)
// against the index's mapping. A cluster node whose mapping lacks a field another node
// has since added re-reads the catalogue first, so the field is no spurious 400.
func (n *Single) validQuery(ctx context.Context, idx *index, r *search.Request) error {
	problems := search.ValidateQueries(r, idx.meta.Load().mapping)
	if len(problems) > 0 && n.cl != nil {
		if changed, _ := n.reloadIfNewer(ctx, idx); changed {
			problems = search.ValidateQueries(r, idx.meta.Load().mapping)
		}
	}
	if len(problems) > 0 {
		return api.Invalid("the query is invalid", problems...)
	}
	return nil
}

// now is the instant a request's date math resolves at: the one ctx carries
// ([query.WithNow]), else the clock's wall time.
func (n *Single) now(ctx context.Context) time.Time {
	if now, ok := query.NowFrom(ctx); ok {
		return now
	}
	return n.clock.Wall()
}

// requeryRounds bounds how many times a search runs the query phase again on the
// shards whose fetch found their copy's generation gone.
const requeryRounds = 3

// Search implements [api.Coordinator]: a query-then-fetch over every shard, each
// shard's generation held from its query to its fetch. Its date math (the query's,
// post_filter's and the filter aggregations') resolves here, once, at the clock's wall
// time (or the instant ctx carries, [query.WithNow]): every shard, local or a peer's,
// gets the resolved queries.
func (n *Single) Search(ctx context.Context, name string, r *search.Request, opts api.ReadOptions) (res *api.SearchResult, err error) {
	ctx, span := n.readSpan(ctx, "node.search", name)
	defer func() { endSpan(span, err) }()
	idx, err := n.lookupForRead(ctx, name, opts.WaitForSeq)
	if err != nil {
		return nil, err
	}
	if err := n.validQuery(ctx, idx, r); err != nil {
		return nil, err
	}
	if problems := search.ResolveQueries(r, n.now(ctx)); len(problems) > 0 {
		return nil, api.Invalid("the query is invalid", problems...)
	}
	r.Index = name
	fields, inner := r.Fields, search.HasInnerHits(r)
	if inner {
		wide := *r
		wide.Fields = search.InnerHitsFields(r)
		r = &wide
	}
	ts, err := n.acquireTargets(ctx, idx, opts.WaitForSeq, false)
	if err != nil {
		return nil, err
	}
	defer ts.release()
	resp, err := n.searchTargets(ctx, ts, r, n.reacquirer(ctx, idx, opts.WaitForSeq))
	if err != nil {
		return nil, err
	}
	if inner {
		if err := search.FillInnerHits(ctx, resp.Hits, r, idx.meta.Load().mapping, fields); err != nil {
			return nil, err
		}
	}
	return &api.SearchResult{Response: resp, Stale: ts.stale() || n.stale()}, nil
}

// reacquirer returns a fresh read target for shard s of idx, for a search whose
// target's generation went away between its phases: a cluster ranks the copy that lost
// it after the others. An index dropped meanwhile is not found (404).
func (n *Single) reacquirer(ctx context.Context, idx *index, waitSeq int64) func(s int) (ShardTarget, error) {
	return func(s int) (ShardTarget, error) {
		t, err := n.acquireShard(ctx, idx, s, waitSeq, false)
		if err != nil && idx.dropped.Load() {
			return nil, indexNotFound(idx.name)
		}
		return t, err
	}
}

// searchTargets searches ts, one per shard: in one pass with bodies when there is one
// shard and no from (or a size of 0); otherwise the query phase per shard (NoBodies),
// the reduce, then the fetch of the page's bodies alone from the copies that found
// them (a hit's shard is the one its id routes to): a from skips its hits' bodies,
// never read.
//
// A shard whose fetch finds its generation gone (the copy stopped, or the pin expired)
// has its query phase run again on a target reacquire gives, which replaces it in ts,
// within what is left of r.Timeout; the shards' results are then reduced again, so the
// hits, their order, the total and the aggregations are those of the results the
// bodies come from. Only the shards re-queried are fetched again. That happens at most
// requeryRounds times, within ctx; past them the search is unavailable (503).
func (n *Single) searchTargets(ctx context.Context, ts targets, r *search.Request, reacquire func(s int) (ShardTarget, error)) (*search.Response, error) {
	if len(ts) == 1 && (r.From == 0 || r.Size == 0) {
		sr, err := ts[0].Search(ctx, r)
		if err != nil {
			return nil, err
		}
		return search.Reduce(ctx, []*search.ShardResult{sr}, r), nil
	}
	began := n.clock.Now()
	q := *r
	q.NoBodies = true
	results := make([]*search.ShardResult, len(ts))
	if err := queryShards(ctx, ts, &q, results, allShards(len(ts))); err != nil {
		return nil, err
	}
	bodies := make([]map[search.HitRef]json.RawMessage, len(ts))
	for round := 0; ; round++ {
		resp := search.Reduce(ctx, results, &q)
		lost, err := fetchHits(ctx, ts, resp, r.Fields, bodies)
		if err != nil {
			return nil, err
		}
		if len(lost) == 0 {
			return resp, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		shards := slices.Sorted(maps.Keys(lost))
		if round == requeryRounds {
			return nil, api.Unavailable(lost[shards[0]], "the copies of shard %d of index %q went away during the search; retry", shards[0], r.Index)
		}
		for _, s := range shards {
			t, err := reacquire(s)
			if err != nil {
				return nil, err
			}
			ts[s].Release()
			ts[s] = t
			bodies[s] = nil
		}
		rq := q
		if r.Timeout > 0 {
			rq.Timeout = max(r.Timeout-n.clock.Since(began), time.Nanosecond)
		}
		if err := queryShards(ctx, ts, &rq, results, shards); err != nil {
			return nil, err
		}
	}
}

// allShards lists the shards 0 to count-1.
func allShards(count int) []int {
	out := make([]int, count)
	for s := range out {
		out[s] = s
	}
	return out
}

// queryShards runs the query phase q on the shards given, together, into results.
func queryShards(ctx context.Context, ts targets, q *search.Request, results []*search.ShardResult, shards []int) error {
	errs := make([]error, len(ts))
	var wg sync.WaitGroup
	for _, s := range shards {
		wg.Go(func() { results[s], errs[s] = ts[s].Search(ctx, q) })
	}
	wg.Wait()
	return firstError(errs)
}

// fetchHits returns the shards whose generation was gone (search.ErrStaleHit,
// ErrTargetLost) with why, and fails on any other error.
func fetchHits(ctx context.Context, ts targets, resp *search.Response, fields []string, bodies []map[search.HitRef]json.RawMessage) (map[int]error, error) {
	byShard := map[int][]int{}
	for i := range resp.Hits {
		h := &resp.Hits[i]
		if h.Ref == nil {
			return nil, fmt.Errorf("node: hit %q has no ref to fetch", h.ID)
		}
		s := ShardFor(h.ID, len(ts))
		if b, ok := bodies[s][*h.Ref]; ok {
			h.Body = b
			continue
		}
		byShard[s] = append(byShard[s], i)
	}
	errs := make([]error, len(ts))
	fetched := make([][]search.Hit, len(ts))
	var wg sync.WaitGroup
	for s, pos := range byShard {
		wg.Go(func() {
			hits := make([]search.Hit, len(pos))
			for k, i := range pos {
				hits[k] = resp.Hits[i]
			}
			if err := ts[s].Fetch(ctx, hits, fields); err != nil {
				errs[s] = err
				return
			}
			for k, i := range pos {
				resp.Hits[i].Body = hits[k].Body
			}
			fetched[s] = hits
		})
	}
	wg.Wait()
	var lost map[int]error
	for s, err := range errs {
		switch {
		case err == nil:
			for _, h := range fetched[s] {
				if bodies[s] == nil {
					bodies[s] = map[search.HitRef]json.RawMessage{}
				}
				bodies[s][*h.Ref] = h.Body
			}
		case errors.Is(err, search.ErrStaleHit) || errors.Is(err, ErrTargetLost):
			if lost == nil {
				lost = map[int]error{}
			}
			lost[s] = err
		default:
			return nil, err
		}
	}
	return lost, nil
}

func firstError(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// catalogRecheckEvery is the least time between two catalogue reads that percolated
// documents naming unmapped fields make on one index.
const catalogRecheckEvery = 100 * time.Millisecond

// analyzeDocs analyzes the given documents of a percolation under the index's mapping.
// A cluster node re-reads the catalogue once when one does not analyze (a strict
// index refusing a field another node has since mapped), and when one names a field
// the mapping lacks (another node may have mapped it since this node's last sync, and
// the copies analyze saved queries under it), at most every catalogRecheckEvery.
func (n *Single) analyzeDocs(ctx context.Context, idx *index, bodies [][]byte) ([]schema.Doc, error) {
	for attempt := 0; ; attempt++ {
		m := idx.meta.Load().mapping
		recheck := attempt == 0 && n.cl != nil && idx.mayRecheckCatalog(n.clock.Now())
		docs := make([]schema.Doc, 0, len(bodies))
		var failure error
		unmapped := false
		for i, body := range bodies {
			doc, update, err := schema.AnalyzeForMatch(m, "_percolate_"+strconv.Itoa(i), body)
			if err != nil {
				failure = docProblem(err, "docs."+strconv.Itoa(i))
				break
			}
			unmapped = unmapped || (recheck && namesUnmapped(m, update, body))
			docs = append(docs, doc)
		}
		if failure == nil && !unmapped {
			return docs, nil
		}
		if attempt > 0 || n.cl == nil {
			return nil, failure
		}
		if failure == nil {
			idx.catalogRecheck.Store(n.clock.Now().UnixNano())
		}
		if changed, _ := n.reloadIfNewer(ctx, idx); !changed {
			if failure != nil {
				return nil, failure
			}
			return docs, nil
		}
	}
}

// namesUnmapped reports whether a document analyzed under m names a field m lacks:
// update holds the fields dynamic mapping typed, and under dynamic false (which drops
// them) body is typed again as if dynamic were true.
func namesUnmapped(m *schema.Mapping, update schema.MappingUpdate, body []byte) bool {
	if !update.Empty() {
		return true
	}
	if m == nil || m.Dynamic != schema.DynamicFalse {
		return false
	}
	typed := *m
	typed.Dynamic = schema.DynamicTrue
	_, u, err := schema.Check(&typed, "_percolate", body)
	return err == nil && !u.Empty()
}

// mayRecheckCatalog reports whether percolated documents naming unmapped fields may
// re-read idx's catalogue at now: catalogRecheckEvery has passed since the last one.
func (idx *index) mayRecheckCatalog(now time.Time) bool {
	return now.UnixNano()-idx.catalogRecheck.Load() >= int64(catalogRecheckEvery)
}

// Percolate implements [api.Coordinator]. Given documents are analyzed under the
// index's mapping (their new fields are not added to it: no saved query can name a
// field the mapping lacks); stored ones are read from the copies a search would read,
// not realtime from the database as GET reads them: pass wait_for_seq to see a write.
func (n *Single) Percolate(ctx context.Context, name string, req *api.PercolateRequest, opts api.ReadOptions) (res *api.PercolateResponse, err error) {
	ctx, span := n.readSpan(ctx, "node.percolate", name)
	defer func() { endSpan(span, err) }()
	idx, err := n.lookupForRead(ctx, name, opts.WaitForSeq)
	if err != nil {
		return nil, err
	}
	bodies := make([][]byte, len(req.Docs))
	for i := range req.Docs {
		bodies[i] = req.Docs[i]
	}
	docs, err := n.analyzeDocs(ctx, idx, bodies)
	if err != nil {
		return nil, err
	}
	out := make([]api.PercolateResult, 0, len(req.Docs)+len(req.IDs))
	for range req.Docs {
		out = append(out, api.PercolateResult{Found: true})
	}
	ts, err := n.acquireTargets(ctx, idx, opts.WaitForSeq, true)
	if err != nil {
		return nil, err
	}
	defer ts.release()
	m := idx.meta.Load().mapping
	storedPositions := make([]int, 0, len(req.IDs))
	for i, id := range req.IDs {
		loc := "ids." + strconv.Itoa(i)
		if err := schema.ValidateID(id); err != nil {
			return nil, docProblem(err, loc)
		}
		res := api.PercolateResult{ID: id}
		body, seq, ok, err := ts[ShardFor(id, len(ts))].Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if ok {
			doc, _, err := schema.AnalyzeForMatch(m, id, body)
			if err != nil {
				return nil, docProblem(err, loc)
			}
			res.Found, res.Seq = true, seq
			docs = append(docs, doc)
			storedPositions = append(storedPositions, len(out))
		}
		out = append(out, res)
	}
	matches, details, err := n.percolateDocs(ctx, idx, ts, docs, MatchDetails{Versions: req.Matches || req.InnerHits, InnerHits: req.InnerHits})
	if details != nil {
		for i := range out {
			out[i].Matches = []api.PercolateMatch{}
		}
	}
	if err != nil {
		return nil, err
	}
	for i := range req.Docs {
		out[i].Queries = json.RawMessage(matches[i])
		if details != nil {
			out[i].Matches = details[i]
		}
	}
	for k, p := range storedPositions {
		out[p].Queries = json.RawMessage(matches[len(req.Docs)+k])
		if details != nil {
			out[p].Matches = details[len(req.Docs)+k]
		}
	}
	return &api.PercolateResponse{Results: out, Stale: ts.stale() || n.stale()}, nil
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
	idx, err := n.lookupForRead(ctx, name, opts.WaitForSeq)
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
	ts, err := n.acquireTargets(ctx, idx, opts.WaitForSeq, false)
	if err != nil {
		return nil, err
	}
	defer ts.release()
	resp, err := n.searchTargets(ctx, ts, &search.Request{Query: &query.All{}, Aggs: aggs, TrackTotal: search.TrackTotalNone, Index: name}, n.reacquirer(ctx, idx, opts.WaitForSeq))
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

// GetDocument implements [api.Coordinator]: read from the store, so realtime, and so
// with every committed seq in it (a wait_for_seq past the head is a 400, as on every
// read). While the database cannot be reached, or when it does not answer within
// max_lag, it is read from a copy instead, marked stale, once the copy has
// opts.WaitForSeq searchable.
func (n *Single) GetDocument(ctx context.Context, name, id string, opts api.ReadOptions) (*api.Document, error) {
	idx, err := n.lookupForRead(ctx, name, opts.WaitForSeq)
	if err != nil {
		return nil, err
	}
	if opts.WaitForSeq > 0 {
		if err := n.checkHead(ctx, opts.WaitForSeq); err != nil {
			return nil, err
		}
	}
	if n.stale() {
		return n.staleDocument(ctx, idx, id, opts.WaitForSeq, api.Unavailable(store.ErrClosed, "the database cannot be reached"))
	}
	rctx, cancel := context.WithTimeout(ctx, n.recordTimeout())
	defer cancel()
	r, err := n.getRecord(rctx, idx, store.RecordDocument, id)
	if isUnreachable(err) || (errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil) {
		return n.staleDocument(ctx, idx, id, opts.WaitForSeq, err)
	}
	if err != nil {
		return nil, err
	}
	return &api.Document{ID: id, Seq: r.Seq, Version: r.Version, Body: r.Body}, nil
}

// GetQuery implements [api.Coordinator]: read from the store, so realtime; from a
// copy, marked stale, while the database cannot be reached. opts.WaitForSeq is
// honoured as GetDocument honours it.
func (n *Single) GetQuery(ctx context.Context, name, id string, opts api.ReadOptions) (*api.SavedQuery, error) {
	idx, err := n.lookupForRead(ctx, name, opts.WaitForSeq)
	if err != nil {
		return nil, err
	}
	if opts.WaitForSeq > 0 {
		if err := n.checkHead(ctx, opts.WaitForSeq); err != nil {
			return nil, err
		}
	}
	if n.stale() {
		return n.staleQuery(ctx, idx, id, opts.WaitForSeq, api.Unavailable(store.ErrClosed, "the database cannot be reached"))
	}
	rctx, cancel := context.WithTimeout(ctx, n.recordTimeout())
	defer cancel()
	r, err := n.getRecord(rctx, idx, store.RecordQuery, id)
	if isUnreachable(err) || (errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil) {
		return n.staleQuery(ctx, idx, id, opts.WaitForSeq, err)
	}
	if err != nil {
		return nil, err
	}
	return &api.SavedQuery{ID: id, Seq: r.Seq, Version: r.Version, Query: r.Body, Meta: r.Meta}, nil
}

// recordTimeout bounds a realtime read of one record: past it (a database that hangs
// rather than fails) the read falls back to a copy, marked stale.
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

// staleTarget returns a read target for the shard id routes to, for a stale read, once
// it has waitSeq searchable. It fails with cause, unless the wait for waitSeq passed
// its deadline (a 504).
func (n *Single) staleTarget(ctx context.Context, idx *index, id string, waitSeq int64, cause error) (ShardTarget, error) {
	if err := schema.ValidateID(id); err != nil {
		return nil, docProblem(err, "id")
	}
	t, err := n.acquireShard(ctx, idx, ShardFor(id, len(idx.shards)), waitSeq, false)
	var ae *api.Error
	if err != nil && (!errors.As(err, &ae) || ae.Status != http.StatusGatewayTimeout) {
		return nil, cause
	}
	return t, err
}

// staleDocument reads a document from a copy that has waitSeq searchable.
func (n *Single) staleDocument(ctx context.Context, idx *index, id string, waitSeq int64, cause error) (*api.Document, error) {
	t, err := n.staleTarget(ctx, idx, id, waitSeq, cause)
	if err != nil {
		return nil, err
	}
	defer t.Release()
	body, _, ok, err := t.Get(ctx, id)
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

// staleQuery reads a saved query from a copy that has waitSeq searchable.
func (n *Single) staleQuery(ctx context.Context, idx *index, id string, waitSeq int64, cause error) (*api.SavedQuery, error) {
	t, err := n.staleTarget(ctx, idx, id, waitSeq, cause)
	if err != nil {
		return nil, err
	}
	defer t.Release()
	q, ok, err := t.GetQuery(ctx, id)
	if err != nil {
		return nil, cause
	}
	if !ok {
		e := api.NotFound(api.CodeQueryNotFound, "saved query %q is not in this node's copy of index %q (the database cannot be reached)", id, idx.name)
		e.Extra = map[string]any{"stale": true}
		return nil, e
	}
	q.Stale = true
	return q, nil
}

func (n *Single) getRecord(ctx context.Context, idx *index, kind store.RecordKind, id string) (store.Record, error) {
	if err := schema.ValidateID(id); err != nil {
		return store.Record{}, docProblem(err, "id")
	}
	r, err := n.records.GetRecord(ctx, kind, store.ShardID{Index: idx.name, Shard: ShardFor(id, len(idx.shards))}, id)
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
func (n *Single) ListQueries(ctx context.Context, name, after string, size int, lean bool) ([]*api.SavedQuery, error) {
	if _, err := n.lookupForRead(ctx, name, 0); err != nil {
		return nil, err
	}
	recs, err := n.records.ListQueries(ctx, name, after, size, lean)
	if err != nil {
		return nil, storeError(err, name)
	}
	out := make([]*api.SavedQuery, len(recs))
	for i := range recs {
		out[i] = &api.SavedQuery{ID: recs[i].ID, Seq: recs[i].Seq, Version: recs[i].Version, Query: recs[i].Body, Meta: recs[i].Meta}
	}
	return out, nil
}
