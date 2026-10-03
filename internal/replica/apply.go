package replica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// Halt reasons: what kind of change stopped a copy.
const (
	ReasonDocument = "document" // the document does not analyze under the mapping
	ReasonQuery    = "query"    // the saved query or its payload does not parse
	ReasonKind     = "kind"     // a change kind the tailer does not know
	ReasonRefused  = "refused"  // the shard refused the change (too large, invalid)
	ReasonOrder    = "order"    // the shard refused the change's seq: a tailer bug
	ReasonMapping  = "mapping"  // the index's mapping cannot be read or settled
)

// HaltError is a change the store accepted that this copy cannot apply. The copy has
// applied every change before Seq and none from it on; it is marked recovering in the
// registry. It matches [ErrHalted].
type HaltError struct {
	Shard  ShardID
	Seq    int64
	ID     string // the document or saved query id
	Reason string // one of the Reason constants
	Err    error
}

func (e *HaltError) Error() string {
	return fmt.Sprintf("replica: shard copy %s halted at seq %d (%s %q): %v", e.Shard, e.Seq, e.Reason, e.ID, e.Err)
}

// Unwrap returns the cause.
func (e *HaltError) Unwrap() error { return e.Err }

// Is makes a HaltError match ErrHalted.
func (e *HaltError) Is(target error) bool { return target == ErrHalted }

// Rebuild reasons.
const (
	reasonEmpty        = "empty"        // a new copy
	reasonPruned       = "pruned"       // the changelog was pruned past the copy
	reasonIncarnation  = "incarnation"  // the index was dropped and recreated
	reasonInterrupted  = "interrupted"  // a rebuild did not finish
	reasonUnopenable   = "unopenable"   // the directory does not open (damaged)
	reasonFetchedStale = "fetch_unused" // a fetched copy could not be used
)

// rebuildError asks for the copy to be wiped and rebuilt.
type rebuildError struct {
	reason string
	err    error
}

func (e *rebuildError) Error() string {
	return fmt.Sprintf("replica: the copy must be rebuilt (%s): %v", e.reason, e.err)
}

func (e *rebuildError) Unwrap() error { return e.err }

// catalog is the tailer's view of its index's catalogue entry: the incarnation its
// changes must carry and the mapping documents are analyzed under. The store's mapping
// is the one arbiter of a dynamic field's type: before a document that brings a new
// field is applied, the field is in the stored mapping (put there by the writer, or by
// whichever copy got there first), so every copy, whatever order it sees documents
// in (seq order when tailing, id order when rebuilding), types it the same.
type catalog struct {
	idx     store.IndexStore
	name    string
	meta    store.IndexMeta
	mapping *schema.Mapping
}

// load reads the entry. A dropped index is ErrIndexDropped.
func (c *catalog) load(ctx context.Context) error {
	meta, err := c.idx.Get(ctx, c.name)
	if errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("%w: %s", ErrIndexDropped, c.name)
	}
	if err != nil {
		return err
	}
	return c.adopt(meta)
}

func (c *catalog) adopt(meta store.IndexMeta) error {
	m, err := parseMapping(meta.Mapping)
	if err != nil {
		return &HaltError{Reason: ReasonMapping, Err: fmt.Errorf("the mapping of index %q: %w", meta.Name, err)}
	}
	c.meta, c.mapping = meta, m
	return nil
}

func parseMapping(raw []byte) (*schema.Mapping, error) {
	m := &schema.Mapping{}
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	if err := json.Unmarshal(raw, m); err != nil {
		return nil, err
	}
	return m, nil
}

// covered reports whether m maps every field u adds (with whatever type).
func covered(m *schema.Mapping, u schema.MappingUpdate) bool {
	for name := range u.Fields {
		if _, ok := m.Fields[name]; !ok {
			return false
		}
	}
	return true
}

// ensure makes the stored mapping cover u: it re-reads the entry and, when the index
// is dynamic and a field is still missing, adds u's fields with a version-checked
// update, re-reading and retrying when another node got there first. A changed
// incarnation is a rebuildError.
func (c *catalog) ensure(ctx context.Context, u schema.MappingUpdate) error {
	uid := c.meta.UID
	for range 16 {
		if err := c.load(ctx); err != nil {
			return err
		}
		if c.meta.UID != uid {
			return &rebuildError{reason: reasonIncarnation, err: fmt.Errorf("index %q became incarnation %s", c.name, c.meta.UID)}
		}
		if covered(c.mapping, u) || c.mapping.Dynamic != schema.DynamicTrue {
			return nil
		}
		merged, err := c.mapping.Merge(u)
		if err != nil {
			return &HaltError{Reason: ReasonMapping, Err: err}
		}
		raw, err := json.Marshal(merged)
		if err != nil {
			return &HaltError{Reason: ReasonMapping, Err: err}
		}
		meta := c.meta
		meta.Mapping = raw
		updated, err := c.idx.Update(ctx, meta)
		switch {
		case err == nil:
			return c.adopt(updated)
		case errors.Is(err, store.ErrConflict):
			continue // another node changed it: read it again
		case errors.Is(err, store.ErrNotFound):
			return fmt.Errorf("%w: %s", ErrIndexDropped, c.name)
		default:
			return err
		}
	}
	return errors.New("replica: the index mapping kept changing under the update")
}

// item is one change or snapshot record, before analysis.
type item struct {
	seq  int64
	kind shard.ChangeKind
	id   string
	body []byte // a document's JSON, or a saved query's DSL tree
	meta []byte // a saved query's meta
	uid  string
	bad  error // set when the item is malformed: it halts the copy
	why  string
}

func itemFromChange(c *store.Change) item {
	it := item{seq: c.Seq, id: c.ID, uid: c.IndexUID}
	switch c.Kind {
	case store.KindUpsert:
		it.kind, it.body = shard.Upsert, c.Payload
	case store.KindDelete:
		it.kind = shard.Delete
	case store.KindQueryUpsert:
		it.kind = shard.QueryUpsert
		q, err := store.DecodeQueryPayload(c.Payload)
		if err != nil {
			it.bad, it.why = err, ReasonQuery
		}
		it.body, it.meta = q.Query, q.Meta
	case store.KindQueryDelete:
		it.kind = shard.QueryDelete
	default:
		it.bad, it.why = fmt.Errorf("unknown change kind %q", c.Kind), ReasonKind
	}
	return it
}

func itemFromRecord(r *store.Record) item {
	it := item{seq: r.Seq, id: r.ID, body: r.Body, uid: r.IndexUID}
	switch r.Kind {
	case store.RecordDocument:
		it.kind = shard.Upsert
	case store.RecordQuery:
		it.kind, it.meta = shard.QueryUpsert, r.Meta
	default:
		it.bad, it.why = fmt.Errorf("unknown record kind %d", r.Kind), ReasonKind
	}
	return it
}

// normalizeMeta spells an absent meta the way the store keeps it (sl_queries holds
// {} for none), so a tailed and a rebuilt copy hold the same bytes.
func normalizeMeta(meta []byte) []byte {
	if len(meta) == 0 || string(meta) == "null" {
		return []byte("{}")
	}
	return meta
}

// minAnalyzePerWorker is the fewest documents worth a goroutine of their own.
const minAnalyzePerWorker = 32

// convert analyzes items into shard changes, in order. It returns the changes before
// the first item that cannot be converted, and that failure: a *HaltError naming the
// item, or (with no changes) a catalogue failure to retry or act on.
func (t *Tailer) convert(ctx context.Context, items []item) ([]shard.Change, error) {
	out := make([]shard.Change, len(items))
	docs := make([]int, 0, len(items))
	// limit is the first item that cannot be converted (and halt why): only those
	// before it are converted and returned.
	limit := len(items)
	var halt *HaltError
	for i := range items {
		it := &items[i]
		if it.bad != nil {
			limit, halt = i, t.haltAt(it, it.why, it.bad)
			break
		}
		c := shard.Change{Seq: it.seq, Kind: it.kind, IndexUID: it.uid}
		switch it.kind {
		case shard.Upsert:
			docs = append(docs, i)
		case shard.Delete:
			c.DocID = it.id
		case shard.QueryUpsert:
			n, problems := query.Parse(it.body)
			if len(problems) > 0 {
				limit, halt = i, t.haltAt(it, ReasonQuery, fmt.Errorf("the query does not parse: %s", problems[0]))
			}
			c.QueryID, c.Query, c.Meta = it.id, n, normalizeMeta(it.meta)
		case shard.QueryDelete:
			c.QueryID = it.id
		}
		if halt != nil {
			break
		}
		out[i] = c
	}
	out = out[:limit]
	// A document that analyzes with no mapping update under the cached mapping
	// analyzes the same under any later one (mappings only grow), so the batch is
	// analyzed at once, and only a document that brings a field the cached mapping
	// lacks waits for the stored mapping to take it, then is analyzed again.
	analyzed := analyzeAll(t.cat.mapping, items, docs)
	for k, i := range docs {
		a := &analyzed[k]
		for tries := 0; a.err == nil && !a.update.Empty(); tries++ {
			if tries == maxSettleTries {
				return out[:i], t.haltAt(&items[i], ReasonMapping, errors.New("the document's fields never settle in the mapping"))
			}
			if err := t.cat.ensure(ctx, a.update); err != nil {
				var halt *HaltError
				if errors.As(err, &halt) {
					return out[:i], t.haltAt(&items[i], ReasonMapping, err)
				}
				return nil, err
			}
			a.doc, a.update, a.err = schema.Analyze(t.cat.mapping, items[i].id, items[i].body)
		}
		if a.err != nil {
			return out[:i], t.haltAt(&items[i], ReasonDocument, a.err)
		}
		out[i].Doc = &a.doc
	}
	if halt != nil {
		return out, halt
	}
	return out, nil
}

// maxSettleTries bounds how often one document's new fields are settled in the
// stored mapping: once is always enough unless the mapping misbehaves.
const maxSettleTries = 3

type analysis struct {
	doc    schema.Doc
	update schema.MappingUpdate
	err    error
}

// analyzeAll analyzes the documents items[docs[k]] under m, in parallel.
func analyzeAll(m *schema.Mapping, items []item, docs []int) []analysis {
	out := make([]analysis, len(docs))
	run := func(k int) {
		it := &items[docs[k]]
		out[k].doc, out[k].update, out[k].err = schema.Analyze(m, it.id, it.body)
	}
	workers := min(runtime.GOMAXPROCS(0), len(docs)/minAnalyzePerWorker)
	if workers <= 1 {
		for k := range docs {
			run(k)
		}
		return out
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				k := int(next.Add(1) - 1)
				if k >= len(docs) {
					return
				}
				run(k)
			}
		}()
	}
	wg.Wait()
	return out
}

func (t *Tailer) haltAt(it *item, reason string, err error) *HaltError {
	return &HaltError{Shard: t.id, Seq: it.seq, ID: it.id, Reason: reason, Err: err}
}

// applyChanges applies one page of the changelog and moves the applied seq past what
// it applied.
func (t *Tailer) applyChanges(ctx context.Context, sh *shard.Shard, changes []store.Change) (err error) {
	ctx, span := t.tr.Start(ctx, "replica.apply", trace.WithAttributes(
		attribute.String("index", t.id.Index), attribute.Int("shard", t.id.Shard), attribute.Int("changes", len(changes)),
		attribute.Int64("first_seq", changes[0].Seq), attribute.Int64("last_seq", changes[len(changes)-1].Seq)))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "apply failed")
		}
		span.End()
	}()
	start := time.Now()
	items := make([]item, len(changes))
	for i := range changes {
		c := &changes[i]
		if c.IndexUID != "" && c.IndexUID != t.cat.meta.UID {
			return &rebuildError{reason: reasonIncarnation, err: fmt.Errorf("seq %d belongs to incarnation %s, the copy follows %s", c.Seq, c.IndexUID, t.cat.meta.UID)}
		}
		items[i] = itemFromChange(c)
	}
	converted, cerr := t.convert(ctx, items)
	t.syncMapping(sh)
	n, err := t.applyBatch(ctx, sh, converted, false)
	if n > 0 {
		t.applied.Store(converted[n-1].Seq)
	}
	if err != nil {
		return err
	}
	if cerr != nil {
		return cerr
	}
	t.inst.batchSize.Record(ctx, float64(len(changes)), t.inst.attrs)
	t.inst.applyDur.Record(ctx, time.Since(start).Seconds(), t.inst.attrs)
	return nil
}

// syncMapping hands the shard the catalogue's mapping when it changed, so the
// generations it publishes carry it.
func (t *Tailer) syncMapping(sh *shard.Shard) {
	if t.cat.mapping != nil && sh.Mapping() != t.cat.mapping {
		sh.SetMapping(t.cat.mapping)
	}
}

// applyBatch applies (or, with load, loads) changes, waiting out backpressure: on a
// full buffer it refreshes and retries, never giving up. It returns how many changes
// were applied: all of them, or, when the shard refuses one, those before it, and a
// *HaltError for that one.
func (t *Tailer) applyBatch(ctx context.Context, sh *shard.Shard, changes []shard.Change, load bool) (int, error) {
	if len(changes) == 0 {
		return 0, nil
	}
	wait := time.Duration(0)
	for {
		var err error
		if load {
			err = sh.Load(ctx, changes)
		} else {
			err = sh.Apply(ctx, changes)
		}
		if err == nil {
			return len(changes), nil
		}
		if errors.Is(err, shard.ErrBackpressure) {
			t.inst.backpressure.Add(ctx, 1, t.inst.attrs)
			if err := t.drain(ctx, sh, &wait); err != nil {
				return 0, err
			}
			continue
		}
		var ce *shard.ChangeError
		if !errors.As(err, &ce) {
			return 0, err // closed, failed, cancelled
		}
		if errors.Is(err, shard.ErrIndexUID) {
			return 0, &rebuildError{reason: reasonIncarnation, err: err}
		}
		// Stop exactly before the refused change: apply those before it.
		n, perr := t.applyBatch(ctx, sh, changes[:ce.Pos], load)
		if perr != nil {
			return n, perr
		}
		reason := ReasonRefused
		if errors.Is(err, shard.ErrSeqOrder) {
			reason = ReasonOrder
		}
		bad := &changes[ce.Pos]
		return n, &HaltError{Shard: t.id, Seq: bad.Seq, ID: ce.ID, Reason: reason, Err: err}
	}
}

// drain makes room in a full write buffer: it refreshes the shard and waits for the
// refresh to be published. A refresh that fails (and keeps its buffer) is retried
// after a growing pause; a failed or closed shard ends the wait.
func (t *Tailer) drain(ctx context.Context, sh *shard.Shard, wait *time.Duration) error {
	if err := sh.Refresh(ctx); err != nil {
		if errors.Is(err, shard.ErrFailed) || errors.Is(err, shard.ErrClosed) || ctx.Err() != nil {
			return err
		}
		t.log.DebugContext(ctx, "refresh under backpressure failed; retrying", slog.Any("error", err))
		*wait = min(max(2**wait, time.Millisecond), t.opts.RetryCap)
		sleepCtx(ctx, *wait)
		return ctx.Err()
	}
	if err := sh.WaitRefreshed(ctx, sh.AppliedSeq()); err != nil {
		return err
	}
	*wait = 0
	return nil
}

// halt stops the copy on a change it cannot apply: it is counted, logged loudly, and
// marked recovering in the registry, and Run returns h.
func (t *Tailer) halt(ctx context.Context, h *HaltError) error {
	if h.Shard == (ShardID{}) {
		h.Shard = t.id
	}
	t.setState(StateHalted)
	t.inst.halts.Add(ctx, 1, t.inst.with(attribute.String("reason", h.Reason)))
	_, span := t.tr.Start(ctx, "replica.halt", trace.WithAttributes(
		attribute.String("index", t.id.Index), attribute.Int("shard", t.id.Shard),
		attribute.Int64("seq", h.Seq), attribute.String("reason", h.Reason)))
	span.RecordError(h)
	span.SetStatus(codes.Error, "shard copy halted")
	span.End()
	t.log.ErrorContext(ctx, "shard copy halted: the store accepted a change this copy cannot apply",
		slog.Int64("seq", h.Seq), slog.String("id", h.ID), slog.String("reason", h.Reason),
		slog.Int64("applied", t.Applied()), slog.Any("error", h.Err))
	for attempt := 1; ; attempt++ {
		err := t.setCopyState(ctx, store.CopyRecovering)
		if err == nil {
			return h
		}
		if errors.Is(err, store.ErrLeaseLost) || ctx.Err() != nil || attempt == 5 {
			t.log.WarnContext(ctx, "could not mark the halted copy recovering", slog.Any("error", err))
			return errors.Join(h, err)
		}
		t.backoff(ctx)
	}
}
