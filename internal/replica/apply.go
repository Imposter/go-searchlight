package replica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// Halt reasons: what kind of change stopped a copy.
const (
	ReasonDocument = "document" // the document body does not analyze
	ReasonQuery    = "query"    // the saved query or its payload does not parse
	ReasonKind     = "kind"     // a change kind the tailer does not know
	ReasonRefused  = "refused"  // the shard refused the change (too large, invalid)
	ReasonOrder    = "order"    // the shard refused the change's seq: a tailer bug
	ReasonMapping  = "mapping"  // a mapping change that does not parse
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

	// class is what ID names (a document, a saved query or the mapping), so that
	// only a later change of the same thing counts as superseding it; "" matches
	// any.
	class string
}

// Classes of the things a change targets.
const (
	classDocument = "document"
	classQuery    = "query"
	classMapping  = "mapping"
)

func classOfShardKind(k shard.ChangeKind) string {
	switch k {
	case shard.Upsert, shard.Delete:
		return classDocument
	case shard.QueryUpsert, shard.QueryDelete:
		return classQuery
	case shard.Remap:
		return classMapping
	}
	return ""
}

func classOfStoreKind(k store.Kind) string {
	switch k {
	case store.KindUpsert, store.KindDelete:
		return classDocument
	case store.KindQueryUpsert, store.KindQueryDelete:
		return classQuery
	case store.KindMapping:
		return classMapping
	}
	return ""
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
	reasonEmpty         = "empty"          // a new copy
	reasonPruned        = "pruned"         // the changelog was pruned past the copy
	reasonIncarnation   = "incarnation"    // the index was dropped and recreated
	reasonInterrupted   = "interrupted"    // a rebuild did not finish
	reasonUnopenable    = "unopenable"     // the directory does not open (damaged)
	reasonFetchedStale  = "fetch_unused"   // a fetched copy could not be used
	reasonRemap         = "remap"          // a mapping change re-analyzes live documents
	reasonMappingOrder  = "mapping_order"  // a change's mapping version is not the copy's
	reasonMappingBehind = "mapping_behind" // the catalogue's mapping takes what the copy's does not
	reasonHalted        = "halted"         // halted twice at the same change
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

// catalog is the tailer's view of its index's catalogue entry. It is not where the
// copy's mapping comes from (that is the changelog's); it tells whether the index,
// and the tailer's shard of it, still exist, and which incarnation is current.
type catalog struct {
	idx   store.IndexStore
	name  string
	clock clock.Clock
	meta  store.IndexMeta
	// missingSince is when the catalogue was first seen without the index (or the
	// shard) since it was last seen with it.
	missingSince time.Time
}

// load reads the entry. A dropped index, or one with no shard number shardNum any
// more, is ErrIndexDropped.
func (c *catalog) load(ctx context.Context, shardNum int) error {
	meta, err := c.idx.Get(ctx, c.name)
	if errors.Is(err, store.ErrNotFound) {
		return c.missing(fmt.Errorf("%w: %s", ErrIndexDropped, c.name))
	}
	if err != nil {
		return err
	}
	shards, err := meta.Shards()
	if err != nil {
		return err
	}
	if shardNum >= shards {
		return c.missing(fmt.Errorf("%w: %s has %d shards, not shard %d", ErrIndexDropped, c.name, shards, shardNum))
	}
	c.meta, c.missingSince = meta, time.Time{}
	return nil
}

// missing records that the catalogue lacks the index (or shard) now.
func (c *catalog) missing(err error) error {
	if c.missingSince.IsZero() {
		c.missingSince = c.clock.Now()
	}
	return err
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

// item is one change or snapshot record, before analysis.
type item struct {
	seq     int64
	kind    shard.ChangeKind
	id      string
	body    []byte          // a document's JSON, or a saved query's DSL tree
	meta    []byte          // a saved query's meta
	mapping *schema.Mapping // a Remap's
	version int64           // the mapping version it was written under (a Remap's own)
	uid     string
	bad     error // set when the item is malformed: it halts the copy
	why     string
}

func itemFromChange(c *store.Change) item {
	it := item{seq: c.Seq, id: c.ID, uid: c.IndexUID, version: c.MappingVersion}
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
	case store.KindMapping:
		it.kind, it.body = shard.Remap, c.Payload
		m, err := parseMapping(c.Payload)
		if err != nil {
			it.bad, it.why = err, ReasonMapping
		}
		it.mapping = m
	default:
		it.bad, it.why = fmt.Errorf("unknown change kind %q", c.Kind), ReasonKind
	}
	return it
}

func itemFromRecord(r *store.Record) item {
	it := item{seq: r.Seq, id: r.ID, body: r.Body, uid: r.IndexUID, version: r.MappingVersion}
	switch r.Kind {
	case store.RecordDocument:
		it.kind = shard.Upsert
	case store.RecordQuery:
		it.kind, it.meta = shard.QueryUpsert, r.Meta
	case store.RecordMapping:
		it.kind, it.seq = shard.Remap, 0
		m, err := parseMapping(r.Body)
		if err != nil {
			it.bad, it.why = err, ReasonMapping
		}
		it.mapping = m
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

// analyzeDoc analyzes a document body as every copy does: a field the mapping maps is
// analyzed by its type; any other is present but untyped, whatever the mapping's
// dynamic mode (the writer types new fields, and refuses strict ones, before it
// commits). The result depends only on the mapped fields and the body, so tailed and
// rebuilt copies agree, and a mapping change that maps a field some live document
// holds shows as that field's presence. view is the mapping as analysisView makes it;
// dynamic is its real mode. unmapped reports fields a dynamic or strict mapping should
// have mapped: a writer that broke its contract.
func analyzeDoc(view *schema.Mapping, dynamic schema.DynamicMode, id string, body []byte) (doc schema.Doc, unmapped bool, err error) {
	doc, upd, err := schema.Analyze(view, id, body)
	if err != nil {
		return doc, false, err
	}
	for f := range upd.Fields {
		// Typeable content: dynamic inference types this value, so mapping the
		// field would analyze it differently. A value inference does not type
		// (an empty list, an object) analyzes as presence alone whatever the
		// field's type, and is not marked. Segments keep the marks in a bitmap of
		// their own (segment.Reader.Untyped, shard.SegmentView.MarksUntyped).
		doc.Fields[f] = schema.Value{Present: true, Untyped: true}
	}
	return doc, !upd.Empty() && dynamic != schema.DynamicFalse, nil
}

// analysisView is m as analyzeDoc uses it: dynamic true, so every unmapped field is
// reported (and then indexed for presence only), never refused.
func analysisView(m *schema.Mapping) *schema.Mapping {
	if m == nil {
		return &schema.Mapping{Fields: map[string]schema.FieldType{}}
	}
	if m.Dynamic == schema.DynamicTrue {
		return m
	}
	v := m.Clone()
	v.Dynamic = schema.DynamicTrue
	return v
}

// changedFields lists the fields whose analysis differs between old and newer: those
// added, removed or retyped.
func changedFields(old, newer *schema.Mapping) []string {
	var oldFields, newFields map[string]schema.FieldType
	if old != nil {
		oldFields = old.Fields
	}
	if newer != nil {
		newFields = newer.Fields
	}
	var out []string
	for f, t := range newFields {
		if ot, ok := oldFields[f]; !ok || ot != t {
			out = append(out, f)
		}
	}
	for f := range oldFields {
		if _, ok := newFields[f]; !ok {
			out = append(out, f)
		}
	}
	slices.Sort(out)
	return out
}

// minAnalyzePerWorker is the fewest documents worth a goroutine of their own.
const minAnalyzePerWorker = 32

// failure is the first item a run of items cannot convert, at pos.
type failure struct {
	pos    int
	reason string
	err    error
}

// convert analyzes items into shard changes, in order, each document under views[i]
// (its mapping as analysisView makes it; dynamic[i] is the real mode). It returns the
// changes before the first item that cannot be converted, and that failure.
func (t *Tailer) convert(items []item, views []*schema.Mapping, dynamic []schema.DynamicMode) ([]shard.Change, *failure) {
	out := make([]shard.Change, len(items))
	docs := make([]int, 0, len(items))
	limit := len(items)
	var fail *failure
	for i := range items {
		it := &items[i]
		if it.bad != nil {
			limit, fail = i, &failure{pos: i, reason: it.why, err: it.bad}
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
				limit, fail = i, &failure{pos: i, reason: ReasonQuery, err: fmt.Errorf("the query does not parse: %s", problems[0])}
			}
			c.QueryID, c.Query, c.Meta = it.id, n, normalizeMeta(it.meta)
		case shard.QueryDelete:
			c.QueryID = it.id
		case shard.Remap:
			c.Mapping, c.MappingVersion = it.mapping, it.version
		}
		if fail != nil {
			break
		}
		out[i] = c
	}
	out = out[:limit]
	analyzed := analyzeAll(items, views, dynamic, docs)
	unmapped := 0
	for k, i := range docs {
		a := &analyzed[k]
		if a.err != nil {
			return out[:i], &failure{pos: i, reason: ReasonDocument, err: a.err}
		}
		if a.unmapped {
			unmapped++
		}
		out[i].Doc = &a.doc
	}
	if unmapped > 0 {
		if suppressed, ok := t.warn.allow(); ok {
			t.log.Warn("documents hold fields their mapping does not map: the writer must add a dynamic field to the mapping before it commits a document with it",
				slog.Int("documents", unmapped), slog.Int("suppressed", suppressed))
		}
	}
	return out, fail
}

type analysis struct {
	doc      schema.Doc
	unmapped bool
	err      error
}

// analyzeAll analyzes the documents items[docs[k]], in parallel.
func analyzeAll(items []item, views []*schema.Mapping, dynamic []schema.DynamicMode, docs []int) []analysis {
	out := make([]analysis, len(docs))
	run := func(k int) {
		i := docs[k]
		out[k].doc, out[k].unmapped, out[k].err = analyzeDoc(views[i], dynamic[i], items[i].id, items[i].body)
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
	return &HaltError{Shard: t.id, Seq: it.seq, ID: it.id, Reason: reason, Err: err, class: classOfShardKind(it.kind)}
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
	start := t.opts.Clock.Now()
	uid := sh.IndexUID()
	items := make([]item, len(changes))
	for i := range changes {
		c := &changes[i]
		if uid != "" && c.IndexUID != "" && c.IndexUID != uid {
			return &rebuildError{reason: reasonIncarnation, err: fmt.Errorf("seq %d belongs to incarnation %s, the copy holds %s", c.Seq, c.IndexUID, uid)}
		}
		items[i] = itemFromChange(c)
	}
	if err := t.applyItems(ctx, sh, items, false); err != nil {
		return err
	}
	t.inst.batchSize.Record(ctx, float64(len(changes)), t.inst.attrs)
	t.inst.applyDur.Record(ctx, t.opts.Clock.Since(start).Seconds(), t.inst.attrs)
	return nil
}

// applyItems analyzes and applies (or, with load, loads) items in order, each under the
// mapping in force at its position: the copy's, then each Remap's from its seq on. It
// stops before a change whose mapping version is not the copy's (a rebuild), before a
// Remap that would re-analyze a live document (a rebuild), and before an item that
// cannot be applied (the guard decides: a rebuild, or a halt). Whatever precedes the
// stop is applied, and in tailing the applied seq moves past it.
func (t *Tailer) applyItems(ctx context.Context, sh *shard.Shard, items []item, load bool) error {
	m, version := sh.Mapping(), sh.MappingVersion()
	view := analysisView(m)
	views := make([]*schema.Mapping, len(items))
	dynamic := make([]schema.DynamicMode, len(items))
	runStart := 0
	// flush applies items[runStart:end].
	flush := func(end int) error {
		if end <= runStart {
			return nil
		}
		run := items[runStart:end]
		converted, fail := t.convert(run, views[runStart:end], dynamic[runStart:end])
		n, err := t.applyBatch(ctx, sh, converted, load)
		if !load && n > 0 {
			t.applied.Store(converted[n-1].Seq)
		}
		if err != nil {
			return err
		}
		if fail != nil {
			return t.guard(ctx, sh, &run[fail.pos], fail.reason, fail.err)
		}
		runStart = end
		return nil
	}
	for i := range items {
		it := &items[i]
		switch {
		case it.bad != nil:
			// convert stops here; the guard decides.
		case it.kind == shard.Remap:
			if !load {
				if it.version != version+1 {
					if err := flush(i); err != nil {
						return err
					}
					return &rebuildError{reason: reasonMappingOrder, err: fmt.Errorf("seq %d is mapping version %d, the copy is at %d", it.seq, it.version, version)}
				}
				if fields := changedFields(m, it.mapping); len(fields) > 0 {
					// Everything before the remap goes in first: the check must see it.
					if err := flush(i); err != nil {
						return err
					}
					rebuild, err := t.remapReanalyzes(ctx, sh, m, fields)
					if err != nil {
						return err
					}
					if rebuild {
						t.minMappingVersion = it.version
						return &rebuildError{reason: reasonRemap, err: fmt.Errorf("mapping version %d at seq %d changes fields %v that live documents hold", it.version, it.seq, fields)}
					}
				}
			}
			m, version = it.mapping, it.version
			view = analysisView(m)
		case it.version != version:
			if err := flush(i); err != nil {
				return err
			}
			return &rebuildError{reason: reasonMappingOrder, err: fmt.Errorf("seq %d was written under mapping version %d, the copy is at %d", it.seq, it.version, version)}
		default:
			views[i] = view
			if m != nil {
				dynamic[i] = m.Dynamic
			}
		}
	}
	return flush(len(items))
}

// remapReanalyzes reports whether a live document of the copy would analyze
// differently under a mapping change that maps, re-types or unmaps fields: the copy
// must then be rebuilt to be analyzed as a copy rebuilt after the change would be.
// For a field old maps, any live holder counts. For a field it does not, only a holder
// whose value dynamic inference types (marked at analysis; see analyzeDoc): a value
// it does not type ([] or {}) analyzes as presence alone, typed or not, so mapping the
// field changes nothing for it, and the copy adopts the mapping in place. In a segment
// that does not mark, any holder counts. It is a bitmap check, after a refresh puts the
// buffer in segments.
func (t *Tailer) remapReanalyzes(ctx context.Context, sh *shard.Shard, old *schema.Mapping, fields []string) (bool, error) {
	if err := sh.Refresh(ctx); err != nil {
		return false, err
	}
	g := sh.Acquire()
	if g == nil {
		return false, shard.ErrClosed
	}
	defer g.Release()
	for _, sv := range g.Segments {
		for _, f := range fields {
			p := sv.Reader.Present(f)
			if _, mapped := old.Type(f); !mapped && sv.MarksUntyped && f != schema.IDField {
				p = sv.Reader.Untyped(f)
			}
			if p == nil || p.IsEmpty() {
				continue
			}
			live := p.Clone()
			live.AndNot(sv.Deletes)
			if !live.IsEmpty() {
				return true, nil
			}
		}
	}
	return false, nil
}

// guard decides what a change the copy cannot convert means. The catalogue is read
// again first: a new incarnation is a rebuild, and when the catalogue's mapping would
// take what the copy's could not (a mapping the copy has not caught up with, which
// ordering forbids, but which a rebuild fixes), the copy is rebuilt, once per change.
// Otherwise the copy halts at the change.
func (t *Tailer) guard(ctx context.Context, sh *shard.Shard, it *item, reason string, cause error) error {
	if err := t.cat.load(ctx, t.id.Shard); err != nil {
		return err
	}
	t.lastCatalog = t.opts.Clock.Now()
	if uid := sh.IndexUID(); uid != "" && uid != t.cat.meta.UID {
		return &rebuildError{reason: reasonIncarnation, err: fmt.Errorf("the index became incarnation %s", t.cat.meta.UID)}
	}
	if t.guardedSeq != it.seq {
		if m, err := parseMapping(t.cat.meta.Mapping); err == nil {
			retry := false
			switch reason {
			case ReasonDocument:
				_, _, aerr := analyzeDoc(analysisView(m), m.Dynamic, it.id, it.body)
				retry = aerr == nil
			case ReasonMapping:
				retry = t.cat.meta.MappingVersion >= it.version
			}
			if retry {
				t.guardedSeq = it.seq
				return &rebuildError{reason: reasonMappingBehind, err: fmt.Errorf("seq %d: %w", it.seq, cause)}
			}
		}
	}
	return t.haltAt(it, reason, cause)
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
		return n, &HaltError{Shard: t.id, Seq: bad.Seq, ID: ce.ID, Reason: reason, Err: err, class: classOfShardKind(bad.Kind)}
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
		t.pause(ctx, *wait)
		return ctx.Err()
	}
	if err := sh.WaitRefreshed(ctx, sh.AppliedSeq()); err != nil {
		return err
	}
	*wait = 0
	return nil
}

// onHalt records a halt: counted, logged loudly, the copy marked recovering in the
// registry. Run then backs off (HaltRetryBase, doubling to HaltRetryCap) and tails
// again. Halted at the same change again, the copy is rebuilt (a snapshot holds only
// current rows) once the changelog shows a later change of the same document or
// query, superseding the bad one; until then it keeps what it has and only tails
// again, rather than wiping and rescanning a copy that would stop at the same row.
// It returns an error only when Run must stop (the lease is lost).
func (t *Tailer) onHalt(ctx context.Context, h *HaltError) error {
	if h.Shard == (ShardID{}) {
		h.Shard = t.id
	}
	prev := t.halt.Swap(h)
	repeat := prev != nil && prev.Seq == h.Seq && prev.ID == h.ID
	t.setState(StateHalted)
	t.inst.halts.Add(ctx, 1, t.inst.with(attribute.String("reason", h.Reason)))
	_, span := t.tr.Start(ctx, "replica.halt", trace.WithAttributes(
		attribute.String("index", t.id.Index), attribute.Int("shard", t.id.Shard),
		attribute.Int64("seq", h.Seq), attribute.String("reason", h.Reason)))
	span.RecordError(h)
	span.SetStatus(codes.Error, "shard copy halted")
	span.End()
	if repeat {
		t.log.WarnContext(ctx, "shard copy still halted", slog.Int64("seq", h.Seq), slog.String("id", h.ID),
			slog.String("reason", h.Reason))
	} else {
		t.log.ErrorContext(ctx, "shard copy halted: the store accepted a change this copy cannot apply",
			slog.Int64("seq", h.Seq), slog.String("id", h.ID), slog.String("reason", h.Reason),
			slog.Int64("applied", t.Applied()), slog.Any("error", h.Err))
	}
	if err := t.setCopyState(ctx, store.CopyRecovering); err != nil {
		if errors.Is(err, store.ErrLeaseLost) {
			return err
		}
		t.log.WarnContext(ctx, "could not mark the halted copy recovering; retrying with the halt", slog.Any("error", err))
	}
	if repeat && t.gate == nil && t.needRebuild == "" {
		superseded, err := t.superseded(ctx, h)
		switch {
		case err != nil:
			t.log.DebugContext(ctx, "could not look for a change superseding the halted one", slog.Any("error", err))
		case superseded:
			t.needRebuild = reasonHalted
		}
	}
	t.haltWait = nextBackoff(t.haltWait, t.opts.HaltRetryBase, t.opts.HaltRetryCap)
	t.log.InfoContext(ctx, "halted copy retries after a backoff", slog.Duration("backoff", t.haltWait),
		slog.Bool("rebuild", t.needRebuild != ""))
	t.pause(ctx, t.haltWait)
	return nil
}

// clearHalt ends a halt once the copy moves again.
func (t *Tailer) clearHalt() {
	if h := t.halt.Swap(nil); h != nil {
		t.haltWait = 0
		t.log.Info("halted copy moves again", slog.Int64("halted_at", h.Seq), slog.Int64("seq", t.Applied()))
	}
}

// superseded reports whether the changelog holds a change of h's document or query
// (or mapping) after h's seq: the bad row is then no longer current, and a rebuild
// from a snapshot gets past it. The search resumes where the last one for h stopped.
// A changelog pruned past h needs a rebuild anyway: that counts as superseded once.
// After that rebuild has stopped at h again, the record itself decides
// ([Tailer.recordMoved]), so a halted copy is not rescanned at every backoff.
func (t *Tailer) superseded(ctx context.Context, h *HaltError) (bool, error) {
	if t.scanFor == nil || t.scanFor.Seq != h.Seq || t.scanFor.ID != h.ID {
		t.scanFor, t.scanFrom, t.scanPruned = h, h.Seq, false
	}
	for {
		page, err := t.st.ChangesAfter(ctx, t.id, t.scanFrom, t.opts.BatchSize)
		if errors.Is(err, store.ErrPruned) {
			if !t.scanPruned {
				t.scanPruned = true
				return true, nil
			}
			return t.recordMoved(ctx, h)
		}
		if err != nil {
			return false, err
		}
		for i := range page {
			c := &page[i]
			if c.ID == h.ID && (h.class == "" || classOfStoreKind(c.Kind) == h.class) {
				return true, nil
			}
		}
		if len(page) == 0 {
			return false, nil
		}
		t.scanFrom = page[len(page)-1].Seq
		if len(page) < t.opts.BatchSize {
			return false, nil
		}
	}
}

// recordMoved reports whether h's document or saved query is no longer the row h
// halted at: deleted, or written again since. A mapping, or a store that cannot read
// records, never reports it.
func (t *Tailer) recordMoved(ctx context.Context, h *HaltError) (bool, error) {
	rr, ok := t.st.(store.RecordReader)
	if !ok {
		return false, nil
	}
	var kind store.RecordKind
	switch h.class {
	case classDocument:
		kind = store.RecordDocument
	case classQuery:
		kind = store.RecordQuery
	default:
		return false, nil
	}
	r, err := rr.GetRecord(ctx, kind, t.id, h.ID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return true, nil
	case err != nil:
		return false, err
	}
	return r.Seq != h.Seq, nil
}
