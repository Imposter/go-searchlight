package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/percolate"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// prepared is one op ready to commit.
type prepared struct {
	pos    int // the op's position in the request
	change store.Change
	doc    *schema.Doc // an upsert's analyzed document, for percolation
}

// Write implements [api.Coordinator]: see the package documentation.
func (n *Single) Write(ctx context.Context, name string, ops []api.WriteOp, opts api.WriteOptions) (res *api.WriteResult, err error) {
	ctx, span := n.tr.Start(ctx, "node.write", trace.WithAttributes(
		attribute.String(telemetry.KeyIndex, name), attribute.Int("ops", len(ops)), attribute.Bool("percolate", opts.Percolate)))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "write failed")
		}
		span.End()
	}()
	idx, err := n.lookup(name)
	if err != nil {
		return nil, err
	}
	if opts.Percolate && opts.WaitForSeq > 0 {
		if err := n.checkHead(ctx, opts.WaitForSeq); err != nil {
			return nil, err // refused before anything commits
		}
	}
	res = &api.WriteResult{Items: make([]api.ItemResult, len(ops))}
	batch, err := n.prepareWrite(ctx, idx, ops, res.Items, opts.Percolate)
	if err != nil {
		return nil, err
	}
	if len(batch) == 0 {
		return res, nil
	}
	if err := n.admit(idx, batch); err != nil {
		return nil, err
	}
	batch, err = n.commit(ctx, idx, batch, res.Items)
	if err != nil {
		return nil, err
	}
	if len(batch) == 0 {
		return res, nil
	}
	res.Seq = batch[len(batch)-1].change.Seq
	span.SetAttributes(attribute.Int64("seq", res.Seq))

	touched := map[int]bool{}
	for i := range batch {
		touched[batch[i].change.Shard] = true
	}
	for s := range touched {
		idx.copies[s].tailer.Wake()
	}
	if opts.Refresh != api.RefreshNone {
		if err := n.waitWritten(ctx, idx, touched, res.Seq, opts.Refresh); err != nil {
			res.TimedOut = true
			n.log.WarnContext(ctx, "refresh after a write did not finish", slog.String(telemetry.KeyIndex, name), slog.Int64("seq", res.Seq), slog.Any("error", err))
		}
	}
	if opts.Percolate {
		// The write is committed: a percolation that cannot finish is reported in
		// the result (Percolated false), never as an error that hides the seqs.
		if err := n.percolateWritten(ctx, idx, batch, res, opts.WaitForSeq); err != nil {
			res.Percolated = false
			if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				res.TimedOut = true
			}
			n.log.WarnContext(ctx, "percolating a committed write failed", slog.String(telemetry.KeyIndex, name), slog.Int64("seq", res.Seq), slog.Any("error", err))
		}
	}
	return res, nil
}

// prepareWrite validates and analyzes ops, storing an op refused on its own in its
// item, and grows the stored mapping with the documents' new dynamic fields before
// anything commits. It returns the ops to commit. keepDocs keeps each upsert's
// analyzed document (for percolation); otherwise it is dropped as soon as it is
// checked, and so is every op's body once its change holds the analyzed copy.
//
// Mappings are additive, so concurrent writers never fail each other: fields the
// mapping already holds cost nothing, and the new fields of concurrent writes are
// stored together in one catalogue update (ensureFields). A write is analyzed again
// only when a field it typed was meanwhile given another type, or no longer fits the
// field limit.
func (n *Single) prepareWrite(ctx context.Context, idx *index, ops []api.WriteOp, items []api.ItemResult, keepDocs bool) ([]prepared, error) {
	base := idx.meta.Load().mapping
	for range maxCatalogTries {
		batch, working := n.analyzeOps(idx, ops, items, base, keepDocs)
		if working == base {
			return batch, nil
		}
		err := n.ensureFields(ctx, idx, added(base, working))
		switch {
		case err == nil:
			return batch, nil
		case errors.Is(err, errReanalyze):
			base = idx.meta.Load().mapping
		default:
			return nil, err
		}
	}
	return nil, api.Unavailable(store.ErrConflict, "the index's mapping kept changing under the write; retry")
}

// analyzeOps prepares every op under base, returning the batch and base grown by the
// documents' new dynamic fields.
func (n *Single) analyzeOps(idx *index, ops []api.WriteOp, items []api.ItemResult, base *schema.Mapping, keepDocs bool) ([]prepared, *schema.Mapping) {
	working := base
	limit := n.maxIndexFields()
	uid := idx.meta.Load().meta.UID
	batch := make([]prepared, 0, len(ops))
	for i := range ops {
		items[i] = api.ItemResult{}
		p, m, err := n.prepareOp(idx, &ops[i], working, keepDocs)
		if err != nil {
			items[i].Err = err
			continue
		}
		if field := overFieldLimit(working, m, limit); field != "" {
			items[i].Err = api.InvalidAt("body."+field, "the index would hold %d fields, over max_index_fields (%d)", len(m.Fields), limit)
			continue
		}
		if !keepDocs {
			p.doc = nil
		}
		if p.change.Kind == store.KindUpsert {
			// The op's body is now the analyzed copy: the raw one is freed, and
			// analyzing again (the mapping moved) reads the same document.
			ops[i].Body = p.change.Payload
		}
		p.pos = i
		// The incarnation this write means: a write that races a drop and
		// recreate of the index is refused rather than landing in the new one.
		p.change.IndexUID = uid
		working = m
		batch = append(batch, p)
	}
	return batch, working
}

// added lists the fields grown has that base lacks.
func added(base, grown *schema.Mapping) map[string]schema.FieldType {
	out := map[string]schema.FieldType{}
	for name, t := range grown.Fields {
		if _, ok := base.Fields[name]; !ok {
			out[name] = t
		}
	}
	return out
}

// prepareOp turns one op into a change under mapping m, returning the mapping grown
// by an upsert's new dynamic fields.
func (n *Single) prepareOp(idx *index, op *api.WriteOp, m *schema.Mapping, keepDoc bool) (prepared, *schema.Mapping, error) {
	c := store.Change{Index: idx.name, ID: op.ID, IfSeq: op.IfSeq}
	if err := schema.ValidateID(op.ID); err != nil {
		msg := err.Error()
		var ve *schema.ValidationError
		if errors.As(err, &ve) {
			msg = ve.Message
		}
		return prepared{}, m, api.InvalidAt("id", "%s", msg)
	}
	c.Shard = ShardFor(op.ID, len(idx.copies))
	var p prepared
	switch op.Kind {
	case api.OpUpsert:
		if limit := n.cfg.MaxDocBytes; limit > 0 && int64(len(op.Body)) > limit {
			return prepared{}, m, api.TooLarge("the document is %d bytes, over %d (max_doc_bytes)", len(op.Body), limit)
		}
		// A document to percolate is analyzed in full; otherwise it is only
		// checked and typed (schema.Check), which allocates a fraction as much.
		var doc schema.Doc
		var update schema.MappingUpdate
		var err error
		if keepDoc {
			doc, update, err = schema.Analyze(m, op.ID, op.Body)
		} else {
			doc.ID = op.ID
			doc.Body, update, err = schema.Check(m, op.ID, op.Body)
		}
		if err != nil {
			return prepared{}, m, err
		}
		// The analyzed body is what commits: invalid UTF-8 has become U+FFFD (three
		// bytes for one), so the limits apply to it too, and a document no segment
		// can store never reaches the changelog, where it would halt the copy.
		if limit := n.cfg.MaxDocBytes; limit > 0 && int64(len(doc.Body)) > limit {
			return prepared{}, m, api.TooLarge("the document is %d bytes once its invalid UTF-8 is replaced, over %d (max_doc_bytes)", len(doc.Body), limit)
		}
		if err := shard.CheckDocSize(op.ID, doc.Body); err != nil {
			return prepared{}, m, api.TooLarge("%v", err)
		}
		if !update.Empty() {
			grown, err := m.Merge(update)
			if err != nil {
				return prepared{}, m, err
			}
			m = grown
		}
		// The body as analyzed: invalid UTF-8 replaced, as the store requires.
		c.Kind, c.Payload, p.doc = store.KindUpsert, doc.Body, &doc
	case api.OpDelete:
		c.Kind = store.KindDelete
		if c.IfSeq == 0 {
			c.IfSeq = store.IfExists // a delete of nothing is a 404 and writes no change
		}
	case api.OpQueryUpsert:
		payload, err := queryPayload(op, m)
		if err != nil {
			return prepared{}, m, err
		}
		c.Kind, c.Payload = store.KindQueryUpsert, payload
	case api.OpQueryDelete:
		c.Kind = store.KindQueryDelete
		if c.IfSeq == 0 {
			c.IfSeq = store.IfExists
		}
	default:
		return prepared{}, m, api.InvalidAt("op", "unknown op %v", op.Kind)
	}
	p.change = c
	return p, m, nil
}

// queryPayload checks a saved query (it parses, it is valid against the mapping, the
// percolator can index it, its meta is an object of at most 16 KiB) and encodes it.
func queryPayload(op *api.WriteOp, m *schema.Mapping) ([]byte, error) {
	node, problems := query.Parse(op.Query)
	if len(problems) == 0 {
		problems = query.Validate(node, m)
	}
	if len(problems) > 0 {
		return nil, api.Invalid("the saved query is invalid", problems...)
	}
	meta := bytes.TrimSpace(op.Meta)
	if len(meta) == 0 || string(meta) == "null" {
		meta = nil
	} else {
		if len(meta) > shard.MaxMetaBytes {
			return nil, api.TooLarge("meta is %d bytes, over %d", len(meta), shard.MaxMetaBytes)
		}
		var obj map[string]json.RawMessage
		if json.Unmarshal(meta, &obj) != nil || obj == nil {
			return nil, api.InvalidAt("meta", "meta is a JSON object")
		}
	}
	if err := (percolate.Index{}).Check(&shard.StoredQuery{ID: op.ID, Query: node, Meta: meta}); err != nil {
		return nil, api.InvalidAt("query", "the percolator cannot index the query: %v", err)
	}
	payload, err := store.EncodeQueryPayload(op.Query, meta)
	if err != nil {
		return nil, api.InvalidAt("query", "%v", err)
	}
	return payload, nil
}

// admit refuses a write (429) to a shard copy whose write buffer is full while
// refreshes catch up, or that trails what the node committed to it by more than
// MaxApplyLag: the client backs off rather than the changelog outrunning the copies.
func (n *Single) admit(idx *index, batch []prepared) error {
	seen := map[int]bool{}
	retry := max(api.DefaultRetryAfter, time.Duration(idx.refresh.Load()))
	for i := range batch {
		s := batch[i].change.Shard
		if seen[s] {
			continue
		}
		seen[s] = true
		c := idx.copies[s]
		sh := c.shard()
		if sh == nil {
			return api.Unavailable(shard.ErrClosed, "shard %d is unavailable", s)
		}
		if err := c.notServing(); err != nil {
			return err
		}
		if err := sh.Admit(); err != nil {
			if errors.Is(err, shard.ErrBackpressure) {
				return api.TooMany(retry, "shard %d's write buffer is full while refreshes catch up; retry", s)
			}
			return api.Unavailable(err, "shard %d is unavailable", s)
		}
		if lag := c.backlog(); lag > n.maxLag {
			return api.TooMany(retry, "shard %d trails its writes by %d changes; retry", s, lag)
		}
	}
	return nil
}

// commit commits batch through the group committer. An op whose if_seq fails, or
// that the store refuses on its own, is answered in its item and the rest commit
// without it. It returns the committed ops, with their seqs.
func (n *Single) commit(ctx context.Context, idx *index, batch []prepared, items []api.ItemResult) ([]prepared, error) {
	for len(batch) > 0 {
		changes := make([]store.Change, len(batch))
		for i := range batch {
			changes[i] = batch[i].change
		}
		_, _, err := n.gc.Apply(ctx, changes)
		if err == nil || !store.IsTransient(err) {
			n.noteDB(nil)
		} else if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			n.noteDB(err)
		}
		var ce *store.ConflictError
		var che *store.ChangeError
		var inf *store.IndexNotFoundError
		switch {
		case err == nil:
			// A delete of nothing (IfExists) was skipped in the transaction: it
			// took no seq and is a 404.
			var applied []prepared
			for i := range batch {
				if changes[i].Seq == 0 {
					items[batch[i].pos].Err = conditionFailed(&batch[i].change, 0)
					continue
				}
				batch[i].change.Seq = changes[i].Seq
				applied = append(applied, batch[i])
			}
			batch = applied
			if len(batch) == 0 {
				return nil, nil
			}
			for i := range batch {
				items[batch[i].pos].Seq = batch[i].change.Seq
				c := idx.copies[batch[i].change.Shard]
				raise(&c.written, batch[i].change.Seq)
				if k := batch[i].change.Kind; k == store.KindQueryUpsert || k == store.KindQueryDelete {
					raise(&c.querySeq, batch[i].change.Seq)
				}
			}
			n.noteHead(batch[len(batch)-1].change.Seq)
			return batch, nil
		case errors.As(err, &ce):
			drop := map[int]bool{}
			for k, pos := range ce.Positions {
				items[batch[pos].pos].Err = conditionFailed(&batch[pos].change, ce.Current[k])
				drop[pos] = true
			}
			batch = without(batch, drop)
		case errors.As(err, &che):
			items[batch[che.Position].pos].Err = api.InvalidAt("body", "%v", che.Err)
			batch = without(batch, map[int]bool{che.Position: true})
		case errors.As(err, &inf):
			return nil, indexNotFound(idx.name)
		default:
			return nil, storeError(err, idx.name)
		}
	}
	return nil, nil
}

// conditionFailed answers a change whose condition failed: a delete of nothing is a
// 404 (result not_found), anything else a 409 with the target's current seq.
func conditionFailed(c *store.Change, current int64) *api.Error {
	if current == 0 && (c.Kind == store.KindDelete || c.Kind == store.KindQueryDelete) {
		code, what := api.CodeDocumentNotFound, "document"
		if c.Kind == store.KindQueryDelete {
			code, what = api.CodeQueryNotFound, "saved query"
		}
		e := api.NotFound(code, "%s %q does not exist in index %q", what, c.ID, c.Index)
		e.Extra = map[string]any{"result": "not_found"}
		return e
	}
	e := api.Conflict(api.CodeConflict, "the if_seq condition failed")
	e.Extra = map[string]any{"current_seq": current}
	return e
}

func without(batch []prepared, drop map[int]bool) []prepared {
	out := batch[:0:0]
	for i := range batch {
		if !drop[i] {
			out = append(out, batch[i])
		}
	}
	return out
}

// raise sets v to seq if seq is higher.
func raise(v *atomic.Int64, seq int64) {
	for {
		cur := v.Load()
		if seq <= cur || v.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// waitWritten waits, under ctx's deadline, for the written shards to have seq
// searchable: by their next refresh (wait_for), or at once (true: once applied, the
// copies are refreshed).
func (n *Single) waitWritten(ctx context.Context, idx *index, touched map[int]bool, seq int64, mode api.RefreshMode) error {
	shards := make([]int, 0, len(touched))
	for s := range touched {
		shards = append(shards, s)
	}
	slices.Sort(shards)
	for _, s := range shards {
		c := idx.copies[s]
		if mode == api.RefreshTrue {
			if err := waitApplied(ctx, c, seq); err != nil {
				return err
			}
			if sh := c.shard(); sh != nil && sh.RefreshedSeq() < seq {
				if err := sh.Refresh(ctx); err != nil {
					return err
				}
			}
		}
		if err := c.waitRefreshed(ctx, seq); err != nil {
			return err
		}
	}
	return nil
}

// waitApplied polls until the copy has applied seq.
func waitApplied(ctx context.Context, c *copyState, seq int64) error {
	delay := 200 * time.Microsecond
	for {
		sh := c.shard()
		if sh == nil {
			return shard.ErrClosed
		}
		if sh.AppliedSeq() >= seq {
			return nil
		}
		if err := c.notServing(); err != nil {
			return err
		}
		if err := sh.Err(); err != nil {
			return err
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
		delay = min(2*delay, 10*time.Millisecond)
	}
}

// percolateWritten fills each committed upsert's item with the saved queries its
// document matches, as written: against every saved query committed through this node
// before the percolation (waitQueries), and every change up to waitFor when it is
// set.
func (n *Single) percolateWritten(ctx context.Context, idx *index, batch []prepared, res *api.WriteResult, waitFor int64) error {
	var docs []schema.Doc
	var pos []int
	for i := range batch {
		if batch[i].doc != nil {
			docs = append(docs, *batch[i].doc)
			pos = append(pos, batch[i].pos)
		}
	}
	if len(docs) == 0 {
		res.Percolated = true
		return nil
	}
	if waitFor > 0 {
		if err := n.waitSeq(ctx, idx, waitFor); err != nil {
			return err
		}
	}
	if err := n.waitQueries(ctx, idx); err != nil {
		return err
	}
	matches, err := n.percolateDocs(ctx, idx, docs)
	if err != nil {
		return err
	}
	for k, p := range pos {
		res.Items[p].Queries = matches[k]
	}
	res.Percolated = true
	return nil
}

// percolateDocs percolates docs against the saved queries of every shard of idx (a
// saved query lives on the shard its id hashes to), merging each document's matches.
func (n *Single) percolateDocs(ctx context.Context, idx *index, docs []schema.Doc) ([][]string, error) {
	gens, release, err := n.acquire(idx)
	if err != nil {
		return nil, err
	}
	defer release()
	out := make([][]string, len(docs))
	for s, g := range gens {
		if g.NumQueries() == 0 {
			continue
		}
		ids, err := idx.copies[s].perc.Percolate(ctx, g, docs)
		if err != nil {
			return nil, fmt.Errorf("percolate shard %d: %w", s, err)
		}
		for i := range ids {
			out[i] = append(out[i], ids[i]...)
		}
	}
	if len(gens) > 1 {
		for i := range out {
			slices.Sort(out[i])
			out[i] = slices.Compact(out[i])
		}
	}
	return out, nil
}
