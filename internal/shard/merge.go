package shard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/Imposter/go-searchlight/internal/segment"
)

// Merges.
//
// segment.Merge is in memory: it reads every live input document's stored body into
// memory before writing the merged file, so a merge needs about its result's stored
// size in RAM (TieredPolicy.MaxMergedBytes, 1 GiB by default, bounds it), and its
// accumulation phase cannot be cancelled: only the write phase checks the budget's
// throttle, and through it the context. Close therefore waits for a merge in its
// accumulation phase to reach its first write. (A streaming, cancellable merge is
// parked to Task 14.)
//
// The merge loop asks the policy for merges after every commit, reserves their inputs
// (a reserved segment is in no other merge), and runs each in its own goroutine, which
// first waits for MergeBudget tokens. A merge works from a generation it acquires: its
// inputs stay mapped, and their deletes as of that generation are its snapshot. It
// writes the merged segment with no lock held, so refreshes and readers carry on.
//
// Then it commits, under the commit lock: the merged segment replaces its inputs, at the
// first input's place. Deletes that refreshes added to the inputs since the snapshot are
// carried over: a document the snapshot had live is in the merged segment at ordinal
// base + ord - (snapshot deletes up to ord), where base is the live count of the inputs
// before it, which is exactly the order segment.Merge assigns. A merge whose inputs are
// wholly deleted writes nothing and only drops them.

// mergePlan is one merge: its inputs, all of one kind.
type mergePlan struct {
	kind   segKind
	inputs []*segRef
}

func (s *Shard) wakeMerges() {
	if !s.opts.DisableMerges {
		wake(s.mergeWake)
	}
}

func (s *Shard) mergeLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.bg.Done():
			return
		case <-s.mergeWake:
		}
		if s.Err() != nil {
			continue
		}
		for _, p := range s.planMerges() {
			s.wg.Add(1)
			go func(p mergePlan) {
				defer s.wg.Done()
				_, err := s.runMerge(s.bg, p)
				if err != nil && s.bg.Err() == nil && !errors.Is(err, ErrClosed) {
					s.log.WarnContext(s.bg, "merge failed", slog.String("kind", p.kind.String()), slog.Int("segments", len(p.inputs)), slog.Any("error", err))
				}
			}(p)
		}
	}
}

// planMerges asks the policy for merges of the current generation's segments that no
// merge holds, and reserves them.
func (s *Shard) planMerges() []mergePlan {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	cur := s.cur.Load()
	if cur == nil {
		return nil
	}
	var plans []mergePlan
	for _, kind := range []segKind{kindDocs, kindQueries} {
		list := cur.docs
		if kind == kindQueries {
			list = cur.queries
		}
		cands := make([]MergeCandidate, len(list))
		byID := make(map[string]*segRef, len(list))
		for i := range list {
			st := &list[i]
			cands[i] = MergeCandidate{
				ID: st.ref.id, Bytes: st.ref.bytes, Docs: st.ref.numDocs,
				Deleted: uint32(st.deletes.GetCardinality()), //nolint:gosec // ordinals of one segment
				Merging: s.merging[st.ref],
			}
			byID[st.ref.id] = st.ref
		}
		for _, ids := range s.opts.MergePolicy.FindMerges(cands) {
			p := mergePlan{kind: kind}
			for _, id := range ids {
				p.inputs = append(p.inputs, byID[id])
			}
			s.reserve(p)
			plans = append(plans, p)
		}
	}
	return plans
}

// reserve marks p's inputs as merging; the caller holds commitMu. The merge backlog
// metric is the segments reserved.
func (s *Shard) reserve(p mergePlan) {
	for _, ref := range p.inputs {
		s.merging[ref] = true
	}
	s.inflight++
	s.inst.recordBacklog(s.bg, len(s.merging))
}

// unreserve ends p's reservation and wakes ForceMerge and the merge loop.
func (s *Shard) unreserve(p mergePlan) {
	s.commitMu.Lock()
	for _, ref := range p.inputs {
		delete(s.merging, ref)
	}
	s.inflight--
	s.inst.recordBacklog(s.bg, len(s.merging))
	close(s.mergeDone)
	s.mergeDone = make(chan struct{})
	s.commitMu.Unlock()
}

// runMerge merges p's inputs and commits the result, which it returns (nil when the
// inputs were wholly deleted). p is reserved; runMerge releases it.
func (s *Shard) runMerge(ctx context.Context, p mergePlan) (merged *segRef, err error) {
	defer s.unreserve(p)
	ctx, span := s.startSpan(ctx, "shard.merge",
		attribute.String("kind", p.kind.String()), attribute.Int("segments", len(p.inputs)))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "merge failed")
			s.inst.mergeFailures.Add(ctx, 1, s.inst.attrs)
		}
		span.End()
	}()

	budget := s.opts.MergeBudget
	tokens, err := budget.acquire(ctx, budget.Threads())
	if err != nil {
		return nil, err
	}
	defer budget.release(tokens)
	start := s.opts.Clock.Now()

	g := s.Acquire()
	if g == nil {
		return nil, ErrClosed
	}
	defer g.Release()
	list := g.docs
	if p.kind == kindQueries {
		list = g.queries
	}
	snap := make([]*roaring.Bitmap, len(p.inputs))
	bases := make([]uint64, len(p.inputs)) // each input's first ordinal in the merged segment
	var live uint64
	for i, ref := range p.inputs {
		j := slices.IndexFunc(list, func(st segState) bool { return st.ref == ref })
		if j < 0 {
			return nil, fmt.Errorf("shard: merge input %s is not in the current generation", ref.id)
		}
		snap[i] = list[j].deletes
		bases[i] = live
		live += uint64(ref.numDocs) - snap[i].GetCardinality()
	}

	var out *segRef
	var written int64
	if live > 0 {
		out, written, err = s.writeMerged(ctx, p, snap, g, tokens)
		if err != nil {
			return nil, err
		}
	}
	if err := s.hook(pointMergeBuilt); err != nil {
		if out != nil {
			_ = out.close()
			if !isCrash(err) {
				s.jan.removeLater(segmentFiles(s.dir, out.id)...)
			}
		}
		if isCrash(err) {
			s.fail(err)
		}
		return nil, err
	}

	published, err := s.commitMerge(ctx, p, out, snap, bases)
	if !published && out != nil {
		_ = out.close()
		if !isCrash(err) {
			s.jan.removeLater(segmentFiles(s.dir, out.id)...)
		}
	}
	if err != nil {
		return nil, err
	}
	d := s.opts.Clock.Since(start)
	s.inst.recordMerge(ctx, d, written)
	s.log.DebugContext(ctx, "merged", slog.String("kind", p.kind.String()), slog.Int("segments", len(p.inputs)),
		slog.Uint64("live", live), slog.Int64("bytes", written), slog.Float64(telemetryDuration, float64(d.Microseconds())/1000))
	s.wakeMerges()
	return out, nil
}

// writeMerged writes p's live documents or queries, as of the snapshot deletes, as one
// new segment, with threads goroutines (the budget tokens the merge holds).
func (s *Shard) writeMerged(ctx context.Context, p mergePlan, snap []*roaring.Bitmap, g *Generation, threads int) (*segRef, int64, error) {
	if p.kind == kindQueries {
		var queries []StoredQuery
		for i, ref := range p.inputs {
			for ord := range ref.numDocs {
				if snap[i].Contains(ord) {
					continue
				}
				q, err := ref.qs.Query(ord)
				if err != nil {
					return nil, 0, fmt.Errorf("shard: reading query %d of %s: %w", ord, ref.id, err)
				}
				queries = append(queries, q)
			}
		}
		out, err := s.buildQuerySegment(ctx, queries, g)
		if err != nil {
			return nil, 0, err
		}
		return out, out.bytes, nil
	}

	readers := make([]*segment.Reader, len(p.inputs))
	for i, ref := range p.inputs {
		readers[i] = ref.reader
	}
	var written int64
	name := newSegmentName()
	meta, err := segment.Merge(s.dir, readers, snap, segment.MergeOptions{
		Name:      name,
		Threads:   threads,
		NoDirSync: true, // the commit syncs the directory (see commit)
		Throttle: func(n int) error {
			written += int64(n)
			return s.opts.MergeBudget.throttle(ctx, n)
		},
	})
	if err != nil {
		s.jan.removeLater(segmentFiles(s.dir, name)...)
		return nil, 0, fmt.Errorf("shard: merging %d segments: %w", len(p.inputs), err)
	}
	out, err := s.openDocSegment(meta)
	if err != nil {
		s.jan.removeLater(segmentFiles(s.dir, name)...)
		return nil, 0, err
	}
	return out, written, nil
}

// commitMerge replaces p's inputs with out (nil: they were wholly deleted), carrying
// over the deletes made since snap.
func (s *Shard) commitMerge(ctx context.Context, p mergePlan, out *segRef, snap []*roaring.Bitmap, bases []uint64) (bool, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	cur := s.cur.Load()
	if cur == nil {
		return false, ErrClosed
	}
	if err := s.usable(); err != nil {
		return false, err
	}
	list := cur.docs
	if p.kind == kindQueries {
		list = cur.queries
	}
	inputAt := make(map[*segRef]int, len(p.inputs))
	for i, ref := range p.inputs {
		inputAt[ref] = i
	}
	merged := roaring.New()
	next := make([]segState, 0, len(list))
	place := -1
	for _, st := range list {
		i, isInput := inputAt[st.ref]
		if !isInput {
			next = append(next, st)
			continue
		}
		if place < 0 {
			place = len(next)
		}
		if out == nil {
			continue
		}
		// Deletes refreshes added since the snapshot: the merged segment holds those
		// documents (they were live in the snapshot), so they are deleted there too.
		it := roaring.AndNot(st.deletes, snap[i]).Iterator()
		for it.HasNext() {
			ord := it.Next()
			merged.Add(uint32(bases[i] + uint64(ord) - snap[i].Rank(ord))) //nolint:gosec // an ordinal of the merged segment
		}
	}
	if place < 0 {
		return false, errors.New("shard: merge inputs vanished from the current generation")
	}
	if out != nil {
		st := segState{ref: out, deletes: emptyDeletes}
		if !merged.IsEmpty() {
			st.deletes, st.dirty = merged, true
		}
		next = slices.Insert(next, place, st)
	}
	docs, queries := next, slices.Clone(cur.queries)
	if p.kind == kindQueries {
		docs, queries = slices.Clone(cur.docs), next
	}
	published, err := s.commit(ctx, docs, queries, p.inputs, cur.seq, cur.maxSeq, cur.uid, cur.mp)
	if published && out != nil && s.forceEligible != nil {
		// A merge of only segments a ForceMerge is responsible for (a background merge
		// in flight when it started, whose inputs all existed then, or its own) makes a
		// segment it is responsible for. One that also took a segment refreshed since
		// does not: ForceMerge would otherwise keep adopting new writes.
		all := true
		for _, ref := range p.inputs {
			if !s.forceEligible[ref] {
				all = false
				break
			}
		}
		if all {
			s.forceEligible[out] = true
		}
	}
	return published, err
}

// ForceMerge merges the segments the shard holds when it is called down to at most
// maxSegments document segments (and as many query segments), and rewrites each of them
// that has deletes without them, like Lucene's forceMerge: the smallest are merged into
// one. The segments it is responsible for are those present when it starts and every
// segment a merge of them makes, its own or a background merge's (one in flight when
// it starts included); segments refreshes add meanwhile are left to the merge policy.
// Each segment is rewritten for its deletes at most once, so a steady stream of
// deletes cannot keep it going. It waits for background merges in flight, ignores
// DisableMerges, and runs one at a time. Close cancels it and waits for it to return;
// after Close it returns ErrClosed.
func (s *Shard) ForceMerge(ctx context.Context, maxSegments int) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return ErrClosed
	}
	s.wg.Add(1) // before closing is set, so before shutdown's wg.Wait
	s.mu.Unlock()
	defer s.wg.Done()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(s.bg, cancel)() //nolint:contextcheck // s.bg (cancelled by shutdown) only cancels ctx, which stays derived from the caller's

	ctx, span := s.startSpan(ctx, "shard.force_merge", attribute.Int("max_segments", maxSegments))
	defer span.End()
	maxSegments = max(1, maxSegments)
	// One ForceMerge at a time; waiting for another ends with ctx or the shard.
	select {
	case s.forceSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.bg.Done():
		return ErrClosed
	}
	defer func() { <-s.forceSem }()
	// s.forceEligible: the segments ForceMerge is responsible for (see above), kept
	// under commitMu so commitMerge adds every merge's output of them.
	// rewritten: segments already rewritten for their deletes, never again.
	rewritten := map[*segRef]bool{}
	s.commitMu.Lock()
	eligible := map[*segRef]bool{}
	if cur := s.cur.Load(); cur != nil {
		for _, list := range [][]segState{cur.docs, cur.queries} {
			for _, st := range list {
				eligible[st.ref] = true
			}
		}
	}
	s.forceEligible = eligible
	s.commitMu.Unlock()
	defer func() {
		s.commitMu.Lock()
		s.forceEligible = nil
		s.commitMu.Unlock()
	}()
	for {
		if err := s.usable(); err != nil {
			return err
		}
		s.commitMu.Lock()
		cur := s.cur.Load()
		if cur == nil {
			s.commitMu.Unlock()
			return ErrClosed
		}
		if s.inflight > 0 {
			done := s.mergeDone
			s.commitMu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		plans := forcePlans(kindDocs, onlyEligible(cur.docs, eligible), maxSegments, rewritten)
		plans = append(plans, forcePlans(kindQueries, onlyEligible(cur.queries, eligible), maxSegments, rewritten)...)
		for _, p := range plans {
			s.reserve(p)
		}
		s.commitMu.Unlock()
		if len(plans) == 0 {
			return nil
		}
		for i, p := range plans {
			out, err := s.runMerge(ctx, p)
			if err != nil {
				for _, rest := range plans[i+1:] {
					s.unreserve(rest)
				}
				span.RecordError(err)
				span.SetStatus(codes.Error, "force merge failed")
				return err
			}
			if out != nil && len(p.inputs) == 1 {
				rewritten[out] = true
			}
		}
	}
}

// onlyEligible returns the states of list whose segment is in eligible.
func onlyEligible(list []segState, eligible map[*segRef]bool) []segState {
	out := make([]segState, 0, len(list))
	for _, st := range list {
		if eligible[st.ref] {
			out = append(out, st)
		}
	}
	return out
}

// forcePlans is ForceMerge's next step for one kind: merge the smallest segments into
// one when there are more than maxSegments, else rewrite each segment with deletes that
// is not already a rewrite.
func forcePlans(kind segKind, list []segState, maxSegments int, rewritten map[*segRef]bool) []mergePlan {
	if len(list) > maxSegments {
		sorted := slices.Clone(list)
		slices.SortStableFunc(sorted, func(a, b segState) int {
			switch {
			case a.ref.bytes < b.ref.bytes:
				return -1
			case a.ref.bytes > b.ref.bytes:
				return 1
			}
			return 0
		})
		p := mergePlan{kind: kind}
		for _, st := range sorted[:len(list)-maxSegments+1] {
			p.inputs = append(p.inputs, st.ref)
		}
		return []mergePlan{p}
	}
	var plans []mergePlan
	for _, st := range list {
		if !st.deletes.IsEmpty() && !rewritten[st.ref] {
			plans = append(plans, mergePlan{kind: kind, inputs: []*segRef{st.ref}})
		}
	}
	return plans
}
