package shard

import (
	"math"
	"slices"
	"strings"
)

// TieredPolicy chooses merges like Lucene's TieredMergePolicy: segments are grouped by
// size into tiers, each tier holding up to SegmentsPerTier segments of about the same
// size, and each tier MaxMergeAtOnce times the size of the one below. While a shard has
// more segments than its tiers allow, the policy merges the run of up to MaxMergeAtOnce
// similarly sized segments that scores best: low skew (the merge's biggest input is a
// small share of it, so the work is not mostly rewriting one segment), small result
// (bytes^0.05), and many deletes reclaimed (by the square of the live-to-total ratio).
// A segment's size is its bytes less the share its deletes hold; segments under
// FloorSegmentBytes count as that size, so tiny refreshes are merged eagerly. A segment
// over half of MaxMergedBytes is left alone (it would grow past the cap), unless more
// than DeletesPctAllowed percent of it is deleted; any segment past that is rewritten
// on its own, to reclaim its deletes, when no regular merge picks it.
type TieredPolicy struct {
	// SegmentsPerTier is how many segments a tier holds before it is merged. Default 10.
	SegmentsPerTier int
	// MaxMergeAtOnce is the most segments one merge takes. Default 10.
	MaxMergeAtOnce int
	// MaxMergedBytes caps a merge's result. Default 1 GiB, not Lucene's 5 GiB:
	// segment.Merge holds every live document's stored body in memory while it
	// writes (a streaming merge is parked to Task 14), so this also bounds a merge's
	// memory, once per concurrent merge.
	MaxMergedBytes int64
	// FloorSegmentBytes is the size a smaller segment counts as. Default 2 MiB.
	FloorSegmentBytes int64
	// DeletesPctAllowed is the percentage of a segment that may be deleted before it is
	// rewritten to reclaim them. Default 20.
	DeletesPctAllowed float64
}

// DefaultTieredPolicy returns Lucene's defaults (Elasticsearch's too), except a 1 GiB
// MaxMergedBytes (see its comment).
func DefaultTieredPolicy() TieredPolicy {
	return TieredPolicy{
		SegmentsPerTier:   10,
		MaxMergeAtOnce:    10,
		MaxMergedBytes:    1 << 30,
		FloorSegmentBytes: 2 << 20,
		DeletesPctAllowed: 20,
	}
}

// DefaultQueryTieredPolicy returns the policy for query segments: two segments per
// tier. Percolating a document probes every query segment with all of its atoms, so
// each one costs every document a full probe, while saved queries change rarely and a
// query segment is cheap to rebuild: few, larger query segments are worth the merges.
func DefaultQueryTieredPolicy() TieredPolicy {
	p := DefaultTieredPolicy()
	p.SegmentsPerTier = 2
	return p
}

func (p TieredPolicy) normalized() TieredPolicy {
	d := DefaultTieredPolicy()
	if p.SegmentsPerTier < 2 {
		p.SegmentsPerTier = d.SegmentsPerTier
	}
	if p.MaxMergeAtOnce < 2 {
		p.MaxMergeAtOnce = d.MaxMergeAtOnce
	}
	if p.MaxMergedBytes <= 0 {
		p.MaxMergedBytes = d.MaxMergedBytes
	}
	if p.FloorSegmentBytes <= 0 {
		p.FloorSegmentBytes = d.FloorSegmentBytes
	}
	if p.DeletesPctAllowed <= 0 || p.DeletesPctAllowed > 100 {
		p.DeletesPctAllowed = d.DeletesPctAllowed
	}
	return p
}

// MergeCandidate is one segment as the policy sees it.
type MergeCandidate struct {
	ID    string
	Bytes int64
	// Docs is the segment's ordinals, Deleted how many of them are deleted.
	Docs, Deleted uint32
	// Merging is set for a segment an in-flight merge already holds: it is never
	// chosen, nor counted.
	Merging bool
}

// size is the candidate's bytes less its deleted share.
func (c *MergeCandidate) size() int64 {
	if c.Docs == 0 {
		return 0
	}
	return int64(float64(c.Bytes) * (1 - float64(c.Deleted)/float64(c.Docs)))
}

func (c *MergeCandidate) deletedPct() float64 {
	if c.Docs == 0 {
		return 0
	}
	return 100 * float64(c.Deleted) / float64(c.Docs)
}

// FindMerges returns the merges to run now, as lists of segment IDs, none sharing a
// segment. It never returns a merge of a Merging segment.
func (p TieredPolicy) FindMerges(segs []MergeCandidate) [][]string {
	p = p.normalized()
	var eligible []*MergeCandidate
	var total int64
	minSize := int64(math.MaxInt64)
	for i := range segs {
		c := &segs[i]
		if c.Merging {
			continue
		}
		size := c.size()
		if size > p.MaxMergedBytes/2 && c.deletedPct() <= p.DeletesPctAllowed {
			continue // too big to merge further, and not deleted enough to rewrite
		}
		eligible = append(eligible, c)
		total += size
		minSize = min(minSize, size)
	}
	if len(eligible) == 0 {
		return nil
	}
	slices.SortFunc(eligible, func(a, b *MergeCandidate) int {
		if sa, sb := a.size(), b.size(); sa != sb {
			if sa > sb {
				return -1
			}
			return 1
		}
		return strings.Compare(a.ID, b.ID)
	})

	allowed := p.allowedSegments(total, minSize)
	var merges [][]string
	for len(eligible) > allowed {
		best := p.bestMerge(eligible)
		if best == nil {
			break
		}
		ids := make([]string, len(best))
		chosen := make(map[*MergeCandidate]bool, len(best))
		for i, c := range best {
			ids[i] = c.ID
			chosen[c] = true
		}
		merges = append(merges, ids)
		eligible = slices.DeleteFunc(eligible, func(c *MergeCandidate) bool { return chosen[c] })
	}
	for _, c := range eligible {
		if c.Deleted > 0 && c.deletedPct() > p.DeletesPctAllowed {
			merges = append(merges, []string{c.ID})
		}
	}
	return merges
}

// allowedSegments is how many segments total bytes may take before merging: the
// tiers' capacity, starting at the floored smallest size, and never under
// SegmentsPerTier.
func (p TieredPolicy) allowedSegments(total, minSize int64) int {
	level := max(minSize, p.FloorSegmentBytes)
	left := float64(total)
	allowed := 0.0
	for {
		count := left / float64(level)
		if count < float64(p.SegmentsPerTier) || level >= p.MaxMergedBytes {
			allowed += math.Ceil(count)
			break
		}
		allowed += float64(p.SegmentsPerTier)
		left -= float64(p.SegmentsPerTier) * float64(level)
		level = min(level*int64(p.MaxMergeAtOnce), p.MaxMergedBytes)
	}
	return max(int(allowed), p.SegmentsPerTier)
}

// bestMerge scores, for each starting segment of eligible (sorted largest first), the
// run of up to MaxMergeAtOnce segments from there that fits MaxMergedBytes, and returns
// the lowest-scoring one with at least two segments.
func (p TieredPolicy) bestMerge(eligible []*MergeCandidate) []*MergeCandidate {
	var best []*MergeCandidate
	bestScore := math.Inf(1)
	for start := range eligible {
		var cand []*MergeCandidate
		var bytes int64
		hitTooLarge := false
		for i := start; i < len(eligible) && len(cand) < p.MaxMergeAtOnce; i++ {
			size := eligible[i].size()
			if bytes+size > p.MaxMergedBytes {
				hitTooLarge = true
				continue
			}
			cand = append(cand, eligible[i])
			bytes += size
		}
		if len(cand) < 2 {
			continue
		}
		if score := p.score(cand, hitTooLarge); score < bestScore {
			best, bestScore = cand, score
		}
	}
	return best
}

// score is a merge's cost: lower is better.
func (p TieredPolicy) score(cand []*MergeCandidate, hitTooLarge bool) float64 {
	var before, after, floored, maxFloored float64
	for _, c := range cand {
		size := float64(c.size())
		before += float64(c.Bytes)
		after += size
		f := max(size, float64(p.FloorSegmentBytes))
		floored += f
		maxFloored = max(maxFloored, f)
	}
	skew := maxFloored / floored
	if hitTooLarge {
		// A merge that would reach the cap is as good as a perfectly balanced one.
		skew = 1 / float64(p.MaxMergeAtOnce)
	}
	score := skew * math.Pow(max(after, 1), 0.05)
	if before > 0 {
		ratio := after / before
		score *= ratio * ratio
	}
	return score
}
