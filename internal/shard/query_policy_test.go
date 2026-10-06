package shard

import (
	"fmt"
	"testing"
)

// Saved queries arriving in many small refreshes (a 100k-query load at about 300 per
// refresh, each query about 120 bytes) are never in more than three query segments
// under DefaultQueryTieredPolicy, where DefaultTieredPolicy lets ten accumulate.
func TestQueryPolicyKeepsFewSegments(t *testing.T) {
	peak := func(p TieredPolicy) int {
		var segs []MergeCandidate
		most, next := 0, 0
		for range 330 {
			segs = append(segs, MergeCandidate{ID: fmt.Sprintf("s%04d", next), Bytes: 300 * 120, Docs: 300})
			next++
			for {
				merges := p.FindMerges(segs)
				if len(merges) == 0 {
					break
				}
				for _, ids := range merges {
					merged := MergeCandidate{ID: fmt.Sprintf("s%04d", next)}
					next++
					keep := segs[:0]
					in := map[string]bool{}
					for _, id := range ids {
						in[id] = true
					}
					for _, c := range segs {
						if in[c.ID] {
							merged.Bytes += c.Bytes
							merged.Docs += c.Docs
						} else {
							keep = append(keep, c)
						}
					}
					keep = append(keep, merged)
					segs = keep
				}
			}
			most = max(most, len(segs))
		}
		return most
	}
	queries, docs := peak(DefaultQueryTieredPolicy()), peak(DefaultTieredPolicy())
	if queries > 3 || queries >= docs {
		t.Fatalf("query policy peaks at %d segments, the document policy at %d", queries, docs)
	}
	t.Logf("query policy: %d segments; document policy: %d", queries, docs)
}
