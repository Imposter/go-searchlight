package node

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/percolate"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// MatchDetails asks a percolation for more of each match than its saved query's id.
type MatchDetails struct {
	// Versions reports each matched saved query's client version.
	Versions bool
	// InnerHits reports, for each matched saved query whose nested nodes declare
	// inner_hits, the document's members those nodes' queries match, by key: as a
	// search's inner hits, without the objects.
	InnerHits bool
}

// Any reports whether d asks for anything.
func (d MatchDetails) Any() bool { return d.Versions || d.InnerHits }

// matchDetails returns each document's matches with the details d asks for, in the order
// of its ids (as Percolate returned them from g): the saved queries' versions, and the
// inner hits they ask of the document, read from the query segments that hold them. A
// saved query's inner hits are compiled once for every document, its date math resolved
// at the percolation's now (ctx's, [query.WithNow]), as the percolator resolves it.
func matchDetails(ctx context.Context, g *shard.Generation, d MatchDetails, docs []schema.Doc, ids []percolate.IDs) ([][]api.PercolateMatch, error) {
	now, ok := query.NowFrom(ctx)
	if !ok {
		return nil, errNoNow
	}
	out := make([][]api.PercolateMatch, len(docs))
	compiled := map[string]*search.QueryInnerHits{}
	offsets := 0
	for i := range docs {
		names, err := ids[i].Strings()
		if err != nil {
			return nil, fmt.Errorf("node: document %d's matches: %w", i, err)
		}
		if len(names) == 0 {
			continue
		}
		doc := &docs[i]
		if d.InnerHits && doc.Fields == nil {
			analyzed, _, err := schema.AnalyzeForMatch(g.Mapping(), doc.ID, doc.Body)
			if err != nil {
				return nil, err
			}
			doc = &analyzed
		}
		ms := make([]api.PercolateMatch, len(names))
		for k, id := range names {
			ms[k].ID = id
			seg, ord, ok := g.LookupQuery(id)
			if !ok {
				continue
			}
			qs := g.QuerySegments[seg].Segment
			if d.Versions {
				ms[k].Version = qs.Version(ord)
			}
			if !d.InnerHits {
				continue
			}
			qh, seen := compiled[id]
			if !seen {
				sq, err := qs.Query(ord)
				if err != nil {
					return nil, err
				}
				resolved, _ := query.Resolve(sq.Query, now)
				qh = search.NewQueryInnerHits(resolved)
				compiled[id] = qh
			}
			if qh == nil {
				continue
			}
			hits := qh.Offsets(doc)
			ms[k].InnerHits = make(map[string]*api.PercolateInnerHits, len(hits))
			for key, h := range hits {
				offsets += len(h.Offsets)
				ms[k].InnerHits[key] = &api.PercolateInnerHits{Total: h.Total, Offsets: h.Offsets}
			}
			if offsets > api.MaxPercolateInnerHits {
				return nil, api.ErrTooManyInnerHits
			}
		}
		out[i] = ms
	}
	return out, nil
}

// mergeMatches merges one document's matches from every shard (each sorted by id, as
// its ids are; a saved query lives on one shard, so they are disjoint) into one list
// sorted by id: empty, never nil, for a document that matches none.
func mergeMatches(lists [][]api.PercolateMatch) []api.PercolateMatch {
	n := 0
	for _, l := range lists {
		n += len(l)
	}
	out := make([]api.PercolateMatch, 0, n)
	for _, l := range lists {
		out = append(out, l...)
	}
	slices.SortFunc(out, func(a, b api.PercolateMatch) int { return strings.Compare(a.ID, b.ID) })
	return out
}

// innerHitOffsets counts the inner hit offsets of matches, every document's together.
func innerHitOffsets(matches [][]api.PercolateMatch) int {
	n := 0
	for _, ms := range matches {
		for _, m := range ms {
			for _, h := range m.InnerHits {
				n += len(h.Offsets)
			}
		}
	}
	return n
}

// errDetailsCount is a target that answered details for another number of documents.
var errDetailsCount = errors.New("node: a percolation's details are not one per document")

// errNoNow is a percolation's details asked for with no now in its context: every
// percolation sets one (percolateDocs, the peer API), which date math resolves at.
var errNoNow = errors.New("node: a percolation's details need its now (query.WithNow)")
