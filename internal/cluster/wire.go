package cluster

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/percolate"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
)

// The internal peer API's messages, as JSON. They are versioned with the binary: a
// rolling upgrade runs nodes one release apart, so fields are only ever added.

// peerPrefix is the path prefix of the internal peer API.
const peerPrefix = "/_internal/"

// Headers of the peer API.
const (
	// headerDeadline carries the caller's remaining deadline, less a reserve
	// (peerReserve), in milliseconds: the peer works under it, so a search past it
	// answers timed_out as a local one does, in time for the caller to use it.
	headerDeadline = "X-Searchlight-Deadline-Ms"
	// headerServiceTime and headerQueue carry the peer's service time (seconds) and
	// its peer requests in flight, for adaptive replica selection.
	headerServiceTime = "X-Searchlight-Service-Time"
	headerQueue       = "X-Searchlight-Queue"
)

// shardRef names one shard copy's shard and the seq a read needs searchable on it.
// Without AllowStale a peer whose copy is stale (it trails by more than max_lag, its
// node cannot reach the database) refuses the read with a retryable 503 (codeStale).
type shardRef struct {
	Index      string `json:"index"`
	Shard      int    `json:"shard"`
	WaitSeq    int64  `json:"wait_seq,omitempty"`
	AllowStale bool   `json:"allow_stale,omitempty"`
	// ReadsMajor, in a snapshot request, is the newest segment format major the
	// requester reads; absent, legacyReadsMajor. MinMajor, when set, asks the peer to
	// refuse if its segments are older than it.
	ReadsMajor int `json:"reads_major,omitempty"`
	MinMajor   int `json:"min_major,omitempty"`
}

// legacyReadsMajor is the segment format major a snapshot request without reads_major
// reads: the requester predates the field, and read only that major.
const legacyReadsMajor = 3

// Problem codes of a snapshot a peer refused for its segments' format major.
const (
	codeNewerSegments = "segments_newer_format"
	codeOlderSegments = "segments_older_format"
)

// codeStale is the problem code of a read a peer refused because its copy is stale.
const codeStale = "stale_copy"

// searchLevel is the search wire level this binary speaks. 1.0.0 spoke 0, and its
// decoder drops what it does not know: level 1 adds post_filter and the filter, filters
// and global aggregations, which a level-0 peer would ignore or refuse, and level 2 a
// terms aggregation's include and exclude (levelTermsFilter), which a lower peer
// would ignore, counting every value, and level 3 from (levelFrom), which a lower peer
// would ignore, answering with the first page, and level 4 the min and max
// aggregations and a cardinality's threshold (levelMetrics), which a lower peer would
// refuse as unknown or ignore, estimating past it, and level 5 the box query on point
// fields (levelBox), which a lower peer would refuse as an unknown op. Level 6 reads
// nested fields' objects (levelNested): a nested node, an aggregation or sort on a
// sub-field, which a lower peer would refuse, or answer reading the sub-field as a
// field of no document. Level 7 collects sub-aggregations under a terms aggregation
// on a sub-field (levelNestedSubAggs), which a lower peer would refuse, and level 8 the
// hits' seqs (levelSeqNo), which a lower peer would leave out. A request says
// the level it needs (wireRequest.Needs), and goes to that level's endpoint
// (searchPath), and a reply the level it was answered at
// (searchReply.Level): a peer refuses a request past its level, and the coordinator a
// reply below the request's, so a search is never answered with a part of it unread.
// A wire addition a peer must not ignore raises it.
const searchLevel = levelSeqNo

// The search wire levels of the additions a peer must not ignore.
const (
	levelFilterAggs    = 1
	levelTermsFilter   = 2
	levelFrom          = 3
	levelMetrics       = 4
	levelBox           = 5
	levelNested        = 6
	levelNestedSubAggs = 7
	levelSeqNo         = 8
)

// levelNeeds names what a search of wire level needs carries, for the errors of a
// peer below it.
func levelNeeds(needs int) string {
	switch {
	case needs >= levelSeqNo:
		return "seq_no, the hits' seqs"
	case needs >= levelNestedSubAggs:
		return "sub-aggregations under a terms aggregation on a nested field's sub-field"
	case needs >= levelNested:
		return "a nested field's objects: a nested query, or an aggregation or sort on a sub-field"
	case needs >= levelBox:
		return "a box query on a point field"
	case needs >= levelMetrics:
		return "a min or max aggregation, or a cardinality threshold"
	case needs >= levelFrom:
		return "from, a page past the first"
	case needs >= levelTermsFilter:
		return "a terms aggregation's include or exclude"
	}
	return "post_filter, or a filter, filters or global aggregation"
}

// noPeerEndpoint is the detail of a peer's 404 for a path it does not serve, as every
// release since 1.0.0 words it.
const noPeerEndpoint = "no peer endpoint here"

// searchPath is the peer path of a search needing wire level needs: the search
// endpoint for level 0, and from level 1 its level's own, which a peer of a lower level
// does not serve (404, noPeerEndpoint). A 1.0.0 node would otherwise refuse a filter
// aggregation with its own 400, and run a post_filter search without the post_filter.
func searchPath(needs int) string {
	if needs == 0 {
		return peerPrefix + "search"
	}
	return peerPrefix + "search/" + strconv.Itoa(needs)
}

// wireRequest is a search.Request on the wire: the queries as their DSL JSON.
type wireRequest struct {
	Query      json.RawMessage    `json:"query"`
	PostFilter json.RawMessage    `json:"post_filter,omitempty"`
	Sort       []search.SortField `json:"sort,omitempty"`
	// SortWhere are the sort keys' wheres as their DSL JSON, by key (null for none).
	SortWhere   []json.RawMessage  `json:"sort_where,omitempty"`
	Size        int                `json:"size"`
	From        int                `json:"from,omitempty"`
	SearchAfter []any              `json:"search_after,omitempty"`
	TrackTotal  int                `json:"track_total"`
	Aggs        map[string]wireAgg `json:"aggs,omitempty"`
	Fields      []string           `json:"fields,omitempty"`
	TimeoutNs   int64              `json:"timeout_ns,omitempty"`
	Index       string             `json:"index"`
	NoBodies    bool               `json:"no_bodies,omitempty"`
	SeqNo       bool               `json:"seq_no,omitempty"`
	// NoRequestCache is search.Request's. It needs no wire level: a peer that ignores
	// it caches nothing anyway (request caches came after level 4).
	NoRequestCache bool `json:"no_request_cache,omitempty"`
	// Needs is the search wire level a peer must speak to answer the request exactly
	// (searchLevel): 1 for a post_filter or a filter, filters or global aggregation, 2
	// for a terms aggregation's include or exclude, 3 for a from past 0, 4 for a min
	// or max aggregation or a cardinality's threshold, 5 for a box query, 6 for a
	// nested field's objects (search.Request.Nested, or a nested node), 7 for
	// sub-aggregations under a terms aggregation on a sub-field
	// (search.Request.NestedSubAggs), 8 for seq_no on a search with hits.
	Needs int `json:"needs,omitempty"`
}

// wireAgg is a search.Agg on the wire: a filter or filters aggregation's queries as
// their DSL JSON, and the aggregations under it as wireAggs.
type wireAgg struct {
	search.Agg
	Filter  json.RawMessage            `json:"Filter,omitempty"`
	Where   json.RawMessage            `json:"Where,omitempty"`
	Filters map[string]json.RawMessage `json:"Filters,omitempty"`
	Aggs    map[string]wireAgg         `json:"Aggs,omitempty"`
}

func encodeRequest(r *search.Request) (*wireRequest, error) {
	q, err := percolate.EncodeQuery(r.Query)
	if err != nil {
		return nil, fmt.Errorf("cluster: encoding the query: %w", err)
	}
	w := &wireRequest{
		Query: q, Sort: r.Sort, Size: r.Size, From: r.From, SearchAfter: r.SearchAfter, TrackTotal: r.TrackTotal,
		Fields: r.Fields, TimeoutNs: int64(r.Timeout), Index: r.Index, NoBodies: r.NoBodies, SeqNo: r.SeqNo,
		NoRequestCache: r.NoRequestCache,
	}
	if r.PostFilter != nil {
		if w.PostFilter, err = percolate.EncodeQuery(r.PostFilter); err != nil {
			return nil, fmt.Errorf("cluster: encoding the post_filter: %w", err)
		}
	}
	if w.Aggs, err = encodeAggs(r.Aggs); err != nil {
		return nil, err
	}
	for i, f := range r.Sort {
		if f.Where == nil {
			continue
		}
		if w.SortWhere == nil {
			w.SortWhere = make([]json.RawMessage, len(r.Sort))
		}
		if w.SortWhere[i], err = percolate.EncodeQuery(f.Where); err != nil {
			return nil, fmt.Errorf("cluster: encoding sort key %q's where: %w", f.Field, err)
		}
	}
	switch {
	case r.SeqNo && r.Size > 0:
		w.Needs = levelSeqNo
	case r.NestedSubAggs:
		w.Needs = levelNestedSubAggs
	case r.Nested || nestedQueries(r):
		w.Needs = levelNested
	case boxQueries(r):
		w.Needs = levelBox
	case metricAggs(r.Aggs):
		w.Needs = levelMetrics
	case r.From > 0 && r.Size > 0:
		w.Needs = levelFrom
	case termsFilters(r.Aggs):
		w.Needs = levelTermsFilter
	case r.PostFilter != nil || filterAggs(r.Aggs):
		w.Needs = levelFilterAggs
	}
	return w, nil
}

func encodeAggs(aggs map[string]search.Agg) (map[string]wireAgg, error) {
	if aggs == nil {
		return nil, nil
	}
	out := make(map[string]wireAgg, len(aggs))
	for name := range aggs {
		a := aggs[name]
		w := wireAgg{Agg: a}
		var err error
		if a.Filter != nil {
			if w.Filter, err = percolate.EncodeQuery(a.Filter); err != nil {
				return nil, fmt.Errorf("cluster: encoding aggregation %q's filter: %w", name, err)
			}
		}
		if a.Where != nil {
			if w.Where, err = percolate.EncodeQuery(a.Where); err != nil {
				return nil, fmt.Errorf("cluster: encoding aggregation %q's where: %w", name, err)
			}
		}
		if a.Filters != nil {
			w.Filters = make(map[string]json.RawMessage, len(a.Filters))
			for key, f := range a.Filters {
				if w.Filters[key], err = percolate.EncodeQuery(f); err != nil {
					return nil, fmt.Errorf("cluster: encoding aggregation %q's filter %q: %w", name, key, err)
				}
			}
		}
		if w.Aggs, err = encodeAggs(a.Aggs); err != nil {
			return nil, err
		}
		out[name] = w
	}
	return out, nil
}

func (w *wireRequest) decode() (*search.Request, error) {
	q, problems := query.Parse(w.Query)
	if len(problems) > 0 {
		return nil, api.Invalid("the query is invalid", problems...)
	}
	r := &search.Request{
		Query: q, Sort: w.Sort, Size: w.Size, From: w.From, SearchAfter: w.SearchAfter, TrackTotal: w.TrackTotal,
		Fields: w.Fields, Timeout: time.Duration(w.TimeoutNs), Index: w.Index, NoBodies: w.NoBodies, SeqNo: w.SeqNo,
		NoRequestCache: w.NoRequestCache,
	}
	if w.PostFilter != nil {
		if r.PostFilter, problems = query.Parse(w.PostFilter); len(problems) > 0 {
			return nil, api.Invalid("the post_filter is invalid", problems...)
		}
	}
	if len(w.SortWhere) > 0 && len(w.SortWhere) != len(r.Sort) {
		return nil, api.Invalid("the sort holds " + strconv.Itoa(len(r.Sort)) + " keys and " + strconv.Itoa(len(w.SortWhere)) + " wheres")
	}
	for i, raw := range w.SortWhere {
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		if r.Sort[i].Where, problems = query.Parse(raw); len(problems) > 0 {
			return nil, api.Invalid("sort key "+r.Sort[i].Field+"'s where is invalid", problems...)
		}
	}
	aggs, err := decodeAggs(w.Aggs)
	if err != nil {
		return nil, err
	}
	r.Aggs = aggs
	return r, nil
}

func decodeAggs(aggs map[string]wireAgg) (map[string]search.Agg, error) {
	if aggs == nil {
		return nil, nil
	}
	out := make(map[string]search.Agg, len(aggs))
	for name := range aggs {
		w := aggs[name]
		a := w.Agg
		var problems []query.Problem
		if w.Filter != nil {
			if a.Filter, problems = query.Parse(w.Filter); len(problems) > 0 {
				return nil, api.Invalid("aggregation "+name+"'s filter is invalid", problems...)
			}
		}
		if w.Where != nil {
			if a.Where, problems = query.Parse(w.Where); len(problems) > 0 {
				return nil, api.Invalid("aggregation "+name+"'s where is invalid", problems...)
			}
		}
		if w.Filters != nil {
			a.Filters = make(map[string]query.Node, len(w.Filters))
			for key, raw := range w.Filters {
				if a.Filters[key], problems = query.Parse(raw); len(problems) > 0 {
					return nil, api.Invalid("aggregation "+name+"'s filter "+key+" is invalid", problems...)
				}
			}
		}
		subs, err := decodeAggs(w.Aggs)
		if err != nil {
			return nil, err
		}
		a.Aggs = subs
		out[name] = a
	}
	return out, nil
}

// searchMsg asks a peer to search its copy; Pin keeps the generation for a fetch.
type searchMsg struct {
	shardRef
	Request *wireRequest `json:"request"`
	Pin     bool         `json:"pin,omitempty"`
}

type searchReply struct {
	Result *search.ShardResult `json:"result"`
	Pin    string              `json:"pin,omitempty"`
	Stale  bool                `json:"stale,omitempty"`
	// Level is the search wire level the peer speaks (searchLevel); a 1.0.0 peer, which
	// speaks 0, sends none.
	Level int `json:"level,omitempty"`
}

// fetchMsg asks a peer to fill hits' bodies from a pinned generation.
type fetchMsg struct {
	Pin    string       `json:"pin"`
	Hits   []search.Hit `json:"hits"`
	Fields []string     `json:"fields,omitempty"`
}

type fetchReply struct {
	Bodies []json.RawMessage `json:"bodies"`
}

// percolateLevel is the percolate wire level this binary speaks, as searchLevel is
// the search's. 1.0.0 spoke 0: it reads no date math in saved queries and ignores
// percolateMsg.Now. Level 1 adds both. A percolation of an index whose mapping holds a
// date field (the only fields date math applies to) needs it, and goes to its level's
// endpoint (percolatePath), which a 1.0.0 peer does not serve; a reply below it is
// refused, so the shard's next copy answers, or none does (503). Level 2 adds point
// fields and their box queries: a percolation under a mapping holding a point field
// needs it, as a lower peer reads neither the mapping nor the saved boxes. Level 3
// adds nested fields: a percolation under a mapping holding one needs it, for the
// same reasons. Level 4 reports details of each match (percolateMsg.Details): the
// saved queries' versions and inner hits, which a lower peer would leave out.
const percolateLevel = percolateDetails

// Percolate wire levels of the additions a peer must not ignore.
const (
	percolateDates   = 1
	percolatePoints  = 2
	percolateNested  = 3
	percolateDetails = 4
)

// percolatePath is the peer path of a percolation needing wire level needs.
func percolatePath(needs int) string {
	if needs == 0 {
		return peerPrefix + "percolate"
	}
	return peerPrefix + "percolate/" + strconv.Itoa(needs)
}

// percolateNeeds is the percolate wire level a percolation under mapping (the
// catalogue's JSON) needs: 3 when it holds a nested field, 2 when it holds a point
// field (or a point axis is a date), 1 when it holds a date field, whose saved
// conditions may hold date math; 0 otherwise. A mapping it cannot read needs
// percolateLevel.
func percolateNeeds(mapping json.RawMessage) int {
	var m schema.Mapping
	if len(mapping) > 0 && json.Unmarshal(mapping, &m) != nil {
		return percolateLevel
	}
	needs := 0
	for _, t := range m.Fields {
		switch {
		case t == schema.Nested:
			return percolateNested
		case t.IsPoint():
			needs = max(needs, percolatePoints)
		case t == schema.Date:
			needs = max(needs, percolateDates)
		}
	}
	return needs
}

// percolateLevelNeeds names what a percolation of wire level needs carries, for the
// errors of a peer below it.
func percolateLevelNeeds(needs int) string {
	if needs >= percolateDetails {
		return "the details of each match: the saved queries' versions or inner hits"
	}
	if needs >= percolateNested {
		return "an index with a nested field, whose saved queries may hold nested conditions"
	}
	if needs >= percolatePoints {
		return "an index with a point field, whose saved queries may hold box conditions"
	}
	return "an index with a date field, whose saved queries may hold date math"
}

// percolateMsg asks a peer to percolate documents, analyzed under Mapping (the
// catalogue's JSON), against its copy's saved queries.
type percolateMsg struct {
	shardRef
	Mapping json.RawMessage `json:"mapping,omitempty"`
	Docs    []wireDoc       `json:"docs"`
	// Now is the coordinator's instant for date math, Unix ms (query.WithNow): the
	// peer resolves at it, never at its own clock. Nil from a coordinator of an older
	// release, which sends none: the peer then resolves at its node clock's wall time.
	Now *int64 `json:"now,omitempty"`
	// Details, when set, asks for each match's details (node.MatchDetails), which
	// needs percolate level 4.
	Details *node.MatchDetails `json:"details,omitempty"`
	// Needs is the percolate wire level a peer must speak (percolateLevel).
	Needs int `json:"needs,omitempty"`
}

type wireDoc struct {
	ID   string          `json:"id"`
	Body json.RawMessage `json:"body"`
}

// percolateReply is a peer's percolation; Level is the percolate wire level it speaks
// (a 1.0.0 peer sends none).
type percolateReply struct {
	Matches []json.RawMessage `json:"matches"`
	// Details are each document's matches with the details asked for, in Matches'
	// order: set when percolateMsg.Details was.
	Details [][]api.PercolateMatch `json:"details,omitempty"`
	Stale   bool                   `json:"stale,omitempty"`
	Level   int                    `json:"level,omitempty"`
}

// getMsg reads a document or a saved query from a peer's copy.
type getMsg struct {
	shardRef
	ID    string `json:"id"`
	Query bool   `json:"query,omitempty"`
}

type getReply struct {
	Found bool            `json:"found"`
	Body  json.RawMessage `json:"body,omitempty"`
	// Seq is the stored document's seq as the copy holds it; a peer of a release
	// before it sends none.
	Seq   int64           `json:"seq,omitempty"`
	Saved *api.SavedQuery `json:"saved,omitempty"`
	Stale bool            `json:"stale,omitempty"`
}

// waitMsg waits until a peer's copy has Seq searchable (a write's refresh).
type waitMsg struct {
	shardRef
	Seq     int64 `json:"seq"`
	Refresh bool  `json:"refresh,omitempty"`
}

type countsReply struct {
	Docs    uint64 `json:"docs"`
	Queries uint64 `json:"queries"`
}

// hintMsg is a batch of push hints: each shard's changelog reached Seq.
type hintMsg struct {
	Hints []hint `json:"hints"`
}

type hint struct {
	Index string `json:"index"`
	Shard int    `json:"shard"`
	Seq   int64  `json:"seq"`
}

// snapshotReply describes a snapshot a peer took for a recovery.
type snapshotReply struct {
	ID             string     `json:"id"`
	Seq            int64      `json:"seq"`
	IndexUID       string     `json:"index_uid"`
	MappingVersion int64      `json:"mapping_version"`
	Files          []wireFile `json:"files"`
	// FormatMajor is the oldest segment format major among the snapshot's segments;
	// 0 from a peer too old to say (whose segments are the previous major).
	FormatMajor int `json:"format_major,omitempty"`
}

type wireFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// copiesReply lists a peer's copies.
type copiesReply struct {
	Copies []peerCopy `json:"copies"`
}

// peerCopy is one copy a peer holds: its description, its registry epoch, and a
// progress counter that moves while the copy applies, loads or fetches anything (the
// prune leader's stall detection).
type peerCopy struct {
	api.ShardInfo
	Epoch    int64 `json:"epoch"`
	Progress int64 `json:"progress"`
	Paused   bool  `json:"paused,omitempty"`
}

// errorReply is an error on the wire.
type errorReply struct {
	Status       int             `json:"status"`
	Code         string          `json:"code"`
	Detail       string          `json:"detail,omitempty"`
	Problems     []query.Problem `json:"problems,omitempty"`
	RetryAfterMs int64           `json:"retry_after_ms,omitempty"`
}

// filterAggs reports whether aggs hold a filter, filters or global aggregation, at
// any level.
func filterAggs(aggs map[string]search.Agg) bool {
	for name := range aggs {
		switch a := aggs[name]; a.Type {
		case search.AggFilter, search.AggFilters, search.AggGlobal:
			return true
		default:
			if filterAggs(a.Aggs) {
				return true
			}
		}
	}
	return false
}

// boxQueries reports whether r's query, post_filter or any filter or filters
// aggregation's queries hold a box condition.
func boxQueries(r *search.Request) bool {
	return queriesHold(r, func(node query.Node) bool {
		l, ok := node.(*query.Leaf)
		return ok && l.Op == query.OpBox
	})
}

// nestedQueries reports whether r's query, post_filter or any filter or filters
// aggregation's queries hold a nested node, or a where of an aggregation or a sort is
// given.
func nestedQueries(r *search.Request) bool {
	if slices.ContainsFunc(r.Sort, func(f search.SortField) bool { return f.Where != nil || f.Mode != "" }) || aggWheres(r.Aggs) {
		return true
	}
	return queriesHold(r, func(node query.Node) bool {
		_, ok := node.(*query.Nested)
		return ok
	})
}

// queriesHold reports whether holds holds of a node of r's query, post_filter or any
// filter or filters aggregation's queries.
func queriesHold(r *search.Request, holds func(query.Node) bool) bool {
	found := false
	check := func(n query.Node) {
		query.Walk(n, func(_ string, node query.Node) bool {
			found = found || holds(node)
			return !found
		})
	}
	check(r.Query)
	if r.PostFilter != nil {
		check(r.PostFilter)
	}
	var aggs func(map[string]search.Agg)
	aggs = func(m map[string]search.Agg) {
		for name := range m {
			a := m[name]
			if a.Filter != nil {
				check(a.Filter)
			}
			for _, f := range a.Filters {
				check(f)
			}
			aggs(a.Aggs)
		}
	}
	aggs(r.Aggs)
	return found
}

// metricAggs reports whether aggs hold a min or max aggregation, or a cardinality
// with a threshold, at any level.
func metricAggs(aggs map[string]search.Agg) bool {
	for name := range aggs {
		a := aggs[name]
		if a.Type == search.AggMin || a.Type == search.AggMax || a.Threshold > 0 || metricAggs(a.Aggs) {
			return true
		}
	}
	return false
}

// termsFilters reports whether aggs hold a terms aggregation with an include or an
// exclude, at any level.
func termsFilters(aggs map[string]search.Agg) bool {
	for name := range aggs {
		a := aggs[name]
		if a.Include != nil || a.Exclude != nil || termsFilters(a.Aggs) {
			return true
		}
	}
	return false
}

// dropMetrics removes every min and max aggregation from aggs, and every cardinality's
// threshold, in place: what a peer of a level below levelMetrics leaves of a search
// holding them, had it not refused it (testHooks.olderPeer).
func dropMetrics(aggs map[string]search.Agg) {
	for name := range aggs {
		a := aggs[name]
		if a.Type == search.AggMin || a.Type == search.AggMax {
			delete(aggs, name)
			continue
		}
		a.Threshold = 0
		dropMetrics(a.Aggs)
		aggs[name] = a
	}
}

// dropTermsFilters removes every include and exclude from aggs, in place: what a peer
// of a level below levelTermsFilter reads of them (testHooks.olderPeer).
func dropTermsFilters(aggs map[string]search.Agg) {
	for name := range aggs {
		a := aggs[name]
		a.Include, a.Exclude = nil, nil
		dropTermsFilters(a.Aggs)
		aggs[name] = a
	}
}

// aggWheres reports whether aggs hold an aggregation with a where, at any level.
func aggWheres(aggs map[string]search.Agg) bool {
	for name := range aggs {
		a := aggs[name]
		if a.Where != nil || aggWheres(a.Aggs) {
			return true
		}
	}
	return false
}
