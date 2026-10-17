package cluster

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
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
// would ignore, answering with the first page. A request says the level it needs
// (wireRequest.Needs), and goes to that level's endpoint (searchPath), and a reply
// the level it was answered at (searchReply.Level): a peer refuses a request past its
// level, and the coordinator a reply below the request's, so a search is never
// answered with a part of it unread.
// A wire addition a peer must not ignore raises it.
const searchLevel = levelFrom

// The search wire levels of the additions a peer must not ignore.
const (
	levelFilterAggs  = 1
	levelTermsFilter = 2
	levelFrom        = 3
)

// levelNeeds names what a search of wire level needs carries, for the errors of a
// peer below it.
func levelNeeds(needs int) string {
	switch {
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
	Query       json.RawMessage    `json:"query"`
	PostFilter  json.RawMessage    `json:"post_filter,omitempty"`
	Sort        []search.SortField `json:"sort,omitempty"`
	Size        int                `json:"size"`
	From        int                `json:"from,omitempty"`
	SearchAfter []any              `json:"search_after,omitempty"`
	TrackTotal  int                `json:"track_total"`
	Aggs        map[string]wireAgg `json:"aggs,omitempty"`
	Fields      []string           `json:"fields,omitempty"`
	TimeoutNs   int64              `json:"timeout_ns,omitempty"`
	Index       string             `json:"index"`
	NoBodies    bool               `json:"no_bodies,omitempty"`
	// Needs is the search wire level a peer must speak to answer the request exactly
	// (searchLevel): 1 for a post_filter or a filter, filters or global aggregation, 2
	// for a terms aggregation's include or exclude, 3 for a from past 0.
	Needs int `json:"needs,omitempty"`
}

// wireAgg is a search.Agg on the wire: a filter or filters aggregation's queries as
// their DSL JSON, and the aggregations under it as wireAggs.
type wireAgg struct {
	search.Agg
	Filter  json.RawMessage            `json:"Filter,omitempty"`
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
		Fields: r.Fields, TimeoutNs: int64(r.Timeout), Index: r.Index, NoBodies: r.NoBodies,
	}
	if r.PostFilter != nil {
		if w.PostFilter, err = percolate.EncodeQuery(r.PostFilter); err != nil {
			return nil, fmt.Errorf("cluster: encoding the post_filter: %w", err)
		}
	}
	if w.Aggs, err = encodeAggs(r.Aggs); err != nil {
		return nil, err
	}
	switch {
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
		Fields: w.Fields, Timeout: time.Duration(w.TimeoutNs), Index: w.Index, NoBodies: w.NoBodies,
	}
	if w.PostFilter != nil {
		if r.PostFilter, problems = query.Parse(w.PostFilter); len(problems) > 0 {
			return nil, api.Invalid("the post_filter is invalid", problems...)
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
// refused, so the shard's next copy answers, or none does (503).
const percolateLevel = 1

// percolatePath is the peer path of a percolation needing wire level needs.
func percolatePath(needs int) string {
	if needs == 0 {
		return peerPrefix + "percolate"
	}
	return peerPrefix + "percolate/" + strconv.Itoa(needs)
}

// percolateNeeds is the percolate wire level a percolation under mapping (the
// catalogue's JSON) needs: 1 when it holds a date field, whose saved conditions may
// hold date math; 0 otherwise. A mapping it cannot read needs 1.
func percolateNeeds(mapping json.RawMessage) int {
	var m schema.Mapping
	if len(mapping) > 0 && json.Unmarshal(mapping, &m) != nil {
		return 1
	}
	for _, t := range m.Fields {
		if t == schema.Date {
			return 1
		}
	}
	return 0
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
	Stale   bool              `json:"stale,omitempty"`
	Level   int               `json:"level,omitempty"`
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
