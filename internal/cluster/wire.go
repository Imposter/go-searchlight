package cluster

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/percolate"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
)

// The internal peer API's messages, as JSON. They are versioned with the binary: a
// rolling upgrade runs nodes one release apart, so fields are only ever added.

// peerPrefix is the path prefix of the internal peer API.
const peerPrefix = "/_internal/"

// Headers of the peer API.
const (
	// headerDeadline carries the caller's remaining deadline in milliseconds: the
	// peer works under it, so a search past it answers timed_out as a local one does.
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

// wireRequest is a search.Request on the wire: the query as its DSL JSON.
type wireRequest struct {
	Query       json.RawMessage       `json:"query"`
	Sort        []search.SortField    `json:"sort,omitempty"`
	Size        int                   `json:"size"`
	SearchAfter []any                 `json:"search_after,omitempty"`
	TrackTotal  int                   `json:"track_total"`
	Aggs        map[string]search.Agg `json:"aggs,omitempty"`
	Fields      []string              `json:"fields,omitempty"`
	TimeoutNs   int64                 `json:"timeout_ns,omitempty"`
	Index       string                `json:"index"`
	NoBodies    bool                  `json:"no_bodies,omitempty"`
}

func encodeRequest(r *search.Request) (*wireRequest, error) {
	q, err := percolate.EncodeQuery(r.Query)
	if err != nil {
		return nil, fmt.Errorf("cluster: encoding the query: %w", err)
	}
	return &wireRequest{
		Query: q, Sort: r.Sort, Size: r.Size, SearchAfter: r.SearchAfter, TrackTotal: r.TrackTotal,
		Aggs: r.Aggs, Fields: r.Fields, TimeoutNs: int64(r.Timeout), Index: r.Index, NoBodies: r.NoBodies,
	}, nil
}

func (w *wireRequest) decode() (*search.Request, error) {
	q, problems := query.Parse(w.Query)
	if len(problems) > 0 {
		return nil, api.Invalid("the query is invalid", problems...)
	}
	return &search.Request{
		Query: q, Sort: w.Sort, Size: w.Size, SearchAfter: w.SearchAfter, TrackTotal: w.TrackTotal,
		Aggs: w.Aggs, Fields: w.Fields, Timeout: time.Duration(w.TimeoutNs), Index: w.Index, NoBodies: w.NoBodies,
	}, nil
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

// percolateMsg asks a peer to percolate documents, analyzed under Mapping (the
// catalogue's JSON), against its copy's saved queries.
type percolateMsg struct {
	shardRef
	Mapping json.RawMessage `json:"mapping,omitempty"`
	Docs    []wireDoc       `json:"docs"`
}

type wireDoc struct {
	ID   string          `json:"id"`
	Body json.RawMessage `json:"body"`
}

type percolateReply struct {
	Matches [][]string `json:"matches"`
	Stale   bool       `json:"stale,omitempty"`
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
