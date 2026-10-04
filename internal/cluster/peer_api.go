package cluster

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// The internal peer API, under /_internal/ on the node's listener. Every request
// carries the cluster token (Authorization: Bearer <cluster_token>); with none
// configured the API refuses every peer, unless insecure_no_auth is set.
//
//	POST   /_internal/hint                      push hints: wake the named copies' tailers
//	POST   /_internal/search                    a shard's query (or whole search) on this node's copy
//	POST   /_internal/fetch                     bodies of hits from a pinned generation
//	DELETE /_internal/pins/{id}                 unpin a generation
//	POST   /_internal/percolate                 percolate documents against a copy's saved queries
//	POST   /_internal/get                       a stored document or saved query from a copy
//	POST   /_internal/wait                      wait until a copy has a seq searchable
//	POST   /_internal/counts                    a copy's live documents and saved queries
//	GET    /_internal/copies                    this node's copies and their progress
//	POST   /_internal/snapshots                 take a snapshot of a copy for a recovery
//	GET    /_internal/snapshots/{id}/files/{name}  stream one snapshot file (Range resumes it)
//	DELETE /_internal/snapshots/{id}            release a snapshot
type peerAPI struct {
	n        *Node
	inflight atomic.Int64
}

// maxPeerBody bounds a peer request's body.
const maxPeerBody = 256 << 20

func (p *peerAPI) handler() http.Handler {
	mux := http.NewServeMux()
	handle := func(pattern string, fn func(r *http.Request) (any, error)) {
		mux.Handle(pattern, p.wrap(pattern, fn))
	}
	handle("POST /_internal/hint", p.hint)
	handle("POST /_internal/search", p.search)
	handle("POST /_internal/fetch", p.fetch)
	handle("DELETE /_internal/pins/{id}", p.unpin)
	handle("POST /_internal/percolate", p.percolate)
	handle("POST /_internal/get", p.get)
	handle("POST /_internal/wait", p.wait)
	handle("POST /_internal/counts", p.counts)
	handle("GET /_internal/copies", p.copies)
	handle("POST /_internal/snapshots", p.snapshot)
	handle("DELETE /_internal/snapshots/{id}", p.release)
	mux.Handle("GET /_internal/snapshots/{id}/files/{name}", p.wrapStream("GET /_internal/snapshots/{id}/files/{name}", p.file))
	mux.Handle("/", p.wrap("unmatched", func(*http.Request) (any, error) {
		return nil, api.NotFound(api.CodeNotFound, "no peer endpoint here")
	}))
	return mux
}

// authorized checks a peer request's cluster token.
func (p *peerAPI) authorized(r *http.Request) *api.Error {
	token := p.n.cfg.ClusterToken
	if token == "" {
		if p.n.cfg.InsecureNoAuth {
			return nil
		}
		return &api.Error{Status: http.StatusUnauthorized, Code: api.CodeUnauthorized, Detail: "cluster_token is not set on this node: the peer API is closed"}
	}
	scheme, presented, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return &api.Error{Status: http.StatusUnauthorized, Code: api.CodeUnauthorized, Detail: "the cluster token is required"}
	}
	want := sha256.Sum256([]byte(token))
	got := sha256.Sum256([]byte(strings.TrimSpace(presented)))
	if subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
		return &api.Error{Status: http.StatusUnauthorized, Code: api.CodeUnauthorized, Detail: "the cluster token is not valid"}
	}
	return nil
}

// begin starts a peer request: auth, the caller's trace and deadline, a span. It
// returns the request under the context the work runs in.
func (p *peerAPI) begin(w http.ResponseWriter, r *http.Request, route string) (*http.Request, trace.Span, func(), *api.Error) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := p.n.tr.Start(ctx, "peer "+route, trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attribute.String("peer.route", route)))
	cancel := func() {}
	if ms, err := strconv.ParseInt(r.Header.Get(headerDeadline), 10, 64); err == nil && ms > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
	}
	r = r.WithContext(ctx)
	if e := p.authorized(r); e != nil {
		return r, span, cancel, e
	}
	q := p.inflight.Add(1)
	w.Header().Set(headerQueue, strconv.FormatInt(q-1, 10))
	end := cancel
	cancel = func() {
		p.inflight.Add(-1)
		end()
	}
	return r, span, cancel, nil
}

func (p *peerAPI) wrap(route string, fn func(r *http.Request) (any, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		r, span, done, e := p.begin(w, r, route)
		defer span.End()
		defer done()
		if e != nil {
			writeError(w, e)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxPeerBody)
		out, err := fn(r)
		w.Header().Set(headerServiceTime, strconv.FormatFloat(time.Since(start).Seconds(), 'f', -1, 64))
		if err != nil {
			e := api.ProblemFor(err)
			if e.Status >= 500 {
				span.RecordError(err)
				span.SetStatus(codes.Error, e.Detail)
				p.n.log.DebugContext(r.Context(), "peer request failed", slog.String("route", route), slog.Any("error", err)) //nolint:contextcheck // r carries the request context
			}
			writeError(w, e)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if out == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_ = json.NewEncoder(w).Encode(out)
	})
}

func (p *peerAPI) wrapStream(route string, fn func(w http.ResponseWriter, r *http.Request) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r, span, done, e := p.begin(w, r, route)
		defer span.End()
		defer done()
		if e != nil {
			writeError(w, e)
			return
		}
		if err := fn(w, r); err != nil {
			writeError(w, api.ProblemFor(err))
		}
	})
}

// writeError writes an error reply.
func writeError(w http.ResponseWriter, e *api.Error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.Status)
	_ = json.NewEncoder(w).Encode(errorReply{Status: e.Status, Code: e.Code, Detail: e.Detail, Problems: e.Problems, RetryAfterMs: e.RetryAfter.Milliseconds()})
}

func decode(r *http.Request, v any) error {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		return api.InvalidAt("body", "the peer request is not valid JSON: %v", err)
	}
	return nil
}

func (p *peerAPI) hint(r *http.Request) (any, error) {
	var msg hintMsg
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	for _, h := range msg.Hints {
		p.n.Wake(store.ShardID{Index: h.Index, Shard: h.Shard}, h.Seq)
	}
	return nil, nil
}

func (p *peerAPI) search(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg searchMsg
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	if msg.Request == nil {
		return nil, api.InvalidAt("request", "no search request")
	}
	req, err := msg.Request.decode()
	if err != nil {
		return nil, err
	}
	t, err := p.n.LocalTarget(ctx, msg.Index, msg.Shard, msg.WaitSeq)
	if err != nil {
		return nil, err
	}
	res, err := t.Search(ctx, req)
	if err != nil {
		t.Release()
		return nil, err
	}
	reply := &searchReply{Result: res, Stale: t.Stale()}
	if msg.Pin {
		reply.Pin = p.n.pins.add(t)
	} else {
		t.Release()
	}
	return reply, nil
}

func (p *peerAPI) fetch(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg fetchMsg
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	var reply fetchReply
	ok := p.n.pins.use(msg.Pin, func(t node.ShardTarget) error {
		if err := t.Fetch(ctx, msg.Hits, msg.Fields); err != nil {
			return err
		}
		reply.Bodies = make([]json.RawMessage, len(msg.Hits))
		for i := range msg.Hits {
			reply.Bodies[i] = msg.Hits[i].Body
		}
		return nil
	})
	if errors.Is(ok, errPinGone) {
		return nil, &api.Error{Status: http.StatusGone, Code: api.CodeNotFound, Detail: "the pinned generation expired"}
	}
	if ok != nil {
		return nil, ok
	}
	return &reply, nil
}

func (p *peerAPI) unpin(r *http.Request) (any, error) {
	p.n.pins.remove(r.PathValue("id"))
	return nil, nil
}

func (p *peerAPI) percolate(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg percolateMsg
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	m := &schema.Mapping{Fields: map[string]schema.FieldType{}}
	if len(msg.Mapping) > 0 {
		if err := json.Unmarshal(msg.Mapping, m); err != nil {
			return nil, api.InvalidAt("mapping", "%v", err)
		}
	}
	docs := make([]schema.Doc, len(msg.Docs))
	for i, d := range msg.Docs {
		doc, _, err := schema.Analyze(m, d.ID, d.Body)
		if err != nil {
			return nil, api.InvalidAt("docs."+strconv.Itoa(i), "%v", err)
		}
		docs[i] = doc
	}
	t, err := p.n.LocalTarget(ctx, msg.Index, msg.Shard, msg.WaitSeq)
	if err != nil {
		return nil, err
	}
	defer t.Release()
	matches, err := t.Percolate(ctx, msg.Mapping, docs)
	if err != nil {
		return nil, err
	}
	for i := range matches {
		if matches[i] == nil {
			matches[i] = []string{}
		}
	}
	return &percolateReply{Matches: matches, Stale: t.Stale()}, nil
}

func (p *peerAPI) get(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg getMsg
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	t, err := p.n.LocalTarget(ctx, msg.Index, msg.Shard, msg.WaitSeq)
	if err != nil {
		return nil, err
	}
	defer t.Release()
	reply := &getReply{Stale: t.Stale()}
	if msg.Query {
		reply.Saved, reply.Found, err = t.GetQuery(ctx, msg.ID)
	} else {
		reply.Body, reply.Found, err = t.Get(ctx, msg.ID)
	}
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func (p *peerAPI) wait(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg waitMsg
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	mode := api.RefreshWaitFor
	if msg.Refresh {
		mode = api.RefreshTrue
	}
	return nil, p.n.WaitLocal(ctx, msg.Index, msg.Shard, msg.Seq, mode)
}

func (p *peerAPI) counts(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg shardRef
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	docs, queries, err := p.n.Counts(ctx, store.ShardID{Index: msg.Index, Shard: msg.Shard})
	if err != nil {
		return nil, err
	}
	return &countsReply{Docs: docs, Queries: queries}, nil
}

func (p *peerAPI) copies(*http.Request) (any, error) {
	local := p.n.LocalCopies()
	out := &copiesReply{Copies: make([]api.ShardInfo, 0, len(local))}
	for i := range local {
		out.Copies = append(out.Copies, local[i].Info)
	}
	return out, nil
}

func (p *peerAPI) snapshot(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg shardRef
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	sn, err := p.n.Snapshot(ctx, store.ShardID{Index: msg.Index, Shard: msg.Shard})
	if err != nil {
		return nil, err
	}
	reply := &snapshotReply{Seq: sn.Seq(), IndexUID: sn.IndexUID(), MappingVersion: sn.MappingVersion()}
	for _, f := range sn.Files() {
		sum, err := p.n.sums.sum(sn, f)
		if err != nil {
			sn.Release()
			return nil, err
		}
		reply.Files = append(reply.Files, wireFile{Name: f.Name, Size: f.Size, SHA256: sum})
	}
	reply.ID = p.n.snaps.add(sn)
	p.n.log.InfoContext(ctx, "serving a recovery snapshot", slog.String("shard", store.ShardID{Index: msg.Index, Shard: msg.Shard}.String()),
		slog.Int64("seq", reply.Seq), slog.Int("files", len(reply.Files)))
	return reply, nil
}

func (p *peerAPI) release(r *http.Request) (any, error) {
	p.n.snaps.remove(r.PathValue("id"))
	return nil, nil
}

// file streams one snapshot file; a Range header resumes it from an offset.
func (p *peerAPI) file(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("name")
	return p.n.snaps.use(r.PathValue("id"), func(sn *shard.Snapshot) error {
		f, err := sn.Open(name)
		if errors.Is(err, shard.ErrNoSuchFile) {
			return api.NotFound(api.CodeNotFound, "the snapshot has no file %q", name)
		}
		if err != nil {
			return err
		}
		defer f.Close()
		// A big file outlasts the listener's write timeout: the stream has none (a
		// stalled peer is cut by its own side, and the transfer resumes).
		_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
		if p.n.opts.hooks != nil && p.n.opts.hooks.peerFile != nil {
			w = p.n.opts.hooks.peerFile(name, w)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, name, time.Time{}, f)
		return nil
	})
}

// --- pins and snapshots ---------------------------------------------------------------

// errPinGone is a pin that expired or was released.
var errPinGone = errors.New("cluster: the pin is gone")

// pinTable holds the generations searches pinned on this node for their fetch phase.
type pinTable struct {
	ttl time.Duration
	mu  sync.Mutex
	m   map[string]*pinEntry
}

type pinEntry struct {
	mu      sync.Mutex // held while a fetch uses the target
	t       node.ShardTarget
	expires time.Time
	gone    bool
}

func newPinTable(ttl time.Duration) *pinTable { return &pinTable{ttl: ttl, m: map[string]*pinEntry{}} }

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (p *pinTable) add(t node.ShardTarget) string {
	id := newID()
	p.mu.Lock()
	p.m[id] = &pinEntry{t: t, expires: time.Now().Add(p.ttl)}
	p.mu.Unlock()
	return id
}

// use runs fn on a pinned target, keeping it pinned meanwhile.
func (p *pinTable) use(id string, fn func(node.ShardTarget) error) error {
	p.mu.Lock()
	e := p.m[id]
	if e != nil {
		e.expires = time.Now().Add(p.ttl)
	}
	p.mu.Unlock()
	if e == nil {
		return errPinGone
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.gone {
		return errPinGone
	}
	return fn(e.t)
}

func (p *pinTable) remove(id string) {
	p.mu.Lock()
	e := p.m[id]
	delete(p.m, id)
	p.mu.Unlock()
	if e != nil {
		e.drop()
	}
}

func (e *pinEntry) drop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.gone {
		e.gone = true
		e.t.Release()
	}
}

// sweep releases the pins whose time is up.
func (p *pinTable) sweep() {
	now := time.Now()
	var expired []*pinEntry
	p.mu.Lock()
	for id, e := range p.m {
		if now.After(e.expires) {
			expired = append(expired, e)
			delete(p.m, id)
		}
	}
	p.mu.Unlock()
	for _, e := range expired {
		e.drop()
	}
}

func (p *pinTable) closeAll() {
	p.mu.Lock()
	list := make([]*pinEntry, 0, len(p.m))
	for id, e := range p.m {
		list = append(list, e)
		delete(p.m, id)
	}
	p.mu.Unlock()
	for _, e := range list {
		e.drop()
	}
}

// snapTable holds the snapshots peers are recovering from.
type snapTable struct {
	ttl time.Duration
	mu  sync.Mutex
	m   map[string]*snapEntry
}

type snapEntry struct {
	sn      *shard.Snapshot
	expires time.Time
	users   int
	gone    bool
}

func newSnapTable(ttl time.Duration) *snapTable {
	return &snapTable{ttl: ttl, m: map[string]*snapEntry{}}
}

func (s *snapTable) add(sn *shard.Snapshot) string {
	id := newID()
	s.mu.Lock()
	s.m[id] = &snapEntry{sn: sn, expires: time.Now().Add(s.ttl)}
	s.mu.Unlock()
	return id
}

// use runs fn on a snapshot, which is kept meanwhile and for SnapshotTTL after.
func (s *snapTable) use(id string, fn func(*shard.Snapshot) error) error {
	s.mu.Lock()
	e := s.m[id]
	if e == nil {
		s.mu.Unlock()
		return &api.Error{Status: http.StatusGone, Code: api.CodeNotFound, Detail: "the snapshot expired"}
	}
	e.users++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		e.users--
		e.expires = time.Now().Add(s.ttl)
		release := e.gone && e.users == 0
		s.mu.Unlock()
		if release {
			e.sn.Release()
		}
	}()
	return fn(e.sn)
}

func (s *snapTable) remove(id string) {
	s.mu.Lock()
	e := s.m[id]
	delete(s.m, id)
	release := false
	if e != nil {
		e.gone = true
		release = e.users == 0
	}
	s.mu.Unlock()
	if release {
		e.sn.Release()
	}
}

func (s *snapTable) sweep() {
	now := time.Now()
	var release []*shard.Snapshot
	s.mu.Lock()
	for id, e := range s.m {
		if e.users == 0 && now.After(e.expires) {
			delete(s.m, id)
			e.gone = true
			release = append(release, e.sn)
		}
	}
	s.mu.Unlock()
	for _, sn := range release {
		sn.Release()
	}
}

func (s *snapTable) closeAll() {
	s.mu.Lock()
	var release []*shard.Snapshot
	for id, e := range s.m {
		delete(s.m, id)
		e.gone = true
		if e.users == 0 {
			release = append(release, e.sn)
		}
	}
	s.mu.Unlock()
	for _, sn := range release {
		sn.Release()
	}
}

// janitorLoop expires pins and snapshots.
func (n *Node) janitorLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n.pins.sweep()
		n.snaps.sweep()
	}
}

// sumCache remembers the SHA-256 of immutable snapshot files: segment, query segment
// and deletes sidecar files never change once written (their names are unique), so a
// file is hashed once whatever the number of recoveries. The manifest is hashed each
// time.
type sumCache struct {
	mu sync.Mutex
	m  map[string]sumEntry
}

type sumEntry struct {
	size int64
	sum  string
}

// maxSums bounds the cache.
const maxSums = 1 << 16

func newSumCache() *sumCache { return &sumCache{m: map[string]sumEntry{}} }

func (c *sumCache) sum(sn *shard.Snapshot, f shard.SnapshotFile) (string, error) {
	if f.Name != shard.ManifestName {
		c.mu.Lock()
		e, ok := c.m[f.Name]
		c.mu.Unlock()
		if ok && e.size == f.Size {
			return e.sum, nil
		}
	}
	r, err := sn.Open(f.Name)
	if err != nil {
		return "", err
	}
	defer r.Close()
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if f.Name != shard.ManifestName {
		c.mu.Lock()
		if len(c.m) >= maxSums {
			clear(c.m)
		}
		c.m[f.Name] = sumEntry{size: f.Size, sum: sum}
		c.mu.Unlock()
	}
	return sum, nil
}
