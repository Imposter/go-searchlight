package cluster

import (
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

// Adaptive replica selection, after Elasticsearch's (itself after C3, Suresh et al.,
// NSDI 2015). For each peer the coordinator keeps exponentially weighted moving
// averages of the response time it measures (R), and of the service time (S) and queue
// size (q) the peer reports with each response; it ranks the peer
//
//	Ψ = R − S + q̂³·S,  q̂ = 1 + outstanding·clients + q
//
// lowest first: a peer whose queue grows is penalized cubically, ahead of its response
// times rising. outstanding is this coordinator's requests in flight to the peer, and
// clients the number of coordinators (the live nodes). A peer that has just failed a
// request ranks after every healthy one for a short while, and one that reported its
// copy stale after every fresh one. Ties (an unmeasured peer ranks 0, so new peers are
// tried) are broken at random, spreading the load.

// arsAlpha is the EWMA weight of a new sample (Elasticsearch's 0.3).
const arsAlpha = 0.3

// Penalties added to a rank.
const (
	failedPenalty = 1e9
	stalePenalty  = 1e6
)

// suspectFor is how long a peer that failed a request ranks behind healthy ones.
const suspectFor = 5 * time.Second

// staleFor is how long a copy that reported itself stale ranks behind fresh ones.
const staleFor = 2 * time.Second

type ars struct {
	mu    sync.Mutex
	peers map[string]*peerStats
	// stale is when each copy (node and shard key) last answered stale.
	stale map[string]time.Time
}

type peerStats struct {
	resp, service, queue float64 // EWMAs, seconds and requests
	measured             bool
	outstanding          int
	failedAt             time.Time
}

func newARS() *ars {
	return &ars{peers: map[string]*peerStats{}, stale: map[string]time.Time{}}
}

func (a *ars) peer(id string) *peerStats {
	p := a.peers[id]
	if p == nil {
		p = &peerStats{}
		a.peers[id] = p
	}
	return p
}

// start notes a request to peer id going out; the returned func notes its outcome:
// the response time, and the service time and queue the peer reported (negative when
// it reported none), or a failure.
func (a *ars) start(id string) func(failed bool, service float64, queue float64) {
	began := time.Now()
	a.mu.Lock()
	a.peer(id).outstanding++
	a.mu.Unlock()
	return func(failed bool, service, queue float64) {
		a.mu.Lock()
		defer a.mu.Unlock()
		p := a.peer(id)
		p.outstanding--
		if failed {
			p.failedAt = time.Now()
			return
		}
		p.failedAt = time.Time{}
		resp := time.Since(began).Seconds()
		if !p.measured {
			p.resp, p.measured = resp, true
			if service >= 0 {
				p.service = service
			}
			if queue >= 0 {
				p.queue = queue
			}
			return
		}
		p.resp = ewma(p.resp, resp)
		if service >= 0 {
			p.service = ewma(p.service, service)
		}
		if queue >= 0 {
			p.queue = ewma(p.queue, queue)
		}
	}
}

func ewma(avg, sample float64) float64 { return arsAlpha*sample + (1-arsAlpha)*avg }

// noteStale records whether copy key last answered stale.
func (a *ars) noteStale(key string, stale bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if stale {
		a.stale[key] = time.Now()
	} else {
		delete(a.stale, key)
	}
}

// rank is peer id's rank for a read of copy key, lower first.
func (a *ars) rank(id, key string, clients int) float64 {
	p := a.peers[id]
	var r float64
	if p != nil {
		q := 1 + float64(p.outstanding)*float64(max(1, clients)) + p.queue
		r = p.resp - p.service + q*q*q*p.service
		if !p.failedAt.IsZero() && time.Since(p.failedAt) < suspectFor {
			r += failedPenalty
		}
	}
	if at, ok := a.stale[key]; ok && time.Since(at) < staleFor {
		r += stalePenalty
	}
	return r
}

// order sorts cands (peer ids with their copy keys) by rank, ties at random.
func (a *ars) order(cands []candidate, clients int) {
	a.mu.Lock()
	ranks := make(map[string]float64, len(cands))
	for _, c := range cands {
		ranks[c.node] = a.rank(c.node, c.key(), clients)
	}
	a.mu.Unlock()
	rand.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] }) //nolint:gosec // load spreading, not security
	slices.SortStableFunc(cands, func(x, y candidate) int {
		switch rx, ry := ranks[x.node], ranks[y.node]; {
		case rx < ry:
			return -1
		case rx > ry:
			return 1
		}
		return 0
	})
}
