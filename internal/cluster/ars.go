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
// size (q) the peer reports with each read it serves; it ranks the peer
//
//	Ψ = R − S + q̂³·S,  q̂ = 1 + outstanding·clients + q
//
// lowest first: a peer whose queue grows is penalized cubically, ahead of its response
// times rising. outstanding is this coordinator's reads in flight to the peer, and
// clients the number of coordinators (the live nodes). Only reads feed it: hints,
// waits and recoveries say nothing of a peer's read latency.
//
// A copy that failed a read is suspected, and ranks after every unsuspected one, for a
// time that doubles with each failure in a row (2 s, 4 s, ... up to 2 minutes) and
// resets on a success: a peer that heartbeats but hangs stops being tried first
// without being forgotten. A copy that answered stale ranks after fresh ones for 2 s.
// Ties (an unmeasured peer ranks 0, so new peers are tried) are broken at random,
// spreading the load.

// arsAlpha is the EWMA weight of a new sample (Elasticsearch's 0.3).
const arsAlpha = 0.3

// Penalties added to a rank.
const (
	failedPenalty = 1e9
	stalePenalty  = 1e6
)

// Suspicion of a copy that failed: suspectBase doubled per failure in a row, at most
// suspectCap.
const (
	suspectBase = 2 * time.Second
	suspectCap  = 2 * time.Minute
)

// staleFor is how long a copy that reported itself stale ranks behind fresh ones.
const staleFor = 2 * time.Second

type ars struct {
	mu    sync.Mutex
	peers map[string]*peerStats
	// copies are the copies' (node and shard) suspicion and staleness.
	copies map[string]*copyStats
}

type peerStats struct {
	resp, service, queue float64 // EWMAs, seconds and requests
	measured             bool
	outstanding          int
}

type copyStats struct {
	failures     int
	suspectUntil time.Time
	staleAt      time.Time
}

func newARS() *ars {
	return &ars{peers: map[string]*peerStats{}, copies: map[string]*copyStats{}}
}

func (a *ars) peer(id string) *peerStats {
	p := a.peers[id]
	if p == nil {
		p = &peerStats{}
		a.peers[id] = p
	}
	return p
}

func (a *ars) copy(key string) *copyStats {
	c := a.copies[key]
	if c == nil {
		c = &copyStats{}
		a.copies[key] = c
	}
	return c
}

// start notes a read of copy key on peer id going out; the returned func notes its
// outcome: the service time and queue the peer reported (negative when it reported
// none), or a failure of the copy.
func (a *ars) start(id, key string) func(failed bool, service float64, queue float64) {
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
			a.suspectLocked(key)
			return
		}
		c := a.copy(key)
		c.failures, c.suspectUntil = 0, time.Time{}
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

// suspect marks copy key as having failed.
func (a *ars) suspect(key string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.suspectLocked(key)
}

func (a *ars) suspectLocked(key string) {
	c := a.copy(key)
	c.failures++
	d := suspectBase << min(c.failures-1, 16)
	c.suspectUntil = time.Now().Add(min(d, suspectCap))
}

func ewma(avg, sample float64) float64 { return arsAlpha*sample + (1-arsAlpha)*avg }

// noteStale records whether copy key last answered stale.
func (a *ars) noteStale(key string, stale bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if stale {
		a.copy(key).staleAt = time.Now()
	} else if c := a.copies[key]; c != nil {
		c.staleAt = time.Time{}
	}
}

// rank is peer id's rank for a read of copy key, lower first.
func (a *ars) rank(id, key string, clients int) float64 {
	var r float64
	if p := a.peers[id]; p != nil {
		q := 1 + float64(p.outstanding)*float64(max(1, clients)) + p.queue
		r = p.resp - p.service + q*q*q*p.service
	}
	if c := a.copies[key]; c != nil {
		now := time.Now()
		if now.Before(c.suspectUntil) {
			r += failedPenalty
		}
		if !c.staleAt.IsZero() && now.Sub(c.staleAt) < staleFor {
			r += stalePenalty
		}
	}
	return r
}

// order sorts cands (peer ids with their copy keys) by rank, ties at random.
func (a *ars) order(cands []candidate, clients int) {
	a.mu.Lock()
	ranks := make(map[string]float64, len(cands))
	for _, c := range cands {
		ranks[c.key()] = a.rank(c.node, c.key(), clients)
	}
	a.mu.Unlock()
	rand.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] }) //nolint:gosec // load spreading, not security
	slices.SortStableFunc(cands, func(x, y candidate) int {
		switch rx, ry := ranks[x.key()], ranks[y.key()]; {
		case rx < ry:
			return -1
		case rx > ry:
			return 1
		}
		return 0
	})
}
