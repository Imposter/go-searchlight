package workloads

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Op is one iteration of a workload: the i-th request. It returns how many documents
// it handled (for docs/s; 0 counts as 1), and optionally the latency to record in
// place of the call's own wall time. Most ops return 0 and keep the wall time; an op
// whose unmeasured tail work (a confirming read after a timed write, say) must not
// count toward the measured latency returns that latency instead.
type Op func(ctx context.Context, i int) (docs int, timed time.Duration, err error)

// RunOptions say how a workload runs.
type RunOptions struct {
	// Warmup iterations run first, unrecorded; Iterations are measured.
	Warmup, Iterations int
	// Concurrency is how many requests are in flight (closed loop), or the most in
	// flight at once under Rate.
	Concurrency int
	// Rate, when above 0, issues requests at this many per second (open loop),
	// measuring each from its scheduled start.
	Rate float64
	// Duration, when above 0, bounds the measured phase by time instead of
	// Iterations (the mixed workload).
	Duration time.Duration
}

// Measurement is a run's raw result.
type Measurement struct {
	Hist     *Histogram
	Ops      int64
	Docs     int64
	Errors   int64
	Elapsed  time.Duration
	FirstErr error
}

// Throughput is measured operations per second.
func (m *Measurement) Throughput() float64 {
	if m.Elapsed <= 0 {
		return 0
	}
	return float64(m.Ops) / m.Elapsed.Seconds()
}

// DocsPerSec is documents per second.
func (m *Measurement) DocsPerSec() float64 {
	if m.Elapsed <= 0 {
		return 0
	}
	return float64(m.Docs) / m.Elapsed.Seconds()
}

// maxErrors bounds how many errors a run keeps going past: a run that fails this
// often stops early.
const maxErrors = 100

// Run runs op: o.Warmup iterations (closed loop, unrecorded), then the measured phase.
// Iteration numbers continue from the warmup, so a workload cycling through query
// variants does not measure only the ones the warmup cached.
func Run(ctx context.Context, o RunOptions, op Op) *Measurement {
	o.Concurrency = max(o.Concurrency, 1)
	if o.Warmup > 0 {
		closedLoop(ctx, o.Concurrency, 0, o.Warmup, 0, op, nil)
	}
	m := &Measurement{Hist: NewHistogram()}
	if o.Rate > 0 {
		openLoop(ctx, o, op, m)
	} else {
		closedLoop(ctx, o.Concurrency, o.Warmup, o.Iterations, o.Duration, op, m)
	}
	return m
}

// closedLoop runs iterations [start, start+n) (or until d passes) with conc workers,
// recording into m when it is not nil.
func closedLoop(ctx context.Context, conc, start, n int, d time.Duration, op Op, m *Measurement) {
	var next atomic.Int64
	next.Store(int64(start))
	end := int64(start + n)
	var deadline time.Time
	if d > 0 {
		deadline = time.Now().Add(d)
		end = int64(^uint64(0) >> 1)
	}
	var mu sync.Mutex
	var errs atomic.Int64
	var wg sync.WaitGroup
	t0 := time.Now()
	for range conc {
		wg.Go(func() {
			h := NewHistogram()
			var ops, docs int64
			var firstErr error
			for ctx.Err() == nil && errs.Load() < maxErrors {
				i := next.Add(1) - 1
				if i >= end || (!deadline.IsZero() && time.Now().After(deadline)) {
					break
				}
				s := nanotime()
				nd, timed, err := op(ctx, int(i))
				lat := time.Duration(nanotime() - s)
				if timed > 0 {
					lat = timed
				}
				if err != nil {
					if errs.Add(1) == 1 {
						firstErr = err
					}
					continue
				}
				h.RecordDuration(lat)
				ops++
				docs += int64(max(nd, 1))
			}
			if m != nil {
				mu.Lock()
				m.Hist.Merge(h)
				m.Ops += ops
				m.Docs += docs
				if firstErr != nil && m.FirstErr == nil {
					m.FirstErr = firstErr
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if m != nil {
		m.Elapsed = time.Since(t0)
		m.Errors = errs.Load()
	}
}

// openLoop issues the measured iterations at o.Rate per second, at most
// o.Concurrency (or 256) in flight; each latency runs from the scheduled start, so a
// full window (the engine falling behind) is charged to the engine.
func openLoop(ctx context.Context, o RunOptions, op Op, m *Measurement) {
	inflight := o.Concurrency
	if inflight <= 1 {
		inflight = 256
	}
	sem := make(chan struct{}, inflight)
	interval := int64(float64(time.Second) / o.Rate)
	n := o.Iterations
	if o.Duration > 0 {
		n = int(o.Duration.Seconds()*o.Rate) + 1
	}
	var mu sync.Mutex
	var errs atomic.Int64
	var wg sync.WaitGroup
	t0 := time.Now()
	base := nanotime()
	for k := range n {
		if ctx.Err() != nil || errs.Load() >= maxErrors {
			break
		}
		at := base + int64(k)*interval
		if wait := time.Duration(at - nanotime()); wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
			}
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			continue
		}
		wg.Go(func() {
			defer func() { <-sem }()
			nd, timed, err := op(ctx, o.Warmup+k)
			lat := time.Duration(nanotime() - at)
			if timed > 0 {
				lat = timed
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if errs.Add(1) == 1 {
					m.FirstErr = err
				}
				return
			}
			m.Hist.RecordDuration(lat)
			m.Ops++
			m.Docs += int64(max(nd, 1))
		})
	}
	wg.Wait()
	m.Elapsed = time.Since(t0)
	m.Errors = errs.Load()
}
