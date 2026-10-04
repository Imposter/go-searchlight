package store

import (
	"context"
	"database/sql"
	"sync"
)

// The write gate. A dialect whose writes go through one connection (SQLite: one writer
// at a time, so the store's pool holds a single write connection) would otherwise hand
// that connection to whichever waiter database/sql picks, at random, so a lease renewal
// could wait behind any number of bulk commits and the lease lapse. The gate queues
// writers in two FIFO lanes and lets one at a time reach the pool: the high lane (the
// cluster registry's lease and membership writes: renewals, claims, heartbeats,
// releases, state changes) always goes before the low lane (changelog commits, applied
// reports, blobs, the catalogue, pruning). A renewal then waits for at most the
// transaction in flight and the one writer already at the pool.
//
// A writer holds the gate until its statement (or BeginTx, or Conn) has the
// connection, not until its transaction ends: the next writer then waits at the pool,
// alone, so the pool's own order no longer matters.

// Lane selects a write's lane in the gate.
type lane uint8

const (
	laneLow lane = iota
	laneHigh
)

type laneKey struct{}

// withHighLane marks ctx's writes as registry writes: the gate's high lane.
func withHighLane(ctx context.Context) context.Context {
	return context.WithValue(ctx, laneKey{}, laneHigh)
}

func laneOf(ctx context.Context) lane {
	if l, ok := ctx.Value(laneKey{}).(lane); ok {
		return l
	}
	return laneLow
}

// The gates of this process, by database: every store this process opens on one SQLite
// file shares its gate, so their writers take turns first come, first served, rather
// than all waiting in SQLite's busy handler, which favours no one (in-process tests
// run several nodes over one file).
var gates = struct {
	sync.Mutex
	m map[string]*sharedGate
}{m: map[string]*sharedGate{}}

type sharedGate struct {
	g    *gate
	refs int
}

// acquireGate returns key's gate (a new one for ""), counting the reference.
func acquireGate(key string) *gate {
	if key == "" {
		return &gate{}
	}
	gates.Lock()
	defer gates.Unlock()
	sg := gates.m[key]
	if sg == nil {
		sg = &sharedGate{g: &gate{}}
		gates.m[key] = sg
	}
	sg.refs++
	return sg.g
}

// releaseGate drops a reference to key's gate.
func releaseGate(key string) {
	if key == "" {
		return
	}
	gates.Lock()
	defer gates.Unlock()
	if sg := gates.m[key]; sg != nil {
		sg.refs--
		if sg.refs <= 0 {
			delete(gates.m, key)
		}
	}
}

// gate is a two-lane FIFO mutex.
type gate struct {
	mu    sync.Mutex
	busy  bool
	lanes [2][]chan struct{} // laneLow, laneHigh
}

// acquire waits for the gate in ctx's lane, or until ctx ends.
func (g *gate) acquire(ctx context.Context) error {
	g.mu.Lock()
	if !g.busy {
		g.busy = true
		g.mu.Unlock()
		return nil
	}
	l := laneOf(ctx)
	ch := make(chan struct{})
	g.lanes[l] = append(g.lanes[l], ch)
	g.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		for i, w := range g.lanes[l] {
			if w == ch {
				g.lanes[l] = append(g.lanes[l][:i], g.lanes[l][i+1:]...)
				g.mu.Unlock()
				return ctx.Err()
			}
		}
		g.mu.Unlock()
		// Granted meanwhile: pass it on.
		g.release()
		return ctx.Err()
	}
}

// release hands the gate to the first high-lane waiter, else the first low-lane one.
func (g *gate) release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, l := range []lane{laneHigh, laneLow} {
		if q := g.lanes[l]; len(q) > 0 {
			g.lanes[l] = q[1:]
			close(q[0]) // busy stays set: the waiter holds it now
			return
		}
	}
	g.busy = false
}

// gatedDB is the write pool behind the gate (no gate: the pool as it is).
type gatedDB struct {
	*sql.DB
	g *gate
}

func (d *gatedDB) enter(ctx context.Context) (func(), error) {
	if d.g == nil {
		return func() {}, nil
	}
	if err := d.g.acquire(ctx); err != nil {
		return nil, err
	}
	return d.g.release, nil
}

func (d *gatedDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	done, err := d.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return d.DB.ExecContext(ctx, query, args...)
}

func (d *gatedDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	done, err := d.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return d.DB.QueryContext(ctx, query, args...)
}

func (d *gatedDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	done, err := d.enter(ctx)
	if err != nil {
		// A Row carries its error to Scan; the context's error is what the
		// caller sees there.
		return d.DB.QueryRowContext(ctx, query, args...)
	}
	defer done()
	return d.DB.QueryRowContext(ctx, query, args...)
}

func (d *gatedDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	done, err := d.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return d.DB.BeginTx(ctx, opts)
}

func (d *gatedDB) Conn(ctx context.Context) (*sql.Conn, error) {
	done, err := d.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return d.DB.Conn(ctx)
}
