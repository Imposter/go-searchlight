package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // the "pgx" database/sql driver
)

// pgSample is what a Postgres did during a load. Waits are the average number of backends
// seen in each state, keyed backend type/wait type/wait event ("CPU" for an active backend
// with no wait event). The other maps are the numeric columns of a statistics view, as
// the change over the load: Postgres 17's pg_stat_wal and pg_stat_checkpointer, and the
// load database's pg_stat_database.
type pgSample struct {
	Seconds     float64            `json:"seconds"`
	Samples     int                `json:"samples"`
	Waits       map[string]float64 `json:"waits"`
	WAL         map[string]float64 `json:"wal"`
	Checkpoints map[string]float64 `json:"checkpoints"`
	Database    map[string]float64 `json:"database"`
}

func (s *pgSample) print(w io.Writer) {
	keys := make([]string, 0, len(s.Waits))
	for k := range s.Waits {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b string) int {
		switch {
		case s.Waits[a] > s.Waits[b]:
			return -1
		case s.Waits[a] < s.Waits[b]:
			return 1
		}
		return 0
	})
	fmt.Fprintf(w, "postgres: %.1f commits/s, %.1f MB/s of WAL", s.Database["xact_commit"]/s.Seconds, s.WAL["wal_bytes"]/s.Seconds/(1<<20))
	if n := s.WAL["wal_sync"]; n > 0 {
		fmt.Fprintf(w, ", %.1f WAL syncs/s of %.2f ms", n/s.Seconds, s.WAL["wal_sync_time"]/n)
	}
	fmt.Fprintln(w)
	for _, k := range keys[:min(len(keys), 8)] {
		fmt.Fprintf(w, "  %6.2f  %s\n", s.Waits[k], k)
	}
}

// pgStatViews are the statistics a sample reports the change of, one row each.
var pgStatViews = map[string]string{
	"wal":         "SELECT to_jsonb(s) FROM pg_stat_wal s",
	"checkpoints": "SELECT to_jsonb(s) FROM pg_stat_checkpointer s",
	"database":    "SELECT to_jsonb(s) FROM pg_stat_database s WHERE datname = current_database()",
}

// pgWaits reads what the load database's sessions and Postgres's own processes wait on.
const pgWaits = `SELECT backend_type, COALESCE(wait_event_type, 'CPU'), COALESCE(wait_event, ''), count(*)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND ((datname = current_database() AND state IN ('active', 'idle in transaction'))
    OR backend_type IN ('walwriter', 'checkpointer', 'background writer', 'autovacuum worker'))
GROUP BY 1, 2, 3`

// pgSampler reads pg_stat_activity every 100 ms on a connection of its own until stopped.
type pgSampler struct {
	db     *sql.DB
	cancel context.CancelFunc
	done   chan struct{}
	began  time.Time
	before map[string]map[string]float64

	mu      sync.Mutex
	samples int
	waits   map[string]int
}

func startPGSampler(ctx context.Context, url string) (*pgSampler, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	db.SetMaxOpenConns(1)
	before, err := pgStats(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &pgSampler{db: db, cancel: cancel, done: make(chan struct{}), began: time.Now(), before: before, waits: map[string]int{}}
	go func() {
		defer close(s.done)
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.sample(ctx)
			}
		}
	}()
	return s, nil
}

func (s *pgSampler) sample(ctx context.Context) {
	rows, err := s.db.QueryContext(ctx, pgWaits)
	if err != nil {
		return
	}
	defer rows.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for rows.Next() {
		var backend, kind, event string
		var n int
		if rows.Scan(&backend, &kind, &event, &n) == nil {
			s.waits[backend+"/"+kind+"/"+event] += n
		}
	}
	s.samples++
}

// stop ends the sampling and returns the sample, with whichever views could be read.
func (s *pgSampler) stop(ctx context.Context) *pgSample {
	s.cancel()
	<-s.done
	defer s.db.Close()
	out := &pgSample{Seconds: time.Since(s.began).Seconds(), Samples: s.samples, Waits: map[string]float64{}}
	for k, n := range s.waits {
		out.Waits[k] = float64(n) / float64(max(1, s.samples))
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	after, err := pgStats(ctx, s.db)
	if err != nil {
		return out
	}
	out.WAL = pgDelta(s.before["wal"], after["wal"])
	out.Checkpoints = pgDelta(s.before["checkpoints"], after["checkpoints"])
	out.Database = pgDelta(s.before["database"], after["database"])
	return out
}

// pgStats reads pgStatViews' numeric columns. A view this Postgres lacks reads as empty.
func pgStats(ctx context.Context, db *sql.DB) (map[string]map[string]float64, error) {
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}
	out := map[string]map[string]float64{}
	for name, q := range pgStatViews {
		var raw []byte
		if err := db.QueryRowContext(ctx, q).Scan(&raw); err != nil {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, err
		}
		nums := map[string]float64{}
		for k, v := range row {
			if f, ok := v.(float64); ok {
				nums[k] = f
			}
		}
		out[name] = nums
	}
	return out, nil
}

func pgDelta(before, after map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(after))
	for k, v := range after {
		out[k] = v - before[k]
	}
	return out
}
