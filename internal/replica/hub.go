package replica

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// HubOptions configures a [Hub].
type HubOptions struct {
	// Logger and Meter are the hub's telemetry; nil means slog.Default() and no
	// metrics.
	Logger *slog.Logger
	Meter  metric.Meter
	// RetryBase and RetryCap bound the backoff between watch restarts. 0 means
	// DefaultRetryBase and DefaultRetryCap.
	RetryBase, RetryCap time.Duration
	// Clock times the restarts' backoff. Nil means clock.Real.
	Clock clock.Clock
}

// Hub turns a store's commit notifications (Postgres LISTEN/NOTIFY) into wake-ups of
// the tailers of the shards they name, over one connection for a whole node: a node
// runs one Hub and passes it to every tailer (Options.Hub), which otherwise only
// poll. The connection is the store's pooled one, held for the Hub's life. When the
// subscription fails it is restarted with backoff; every time it becomes active, every
// tailer is woken to poll, so a notification lost while it was down never stalls a
// copy. While it is down the tailers poll at their normal interval.
type Hub struct {
	w          store.Watcher
	log        *slog.Logger
	reconnects metric.Int64Counter
	retryBase  time.Duration
	retryCap   time.Duration
	clock      clock.Clock
	warn       rateLimitedWarn

	watching atomic.Bool
	mu       sync.Mutex
	subs     map[ShardID]map[*Tailer]struct{}
}

// NewHub returns a hub over st's notifications, or nil when st has none (MySQL,
// SQLite): tailers then rely on polling and local wake-ups.
func NewHub(st store.Store, opts HubOptions) *Hub {
	w, ok := st.(store.Watcher)
	if !ok {
		return nil
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Meter == nil {
		opts.Meter = metricnoop.NewMeterProvider().Meter(telemetry.ScopeName)
	}
	if opts.RetryBase <= 0 {
		opts.RetryBase = DefaultRetryBase
	}
	if opts.RetryCap <= 0 {
		opts.RetryCap = DefaultRetryCap
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real{}
	}
	in := telemetry.NewInstruments(opts.Meter)
	h := &Hub{
		w:          w,
		log:        opts.Logger.With(slog.String("component", "replica_hub")),
		reconnects: in.Counter(telemetry.MetricReplicaWatchReconnects),
		retryBase:  opts.RetryBase,
		retryCap:   max(opts.RetryCap, opts.RetryBase),
		clock:      opts.Clock,
		warn:       rateLimitedWarn{clock: opts.Clock, every: 30 * time.Second},
		subs:       map[ShardID]map[*Tailer]struct{}{},
	}
	if err := in.Err(); err != nil {
		h.log.Error("replica hub metrics unavailable", slog.Any("error", err))
	}
	return h
}

// Watching reports whether the subscription is active, so notifications flow.
func (h *Hub) Watching() bool { return h != nil && h.watching.Load() }

// Run keeps the subscription up until ctx ends (it returns nil then) or the store is
// closed (store.ErrClosed).
func (h *Hub) Run(ctx context.Context) error {
	var retry time.Duration
	for {
		started := h.clock.Now()
		err := h.w.Watch(ctx, h.ready, h.notify)
		h.watching.Store(false)
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if errors.Is(err, store.ErrClosed) {
			return err
		}
		h.reconnects.Add(ctx, 1)
		if suppressed, ok := h.warn.allow(); ok {
			h.log.WarnContext(ctx, "change notifications stopped; restarting them", slog.Any("error", err), slog.Int("suppressed", suppressed))
		}
		// Back to polling meanwhile: wake every tailer to take up its poll interval.
		h.wakeAll()
		if h.clock.Since(started) > time.Minute {
			retry = 0 // it was healthy for a while: this is a fresh failure
		}
		if retry == 0 {
			retry = h.retryBase
		} else {
			retry = min(2*retry, h.retryCap)
		}
		_ = h.clock.Sleep(ctx, retry)
	}
}

// ready runs once the subscription is active: every tailer polls now, which covers
// whatever committed while there was none.
func (h *Hub) ready() {
	h.watching.Store(true)
	h.wakeAll()
}

func (h *Hub) notify(n store.Notification) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for t := range h.subs[n.Shard] {
		if n.Seq > t.Applied() {
			t.Wake()
		}
	}
}

func (h *Hub) wakeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ts := range h.subs {
		for t := range ts {
			t.Wake()
		}
	}
}

// subscribe routes t's shard's notifications to it until the returned function is
// called.
func (h *Hub) subscribe(t *Tailer) func() {
	h.mu.Lock()
	defer h.mu.Unlock()
	ts := h.subs[t.id]
	if ts == nil {
		ts = map[*Tailer]struct{}{}
		h.subs[t.id] = ts
	}
	ts[t] = struct{}{}
	return func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.subs[t.id], t)
		if len(h.subs[t.id]) == 0 {
			delete(h.subs, t.id)
		}
	}
}
