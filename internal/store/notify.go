package store

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// notifyTimeout bounds one announcement.
const notifyTimeout = 5 * time.Second

// notifyWarnEvery spaces the warnings of lost notifications.
const notifyWarnEvery = time.Minute

// notifier announces each shard's newest seq once the transactions that took it have
// committed, on the dialect's notification channel (Changelog.Notify). It runs apart
// from the commits: a notification sent inside a transaction would hold Postgres's
// instance-wide notify lock from that transaction's commit record to its end, so
// commits to different shards would commit one at a time. Announcements coalesce, the
// newest seq of a shard replacing an older one not yet sent, and go out in one
// statement that does not wait for its own flush; a statement that fails is sent once
// more. A notification is only a wake-up: one lost to a crash or to a statement that
// fails twice costs a tailer at most one poll, and while notifications flow a tailer
// polls only every half max_lag (1 s by default). Lost ones are counted and warned
// about.
type notifier struct {
	s       *sqlStore
	mu      sync.Mutex
	pending map[ShardID]int64
	wake    chan struct{}
	stop    context.CancelFunc
	done    chan struct{}
	warned  time.Time // when a loss was last warned about; only run touches it
}

// startNotifier starts s's notifier; close stops it.
func startNotifier(s *sqlStore) *notifier {
	ctx, cancel := context.WithCancel(context.Background())
	n := &notifier{s: s, pending: map[ShardID]int64{}, wake: make(chan struct{}, 1), stop: cancel, done: make(chan struct{})}
	go n.run(ctx)
	return n
}

// add announces that shard's changelog reached seq.
func (n *notifier) add(shard ShardID, seq int64) {
	n.mu.Lock()
	if seq > n.pending[shard] {
		n.pending[shard] = seq
	}
	n.mu.Unlock()
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

// close sends what is pending, then stops.
func (n *notifier) close() {
	n.stop()
	<-n.done
}

func (n *notifier) run(ctx context.Context) {
	defer close(n.done)
	for {
		select {
		case <-n.wake:
			n.send(ctx)
		case <-ctx.Done():
			n.send(context.WithoutCancel(ctx))
			return
		}
	}
}

// send announces everything pending in one statement.
func (n *notifier) send(ctx context.Context) {
	n.mu.Lock()
	pending := n.pending
	if len(pending) == 0 {
		n.mu.Unlock()
		return
	}
	n.pending = make(map[ShardID]int64, len(pending))
	n.mu.Unlock()
	ids := make([]ShardID, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, compareShards)
	payloads := make([]string, 0, len(ids))
	for _, id := range ids {
		b, err := json.Marshal(notification{Index: id.Index, Shard: id.Shard, Seq: pending[id]})
		if err != nil {
			continue
		}
		payloads = append(payloads, string(b))
	}
	var err error
	for range 2 {
		sctx, cancel := context.WithTimeout(ctx, notifyTimeout)
		_, err = n.s.w.ExecContext(sctx, n.s.d.Changelog.Notify, payloads)
		cancel()
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			n.requeue(pending) // close cut it off: the last send, which close waits for, sends it
			return
		}
	}
	n.s.lost.Add(ctx, int64(len(payloads)), metric.WithAttributes(n.s.attr))
	if now := n.s.clock.Now(); now.Sub(n.warned) >= notifyWarnEvery {
		n.warned = now
		n.s.log.WarnContext(ctx, "commit notifications could not be sent; tailers poll for those changes instead",
			slog.Int("shards", len(payloads)), slog.Any("error", err))
	}
}

// requeue puts announcements a send did not make back with those pending since.
func (n *notifier) requeue(pending map[ShardID]int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id, seq := range pending {
		if seq > n.pending[id] {
			n.pending[id] = seq
		}
	}
}

// notification is a payload of the commit notification channel.
type notification struct {
	Index string `json:"index"`
	Shard int    `json:"shard"`
	Seq   int64  `json:"seq"`
}
