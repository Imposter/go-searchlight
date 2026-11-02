package store

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"sync"
	"time"
)

// notifyTimeout bounds one announcement.
const notifyTimeout = 5 * time.Second

// notifier announces each shard's newest seq once the transactions that took it have
// committed, on the dialect's notification channel (Changelog.Notify). It runs apart
// from the commits: a notification sent inside a transaction would hold Postgres's
// instance-wide notify lock from that transaction's commit record to its end, so
// commits to different shards would commit one at a time. Announcements coalesce, the
// newest seq of a shard replacing an older one not yet sent, and go out in one
// statement that does not wait for its own flush. A notification is only a wake-up: one
// lost to a crash or a failed statement costs a tailer at most one poll.
type notifier struct {
	s       *sqlStore
	mu      sync.Mutex
	pending map[ShardID]int64
	wake    chan struct{}
	stop    context.CancelFunc
	done    chan struct{}
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
	ctx, cancel := context.WithTimeout(ctx, notifyTimeout)
	defer cancel()
	if _, err := n.s.w.ExecContext(ctx, n.s.d.Changelog.Notify, payloads); err != nil {
		n.s.log.DebugContext(ctx, "a commit notification was lost; tailers poll for it", slog.Int("shards", len(payloads)), slog.Any("error", err))
	}
}

// notification is a payload of the commit notification channel.
type notification struct {
	Index string `json:"index"`
	Shard int    `json:"shard"`
	Seq   int64  `json:"seq"`
}
