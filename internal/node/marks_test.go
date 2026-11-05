package node

import (
	"context"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/store"
)

// headStore answers a shard's head with head, and runs during after reading it and
// before answering, as a commit landing while the head is read would.
type headStore struct {
	store.Store
	head   int64
	during func()
}

func (s headStore) HeadSeq(context.Context, store.ShardID) (int64, time.Time, error) {
	if s.during != nil {
		s.during()
	}
	return s.head, time.Time{}, nil
}

// TestLoweringToTheHeadKeepsACommitThatLanded: clampQuerySeqs and a rewind lower a
// shard's marks to the head they read from the store, but never under a saved query
// committed through this node while they read it, whether its seq raised the mark
// (the mark was below it) or not (the mark was past it, as after a rewind); without
// a commit meanwhile, a mark past the head falls to it.
func TestLoweringToTheHeadKeepsACommitThatLanded(t *testing.T) {
	const head = 5
	for _, tc := range []struct {
		name   string
		was    int64
		commit int64
		want   int64
	}{
		{name: "a commit raises the mark", was: 3, commit: head + 1, want: head + 1},
		{name: "a commit under a mark past the head", was: 9, commit: head + 1, want: 9},
		{name: "no commit", was: 9, want: head},
	} {
		for _, path := range []string{"clamp", "rewind"} {
			t.Run(tc.name+"/"+path, func(t *testing.T) {
				sl := &shardSlot{id: store.ShardID{Index: "m", Shard: 0}}
				sl.querySeq.Store(tc.was)
				sl.written.Store(tc.was)
				sl.head.Store(tc.was)
				idx := &index{name: "m", shards: []*shardSlot{sl}}
				st := headStore{head: head}
				if tc.commit > 0 {
					st.during = func() {
						c := store.Change{Index: "m", Shard: 0, Kind: store.KindQueryUpsert, ID: "q", Seq: tc.commit}
						noteApplied(idx, []prepared{{change: c}}, make([]api.ItemResult, 1))
					}
				}
				n := &Single{st: st}
				if path == "clamp" {
					n.clampQuerySeqs(t.Context(), idx.shards)
				} else {
					n.rewound(t.Context(), sl, head)
				}
				if got := sl.querySeq.Load(); got != tc.want {
					t.Errorf("querySeq = %d, want %d", got, tc.want)
				}
				if got := sl.written.Load(); got != tc.want {
					t.Errorf("written = %d, want %d", got, tc.want)
				}
			})
		}
	}
}
