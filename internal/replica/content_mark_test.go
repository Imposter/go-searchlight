package replica

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

var cmMapping = &schema.Mapping{Fields: map[string]schema.FieldType{"brand": schema.Keyword}}

func cmShard(t *testing.T) *shard.Shard {
	t.Helper()
	sh, err := shard.Open(context.Background(), t.TempDir(), cmMapping, shard.Options{
		RefreshInterval: -1, DisableMerges: true, Index: "cm", Shard: 0, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sh.Close(context.Background()) })
	return sh
}

func cmDoc(t *testing.T, id string) *schema.Doc {
	t.Helper()
	d, _, err := schema.Analyze(cmMapping, id, []byte(`{"brand":"b"}`))
	if err != nil {
		t.Fatal(err)
	}
	return &d
}

func cmMark(seq int64, kind store.Kind, id string) store.ChangeMark {
	return store.ChangeMark{Seq: seq, Index: "cm", Shard: 0, Kind: kind, ID: id, At: time.UnixMilli(1_700_000_000_000 + seq), Nonce: seq * 7}
}

// cmStore is a reqcache.Store in a map.
type cmStore struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (s *cmStore) Name() string         { return "test" }
func (s *cmStore) MaxEntryBytes() int64 { return 1 << 20 }
func (s *cmStore) Close() error         { return nil }
func (s *cmStore) Get(_ context.Context, k string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[k], nil
}

func (s *cmStore) Set(_ context.Context, k string, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] = v
	return nil
}

func cmSearch(t *testing.T, c *search.RequestCache, sh *shard.Shard) *search.ShardResult {
	t.Helper()
	r, ps := search.ParseRequest([]byte(`{"size":0,"track_total":true,"aggs":{"b":{"terms":{"field":"brand"}}}}`))
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	g := sh.Acquire()
	defer g.Release()
	res, err := c.Search(context.Background(), g, r)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func cmRefresh(t *testing.T, sh *shard.Shard) {
	t.Helper()
	if err := sh.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestOwnContentMarkTellsLoadedCopiesApart: 70 upserts X, 80 upserts Y, 90 deletes X.
// A tailing copy at 85 holds {X, Y}; a copy loaded from a snapshot at 100 holds {Y}.
// Both hold 80 as their newest change's seq (a load sees no delete), and yet their
// shared request cache keys differ: they name the shard's newest change, 80 and 90.
// The tailing copy's key then stays put while writes elsewhere move its seq, so a
// copy at a later seq reads what it computed.
func TestOwnContentMarkTellsLoadedCopiesApart(t *testing.T) {
	remap := shard.Change{Seq: 1, Kind: shard.Remap, Mapping: cmMapping, MappingVersion: 1, IndexUID: "uid-cm"}

	tailing := cmShard(t)
	var rc rewindCheck
	if err := tailing.Apply(context.Background(), []shard.Change{
		remap,
		{Seq: 70, Kind: shard.Upsert, Doc: cmDoc(t, "X"), IndexUID: "uid-cm"},
		{Seq: 80, Kind: shard.Upsert, Doc: cmDoc(t, "Y"), IndexUID: "uid-cm"},
	}); err != nil {
		t.Fatal(err)
	}
	noteApplied(tailing, &rc, cmMark(80, store.KindUpsert, "Y"))
	rc.promoteOwn(tailing, 80, &store.Tip{})
	if err := tailing.Advance(85); err != nil {
		t.Fatal(err)
	}
	noteAdvanced(tailing, &rc, 80, 85, cmMark(85, store.KindUpsert, "other-shard"))
	cmRefresh(t, tailing)

	loaded := cmShard(t)
	if err := loaded.Load(context.Background(), []shard.Change{
		{Kind: shard.Remap, Mapping: cmMapping, MappingVersion: 1, IndexUID: "uid-cm"},
		{Seq: 80, Kind: shard.Upsert, Doc: cmDoc(t, "Y"), IndexUID: "uid-cm"},
	}); err != nil {
		t.Fatal(err)
	}
	loaded.SetOwnMark(encodeOwn(ownCheck{}, ownCheck{last: cmMark(90, store.KindDelete, "X"), upTo: 100}))
	if err := loaded.Advance(100); err != nil {
		t.Fatal(err)
	}
	cmRefresh(t, loaded)

	gt, gl := tailing.Acquire(), loaded.Acquire()
	if gt.MaxSeq() != 80 || gl.MaxSeq() != 80 {
		t.Fatalf("MaxSeq %d and %d: the scenario wants both at 80", gt.MaxSeq(), gl.MaxSeq())
	}
	it, il := gt.CacheIdentity(), gl.CacheIdentity()
	gt.Release()
	gl.Release()
	ct := OwnContentMark(it.OwnMark, it.Seq)
	if !bytes.Equal(ct, encodeMark(cmMark(80, store.KindUpsert, "Y"))) {
		t.Fatalf("the tailing copy's content mark is %s", ct)
	}
	if cl := OwnContentMark(il.OwnMark, il.Seq); cl != nil {
		t.Fatalf("the loaded copy's unchecked own mark named its contents: %s", cl)
	}

	st := &cmStore{m: map[string][]byte{}}
	node := func() *search.RequestCache {
		return search.NewRequestCache(search.RequestCacheOptions{MaxBytes: 1 << 20, Store: st, ContentMark: OwnContentMark})
	}
	a := node()
	if got := cmSearch(t, a, tailing); got.Total != 2 {
		t.Fatalf("the tailing copy counts %d", got.Total)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	// A poll checks the loaded copy's own mark: its contents are named, apart.
	var lrc rewindCheck
	lrc.own, lrc.ownPending = decodeOwn(loaded.OwnMark())
	lrc.promoteOwn(loaded, 100, &store.Tip{})
	cmRefresh(t, loaded)
	gl = loaded.Acquire()
	il = gl.CacheIdentity()
	gl.Release()
	if cl := OwnContentMark(il.OwnMark, il.Seq); cl == nil || bytes.Equal(cl, ct) {
		t.Fatalf("the loaded copy's content mark is %s, the tailing one's %s", cl, ct)
	}
	b := node()
	if got := cmSearch(t, b, loaded); got.Total != 1 {
		t.Fatalf("the loaded copy counts %d: it read the tailing copy's result", got.Total)
	}
	if s := b.Stats(); s.StoreHits != 0 || s.StoreMisses != 1 {
		t.Fatalf("loaded copy stats %v", s)
	}

	// Writes to other shards and indexes move the tailing copy's seq, not its key.
	if err := tailing.Advance(500); err != nil {
		t.Fatal(err)
	}
	noteAdvanced(tailing, &rc, 85, 500, cmMark(500, store.KindUpsert, "elsewhere"))
	cmRefresh(t, tailing)
	c := node()
	if got := cmSearch(t, c, tailing); got.Total != 2 {
		t.Fatalf("the tailing copy at 500 counts %d", got.Total)
	}
	if s := c.Stats(); s.StoreHits != 1 {
		t.Fatalf("stats %v: the copy at seq 500 did not read the result computed at 85", s)
	}
}

func TestOwnContentMarkNeedsAKnownCheckedState(t *testing.T) {
	m70, m80 := cmMark(70, store.KindUpsert, "a"), cmMark(80, store.KindUpsert, "b")
	for _, tc := range []struct {
		name          string
		checked, pend ownCheck
		seq           int64
		want          *store.ChangeMark
	}{
		{"checked up to seq", ownCheck{m70, 90}, ownCheck{}, 90, &m70},
		{"pending extends checked", ownCheck{m70, 75}, ownCheck{m70, 90}, 90, &m70},
		{"newer change unchecked", ownCheck{m70, 75}, ownCheck{m80, 90}, 90, nil},
		{"pending of another seq", ownCheck{m70, 75}, ownCheck{m70, 95}, 90, nil},
		{"checked of another seq", ownCheck{m70, 75}, ownCheck{}, 90, nil},
		{"only pending", ownCheck{}, ownCheck{m70, 90}, 90, nil},
		{"no change of the shard", ownCheck{store.ChangeMark{}, 90}, ownCheck{}, 90, nil},
	} {
		got := OwnContentMark(encodeOwn(tc.checked, tc.pend), tc.seq)
		switch {
		case tc.want == nil && got != nil:
			t.Errorf("%s: %s, want nil", tc.name, got)
		case tc.want != nil && !bytes.Equal(got, encodeMark(*tc.want)):
			t.Errorf("%s: %s", tc.name, got)
		}
	}
	if OwnContentMark(nil, 5) != nil {
		t.Error("no own mark named contents")
	}
}
