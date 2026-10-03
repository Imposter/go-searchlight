package shard

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
)

func parseQuery(t testing.TB, raw string) query.Node {
	t.Helper()
	n, problems := query.Parse([]byte(raw))
	if len(problems) > 0 {
		t.Fatalf("Parse(%s): %v", raw, problems)
	}
	return n
}

// TestLoad: a snapshot's records load in any seq order, keep their own seqs (a saved
// query's version), leave AppliedSeq alone until the Advance, and the tail then applies
// on top.
func TestLoad(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, testOptions())
	s := h.s
	// By id, as ScanShard lists them: seqs out of order.
	load := []Change{
		{Seq: 40, Kind: Upsert, Doc: analyze(t, "a", body("a", 40)), IndexUID: "u1"},
		{Seq: 7, Kind: Upsert, Doc: analyze(t, "b", body("b", 7)), IndexUID: "u1"},
		{Seq: 23, Kind: QueryUpsert, QueryID: "q", Query: parseQuery(t, `{"field":"brand","op":"eq","value":"acme"}`), Meta: []byte(`{"k":1}`), IndexUID: "u1"},
	}
	if err := s.Load(ctx, load[:2]); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := s.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Load(ctx, load[2:]); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if s.AppliedSeq() != 0 || s.CommittedSeq() != 0 {
		t.Fatalf("applied %d, committed %d during a load; want 0", s.AppliedSeq(), s.CommittedSeq())
	}
	if err := s.Advance(50); err != nil {
		t.Fatal(err)
	}
	if err := s.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if s.CommittedSeq() != 50 {
		t.Fatalf("CommittedSeq %d after the load's Advance, want 50", s.CommittedSeq())
	}
	if err := s.Load(ctx, load[:1]); !errors.Is(err, ErrSeqOrder) {
		t.Fatalf("Load after the Advance: %v", err)
	}
	if err := s.Apply(ctx, []Change{{Seq: 50, Kind: Delete, DocID: "a"}}); !errors.Is(err, ErrSeqOrder) {
		t.Fatalf("Apply at the snapshot's seq: %v", err)
	}
	if err := s.Apply(ctx, []Change{{Seq: 51, Kind: Delete, DocID: "a", IndexUID: "u1"}}); err != nil {
		t.Fatal(err)
	}
	h.model = map[string]string{"b": body("b", 7)}
	h.refresh()
	h.check()
	g := s.Acquire()
	defer g.Release()
	if g.MaxSeq() != 51 || g.IndexUID() != "u1" {
		t.Fatalf("MaxSeq %d, IndexUID %q", g.MaxSeq(), g.IndexUID())
	}
	seg, ord, ok := g.LookupQuery("q")
	if !ok {
		t.Fatal("loaded query missing")
	}
	q, err := g.QuerySegments[seg].Segment.Query(ord)
	if err != nil || q.Seq != 23 || string(q.Meta) != `{"k":1}` {
		t.Fatalf("query %+v, %v; want seq 23", q, err)
	}
}

func TestLoadRefusals(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, testOptions())
	if err := h.s.Load(ctx, []Change{{Seq: 0, Kind: Upsert, Doc: analyze(t, "a", `{}`)}}); !errors.Is(err, ErrSeqOrder) {
		t.Fatalf("seq 0: %v", err)
	}
	if err := h.s.Load(ctx, []Change{{Seq: 3, Kind: Upsert, Doc: analyze(t, "a", `{}`), IndexUID: "u1"}}); err != nil {
		t.Fatal(err)
	}
	if err := h.s.Load(ctx, []Change{{Seq: 2, Kind: Upsert, Doc: analyze(t, "b", `{}`), IndexUID: "u2"}}); !errors.Is(err, ErrIndexUID) {
		t.Fatalf("another incarnation: %v", err)
	}
	h.upsert("c") // seq 1: applied moves
	if err := h.s.Load(ctx, []Change{{Seq: 9, Kind: Delete, DocID: "z"}}); !errors.Is(err, ErrSeqOrder) {
		t.Fatalf("Load after Apply: %v", err)
	}
}

// TestAbandonLosesOnlyTheUncommitted: Abandon is a crash: the reopened shard has what
// the last commit covered, and a Close after Abandon returns at once.
func TestAbandonLosesOnlyTheUncommitted(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a", "b")
	h.refresh()
	committed := maps.Clone(h.model)
	h.upsert("c")
	h.s.Abandon()
	if h.s.Acquire() != nil {
		t.Fatal("Acquire after Abandon")
	}
	if err := h.s.Close(context.Background()); err != nil {
		t.Fatalf("Close after Abandon: %v", err)
	}
	opts := h.s.Options()
	if opts.RefreshInterval != -1 || opts.Logger == nil || opts.MergePolicy == nil {
		t.Fatalf("Options() not the resolved options: %+v", opts)
	}
	h.open()
	if h.s.CommittedSeq() != 2 || h.s.AppliedSeq() != 2 {
		t.Fatalf("reopened at committed %d, applied %d; want 2", h.s.CommittedSeq(), h.s.AppliedSeq())
	}
	h.model = committed
	h.check()
}
