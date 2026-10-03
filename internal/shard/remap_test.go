package shard

import (
	"context"
	"errors"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
)

var remapped = &schema.Mapping{Fields: map[string]schema.FieldType{
	"title": schema.Text, "brand": schema.Keyword, "tags": schema.KeywordList, "price": schema.Number, "v": schema.Number,
	"color": schema.Keyword,
}}

// TestRemap: a Remap is a changelog entry: the mapping it carries holds from its seq,
// generations publish it with the seq they cover, and the manifest commits it with
// that seq, so a reopened copy has the mapping as of its CommittedSeq.
func TestRemap(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, testOptions())
	h.upsert("a", "b") // seq 1, 2
	h.refresh()
	if h.s.MappingVersion() != 0 {
		t.Fatalf("version %d before any remap", h.s.MappingVersion())
	}
	h.seq++
	h.apply([]Change{{Seq: h.seq, Kind: Remap, Mapping: remapped, MappingVersion: 2}})
	if h.s.Mapping() != remapped || h.s.MappingVersion() != 2 {
		t.Fatal("the applied mapping is not the remap's")
	}
	// Not yet refreshed: the published generation keeps the mapping of its seq, and a
	// merge committed now records that one, not the remap's.
	g := h.s.Acquire()
	if g.Mapping() != testMapping || g.MappingVersion() != 0 {
		t.Fatalf("generation at seq %d has the remap's mapping", g.Seq())
	}
	g.Release()
	h.upsert("c")
	h.refresh()
	h.upsert("d")
	h.forceMerge(1)
	g = h.s.Acquire()
	if g.Mapping() != remapped || g.MappingVersion() != 2 {
		t.Fatal("the refreshed generation lacks the remap")
	}
	g.Release()
	// A remap must move the version on.
	h.seq++
	err := h.s.Apply(ctx, []Change{{Seq: h.seq, Kind: Remap, Mapping: testMapping, MappingVersion: 2}})
	if !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("a remap to the same version: %v", err)
	}
	h.seq--
	h.reopen()
	if h.s.MappingVersion() != 2 || h.s.Mapping().Fields["color"] != schema.Keyword {
		t.Fatalf("reopened at mapping version %d", h.s.MappingVersion())
	}
	h.check()
}

// TestRemapCommittedWithItsSeq: a merge that commits after a remap is applied but
// before a refresh covers it records the mapping as of its own seq, so a crash then
// reopens with the old mapping and replays the remap.
func TestRemapCommittedWithItsSeq(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a")
	h.refresh()
	h.upsert("z")
	h.refresh() // two segments, seq 2
	h.seq++
	h.apply([]Change{{Seq: h.seq, Kind: Remap, Mapping: remapped, MappingVersion: 5}})
	h.upsert("b")
	h.forceMerge(1) // commits the merge at seq 2
	h.abandon()
	h.open()
	if _, ok := h.s.Mapping().Fields["color"]; ok || h.s.CommittedSeq() != 2 || h.s.MappingVersion() != 0 {
		t.Fatalf("reopened at seq %d with mapping version %d", h.s.CommittedSeq(), h.s.MappingVersion())
	}
}

// TestLoadRemap: a snapshot's mapping loads with seq 0, and a remap-only refresh
// commits it.
func TestLoadRemap(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, testOptions())
	if err := h.s.Load(ctx, []Change{{Kind: Remap, Mapping: remapped, MappingVersion: 7}}); err != nil {
		t.Fatal(err)
	}
	if err := h.s.Advance(9); err != nil {
		t.Fatal(err)
	}
	h.seq = 9
	h.reopen()
	if h.s.MappingVersion() != 7 || h.s.CommittedSeq() != 9 {
		t.Fatalf("reopened at %d with mapping version %d", h.s.CommittedSeq(), h.s.MappingVersion())
	}
	if err := h.s.Apply(ctx, []Change{{Seq: 10, Kind: Remap}}); !errors.Is(err, ErrInvalidChange) {
		t.Fatalf("a remap without a mapping: %v", err)
	}
}
