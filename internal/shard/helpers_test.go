package shard

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
)

var testMapping = &schema.Mapping{Fields: map[string]schema.FieldType{
	"title": schema.Text,
	"brand": schema.Keyword,
	"tags":  schema.KeywordList,
	"price": schema.Number,
	"v":     schema.Number,
}}

var quietLogger = slog.New(slog.DiscardHandler)

func analyze(t testing.TB, id, body string) *schema.Doc {
	t.Helper()
	d, _, err := schema.Analyze(testMapping, id, []byte(body))
	if err != nil {
		t.Fatalf("Analyze(%q): %v", id, err)
	}
	return &d
}

// testOptions are quiet, manual options: no background refresh or merges, and a
// filter cache of the test's own (so its Stats are the test's alone).
func testOptions() Options {
	return Options{RefreshInterval: -1, DisableMerges: true, Logger: quietLogger, FilterCache: NewFilterCache(1<<20, nil)}
}

// harness drives one shard against a model: every applied change is mirrored in
// model, and snapshots keeps the model as of every applied seq.
type harness struct {
	t         testing.TB
	dir       string
	opts      Options
	s         *Shard
	seq       int64
	model     map[string]string
	snapshots map[int64]map[string]string
	log       []Change // every change applied, for replays

	qmodel         map[string]string // saved query id to its JSON
	querySnapshots map[int64]map[string]string
}

func (h *harness) queries() map[string]string { return h.qmodel }

func newHarness(t testing.TB, opts Options) *harness {
	t.Helper()
	h := &harness{
		t: t, dir: t.TempDir(), opts: opts,
		model: map[string]string{}, snapshots: map[int64]map[string]string{0: {}},
		qmodel: map[string]string{}, querySnapshots: map[int64]map[string]string{0: {}},
	}
	h.open()
	t.Cleanup(func() {
		if h.s != nil {
			h.s.shutdown()
		}
	})
	return h
}

func (h *harness) open() {
	h.t.Helper()
	s, err := Open(context.Background(), h.dir, testMapping, h.opts)
	if err != nil {
		h.t.Fatalf("Open: %v", err)
	}
	h.s = s
}

func body(id string, seq int64) string {
	return fmt.Sprintf(`{"title":"item %s","brand":"Acme","price":%d,"v":%d,"tags":["t%d"]}`, id, seq%97, seq, seq%5)
}

// upsert applies one upsert of id at the next seq and returns its body.
func (h *harness) upsert(ids ...string) {
	h.t.Helper()
	changes := make([]Change, 0, len(ids))
	for _, id := range ids {
		h.seq++
		b := body(id, h.seq)
		changes = append(changes, Change{Seq: h.seq, Kind: Upsert, Doc: analyze(h.t, id, b)})
		h.model[id] = b
	}
	h.apply(changes)
}

func (h *harness) del(ids ...string) {
	h.t.Helper()
	changes := make([]Change, 0, len(ids))
	for _, id := range ids {
		h.seq++
		changes = append(changes, Change{Seq: h.seq, Kind: Delete, DocID: id})
		delete(h.model, id)
	}
	h.apply(changes)
}

func (h *harness) apply(changes []Change) {
	h.t.Helper()
	if err := h.s.Apply(context.Background(), changes); err != nil {
		h.t.Fatalf("Apply: %v", err)
	}
	h.log = append(h.log, changes...)
	h.snapshots[h.seq] = maps.Clone(h.model)
	h.querySnapshots[h.seq] = maps.Clone(h.qmodel)
}

func (h *harness) refresh() {
	h.t.Helper()
	if err := h.s.Refresh(context.Background()); err != nil {
		h.t.Fatalf("Refresh: %v", err)
	}
}

func (h *harness) forceMerge(n int) {
	h.t.Helper()
	if err := h.s.ForceMerge(context.Background(), n); err != nil {
		h.t.Fatalf("ForceMerge: %v", err)
	}
}

// check verifies the shard's current generation holds exactly the model.
func (h *harness) check() {
	h.t.Helper()
	g := h.s.Acquire()
	defer g.Release()
	if err := checkGeneration(g, h.model); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) reopen() {
	h.t.Helper()
	if err := h.s.Close(context.Background()); err != nil {
		h.t.Fatalf("Close: %v", err)
	}
	h.open()
}

// abandon drops the shard as a crash would: no final refresh or commit.
func (h *harness) abandon() {
	h.s.shutdown()
	h.s = nil
}

// checkGeneration verifies g holds exactly want (id to body): every live document
// once, with its newest body, nothing deleted, and the index agreeing with the stored
// documents.
func checkGeneration(g *Generation, want map[string]string) error {
	seen := map[string]bool{}
	var acme uint64
	for _, sv := range g.Segments {
		for ord := range sv.NumDocs {
			if sv.Deletes.Contains(ord) {
				continue
			}
			id, err := sv.Reader.ID(ord)
			if err != nil {
				return err
			}
			if seen[id] {
				return fmt.Errorf("gen %d (seq %d): %q is live twice", g.Gen(), g.Seq(), id)
			}
			seen[id] = true
			wantBody, ok := want[id]
			if !ok {
				return fmt.Errorf("gen %d (seq %d): %q is live but deleted", g.Gen(), g.Seq(), id)
			}
			got, err := sv.Reader.Stored(ord)
			if err != nil {
				return err
			}
			if string(got) != wantBody {
				return fmt.Errorf("gen %d (seq %d): %q is %s, want %s", g.Gen(), g.Seq(), id, got, wantBody)
			}
		}
		live := sv.Reader.Postings("brand", segment.KindValue, "acme").Clone()
		live.AndNot(sv.Deletes)
		acme += live.GetCardinality()
	}
	if len(seen) != len(want) {
		return fmt.Errorf("gen %d (seq %d): %d live documents, want %d", g.Gen(), g.Seq(), len(seen), len(want))
	}
	if g.NumDocs() != uint64(len(want)) || acme != uint64(len(want)) {
		return fmt.Errorf("gen %d (seq %d): NumDocs %d, brand:acme %d, want %d", g.Gen(), g.Seq(), g.NumDocs(), acme, len(want))
	}
	for id, wantBody := range want {
		got, ok, err := g.Get(id)
		if err != nil || !ok || string(got) != wantBody {
			return fmt.Errorf("gen %d (seq %d): Get(%q) = %s, %v, %w; want %s", g.Gen(), g.Seq(), id, got, ok, err, wantBody)
		}
	}
	return nil
}

// dirFiles lists dir's files, sorted.
func dirFiles(t testing.TB, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// referencedFiles lists every file dir's manifest references, sorted.
func referencedFiles(t testing.TB, dir string) []string {
	t.Helper()
	man, err := readManifest(dir, quietLogger)
	if err != nil {
		t.Fatal(err)
	}
	out := []string{manifestName}
	for _, ms := range man.Segments {
		out = append(out, ms.ID+segment.FileExt)
		if ms.DelGen > 0 {
			out = append(out, deletesName(ms.ID, ms.DelGen))
		}
	}
	for _, ms := range man.QuerySegments {
		out = append(out, ms.ID+defaultQueryExt)
		if ms.DelGen > 0 {
			out = append(out, deletesName(ms.ID, ms.DelGen))
		}
	}
	if _, err := os.Stat(filepath.Join(dir, manifestName)); os.IsNotExist(err) {
		out = slices.DeleteFunc(out, func(s string) bool { return s == manifestName })
	}
	sort.Strings(out)
	return out
}
