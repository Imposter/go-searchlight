package percolate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// fieldTable names fields for programs outside a segment.
type fieldTable struct {
	at    map[string]uint32
	names []string
}

func (ft *fieldTable) field(name string) uint32 {
	if i, ok := ft.at[name]; ok {
		return i
	}
	ft.at[name] = uint32(len(ft.names))
	ft.names = append(ft.names, name)
	return ft.at[name]
}

// evalOn evaluates program p on d, its fields laid out by ft.
func evalOn(ft *fieldTable, sc *scratch, p []byte, d *schema.Doc) bool {
	sc.fit(0, 0, len(ft.names))
	for i, name := range ft.names {
		sc.vals[i] = d.Fields[name]
	}
	got := sc.eval(p)
	sc.reset()
	return got
}

// Every program decides as the matcher does: random queries of every op and shape,
// over random documents (missing, wrong-type and long fields), whether or not a
// document would be a candidate.
func TestProgramsMatchTheMatcher(t *testing.T) {
	seeds, queries, docs := []uint64{1, 2, 3}, 600, 300
	if longMode() {
		seeds, queries, docs = []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 3000, 1000
	}
	m := testMapping()
	pairs := 0
	for _, seed := range seeds {
		for _, positive := range []bool{false, true} {
			g := newGen(seed)
			g.positive = positive
			ft := &fieldTable{at: map[string]uint32{}}
			pc := progCompiler{field: ft.field}
			type compiled struct {
				raw  []byte
				prog []byte
				m    *query.Compiled
			}
			var qs []compiled
			for len(qs) < queries {
				raw := g.queryJSON()
				n, problems := query.Parse(raw)
				if len(problems) > 0 {
					continue
				}
				prog := pc.compile(n)
				if err := checkProg(prog, uint32(len(ft.names))); err != nil {
					t.Fatalf("%s: %v", raw, err)
				}
				qs = append(qs, compiled{raw, prog, query.Compile(n)})
			}
			sc := new(scratch)
			for di := range docs {
				id, body := g.docJSON(di)
				d, _, err := schema.Analyze(m, id, body)
				if err != nil {
					t.Fatal(err)
				}
				for _, q := range qs {
					pairs++
					if got, want := evalOn(ft, sc, q.prog, &d), q.m.Match(&d); got != want {
						t.Fatalf("seed %d: query %s on %s: program %v, matcher %v", seed, q.raw, body, got, want)
					}
				}
			}
		}
	}
	t.Logf("%d query/document pairs", pairs)
}

// The percolator gives scrape-bot's verdicts on the parity fixtures
// (testdata/parity/match.json): every fixture query in one segment, every fixture
// document percolated against it.
func TestParityPercolate(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "parity", "match.json"))
	if err != nil {
		t.Fatalf("read fixture: %v (regenerate with make parity)", err)
	}
	var f struct {
		Data struct {
			Mapping schema.Mapping `json:"mapping"`
			Groups  []struct {
				Docs []struct {
					ID   string `json:"id"`
					Body string `json:"body"`
				} `json:"docs"`
				Queries []struct {
					Query   json.RawMessage `json:"query"`
					Matches []string        `json:"matches"`
				} `json:"queries"`
			} `json:"groups"`
		} `json:"data"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&f); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for gi, group := range f.Data.Groups {
		qs := make([]shard.StoredQuery, len(group.Queries))
		for i, fq := range group.Queries {
			n, problems := query.Parse(fq.Query)
			if len(problems) > 0 {
				t.Fatalf("group %d query %d: %v", gi, i, problems)
			}
			qs[i] = shard.StoredQuery{ID: fmt.Sprintf("q%04d", i), Query: n}
		}
		data, err := encodeSegment(context.Background(), qs, nil)
		if err != nil {
			t.Fatal(err)
		}
		seg, err := openData("parity", data)
		if err != nil {
			t.Fatal(err)
		}
		got := make([][]string, len(qs))
		sc := new(scratch)
		sc.fit(seg.NumQueries(), seg.NumEntries(), len(seg.fields))
		v := &view{seg: seg, n: seg.NumQueries()}
		for _, fd := range group.Docs {
			d, _, err := schema.Analyze(&f.Data.Mapping, fd.ID, []byte(fd.Body))
			if err != nil {
				t.Fatal(err)
			}
			seg.collect(&d, sc)
			var st docStats
			verify(v, sc, &st)
			for _, id := range sc.results() {
				var qi int
				if _, err := fmt.Sscanf(id, "q%04d", &qi); err != nil {
					t.Fatal(err)
				}
				got[qi] = append(got[qi], fd.ID)
			}
			sc.reset()
		}
		for i, fq := range group.Queries {
			if !slices.Equal(got[i], fq.Matches) {
				t.Errorf("group %d query %d %s:\n got  %q\n want %q", gi, i, fq.Query, got[i], fq.Matches)
			}
			checked++
		}
	}
	if checked < 300 {
		t.Fatalf("only %d fixture queries", checked)
	}
}

// Percolating slbench's own documents against its saved searches, spread over three
// query segments with some deleted, gives exactly the brute-force answer: the shapes
// T5 runs, through posting filters, proven leaves, phrase pairs and merged runs.
func TestProductsCrossCheck(t *testing.T) {
	numQueries, numDocs := 20_000, 600
	if longMode() {
		numQueries, numDocs = 100_000, 4000
	}
	if testing.Short() {
		numQueries, numDocs = 5000, 200
	}
	qs := make([]shard.StoredQuery, numQueries)
	compiled := make([]*query.Compiled, numQueries)
	for i := range qs {
		qs[i] = productSearch(t, int64(i))
		compiled[i] = query.Compile(qs[i].Query)
	}
	var views []view
	deleted := map[string]bool{}
	for part := range 3 {
		lo, hi := part*numQueries/3, (part+1)*numQueries/3
		data, err := encodeSegment(context.Background(), qs[lo:hi], nil)
		if err != nil {
			t.Fatal(err)
		}
		seg, err := openData(fmt.Sprintf("p%d", part), data)
		if err != nil {
			t.Fatal(err)
		}
		v := view{seg: seg, n: seg.NumQueries()}
		if part == 1 {
			v.deletes = deletesEvery(seg, 7, deleted)
		}
		views = append(views, v)
	}
	p := New(Options{Threads: 1})
	sc := new(scratch)
	for _, v := range views {
		sc.fit(v.n, v.seg.NumEntries(), len(v.seg.fields))
	}
	matches, candidates := 0, 0
	for _, d := range productDocs(t, productsMapping(t), 300_000, numDocs) {
		got, st, err := p.one(context.Background(), nil, views, &d, sc)
		if err != nil {
			t.Fatal(err)
		}
		var want []string
		for i := range qs {
			if !deleted[qs[i].ID] && compiled[i].Match(&d) {
				want = append(want, qs[i].ID)
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("document %s: percolate %d matches, brute force %d\nmissing %v\nextra %v",
				d.ID, len(got), len(want), minus(want, got), minus(got, want))
		}
		matches += len(want)
		candidates += st.candidates
	}
	t.Logf("%d pairs: %d matches, %d candidates", numQueries*numDocs, matches, candidates)
}

// deletesEvery deletes every k-th ordinal of seg, recording the ids in deleted.
func deletesEvery(seg *Segment, k uint32, deleted map[string]bool) *roaring.Bitmap {
	b := roaring.New()
	for ord := uint32(0); ord < seg.NumQueries(); ord += k {
		b.Add(ord)
		q, _ := seg.Query(ord)
		deleted[q.ID] = true
	}
	return b
}

func minus(a, b []string) []string {
	var out []string
	for _, s := range a {
		if !slices.Contains(b, s) {
			out = append(out, s)
		}
	}
	return out
}
