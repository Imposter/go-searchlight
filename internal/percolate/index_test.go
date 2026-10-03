package percolate

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

func parseQuery(t testing.TB, raw string) query.Node {
	t.Helper()
	n, problems := query.Parse([]byte(raw))
	if len(problems) > 0 {
		t.Fatalf("%s: %v", raw, problems)
	}
	return n
}

var sampleQueries = []string{
	`{"all":[]}`,
	`{"all":[{"field":"brand","op":"eq","value":"Ac\"me"},{"not":{"field":"price","op":"gt","value":10}}]}`,
	`{"any":[{"field":"tags","op":"has_any","value":["a","ß","İ"]},{"field":"title","op":"exists"}]}`,
	`{"all":[{"field":"title","op":"similar","value":{"text":"café","min":0.4}},{"field":"_id","op":"in","value":["x","y"]}]}`,
	`{"field":"price","op":"between","value":[1,5]}`,
	`{"field":"title","op":"words_all","value":["rtx 4090","founders"]}`,
	`{"field":"title","op":"contains","value":"4090"}`,
	`{"field":"stock","op":"eq","value":true}`,
	`{"field":"brand","op":"eq","value":"acme"}`, // the same class as the next
	`{"field":"brand","op":"eq","value":" ACME "}`,
}

func sampleStored(t testing.TB) []shard.StoredQuery {
	t.Helper()
	out := make([]shard.StoredQuery, len(sampleQueries))
	for i, raw := range sampleQueries {
		out[i] = shard.StoredQuery{ID: fmt.Sprintf("q%d", len(sampleQueries)-i), Seq: int64(100 + i), Query: parseQuery(t, raw)}
		if i%2 == 0 {
			out[i].Meta = []byte(`{"n":` + fmt.Sprint(i) + `}`)
		}
	}
	return out
}

func buildSample(t testing.TB) (string, *Segment) {
	t.Helper()
	dir := t.TempDir()
	queries := sampleStored(t)
	size, err := Index{}.Build(context.Background(), dir, "s1", queries, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "s1"+FileExt)
	if info, err := os.Stat(path); err != nil || info.Size() != size {
		t.Fatalf("Build reported %d bytes: %v %v", size, info, err)
	}
	qs, err := Index{}.Open(dir, "s1")
	if err != nil {
		t.Fatal(err)
	}
	seg, ok := qs.(*Segment)
	if !ok {
		t.Fatalf("Open returned %T", qs)
	}
	return path, seg
}

func TestSegmentRoundTrip(t *testing.T) {
	_, seg := buildSample(t)
	queries := sampleStored(t)
	if seg.NumQueries() != uint32(len(queries)) {
		t.Fatalf("NumQueries %d", seg.NumQueries())
	}
	for i, q := range queries {
		ord, ok := seg.Ord(q.ID)
		if !ok || ord != uint32(i) {
			t.Fatalf("Ord(%s) = %d %v, want %d", q.ID, ord, ok, i)
		}
		got, err := seg.Query(ord)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != q.ID || got.Seq != q.Seq || !bytes.Equal(got.Meta, q.Meta) ||
			!bytes.Equal(query.Canonical(got.Query), query.Canonical(q.Query)) {
			t.Fatalf("query %d: %+v, want %+v", i, got, q)
		}
	}
	for _, id := range []string{"", "q", "q0", "q99", "zzz"} {
		if _, ok := seg.Ord(id); ok {
			t.Fatalf("Ord(%q) found", id)
		}
	}
	if _, err := seg.Query(seg.NumQueries()); err == nil {
		t.Fatal("Query past the end succeeded")
	}
	// The root {"all": []} is always-check; the not inside an all with an anchorable
	// child is not.
	if seg.NumAlways() != 1 {
		t.Fatalf("%d always-check queries, want 1", seg.NumAlways())
	}
	// Equivalent queries share a class.
	if a, b := seg.class(8), seg.class(9); a != 8 || b != 8 {
		t.Fatalf("classes %d %d, want 8 8", a, b)
	}
	if seg.Close() != nil {
		t.Fatal("Close")
	}
}

func TestBuildIsDeterministic(t *testing.T) {
	queries := sampleStored(t)
	a, err := encodeSegment(context.Background(), queries, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := encodeSegment(context.Background(), queries, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("two builds of the same queries differ")
	}
}

func TestBuildRefusesDuplicateIDs(t *testing.T) {
	q := parseQuery(t, `{"field":"a","op":"eq","value":1}`)
	_, err := encodeSegment(context.Background(), []shard.StoredQuery{{ID: "x", Query: q}, {ID: "x", Query: q}}, nil)
	if err == nil {
		t.Fatal("duplicate ids built")
	}
}

func TestBuildHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := encodeSegment(ctx, sampleStored(t), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestEmptySegment(t *testing.T) {
	data, err := encodeSegment(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	seg, err := openData("empty", data)
	if err != nil {
		t.Fatal(err)
	}
	if seg.NumQueries() != 0 {
		t.Fatal("not empty")
	}
	sc := new(scratch)
	sc.fit(0)
	d, _, _ := schema.Analyze(testMapping(), "a", []byte(`{"brand":"x","price":1}`))
	seg.collect(&d, sc)
	if len(sc.cands) != 0 {
		t.Fatal("candidates from an empty segment")
	}
}

func TestCheckRefusesWhatCannotRoundTrip(t *testing.T) {
	for _, n := range []query.Node{
		&query.Leaf{Field: "a", Op: "eq", Value: []byte(`{`)},
		&query.All{Children: []query.Node{nil}},
		(*query.Not)(nil),
	} {
		if err := (Index{}).Check(&shard.StoredQuery{ID: "q", Query: n}); err == nil {
			t.Fatalf("Check accepted %#v", n)
		}
	}
	if err := (Index{}).Check(&shard.StoredQuery{ID: "q", Query: parseQuery(t, sampleQueries[1])}); err != nil {
		t.Fatal(err)
	}
}

// Every byte flipped, and every truncation, is refused with a *CorruptError.
func TestCorruptSegmentRefused(t *testing.T) {
	path, _ := buildSample(t)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		bad := bytes.Clone(data)
		bad[i] ^= 0x41
		if _, err := openData("x", bad); !isCorrupt(err) {
			t.Fatalf("byte %d flipped: %v", i, err)
		}
	}
	for n := range len(data) {
		if _, err := openData("x", data[:n]); !isCorrupt(err) {
			t.Fatalf("truncated to %d: %v", n, err)
		}
	}
	// Through Index.Open, as a shard opens it.
	if err := os.WriteFile(path, data[:len(data)-1], 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (Index{}).Open(filepath.Dir(path), "s1"); !isCorrupt(err) {
		t.Fatalf("Open of a truncated file: %v", err)
	}
}

func isCorrupt(err error) bool {
	var ce *CorruptError
	return errors.As(err, &ce)
}

// restamp returns body with the format header and a valid checksum, so that a mutation
// reaches the structural checks behind the checksum.
func restamp(body []byte) []byte {
	data := bytes.Clone(body)
	if len(data) >= len(fileMagic)+8 {
		copy(data, fileMagic[:])
		binary.LittleEndian.PutUint32(data[len(fileMagic):], formatVersion)
		binary.LittleEndian.PutUint32(data[len(fileMagic)+4:], numSections)
	}
	return binary.LittleEndian.AppendUint32(data, crc32.Checksum(data, castagnoli))
}

// Crafted files (mutated, then given a valid checksum) are refused, or open and then
// survive every accessor: no panic, no hang.
func TestCraftedSegmentsFailSafely(t *testing.T) {
	path, _ := buildSample(t)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := data[:len(data)-4]
	r := rand.New(rand.NewPCG(1, 2))
	opened := 0
	for i := range 20000 {
		bad := bytes.Clone(body)
		for range 1 + r.IntN(3) {
			p := r.IntN(len(bad))
			switch r.IntN(3) {
			case 0:
				bad[p] ^= byte(1 << r.IntN(8))
			case 1:
				bad[p] = 0xff
			default:
				bad[p] = 0
			}
		}
		seg, err := openData("crafted", restamp(bad))
		if err != nil {
			if !isCorrupt(err) {
				t.Fatalf("mutation %d: %T %v", i, err, err)
			}
			continue
		}
		opened++
		exercise(seg)
	}
	t.Logf("%d of 20000 crafted files opened (and were exercised)", opened)
}

// exercise calls every accessor of an open segment.
func exercise(seg *Segment) {
	for ord := range seg.NumQueries() {
		q, err := seg.Query(ord)
		if err == nil {
			seg.Ord(q.ID)
		}
		_, _ = seg.compiledQuery(seg.class(ord))
		_ = seg.id(ord)
	}
	seg.Ord("q1")
	sc := new(scratch)
	sc.fit(seg.NumQueries())
	for _, body := range []string{
		`{"brand":"acme","title":"RTX 4090 café founders","tags":["a","ß"],"price":3,"stock":true}`,
		`{"brand":1,"title":["x"],"price":"x","tags":"a, b"}`,
		`{"price":-1e300,"seen":1e300}`,
	} {
		d, _, _ := schema.Analyze(testMapping(), "x", []byte(body))
		seg.collect(&d, sc)
		v := &view{seg: seg, n: seg.NumQueries()}
		var st docStats
		_ = verify(v, &d, sc, &st)
		_ = sc.results(false)
		sc.reset()
	}
}

func TestIntervalTreeStabbing(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for round := range 50 {
		n := r.IntN(300)
		ivs := make([]interval, n)
		for i := range ivs {
			lo, hi := float64(r.IntN(100)), float64(r.IntN(100))
			switch r.IntN(6) {
			case 0:
				lo = math.Inf(-1)
			case 1:
				hi = math.Inf(1)
			case 2:
				hi = lo
			}
			if lo > hi {
				lo, hi = hi, lo
			}
			ivs[i] = interval{lo: lo, hi: hi, ord: uint32(i)}
		}
		var nodes []treeNode
		var byLo, byHi []interval
		root := buildTree(ivs, &nodes, &byLo, &byHi)
		depth := treeDepth(nodes, root)
		if limit := 2 + 2*int(math.Log2(float64(n+1))); depth > limit {
			t.Fatalf("round %d: depth %d over %d for %d intervals", round, depth, limit, n)
		}
		seg := &Segment{n: uint32(n), byLo: appendIntervals(byLo), byHi: appendIntervals(byHi)}
		for _, nd := range nodes {
			seg.nodes = binary.LittleEndian.AppendUint64(seg.nodes, math.Float64bits(nd.center))
			seg.nodes = binary.LittleEndian.AppendUint32(seg.nodes, uint32(nd.left))
			seg.nodes = binary.LittleEndian.AppendUint32(seg.nodes, uint32(nd.right))
			seg.nodes = binary.LittleEndian.AppendUint32(seg.nodes, nd.start)
			seg.nodes = binary.LittleEndian.AppendUint32(seg.nodes, nd.count)
		}
		sc := new(scratch)
		sc.fit(uint32(n))
		for x := -2.0; x <= 102; x += 0.5 {
			if root >= 0 {
				seg.stab(root, x, sc)
			}
			got := slices.Clone(sc.cands)
			slices.Sort(got)
			var want []uint32
			for _, iv := range ivs {
				if iv.lo <= x && x <= iv.hi {
					want = append(want, iv.ord)
				}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("round %d, x=%v: stab %v, want %v", round, x, got, want)
			}
			sc.reset()
		}
	}
}

func treeDepth(nodes []treeNode, at int32) int {
	if at < 0 {
		return 0
	}
	return 1 + max(treeDepth(nodes, nodes[at].left), treeDepth(nodes, nodes[at].right))
}

func TestEachWindowIsSubstrings3(t *testing.T) {
	for _, s := range []string{"", "a", "ab", "abc", "abcd", "aaaa", "café au lait", "日本語テキスト", "éte", "a\xffb\xfec", "😀😀😀😀"} {
		seen := map[string]bool{}
		eachWindow(s, func(w string) { seen[w] = true })
		got := slices.Sorted(func(yield func(string) bool) {
			for w := range seen {
				if !yield(w) {
					return
				}
			}
		})
		if want := analysis.Substrings3(s); !slices.Equal(got, want) && (len(got) != 0 || len(want) != 0) {
			t.Fatalf("%q: windows %q, Substrings3 %q", s, got, want)
		}
	}
}

func TestHashKeyIsHashTerm(t *testing.T) {
	for _, k := range []uint64{0, 1, 0x1fffff, 0xdeadbeefcafe, math.MaxUint64} {
		if hashKey(7, k) != hashTerm(AtomSimKey, 7, simKeyTerm(k)) {
			t.Fatalf("key %x", k)
		}
	}
}

func testMapping() *schema.Mapping {
	return &schema.Mapping{Fields: map[string]schema.FieldType{
		"brand": schema.Keyword,
		"title": schema.Text,
		"tags":  schema.KeywordList,
		"price": schema.Number,
		"stock": schema.Bool,
		"seen":  schema.Date,
	}}
}
