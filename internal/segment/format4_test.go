package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// compatFixtures are the format-3 files in testdata/v3, built from compatCorpora.
var compatFixtures = []string{"dense", "sparse", "marked", "points"}

func readCompatFixture(tb testing.TB, name string) []byte {
	tb.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", fmt.Sprintf("v%d", ReadsMajor), name+FileExt))
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

// openCompatFixture opens a format-3 fixture from a copy of its bytes.
func openCompatFixture(tb testing.TB, name string) *Reader {
	tb.Helper()
	r, err := openData(name, readCompatFixture(tb, name))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = r.Close() })
	return r
}

// currentDocs is docs as a format-4 writer gets them: an untyped mark is Untyped, where
// a format-3 writer set GramsTruncated on a value with no text.
func currentDocs(docs []schema.Doc) []schema.Doc {
	out := make([]schema.Doc, len(docs))
	for i, d := range docs {
		fields := make(map[string]schema.Value, len(d.Fields))
		for name, v := range d.Fields {
			if v.GramsTruncated && v.Text == nil {
				v.GramsTruncated, v.Untyped = false, true
			}
			fields[name] = v
		}
		out[i] = schema.Doc{ID: d.ID, Fields: fields, Body: d.Body}
	}
	return out
}

// sameSegment checks that a and b read alike through every Reader method: the same
// fields, terms, postings, columns, points, presence, truncated and untyped bitmaps,
// stored records and ids.
func sameSegment(t *testing.T, a, b *Reader) {
	t.Helper()
	if a.NumDocs() != b.NumDocs() || a.NumTerms() != b.NumTerms() {
		t.Fatalf("NumDocs %d/%d, NumTerms %d/%d", a.NumDocs(), b.NumDocs(), a.NumTerms(), b.NumTerms())
	}
	names := func(r *Reader) []string {
		var out []string
		for name := range r.fields {
			out = append(out, name)
		}
		slices.Sort(out)
		return out
	}
	if !slices.Equal(names(a), names(b)) {
		t.Fatalf("fields %v / %v", names(a), names(b))
	}
	n := a.NumDocs()
	for _, field := range names(a) {
		for kind := range TermKind(numKinds) {
			var at, bt []string
			a.Terms(field, kind, "", func(term string, df uint32) bool {
				at = append(at, fmt.Sprintf("%s/%d", term, df))
				if !a.Postings(field, kind, term).Equals(b.Postings(field, kind, term)) {
					t.Fatalf("%s %v %q: postings differ", field, kind, term)
				}
				return true
			})
			b.Terms(field, kind, "", func(term string, df uint32) bool {
				bt = append(bt, fmt.Sprintf("%s/%d", term, df))
				return true
			})
			if !slices.Equal(at, bt) {
				t.Fatalf("%s %v: terms differ: %d vs %d", field, kind, len(at), len(bt))
			}
		}
		for name, get := range map[string]func(*Reader, string) *roaring.Bitmap{
			"present": (*Reader).Present, "truncated": (*Reader).Truncated, "untyped": (*Reader).Untyped,
		} {
			if x, y := get(a, field), get(b, field); !x.Equals(y) {
				t.Fatalf("%s %s: %v / %v", field, name, x.ToArray(), y.ToArray())
			}
		}
		ak, bk := a.Keywords(field), b.Keywords(field)
		am, bm := a.Entries(field), b.Entries(field)
		an, bn := a.Numbers(field), b.Numbers(field)
		if ak.Exists() != bk.Exists() || am.Exists() != bm.Exists() || an.Exists() != bn.Exists() || an.Stats() != bn.Stats() {
			t.Fatalf("%s: columns differ", field)
		}
		for doc := range n {
			ao, aok := ak.Ord(doc)
			bo, bok := bk.Ord(doc)
			if aok != bok || ao != bo || (aok && ak.Term(ao) != bk.Term(bo)) {
				t.Fatalf("%s doc %d: keyword %d,%v / %d,%v", field, doc, ao, aok, bo, bok)
			}
			if x, y := am.Ords(doc, nil), bm.Ords(doc, nil); !slices.Equal(x, y) {
				t.Fatalf("%s doc %d: entries %v / %v", field, doc, x, y)
			}
			av, aok := an.Value(doc)
			bv, bok := bn.Value(doc)
			if aok != bok || av != bv {
				t.Fatalf("%s doc %d: number %v,%v / %v,%v", field, doc, av, aok, bv, bok)
			}
		}
		if an.Exists() {
			st := an.Stats()
			for _, rg := range [][2]float64{{st.Min, st.Max}, {st.Min, (st.Min + st.Max) / 2}, {(st.Min + st.Max) / 3, st.Max}} {
				for _, inc := range [][2]bool{{true, true}, {false, false}, {true, false}} {
					if x, y := an.Range(rg[0], rg[1], inc[0], inc[1]), bn.Range(rg[0], rg[1], inc[0], inc[1]); !x.Equals(y) {
						t.Fatalf("%s range %v %v: %d / %d docs", field, rg, inc, x.GetCardinality(), y.GetCardinality())
					}
				}
			}
		}
	}
	for doc := range n {
		ab, aerr := a.Stored(doc)
		bb, berr := b.Stored(doc)
		aid, _ := a.ID(doc)
		bid, _ := b.ID(doc)
		if aerr != nil || berr != nil || !bytes.Equal(ab, bb) || aid != bid {
			t.Fatalf("doc %d: stored %v/%v, ids %q/%q", doc, aerr, berr, aid, bid)
		}
		if o, ok := b.Ord(aid); !ok || o != doc {
			t.Fatalf("Ord(%q) = %d, %v, want %d", aid, o, ok, doc)
		}
		if x, ok := a.IDAt(doc); !ok {
			t.Fatalf("IDAt(%d) missing", doc)
		} else if y, _ := b.IDAt(doc); x != y {
			t.Fatalf("IDAt(%d) = %q / %q", doc, x, y)
		}
	}
}

// TestReadsPreviousMajor opens each format-3 fixture and checks it reads exactly as a
// format-4 build of the same documents does, untyped marks included.
func TestReadsPreviousMajor(t *testing.T) {
	corpora := compatCorpora(t)
	for _, name := range compatFixtures {
		t.Run(name, func(t *testing.T) {
			old := openCompatFixture(t, name)
			if old.FormatMajor() != ReadsMajor || old.MarksUntyped() {
				t.Fatalf("fixture is format %d, marks %v", old.FormatMajor(), old.MarksUntyped())
			}
			cur := mustBuild(t, currentDocs(corpora[name]))
			if cur.FormatMajor() != FormatMajor {
				t.Fatalf("Build wrote format %d", cur.FormatMajor())
			}
			sameSegment(t, old, cur)
		})
	}
}

// TestMergeRewritesPreviousMajor merges format-3 fixtures, with deletes, into one
// format-4 segment: byte for byte a fresh Build of the live documents.
func TestMergeRewritesPreviousMajor(t *testing.T) {
	corpora := compatCorpora(t)
	var inputs []*Reader
	var deletes []*roaring.Bitmap
	var live []schema.Doc
	for i, name := range compatFixtures {
		inputs = append(inputs, openCompatFixture(t, name))
		del := roaring.New()
		for j, d := range currentDocs(corpora[name]) {
			if (i+j)%4 == 0 {
				del.Add(uint32(j))
				continue
			}
			live = append(live, d)
		}
		deletes = append(deletes, del)
	}
	dir := t.TempDir()
	meta, err := Merge(dir, inputs, deletes, MergeOptions{Name: "merged"})
	if err != nil {
		t.Fatal(err)
	}
	merged, err := os.ReadFile(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt := buildFile(t, live)
	if !bytes.Equal(merged, rebuilt) {
		t.Fatalf("a merge of format-3 inputs wrote %d bytes, a rebuild %d, and they differ", len(merged), len(rebuilt))
	}
	if binary.LittleEndian.Uint16(merged[8:]) != FormatMajor {
		t.Fatal("the merge did not write the current major")
	}
}

func TestUntypedMarks(t *testing.T) {
	docs := currentDocs(markedDocs(t, 60))
	dir := t.TempDir()
	for _, marks := range []bool{false, true} {
		meta, err := Build(dir, docs, BuildOptions{MarksUntyped: marks})
		if err != nil {
			t.Fatal(err)
		}
		r, err := Open(meta.Path)
		if err != nil {
			t.Fatal(err)
		}
		if r.MarksUntyped() != marks {
			t.Fatalf("MarksUntyped = %v, built with %v", r.MarksUntyped(), marks)
		}
		want := roaring.New()
		for i := range docs {
			if docs[i].Fields["extra"].Untyped {
				want.Add(uint32(i))
			}
		}
		if got := r.Untyped("extra"); want.IsEmpty() || !got.Equals(want) {
			t.Fatalf("Untyped(extra) = %v, want %v", got.ToArray(), want.ToArray())
		}
		if !r.Truncated("extra").IsEmpty() || !r.Untyped("title").IsEmpty() || r.Truncated("title").IsEmpty() {
			t.Fatal("marks and truncation are mixed up")
		}
		_ = r.Close()
	}
	// A merge marks only when told every input does.
	in, err := Build(dir, docs, BuildOptions{MarksUntyped: true})
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(in.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, marks := range []bool{false, true} {
		meta, err := Merge(dir, []*Reader{r}, nil, MergeOptions{MarksUntyped: marks})
		if err != nil {
			t.Fatal(err)
		}
		m, err := Open(meta.Path)
		if err != nil {
			t.Fatal(err)
		}
		if m.MarksUntyped() != marks || !m.Untyped("extra").Equals(r.Untyped("extra")) {
			t.Fatalf("merge with MarksUntyped %v: flag %v, untyped %v", marks, m.MarksUntyped(), m.Untyped("extra").ToArray())
		}
		_ = m.Close()
	}
}

// TestSeparatorKeys: the block index's separator keys find every term, and Terms with a
// prefix starts at the right block, across many blocks of long values.
func TestSeparatorKeys(t *testing.T) {
	docs := genCorpus(500)
	r := mustBuild(t, docs)
	kc := r.Keywords("description")
	for i, doc := range docs {
		ord, ok := kc.Ord(uint32(i))
		text := *doc.Fields["description"].Text
		if !ok || kc.Term(ord) != text {
			t.Fatalf("doc %d: Term(%d) differs", i, ord)
		}
		if got, ok := kc.Lookup(text); !ok || got != ord {
			t.Fatalf("doc %d: Lookup = %d, %v, want %d", i, got, ok, ord)
		}
		var first []byte
		r.Terms("description", KindValue, text[:8], func(term string, _ uint32) bool {
			first = []byte(term)
			return false
		})
		if first == nil || !bytes.HasPrefix(first, []byte(text[:8])) {
			t.Fatalf("doc %d: Terms(prefix) found %q", i, first)
		}
	}
	if _, ok := kc.Lookup("tok"); ok {
		t.Fatal("Lookup of an absent term succeeded")
	}
}

// TestStoredDictionaryThresholds: a small segment has no stored dictionary, a large one
// does, and both read every record back.
func TestStoredDictionaryThresholds(t *testing.T) {
	for _, n := range []int{10, 3000} {
		docs := genCorpus(n)
		r := mustBuild(t, docs)
		if hasDict := len(r.stored.dict) > 0; hasDict != (n > 10) {
			t.Fatalf("%d documents: dictionary of %d bytes", n, len(r.stored.dict))
		}
		for i, d := range docs {
			body, err := r.Stored(uint32(i))
			if err != nil || !bytes.Equal(body, d.Body) {
				t.Fatalf("%d documents: Stored(%d): %v", n, i, err)
			}
		}
		if _, err := r.Stored(uint32(n)); err == nil {
			t.Fatal("Stored past the documents succeeded")
		}
	}
}

// v4StoredTable locates a format-4 stored block table's packed arrays.
type v4StoredTable struct {
	at                           uint64 // the table's absolute start
	numBlocks                    uint64
	ordWidth, offWidth, rawWidth uint8
	firstOrds, offs, raws        uint64 // absolute starts of the packed arrays
}

func storedTableOf(tb testing.TB, data []byte) v4StoredTable {
	tb.Helper()
	at := layoutOf(tb, data).storedIndexAbs
	st := v4StoredTable{at: at, numBlocks: uint64(binary.LittleEndian.Uint32(data[at:]))}
	st.ordWidth, st.offWidth, st.rawWidth = data[at+8], data[at+9], data[at+10]
	st.firstOrds = at + storedIndexHeaderLen
	st.offs = st.firstOrds + packedSize(st.numBlocks, st.ordWidth)
	st.raws = st.offs + packedSize(st.numBlocks+1, st.offWidth)
	return st
}

// setPacked overwrites value i of the packed array of width w at data[at:].
func setPacked(data []byte, at, i uint64, w uint8, v uint64) {
	pos := i * uint64(w)
	for k := range uint64(w) {
		bit := at*8 + pos + k
		if v>>k&1 != 0 {
			data[bit/8] |= 1 << (bit % 8)
		} else {
			data[bit/8] &^= 1 << (bit % 8)
		}
	}
}

func TestStoredTableRefusesInconsistentBlocks(t *testing.T) {
	cases := map[string]func(data []byte, st v4StoredTable){
		"block count":            func(d []byte, st v4StoredTable) { binary.LittleEndian.PutUint32(d[st.at:], 0xFFFFFFFF) },
		"dictionary past table":  func(d []byte, st v4StoredTable) { binary.LittleEndian.PutUint32(d[st.at+4:], 1<<30) },
		"width over 64":          func(d []byte, st v4StoredTable) { d[st.at+9] = 60 },
		"first ordinal not zero": func(d []byte, st v4StoredTable) { setPacked(d, st.firstOrds, 0, st.ordWidth, 1) },
		"ordinals out of order": func(d []byte, st v4StoredTable) {
			setPacked(d, st.firstOrds, 1, st.ordWidth, 0)
		},
		"offset past table": func(d []byte, st v4StoredTable) {
			setPacked(d, st.offs, st.numBlocks, st.offWidth, 1<<st.offWidth-1)
		},
		"offsets backwards": func(d []byte, st v4StoredTable) { setPacked(d, st.offs, 1, st.offWidth, 0) },
		"raw under 2/doc":   func(d []byte, st v4StoredTable) { setPacked(d, st.raws, 0, st.rawWidth, 1) },
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			data := buildFile(t, genCorpus(60)) // several blocks
			st := storedTableOf(t, data)
			if st.numBlocks < 2 {
				t.Fatalf("%d blocks, want several", st.numBlocks)
			}
			damage(data, st)
			_, err := openCrafted(t, data)
			wantCorrupt(t, err, "stored")
		})
	}
}

// TestStoredRawLenMismatchIsAnError: a block whose recorded rawLen disagrees with what
// it decompresses to is refused when read, with an error - never a panic, and never a
// buffer sized from the zstd frame's own (unchecked) content size.
func TestStoredRawLenMismatchIsAnError(t *testing.T) {
	for _, delta := range []int{-1, +1} {
		data := buildFile(t, genCorpus(60))
		st := storedTableOf(t, data)
		raw := unpack(data[st.raws:], 0, st.rawWidth)
		if raw+1 >= 1<<st.rawWidth {
			t.Skip("no room in the width to grow rawLen")
		}
		setPacked(data, st.raws, 0, st.rawWidth, uint64(int(raw)+delta))
		r, err := openCrafted(t, data)
		if err != nil {
			t.Fatalf("delta %d: Open: %v", delta, err)
		}
		if _, err := r.Stored(0); err == nil {
			t.Fatalf("delta %d: Stored of a block with the wrong rawLen succeeded", delta)
		}
	}
}

// TestPreviousMajorStoredTableDamage keeps format 3's table checks covered.
func TestPreviousMajorStoredTableDamage(t *testing.T) {
	cases := map[string]func(entry []byte){
		"offset past section": func(e []byte) { binary.LittleEndian.PutUint64(e[0:], 1<<40) },
		"rawLen over ceiling": func(e []byte) { binary.LittleEndian.PutUint32(e[12:], maxStoredBlockRaw+1) },
		"firstOrd not zero":   func(e []byte) { binary.LittleEndian.PutUint32(e[16:], 1) },
		"empty block":         func(e []byte) { binary.LittleEndian.PutUint32(e[20:], 0) },
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			data := readCompatFixture(t, "sparse")
			damage(data[layoutOf(t, data).storedIndexAbs+4:])
			_, err := openCrafted(t, data)
			wantCorrupt(t, err, "stored")
		})
	}
}

func craftStoredOffset(tb testing.TB) []byte {
	data := buildFile(tb, genCorpus(60))
	st := storedTableOf(tb, data)
	setPacked(data, st.offs, 1, st.offWidth, 1<<st.offWidth-1)
	fixChecksums(data)
	return data
}

func craftStoredFirstOrd(tb testing.TB) []byte {
	data := buildFile(tb, genCorpus(60))
	st := storedTableOf(tb, data)
	setPacked(data, st.firstOrds, 1, st.ordWidth, 1<<st.ordWidth-1)
	fixChecksums(data)
	return data
}

func craftUntypedBitmap(tb testing.TB) []byte {
	data := buildFile(tb, currentDocs(markedDocs(tb, 40)))
	rg := openValid(tb, data).fields["extra"].untypedRegion
	if rg.n == 0 {
		tb.Fatal("no untyped bitmap")
	}
	binary.LittleEndian.PutUint32(data[rg.off:], 0xdeadbeef)
	fixChecksums(data)
	return data
}

func craftMetaFlags(tb testing.TB) []byte {
	data := buildFile(tb, testDocs(tb))
	meta := layoutOf(tb, data).sections[sectionMeta]
	binary.LittleEndian.PutUint32(data[meta.off+4:], 1<<31)
	fixChecksums(data)
	return data
}

func craftV3PointBlockOffset(tb testing.TB) []byte {
	data := readCompatFixture(tb, "dense")
	p := openValid(tb, data).fields["price"].numberCol.points
	binary.LittleEndian.PutUint64(data[p.base+pointsHeaderLen(ReadsMajor)+20:], 1<<40)
	fixChecksums(data)
	return data
}

// craftShortPointBlock moves an entry from a point index's first block to its last: a
// table no writer lays out, whose block starts are no longer the index times the block
// size.
func craftShortPointBlock(tb testing.TB) []byte {
	data := buildFile(tb, genCorpus(300))
	p := openValid(tb, data).fields["price"].numberCol.points
	entry := func(i uint32) uint64 { return p.base + pointsHeaderLen(FormatMajor) + uint64(i)*pointsEntryLen + 16 }
	last := p.numBlocks - 1
	binary.LittleEndian.PutUint32(data[entry(0):], pointsBlockSize-1)
	binary.LittleEndian.PutUint32(data[entry(last):], binary.LittleEndian.Uint32(data[entry(last):])+1)
	fixChecksums(data)
	return data
}

// TestCraftedFailSafelyNamed checks that the format-4 crafted files are refused at Open
// in the section they damage, or read safely.
func TestCraftedFailSafelyNamed(t *testing.T) {
	for name, c := range map[string]struct {
		make    func(testing.TB) []byte
		section string // "" when the damage is found on read, not at Open
	}{
		"untyped bitmap":    {craftUntypedBitmap, "presence"},
		"meta flags":        {craftMetaFlags, "meta"},
		"stored offset":     {craftStoredOffset, "stored"},
		"stored first ord":  {craftStoredFirstOrd, "stored"},
		"v3 point block":    {craftV3PointBlockOffset, "points"},
		"short point block": {craftShortPointBlock, "points"},
	} {
		t.Run(name, func(t *testing.T) {
			r, err := openData("crafted", c.make(t))
			if c.section != "" {
				wantCorrupt(t, err, c.section)
				return
			}
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			exerciseReader(r)
			_ = r.Close()
		})
	}
}

// TestHeldFieldsSpill: with every held field spilled to a temp file, Build and Merge
// write the same bytes at any thread count, and leave no temp file behind.
func TestHeldFieldsSpill(t *testing.T) {
	defer func(n int) { spillAt = n }(spillAt)
	docs := genCorpus(400)
	dir := t.TempDir()
	want := buildBytes(t, dir, "t1", docs, 1)
	spillAt = 64
	for _, threads := range []int{2, 7} {
		if got := buildBytes(t, dir, fmt.Sprintf("t%d", threads), docs, threads); !bytes.Equal(got, want) {
			t.Fatalf("Threads %d with spilling wrote different bytes", threads)
		}
	}
	checkMergeEqualsRebuild(t, docs, 3, true)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) != FileExt {
			t.Fatalf("left behind %s", e.Name())
		}
	}
}

// TestMergeCancelStopsWriting: a merge whose Throttle fails at its first write returns
// that error well before a full merge would have finished, and leaves no file behind.
func TestMergeCancelStopsWriting(t *testing.T) {
	docs := genCorpus(20000)
	dir := t.TempDir()
	var inputs []*Reader
	for i, part := range [][]schema.Doc{docs[:10000], docs[10000:]} {
		meta, err := Build(dir, part, BuildOptions{Name: fmt.Sprintf("in%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		r, err := Open(meta.Path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		inputs = append(inputs, r)
	}
	start := time.Now()
	full, err := Merge(dir, inputs, nil, MergeOptions{Threads: 4})
	if err != nil {
		t.Fatal(err)
	}
	whole := time.Since(start)
	errStop := errors.New("cancelled")
	var failedAt time.Time
	throttle := func(n int) error {
		if n > 0 && failedAt.IsZero() {
			failedAt = time.Now()
		}
		if !failedAt.IsZero() {
			return errStop
		}
		return nil
	}
	_, err = Merge(dir, inputs, nil, MergeOptions{Threads: 4, Throttle: throttle})
	if !errors.Is(err, errStop) {
		t.Fatalf("Merge = %v, want the throttle's error", err)
	}
	if after := time.Since(failedAt); after > whole/3 {
		t.Fatalf("the merge ran %v after its first refused write; a whole merge takes %v", after, whole)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if name := e.Name(); name != "in0"+FileExt && name != "in1"+FileExt && name != filepath.Base(full.Path) {
			t.Fatalf("the cancelled merge left %s", name)
		}
	}
}

// TestDeletesStampedWithTheirMajor: a sidecar is written in the major asked for, reads
// back either way, and cannot be stamped with a major Open does not read.
func TestDeletesStampedWithTheirMajor(t *testing.T) {
	dir := t.TempDir()
	want := roaring.BitmapOf(1, 5, 9)
	for _, major := range []int{ReadsMajor, FormatMajor} {
		if err := WriteDeletes(dir, "seg", uint64(major), want, DeletesOptions{Major: major}); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(dir, DeletesName("seg", uint64(major))))
		if err != nil {
			t.Fatal(err)
		}
		if got := int(binary.LittleEndian.Uint16(data[8:])); got != major {
			t.Fatalf("stamped %d, want %d", got, major)
		}
		got, err := LoadDeletes(dir, "seg", uint64(major))
		if err != nil || !got.Equals(want) {
			t.Fatalf("major %d: %v, %v", major, got.ToArray(), err)
		}
	}
	var buf bytes.Buffer
	for _, major := range []int{-1, ReadsMajor - 1, FormatMajor + 1} {
		if err := EncodeDeletes(&buf, want, major); err == nil {
			t.Fatalf("a sidecar stamped %d was written", major)
		}
	}
}
