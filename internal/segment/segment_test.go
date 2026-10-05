package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/testtier"
)

func testMapping() *schema.Mapping {
	return &schema.Mapping{Fields: map[string]schema.FieldType{
		"title":   schema.Text,
		"brand":   schema.Keyword,
		"tags":    schema.KeywordList,
		"price":   schema.Number,
		"active":  schema.Bool,
		"created": schema.Date,
	}}
}

func mustAnalyze(t testing.TB, m *schema.Mapping, id, body string) schema.Doc {
	t.Helper()
	doc, _, err := schema.Analyze(m, id, []byte(body))
	if err != nil {
		t.Fatalf("Analyze(%q, %q): %v", id, body, err)
	}
	return doc
}

func mustBuild(t *testing.T, docs []schema.Doc) *Reader {
	t.Helper()
	dir := t.TempDir()
	meta, err := Build(dir, docs, BuildOptions{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	r, err := Open(meta.Path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func testDocs(t testing.TB) []schema.Doc {
	m := testMapping()
	long := strings.Repeat("abcdefghij", 200) // 2000 chars, over MaxGramChars (1024)
	bodies := []struct {
		id, body string
	}{
		{"d1", `{"title": "Straße Ängstlich", "brand": "Acme", "tags": ["red", "blue"], "price": 9.99, "active": true, "created": "2026-01-02T03:04:05Z"}`},
		{"d2", `{"title": "İstanbul", "brand": "Acme", "tags": "red, green, ", "price": -3, "active": false}`},
		{"d3", "{\"title\": \"has a \\u0000 nul\", \"brand\": \"\", \"price\": 0}"},
		{"d4", fmt.Sprintf(`{"title": %q, "brand": "Zeta", "price": 1e300}`, strings.Repeat("x", 500))},
		{"d5", fmt.Sprintf(`{"title": %q, "tags": ["", "dup", "dup"]}`, long)},
		{"d6", `{"untyped": {"nested": true}, "price": "not a number"}`},
		{"d7", `{}`},
	}
	docs := make([]schema.Doc, 0, len(bodies))
	for _, b := range bodies {
		docs = append(docs, mustAnalyze(t, m, b.id, b.body))
	}
	return docs
}

func TestRoundTripBasics(t *testing.T) {
	docs := testDocs(t)
	r := mustBuild(t, docs)

	if got := r.NumDocs(); got != uint32(len(docs)) {
		t.Fatalf("NumDocs = %d, want %d", got, len(docs))
	}

	// ID / Ord round trip for every document.
	for ord := range docs {
		id, err := r.ID(uint32(ord))
		if err != nil {
			t.Fatalf("ID(%d): %v", ord, err)
		}
		if id != docs[ord].ID {
			t.Fatalf("ID(%d) = %q, want %q", ord, id, docs[ord].ID)
		}
		got, ok := r.Ord(docs[ord].ID)
		if !ok || got != uint32(ord) {
			t.Fatalf("Ord(%q) = %d, %v, want %d, true", docs[ord].ID, got, ok, ord)
		}
	}
	if _, ok := r.Ord("no-such-id"); ok {
		t.Fatal("Ord of a missing id succeeded")
	}

	// Stored fields round trip the original body exactly.
	for ord := range docs {
		body, err := r.Stored(uint32(ord))
		if err != nil {
			t.Fatalf("Stored(%d): %v", ord, err)
		}
		if !bytes.Equal(body, docs[ord].Body) {
			t.Fatalf("Stored(%d) = %q, want %q", ord, body, docs[ord].Body)
		}
	}

	// brand is a keyword: "Acme" (normalized) covers d1 and d2.
	acme := r.Postings("brand", KindValue, "acme")
	if !acme.Equals(roaring.BitmapOf(0, 1)) {
		t.Fatalf("brand=acme postings = %v, want {0,1}", acme.ToArray())
	}
	if got := r.TermFreq("brand", KindValue, "acme"); got != 2 {
		t.Fatalf("TermFreq(brand,acme) = %d, want 2", got)
	}
	if got := r.TermFreq("brand", KindValue, "nobody"); got != 0 {
		t.Fatalf("TermFreq(brand,nobody) = %d, want 0", got)
	}

	// tags (keyword_list): d1 has red,blue; d2 has red,green (via the ", "-split
	// string form, with a trailing blank dropped); d5 has "dup" once (blanks dropped,
	// duplicates folded).
	red := r.Postings("tags", KindEntry, "red")
	if !red.Equals(roaring.BitmapOf(0, 1)) {
		t.Fatalf("tags=red postings = %v, want {0,1}", red.ToArray())
	}
	dup := r.Postings("tags", KindEntry, "dup")
	if !dup.Equals(roaring.BitmapOf(4)) {
		t.Fatalf("tags=dup postings = %v, want {4}", dup.ToArray())
	}
	entries := r.Entries("tags")
	var buf []uint32
	buf = entries.Ords(4, buf[:0])
	if len(buf) != 1 {
		t.Fatalf("d5's tags ordinals = %v, want exactly one entry (dup, deduped)", buf)
	}

	// price is a number: stats and value round trip.
	nums := r.Numbers("price")
	if v, ok := nums.Value(0); !ok || v != 9.99 {
		t.Fatalf("price.Value(0) = %v, %v, want 9.99, true", v, ok)
	}
	if v, ok := nums.Value(2); !ok || v != 0 {
		t.Fatalf("price.Value(2) = %v, %v, want 0, true", v, ok)
	}
	if _, ok := nums.Value(6); ok {
		t.Fatal("price.Value(6) (absent field) = true")
	}
	if _, ok := nums.Value(5); ok {
		t.Fatal("price.Value(5) (a string value in a number field) = true")
	}
	st := nums.Stats()
	if st.Count != 4 { // d1, d2, d3, d4 have numeric price; d5 none, d6 wrong type, d7 none
		t.Fatalf("price.Stats().Count = %d, want 4", st.Count)
	}

	// active is a bool, indexed as a term, not a doc-values column.
	trueDocs := r.Postings("active", KindValue, TermTrue)
	if !trueDocs.Equals(roaring.BitmapOf(0)) {
		t.Fatalf("active=true postings = %v, want {0}", trueDocs.ToArray())
	}
	falseDocs := r.Postings("active", KindValue, TermFalse)
	if !falseDocs.Equals(roaring.BitmapOf(1)) {
		t.Fatalf("active=false postings = %v, want {1}", falseDocs.ToArray())
	}
	if r.Keywords("active").Exists() {
		t.Fatal("a bool field has a keyword column")
	}

	// NUL in title becomes U+FFFD; Grams still computed for it (d3 is short).
	if r.Truncated("title").Contains(2) {
		t.Fatal("d3's short title with a NUL is reported truncated")
	}

	// The 2000-char title (d5) is over MaxGramChars and so has no grams, but is
	// recorded truncated.
	if !r.Truncated("title").Contains(4) {
		t.Fatal("d5's long title is not reported truncated")
	}
	if r.Postings("title", KindGram, "abc").Contains(4) {
		t.Fatal("d5's long title contributed a gram despite being truncated")
	}

	// Presence: every present field is recorded, including d6's wrong-typed price
	// and untyped "untyped" is not even in the mapping dynamic=true default, so it
	// becomes its own field; and d7's empty object has neither.
	if !r.Present("price").Contains(5) {
		t.Fatal("d6's wrong-typed price is not recorded present")
	}
	if r.Present("price").Contains(6) {
		t.Fatal("d7 (no price field) reports price present")
	}

	// _id is itself an indexed keyword field via the generic pipeline.
	if !r.Present("_id").Equals(allDocs(len(docs))) {
		t.Fatal("_id is not present on every document")
	}
}

func allDocs(n int) *roaring.Bitmap {
	rb := roaring.New()
	for i := range n {
		rb.Add(uint32(i))
	}
	return rb
}

func TestTermsPrefix(t *testing.T) {
	docs := testDocs(t)
	r := mustBuild(t, docs)
	var got []string
	r.Terms("brand", KindValue, "a", func(term string, _ uint32) bool {
		got = append(got, term)
		return true
	})
	if !slices.Contains(got, "acme") {
		t.Fatalf("Terms(brand,a) = %v, want to contain acme", got)
	}
	for _, term := range got {
		if !strings.HasPrefix(term, "a") {
			t.Fatalf("Terms(brand,a) returned %q, missing the prefix", term)
		}
	}
}

func TestRange(t *testing.T) {
	m := &schema.Mapping{Fields: map[string]schema.FieldType{"n": schema.Number}}
	var docs []schema.Doc
	for i := range 50 {
		docs = append(docs, mustAnalyze(t, m, fmt.Sprintf("d%d", i), fmt.Sprintf(`{"n": %d}`, i)))
	}
	r := mustBuild(t, docs)
	got := r.Numbers("n").Range(10, 20, true, false)
	want := roaring.New()
	for i := 10; i < 20; i++ {
		want.Add(uint32(i))
	}
	if !got.Equals(want) {
		t.Fatalf("Range(10,20,true,false) = %v, want %v", got.ToArray(), want.ToArray())
	}
	got2 := r.Numbers("n").Range(45, 1000, true, true)
	want2 := roaring.New()
	for i := 45; i < 50; i++ {
		want2.Add(uint32(i))
	}
	if !got2.Equals(want2) {
		t.Fatalf("Range(45,1000,true,true) = %v, want %v", got2.ToArray(), want2.ToArray())
	}
}

func TestEmptyBuild(t *testing.T) {
	r := mustBuild(t, nil)
	if r.NumDocs() != 0 {
		t.Fatalf("NumDocs = %d, want 0", r.NumDocs())
	}
	if !r.Postings("x", KindValue, "y").IsEmpty() {
		t.Fatal("Postings on an empty segment is not empty")
	}
}

// TestBuildThreadsByteIdentical pins down the review's requirement that
// BuildOptions.Threads only changes how fast Build runs, never what it writes:
// parallelizing field preparation must produce the exact same bytes as the sequential
// path, since the section writer itself always runs single-threaded afterward, in
// sorted field order.
func TestBuildThreadsByteIdentical(t *testing.T) {
	docs := genCorpus(2000) // enough fields and cardinality to exercise every column kind
	dir := t.TempDir()

	seq, err := Build(dir, docs, BuildOptions{Name: "sequential", Threads: 1})
	if err != nil {
		t.Fatalf("Build(Threads: 1): %v", err)
	}
	for _, threads := range []int{0, 2, 4, 17} {
		par, err := Build(dir, docs, BuildOptions{Name: fmt.Sprintf("threads-%d", threads), Threads: threads})
		if err != nil {
			t.Fatalf("Build(Threads: %d): %v", threads, err)
		}
		seqBytes, err := os.ReadFile(seq.Path)
		if err != nil {
			t.Fatal(err)
		}
		parBytes, err := os.ReadFile(par.Path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(seqBytes, parBytes) {
			t.Fatalf("Threads: %d produced different bytes than Threads: 1 (%d vs %d bytes)",
				threads, len(parBytes), len(seqBytes))
		}
	}
}

func TestCorruptDetected(t *testing.T) {
	docs := testDocs(t)
	dir := t.TempDir()
	meta, err := Build(dir, docs, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte well inside the file, away from the header and footer tail.
	mid := len(data) / 2
	data[mid] ^= 0xFF
	corrupt := filepath.Join(dir, "corrupt.seg")
	if err := os.WriteFile(corrupt, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Open(corrupt)
	if err == nil {
		t.Fatal("Open of a corrupted segment succeeded")
	}
	var ce *CorruptError
	if !errors.As(err, &ce) {
		t.Fatalf("Open error is %T (%v), want *CorruptError", err, err)
	}
}

func TestVersionRefused(t *testing.T) {
	docs := testDocs(t)
	dir := t.TempDir()
	meta, err := Build(dir, docs, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	data[8] = 0xFF // bump the major version byte
	bad := filepath.Join(dir, "badversion.seg")
	if err := os.WriteFile(bad, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Open(bad)
	var ve *VersionError
	if !errors.As(err, &ve) {
		t.Fatalf("Open error = %v (%T), want *VersionError", err, err)
	}
}

// TestSectionOffsetsAreRelocatable pins down the section-relative offset scheme (format.go's
// "Offsets, and why most of them are section-relative"): nothing in a dictionary, a
// doc-values column or a point index is anchored to a fixed absolute position. It
// splices 4 KB of padding in right after the header - pushing TERMS and every section
// after it, plus the footer's own section table, later by exactly that much - updates
// only the section table's recorded offsets and the whole-file checksum (every
// section's own bytes and per-section CRC are untouched, since no section's content
// changed, only where it starts), and checks the result still opens and reads
// correctly. If any reader path assumed a section - or a field's dictionary or column
// within one - sat at some fixed offset rather than the one the footer and META
// actually record, this is what would catch it.
func TestSectionOffsetsAreRelocatable(t *testing.T) {
	m := &schema.Mapping{Fields: map[string]schema.FieldType{"brand": schema.Keyword}}
	var docs []schema.Doc
	for i := range 40 {
		docs = append(docs, mustAnalyze(t, m, fmt.Sprintf("d%d", i), `{"brand": "shared"}`))
	}
	dir := t.TempDir()
	meta, err := Build(dir, docs, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	orig, err := os.ReadFile(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	footer, err := verifyFile(meta.Path, orig)
	if err != nil {
		t.Fatal(err)
	}

	const pad = 4096
	padding := bytes.Repeat([]byte{0xAA}, pad)
	relocated := make([]byte, 0, len(orig)+pad)
	relocated = append(relocated, orig[:headerSize]...)
	relocated = append(relocated, padding...)
	relocated = append(relocated, orig[headerSize:]...)

	kinds := make([]sectionKind, 0, len(footer.sections))
	for k := range footer.sections {
		kinds = append(kinds, k)
	}
	sort.Slice(kinds, func(i, j int) bool { return footer.sections[kinds[i]].off < footer.sections[kinds[j]].off })
	tableStart := len(relocated) - tailSize - len(kinds)*sectionEntrySize
	for i, k := range kinds {
		e := relocated[tableStart+i*sectionEntrySize:]
		binary.LittleEndian.PutUint64(e[4:], footer.sections[k].off+pad)
	}
	binary.LittleEndian.PutUint32(relocated[len(relocated)-4:], crc32c(relocated[:len(relocated)-4]))

	path := filepath.Join(dir, "relocated.seg")
	if err := os.WriteFile(path, relocated, 0o600); err != nil {
		t.Fatal(err)
	}

	original, err := Open(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open of a relocated segment: %v", err)
	}
	defer r.Close()

	if r.NumDocs() != original.NumDocs() {
		t.Fatalf("NumDocs = %d, want %d", r.NumDocs(), original.NumDocs())
	}
	want := allDocs(40)
	if got := r.Postings("brand", KindValue, "shared"); !got.Equals(want) {
		t.Fatalf("Postings after relocation = %v, want %v", got.ToArray(), want.ToArray())
	}
	for i := range uint32(40) {
		id, err := r.ID(i)
		if err != nil {
			t.Fatal(err)
		}
		wantID, err := original.ID(i)
		if err != nil {
			t.Fatal(err)
		}
		if id != wantID {
			t.Fatalf("ID(%d) after relocation = %q, want %q", i, id, wantID)
		}
	}
}

func TestDeletesSidecar(t *testing.T) {
	dir := t.TempDir()
	empty, err := LoadDeletes(dir, "seg1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !empty.IsEmpty() {
		t.Fatal("LoadDeletes before any write is not empty")
	}
	want := roaring.BitmapOf(1, 3, 5)
	if err := WriteDeletes(dir, "seg1", 7, want, DeletesOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := LoadDeletes(dir, "seg1", 7)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equals(want) {
		t.Fatalf("LoadDeletes = %v, want %v", got.ToArray(), want.ToArray())
	}
	// A later generation for the same segment does not see the old one.
	stillEmpty, err := LoadDeletes(dir, "seg1", 8)
	if err != nil {
		t.Fatal(err)
	}
	if !stillEmpty.IsEmpty() {
		t.Fatal("a new generation sees the previous generation's deletes")
	}
}

func TestMergeEqualsRebuild(t *testing.T) {
	m := testMapping()
	var all []schema.Doc
	for i := range 12 {
		body := fmt.Sprintf(`{"title": "item %d", "brand": "Brand%d", "tags": ["t%d", "shared"], "price": %d, "active": %t}`,
			i, i%3, i%4, i, i%2 == 0)
		all = append(all, mustAnalyze(t, m, fmt.Sprintf("doc%d", i), body))
	}
	segA := all[:7]
	segB := all[7:]
	delA := roaring.BitmapOf(1, 3) // doc1, doc3 deleted from segment A
	delB := roaring.BitmapOf(0)    // doc7 deleted from segment B

	dir := t.TempDir()
	metaA, err := Build(dir, segA, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	metaB, err := Build(dir, segB, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rA, err := Open(metaA.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer rA.Close()
	rB, err := Open(metaB.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer rB.Close()

	mergedMeta, err := Merge(dir, []*Reader{rA, rB}, []*roaring.Bitmap{delA, delB}, MergeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	merged, err := Open(mergedMeta.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()

	// The expected rebuild: segA's live docs (skipping 1,3) then segB's (skipping the
	// first), in that order - the same order Merge assigns new ordinals in.
	var expected []schema.Doc
	for i, d := range segA {
		if !delA.Contains(uint32(i)) {
			expected = append(expected, d)
		}
	}
	for i, d := range segB {
		if !delB.Contains(uint32(i)) {
			expected = append(expected, d)
		}
	}
	rebuilt := mustBuild(t, expected)

	if merged.NumDocs() != rebuilt.NumDocs() {
		t.Fatalf("merged NumDocs = %d, rebuilt = %d", merged.NumDocs(), rebuilt.NumDocs())
	}
	for _, field := range []string{"_id", "title", "brand", "tags", "price", "active"} {
		for _, kind := range []TermKind{KindValue, KindEntry, KindWord, KindGram} {
			compareTerms(t, field, kind, merged, rebuilt)
		}
		if !merged.Present(field).Equals(rebuilt.Present(field)) {
			t.Fatalf("field %q: Present differs: merged %v, rebuilt %v",
				field, merged.Present(field).ToArray(), rebuilt.Present(field).ToArray())
		}
	}
	for ord := range int(merged.NumDocs()) {
		mb, err := merged.Stored(uint32(ord))
		if err != nil {
			t.Fatal(err)
		}
		rb, err := rebuilt.Stored(uint32(ord))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(mb, rb) {
			t.Fatalf("Stored(%d): merged %q, rebuilt %q", ord, mb, rb)
		}
		mn, _ := merged.Numbers("price").Value(uint32(ord))
		rn, _ := rebuilt.Numbers("price").Value(uint32(ord))
		if mn != rn {
			t.Fatalf("price.Value(%d): merged %v, rebuilt %v", ord, mn, rn)
		}
	}
}

func compareTerms(t *testing.T, field string, kind TermKind, a, b *Reader) {
	t.Helper()
	seen := map[string]bool{}
	mismatch := false
	check := func(r *Reader) {
		r.Terms(field, kind, "", func(term string, _ uint32) bool {
			seen[term] = true
			if !a.Postings(field, kind, term).Equals(b.Postings(field, kind, term)) {
				mismatch = true
			}
			return true
		})
	}
	check(a)
	check(b)
	if mismatch {
		t.Fatalf("field %q kind %v: postings differ between merged and rebuilt", field, kind)
	}
}

func TestConcurrentReaderClose(t *testing.T) {
	docs := testDocs(t)
	dir := t.TempDir()
	meta, err := Build(dir, docs, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(meta.Path)
	if err != nil {
		t.Fatal(err)
	}

	// Retain every handle up front, while r is unambiguously still open (Retain's own
	// contract: it must not race an unordered Close of the handle it is called on -
	// see its doc comment). What this test means to exercise is the next part: the
	// mapping must stay alive under concurrent use and Close of many handles, with
	// the original's Close racing the others, not whether Retain itself is safe to
	// call concurrently with a Close that could already be dropping the last
	// reference.
	retained := make([]*Reader, 20)
	for i := range retained {
		retained[i] = r.Retain()
	}

	var wg sync.WaitGroup
	for _, rr := range retained {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				rr.Postings("brand", KindValue, "acme")
				_, _ = rr.Stored(0)
				rr.Present("title")
			}
			if err := rr.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	// Close the original concurrently with the retained handles still in flight; the
	// mapping must stay alive until every handle, including this one, closes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = r.Close()
	}()
	wg.Wait()
}

// TestBitmapValidAcrossRetainClose pins down the fix for the zero-copy aliasing bug:
// a bitmap fetched from one handle must keep reading correctly after a DIFFERENT
// handle on the same segment closes, as long as the handle the bitmap came from (or
// another Retain of it) is still open - the pattern Task 5's Generation relies on.
func TestBitmapValidAcrossRetainClose(t *testing.T) {
	docs := testDocs(t)
	dir := t.TempDir()
	meta, err := Build(dir, docs, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original, err := Open(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	retained := original.Retain()

	want := roaring.BitmapOf(0, 1) // brand=acme covers d1, d2
	got := retained.Postings("brand", KindValue, "acme")
	if !got.Equals(want) {
		t.Fatalf("before Close: Postings = %v, want %v", got.ToArray(), want.ToArray())
	}

	// Closing the handle the bitmap did NOT come from must not disturb it: the
	// mapping stays alive because retained is still open.
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	if !got.Equals(want) {
		t.Fatalf("after closing the other handle: Postings = %v, want %v (stale read)", got.ToArray(), want.ToArray())
	}

	// A fresh fetch from the still-open handle must also still work.
	got2 := retained.Postings("brand", KindValue, "acme")
	if !got2.Equals(want) {
		t.Fatalf("fresh fetch after closing the other handle: Postings = %v, want %v", got2.ToArray(), want.ToArray())
	}
	present := retained.Present("title")
	if present.IsEmpty() {
		t.Fatal("Present after closing the other handle returned empty")
	}
	truncated := retained.Truncated("title").Clone() // outlives this Reader by design

	if err := retained.Close(); err != nil {
		t.Fatal(err)
	}
	if !truncated.Contains(4) {
		t.Fatal("a Clone()'d bitmap did not survive Close, as documented it should")
	}
}

// TestDebugGuardFaultsOnUseAfterClose only runs when built with -tags
// searchlight_debug (see debug_on.go): it re-execs itself as a subprocess that keeps a
// zero-copy Postings bitmap past the segment's last Close and then reads it, and
// checks the subprocess died of a memory access fault at exactly that read - not any
// non-zero exit, which a panic, a failed Build or a test failure would also produce.
// Plain `go test` (without the tag) skips it, since debugGuard is false and there is
// nothing to demonstrate.
func TestDebugGuardFaultsOnUseAfterClose(t *testing.T) {
	if !debugGuard {
		t.Skip("only meaningful with -tags searchlight_debug")
	}
	if dir := os.Getenv("SEARCHLIGHT_DEBUG_GUARD_CHILD"); dir != "" {
		runUseAfterCloseChild(filepath.Clean(dir))
		return // unreachable if the guard works: the access above should fault first
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run", "^TestDebugGuardFaultsOnUseAfterClose$", "-test.v")
	cmd.Env = append(os.Environ(), "SEARCHLIGHT_DEBUG_GUARD_CHILD="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("child process exited cleanly instead of faulting on the poisoned mapping; output:\n%s", out)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("child process failed to even start: %v", err)
	}
	text := string(out)
	_, after, found := strings.Cut(text, debugGuardMarker)
	if !found {
		t.Fatalf("child died before reaching the poisoned read (%v); output:\n%s", exitErr, text)
	}
	// The Go runtime reports a fault in Go code as "unexpected fault address" and
	// "fatal error: fault", then the signal: SIGSEGV on unix, the access violation
	// exception code 0xc0000005 on Windows.
	accessFault := strings.Contains(after, "SIGSEGV") || strings.Contains(after, "0xc0000005")
	if !strings.Contains(after, "fatal error: fault") || !accessFault {
		t.Fatalf("child died after the poisoned read, but not of a memory access fault (%v); output:\n%s", exitErr, text)
	}
	t.Logf("child faulted on the poisoned read as expected (%v)", exitErr)
}

// debugGuardMarker is what the child prints just before its use-after-close read, so
// the parent can tell a fault at that read from a failure anywhere earlier.
const debugGuardMarker = "searchlight-debug-guard: reading a bitmap after Close"

// runUseAfterCloseChild is the body of the subprocess TestDebugGuardFaultsOnUseAfterClose
// launches, building its segment in dir (the parent's t.TempDir, which the parent's
// cleanup can remove only because poisoning releases the file): it must crash, not
// return, so the parent never gets to assert on got.
func runUseAfterCloseChild(dir string) {
	m := &schema.Mapping{Fields: map[string]schema.FieldType{"brand": schema.Keyword}}
	// Two documents sharing the term forces the roaring-serialized (docFreq > 1) path,
	// a real zero-copy view over the mapping - a single document would hit the inline
	// (docFreq == 1) path, which is already a fresh, non-aliasing bitmap and would not
	// exercise the bug this guards against.
	doc1, _, err := schema.Analyze(m, "d1", []byte(`{"brand": "acme"}`))
	if err != nil {
		panic(err)
	}
	doc2, _, err := schema.Analyze(m, "d2", []byte(`{"brand": "acme"}`))
	if err != nil {
		panic(err)
	}
	meta, err := Build(dir, []schema.Doc{doc1, doc2}, BuildOptions{})
	if err != nil {
		panic(err)
	}
	r, err := Open(meta.Path)
	if err != nil {
		panic(err)
	}
	got := r.Postings("brand", KindValue, "acme")
	if err := r.Close(); err != nil {
		panic(err)
	}
	// The mapping is now poisoned (an inaccessible reservation), not merely unmapped.
	// got's containers still point into it, so this read must fault.
	fmt.Fprintln(os.Stderr, debugGuardMarker)
	_ = got.ToArray()
}

// emptyTermDocs is the N1 repro: a part whose only term for a (field, kind) is "" must
// still sort "" first. Before the fix, an empty-but-nil arena made "" look like "no
// minimum yet" to writeMergedDict, so the next part's "b" won and "" was written after
// it - out of order, so Postings("") missed and the keyword ordinals were reversed.
func emptyTermDocs(t testing.TB) []schema.Doc {
	t.Helper()
	m := &schema.Mapping{Fields: map[string]schema.FieldType{"brand": schema.Keyword}}
	return []schema.Doc{
		mustAnalyze(t, m, "d0", `{"brand": ""}`),
		mustAnalyze(t, m, "d1", `{"brand": "b"}`),
	}
}

// checkEmptyTermSegment asserts r holds emptyTermDocs exactly: "" sorts first with
// ordinal 0 and doc 0, "b" second with ordinal 1 and doc 1.
func checkEmptyTermSegment(t *testing.T, r *Reader) {
	t.Helper()
	var terms []string
	r.Terms("brand", KindValue, "", func(term string, _ uint32) bool {
		terms = append(terms, term)
		return true
	})
	if !slices.Equal(terms, []string{"", "b"}) {
		t.Fatalf("Terms(brand) = %q, want [\"\" \"b\"]", terms)
	}
	if got := r.Postings("brand", KindValue, ""); !got.Equals(roaring.BitmapOf(0)) {
		t.Fatalf(`Postings(brand, "") = %v, want [0]`, got.ToArray())
	}
	if got := r.Postings("brand", KindValue, "b"); !got.Equals(roaring.BitmapOf(1)) {
		t.Fatalf(`Postings(brand, "b") = %v, want [1]`, got.ToArray())
	}
	kw := r.Keywords("brand")
	for doc, want := range []string{"", "b"} {
		ord, ok := kw.Ord(uint32(doc))
		if !ok || ord != uint32(doc) || kw.Term(ord) != want {
			t.Fatalf("Keywords(brand).Ord(%d) = %d, %v (term %q), want %d (term %q)", doc, ord, ok, kw.Term(ord), doc, want)
		}
	}
}

func TestEmptyTermSortsFirstAcrossParts(t *testing.T) {
	docs := emptyTermDocs(t)
	for _, threads := range []int{1, 2} {
		t.Run(fmt.Sprintf("Build/Threads%d", threads), func(t *testing.T) {
			meta, err := Build(t.TempDir(), docs, BuildOptions{Threads: threads})
			if err != nil {
				t.Fatal(err)
			}
			r, err := Open(meta.Path)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			checkEmptyTermSegment(t, r)
		})
	}
	t.Run("Merge", func(t *testing.T) {
		dir := t.TempDir()
		var readers []*Reader
		for _, d := range docs {
			meta, err := Build(dir, []schema.Doc{d}, BuildOptions{})
			if err != nil {
				t.Fatal(err)
			}
			r, err := Open(meta.Path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = r.Close() })
			readers = append(readers, r)
		}
		meta, err := Merge(dir, readers, nil, MergeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		r, err := Open(meta.Path)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		checkEmptyTermSegment(t, r)
	})
}

// sparseDocs builds n documents where every field is present only some of the time and
// values deliberately include the edge cases a dense corpus never hits: "" keywords and
// text, empty and blank-only lists, a field only one document has, and a field nothing
// but deleted documents might have. Deterministic for a given n.
func sparseDocs(t testing.TB, n int) []schema.Doc {
	t.Helper()
	m := &schema.Mapping{Fields: map[string]schema.FieldType{
		"title":   schema.Text,
		"brand":   schema.Keyword,
		"tags":    schema.KeywordList,
		"price":   schema.Number,
		"active":  schema.Bool,
		"created": schema.Date,
	}}
	brands := []string{`""`, `"b"`, `"a"`, `"Ä"`, `" "`, `"zz"`}
	titles := []string{`""`, `"x"`, `"red shoe"`, `"Straße"`, `"a  b"`}
	tags := []string{`[]`, `[""]`, `"a, , b"`, `["b", "a"]`, `""`, `["only"]`}
	state := uint64(12345)
	next := func(k int) int {
		state = state*6364136223846793005 + 1442695040888963407
		return int((state >> 33) % uint64(k))
	}
	docs := make([]schema.Doc, 0, n)
	for i := range n {
		var parts []string
		if next(3) == 0 {
			parts = append(parts, `"brand": `+brands[next(len(brands))])
		}
		if next(4) == 0 {
			parts = append(parts, `"title": `+titles[next(len(titles))])
		}
		if next(3) == 0 {
			parts = append(parts, `"tags": `+tags[next(len(tags))])
		}
		if next(5) == 0 {
			parts = append(parts, fmt.Sprintf(`"price": %d.%d`, next(7)-3, next(10)))
		}
		if next(6) == 0 {
			parts = append(parts, fmt.Sprintf(`"active": %t`, next(2) == 0))
		}
		if next(9) == 0 {
			parts = append(parts, `"created": "2026-01-02T03:04:05Z"`)
		}
		if i == n/2 {
			parts = append(parts, `"lonely": "one document has this"`)
		}
		body := "{" + strings.Join(parts, ", ") + "}"
		docs = append(docs, mustAnalyze(t, m, fmt.Sprintf("s%d", i), body))
	}
	return docs
}

// identityCorpora is the corpus matrix N8 asks for: dense and sparse documents, the
// Review Focus 1 edge cases, a single document, and none at all.
func identityCorpora(t *testing.T) map[string][]schema.Doc {
	t.Helper()
	return map[string][]schema.Doc{
		"dense":      genCorpus(300),
		"sparse":     sparseDocs(t, 300),
		"testDocs":   testDocs(t),
		"emptyTerm":  emptyTermDocs(t),
		"singleDoc":  sparseDocs(t, 1),
		"singleDoc2": testDocs(t)[:1],
		"noDocs":     nil,
	}
}

func buildBytes(t *testing.T, dir, name string, docs []schema.Doc, threads int) []byte {
	t.Helper()
	meta, err := Build(dir, docs, BuildOptions{Name: name, Threads: threads})
	if err != nil {
		t.Fatalf("Build(%s, Threads: %d): %v", name, threads, err)
	}
	b, err := os.ReadFile(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestBuildThreadsByteIdenticalMatrix extends TestBuildThreadsByteIdentical over
// identityCorpora and thread counts below, at, and far above the document count (the
// short tier: GOMAXPROCS and one count in between).
func TestBuildThreadsByteIdenticalMatrix(t *testing.T) {
	for name, docs := range identityCorpora(t) {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			want := buildBytes(t, dir, "t1", docs, 1)
			for _, threads := range testtier.Pick([]int{0, 17}, []int{0, 2, 5, 17, 64, 301}) {
				got := buildBytes(t, dir, fmt.Sprintf("t%d", threads), docs, threads)
				if !bytes.Equal(got, want) {
					t.Fatalf("Threads: %d wrote different bytes than Threads: 1 (%d vs %d bytes)", threads, len(got), len(want))
				}
			}
		})
	}
}

// TestMergeByteIdenticalToRebuild is the merge half of N8: for every corpus, split into
// several segment counts (one document per segment included), with and without deletes,
// at several GOMAXPROCS values (Merge's own worker count), the merged file must be
// byte-for-byte a fresh Build of the live documents in merge order. The short tier
// runs one segment count and one GOMAXPROCS.
func TestMergeByteIdenticalToRebuild(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	for name, docs := range identityCorpora(t) {
		t.Run(name, func(t *testing.T) {
			for _, segs := range testtier.Pick([]int{5}, []int{1, 2, 5, 17}) {
				for _, withDeletes := range []bool{false, true} {
					for _, procs := range testtier.Pick([]int{3}, []int{1, 3, 16}) {
						runtime.GOMAXPROCS(procs)
						checkMergeEqualsRebuild(t, docs, segs, withDeletes)
					}
				}
			}
		})
	}
}

func checkMergeEqualsRebuild(t *testing.T, docs []schema.Doc, segs int, withDeletes bool) {
	t.Helper()
	dir := t.TempDir()
	ranges := splitRanges(uint32(len(docs)), segs)
	readers := make([]*Reader, 0, len(ranges))
	deletes := make([]*roaring.Bitmap, 0, len(ranges))
	var live []schema.Doc
	for i, rg := range ranges {
		part := docs[rg.start:rg.end]
		meta, err := Build(dir, part, BuildOptions{Name: fmt.Sprintf("in%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		r, err := Open(meta.Path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		readers = append(readers, r)
		del := roaring.New()
		for j := range part {
			if withDeletes && (j+i)%3 == 0 {
				del.Add(uint32(j))
				continue
			}
			live = append(live, part[j])
		}
		deletes = append(deletes, del)
	}
	merged, err := Merge(dir, readers, deletes, MergeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(merged.Path)
	if err != nil {
		t.Fatal(err)
	}
	want := buildBytes(t, dir, "rebuild", live, 1)
	if !bytes.Equal(got, want) {
		t.Fatalf("%d segments, deletes %v, GOMAXPROCS %d: merge wrote %d bytes, a rebuild of the %d live docs %d bytes, and they differ",
			segs, withDeletes, runtime.GOMAXPROCS(0), len(got), len(live), len(want))
	}
}

func TestMergeOptions(t *testing.T) {
	docs := testDocs(t)
	dir := t.TempDir()
	meta, err := Build(dir, docs, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Name and Threads are honoured, and the result is the same for any Threads.
	var files [][]byte
	for i, threads := range []int{1, 3} {
		name := fmt.Sprintf("merged-%d", i)
		m, err := Merge(dir, []*Reader{r}, nil, MergeOptions{Name: name, Threads: threads})
		if err != nil {
			t.Fatal(err)
		}
		if m.ID != name || filepath.Base(m.Path) != name+FileExt {
			t.Fatalf("Merge named %q (%s), want %q", m.ID, m.Path, name)
		}
		b, err := os.ReadFile(m.Path)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, b)
	}
	if !bytes.Equal(files[0], files[1]) {
		t.Fatal("Merge with 1 and 3 threads wrote different files")
	}

	// Throttle sees every byte written, and an error from it aborts the merge and
	// leaves no file behind.
	var written int
	m, err := Merge(dir, []*Reader{r}, nil, MergeOptions{Name: "throttled", Throttle: func(n int) error {
		written += n
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(m.Path)
	if err != nil {
		t.Fatal(err)
	}
	if int64(written) != info.Size() {
		t.Fatalf("Throttle saw %d bytes, the file has %d", written, info.Size())
	}
	stop := errors.New("budget exhausted")
	_, err = Merge(dir, []*Reader{r}, nil, MergeOptions{Name: "aborted", Throttle: func(int) error { return stop }})
	if !errors.Is(err, stop) {
		t.Fatalf("Merge with a failing Throttle: err = %v, want %v", err, stop)
	}
	for _, name := range []string{"aborted" + FileExt, "aborted" + FileExt + ".tmp"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("%s left behind by an aborted merge (stat err %v)", name, err)
		}
	}
}

// Ord finds a document by its exact id, as given: ids that _id's normalization folds
// together (case, ß, İ, runs of spaces) stay distinct, in a built and a merged segment.
func TestExactIDs(t *testing.T) {
	m := testMapping()
	ids := []string{"SKU-1", "sku-1", "Sku-1", "Straße", "STRASSE", "strasse", "İstanbul", "i̇stanbul", "a  b", "a b", " a", "a", "ﬁle", "file"}
	var docs []schema.Doc
	for _, id := range ids {
		docs = append(docs, mustAnalyze(t, m, id, `{"brand":"x"}`))
	}
	check := func(r *Reader, name string, want map[string]uint32) {
		t.Helper()
		for id, ord := range want {
			got, ok := r.Ord(id)
			if !ok || got != ord {
				t.Fatalf("%s: Ord(%q) = %d, %v; want %d", name, id, got, ok, ord)
			}
			if back, err := r.ID(ord); err != nil || back != id {
				t.Fatalf("%s: ID(%d) = %q, %v; want %q", name, ord, back, err, id)
			}
		}
		for _, absent := range []string{"SKU-2", "sKU-1", "straße ", "", "a   b"} {
			if ord, ok := r.Ord(absent); ok {
				t.Fatalf("%s: Ord(%q) = %d for an id no document has", name, absent, ord)
			}
		}
	}
	built := mustBuild(t, docs)
	want := map[string]uint32{}
	for i, id := range ids {
		want[id] = uint32(i)
	}
	check(built, "built", want)

	// Merged with some deleted: the survivors keep exact lookups at their new ordinals.
	dir := t.TempDir()
	del := roaring.BitmapOf(1, 4, 9)
	meta, err := Merge(dir, []*Reader{built}, []*roaring.Bitmap{del}, MergeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	merged, err := Open(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Close()
	want = map[string]uint32{}
	next := uint32(0)
	for i, id := range ids {
		if !del.Contains(uint32(i)) {
			want[id] = next
			next++
		}
	}
	check(merged, "merged", want)
	for _, gone := range []string{"sku-1", "STRASSE", "a b"} {
		if _, ok := merged.Ord(gone); ok {
			t.Fatalf("merged: deleted %q still found", gone)
		}
	}
	// An empty segment has no ids at all.
	empty := mustBuild(t, nil)
	if _, ok := empty.Ord("x"); ok {
		t.Fatal("an empty segment found an id")
	}
}

func TestDuplicateIDsRefused(t *testing.T) {
	m := testMapping()
	a := mustAnalyze(t, m, "same", `{"brand":"a"}`)
	b := mustAnalyze(t, m, "same", `{"brand":"b"}`)
	c := mustAnalyze(t, m, "Same", `{"brand":"c"}`) // a different exact id
	dir := t.TempDir()
	_, err := Build(dir, []schema.Doc{a, c, b}, BuildOptions{})
	var de *DuplicateIDError
	if !errors.As(err, &de) || de.ID != "same" {
		t.Fatalf("Build with a duplicate id: %v, want a DuplicateIDError for %q", err, "same")
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("a refused Build left %d files", len(entries))
	}
	// Two segments that each hold "same", merged with neither deleted.
	r1 := mustBuild(t, []schema.Doc{a, c})
	r2 := mustBuild(t, []schema.Doc{b})
	if _, err := Merge(dir, []*Reader{r1, r2}, nil, MergeOptions{}); !errors.As(err, &de) || de.ID != "same" {
		t.Fatalf("Merge of a duplicate id: %v", err)
	}
	// With one of the copies deleted, the merge is fine.
	if _, err := Merge(dir, []*Reader{r1, r2}, []*roaring.Bitmap{roaring.BitmapOf(0)}, MergeOptions{}); err != nil {
		t.Fatal(err)
	}
}

// NoDirSync still fsyncs each file, but leaves the directory fsync to the caller.
func TestNoDirSync(t *testing.T) {
	docs := testDocs(t)
	dir := t.TempDir()
	count := func(f func()) SyncStats {
		before := SyncCounts()
		f()
		after := SyncCounts()
		return SyncStats{Files: after.Files - before.Files, Dirs: after.Dirs - before.Dirs}
	}
	var meta Meta
	if got := count(func() {
		var err error
		if meta, err = Build(dir, docs, BuildOptions{NoDirSync: true}); err != nil {
			t.Fatal(err)
		}
	}); got.Files < 1 || got.Dirs != 0 {
		t.Fatalf("Build with NoDirSync: %+v syncs, want the file's and no directory's", got)
	}
	if got := count(func() {
		if _, err := Build(dir, docs, BuildOptions{}); err != nil {
			t.Fatal(err)
		}
	}); got.Files < 1 || got.Dirs != 1 {
		t.Fatalf("Build: %+v syncs, want the file's and the directory's", got)
	}
	r, err := Open(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got := count(func() {
		if _, err := Merge(dir, []*Reader{r}, nil, MergeOptions{NoDirSync: true}); err != nil {
			t.Fatal(err)
		}
	}); got.Files < 1 || got.Dirs != 0 {
		t.Fatalf("Merge with NoDirSync: %+v syncs", got)
	}
	if got := count(func() {
		if err := WriteDeletes(dir, meta.ID, 1, roaring.BitmapOf(1), DeletesOptions{NoDirSync: true}); err != nil {
			t.Fatal(err)
		}
	}); got.Files != 1 || got.Dirs != 0 {
		t.Fatalf("WriteDeletes with NoDirSync: %+v syncs", got)
	}
}

func TestNumTermsAndResidentBytes(t *testing.T) {
	r := mustBuild(t, testDocs(t))
	var want uint64
	for name, fi := range r.fields {
		for k := range TermKind(numKinds) {
			if fi.dicts[k] == nil {
				continue
			}
			r.Terms(name, k, "", func(string, uint32) bool { want++; return true })
		}
	}
	if got := r.NumTerms(); got != want || want == 0 {
		t.Fatalf("NumTerms = %d, want %d", got, want)
	}
	for ord := range r.NumDocs() {
		if _, err := r.Stored(ord); err != nil { // touch the stored blocks
			t.Fatal(err)
		}
	}
	got, err := r.ResidentBytes()
	if errors.Is(err, errors.ErrUnsupported) {
		t.Skipf("no residency on %s", runtime.GOOS)
	}
	if err != nil {
		t.Fatal(err)
	}
	if got <= 0 || got > int64(len(r.data)) {
		t.Fatalf("ResidentBytes = %d of a %d-byte mapping", got, len(r.data))
	}
	_ = r.Close()
	if _, err := r.ResidentBytes(); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("ResidentBytes after Close = %v", err)
	}
}
