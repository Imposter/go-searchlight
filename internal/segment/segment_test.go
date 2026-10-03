package segment

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/schema"
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

func mustAnalyze(t *testing.T, m *schema.Mapping, id, body string) schema.Doc {
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

func testDocs(t *testing.T) []schema.Doc {
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
	if err := WriteDeletes(dir, "seg1", 7, want); err != nil {
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

	mergedMeta, err := Merge(dir, []*Reader{rA, rB}, []*roaring.Bitmap{delA, delB})
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
// checks the subprocess crashed rather than returning wrong data. Plain `go test`
// (without the tag) skips it, since debugGuard is false and there is nothing to
// demonstrate.
func TestDebugGuardFaultsOnUseAfterClose(t *testing.T) {
	if !debugGuard {
		t.Skip("only meaningful with -tags searchlight_debug")
	}
	if os.Getenv("SEARCHLIGHT_DEBUG_GUARD_CHILD") == "1" {
		runUseAfterCloseChild()
		return // unreachable if the guard works: the access above should fault first
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run", "TestDebugGuardFaultsOnUseAfterClose", "-test.v")
	cmd.Env = append(os.Environ(), "SEARCHLIGHT_DEBUG_GUARD_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("child process exited cleanly instead of faulting on the poisoned mapping; output:\n%s", out)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("child process failed to even start: %v", err)
	}
	t.Logf("child faulted as expected (%v); output:\n%s", exitErr, out)
}

// runUseAfterCloseChild is the body of the subprocess TestDebugGuardFaultsOnUseAfterClose
// launches: it must crash, not return, so the parent never gets to assert on got.
func runUseAfterCloseChild() {
	dir, err := os.MkdirTemp("", "searchlight-debug-guard")
	if err != nil {
		panic(err)
	}
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
	// The mapping is now poisoned (PROT_NONE / PAGE_NOACCESS), not merely unmapped.
	// got's containers still point into it, so this read must fault.
	_ = got.ToArray()
}
