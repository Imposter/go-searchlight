package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// FuzzOpen feeds arbitrary bytes to the segment reader, after giving them a valid
// header, end magic and checksums (fixChecksums) so that mutations reach the parsers
// behind verifyFile instead of all dying at the CRC - the only way a real damaged file
// could reach them is a checksum collision or a crafted file, which is exactly what this
// guards against. Every input must either be refused by Open with a *CorruptError or
// *VersionError, or open and survive every Reader method (exerciseReader) without a
// panic or a huge allocation.
//
// Run it with: go test -run '^$' -fuzz FuzzOpen ./internal/segment/
// Crashers land in testdata/fuzz/FuzzOpen and replay on every plain `go test`.
func FuzzOpen(f *testing.F) {
	for _, seed := range fuzzSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		data := normalizeFuzzInput(in)
		r, err := openData("fuzz", data)
		if err != nil {
			var ce *CorruptError
			var ve *VersionError
			if !errors.As(err, &ce) && !errors.As(err, &ve) {
				t.Fatalf("Open refused the input with %T (%v), want *CorruptError or *VersionError", err, err)
			}
			return
		}
		exerciseReader(r)
		if err := r.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}

// normalizeFuzzInput copies in and stamps it with the current header and end magic
// and matching checksums, wherever it is long enough to hold them.
func normalizeFuzzInput(in []byte) []byte {
	data := append([]byte(nil), in...)
	if len(data) >= headerSize {
		copy(data, magic[:])
		binary.LittleEndian.PutUint16(data[8:], FormatMajor)
		binary.LittleEndian.PutUint16(data[10:], FormatMinor)
	}
	if len(data) >= headerSize+tailSize {
		copy(data[len(data)-tailSize+4:], endMagic[:])
	}
	fixChecksums(data)
	return data
}

// exerciseReader calls every Reader method, and every method of every column it hands
// out, on a bounded sample of terms and documents, consuming each bitmap the way a
// search would (cardinality, membership, iteration, intersection) rather than by
// materializing it whole: a damaged bitmap may legitimately claim up to 2^32 documents,
// and listing those is the caller's choice, not something a reader method does.
func exerciseReader(r *Reader) {
	n := r.NumDocs()
	docs := []uint32{0, 1, 2, n / 2, n - 1, n, n + 1, math.MaxUint32}
	names := make([]string, 0, len(r.fields))
	for name := range r.fields {
		names = append(names, name)
	}
	slices.Sort(names)
	names = append(names, "no-such-field")

	for _, name := range names {
		for kind := range TermKind(numKinds + 1) {
			var terms []string
			r.Terms(name, kind, "", func(term string, docFreq uint32) bool {
				terms = append(terms, term)
				_ = docFreq
				return len(terms) < 32
			})
			terms = append(terms, "", "zzz", "\xff")
			for _, term := range terms {
				consumeBitmap(r.Postings(name, kind, term))
				_ = r.TermFreq(name, kind, term)
				if term != "" {
					r.Terms(name, kind, term[:1], func(string, uint32) bool { return false })
				}
			}
		}

		kw := r.Keywords(name)
		_, _ = kw.Exists(), kw.NumTerms()
		_ = kw.Term(kw.NumTerms())
		for _, doc := range docs {
			if ord, ok := kw.Ord(doc); ok {
				_, _ = kw.Lookup(kw.Term(ord))
				_ = kw.AppendTerm(nil, ord)
			}
		}

		mc := r.Entries(name)
		_, _ = mc.Exists(), mc.NumTerms()
		_ = mc.Term(mc.NumTerms())
		for _, doc := range docs {
			_ = mc.Count(doc)
			for _, ord := range mc.Ords(doc, nil) {
				_, _ = mc.Lookup(mc.Term(ord))
				_ = mc.AppendTerm(nil, ord)
			}
		}

		nc := r.Numbers(name)
		st := nc.Stats()
		_ = nc.Exists()
		for _, doc := range docs {
			_, _ = nc.Value(doc)
		}
		consumeBitmap(nc.Range(math.Inf(-1), math.Inf(1), true, true))
		consumeBitmap(nc.Range(st.Min, st.Max, true, false))
		consumeBitmap(nc.Range(0, 10, false, true))

		consumeBitmap(r.Present(name))
		consumeBitmap(r.Truncated(name))
	}

	for _, doc := range docs {
		_, _ = r.Stored(doc)
		if id, err := r.ID(doc); err == nil {
			_, _ = r.Ord(id)
		}
	}
	_, _ = r.Ord("no-such-id")

	retained := r.Retain()
	consumeBitmap(retained.Present("_id"))
	_ = retained.Close()
}

// consumeBitmap uses rb as a search would, a bounded amount.
func consumeBitmap(rb *roaring.Bitmap) {
	_ = rb.GetCardinality()
	_ = rb.Contains(0)
	_ = rb.Contains(math.MaxUint32)
	if !rb.IsEmpty() {
		_, _ = rb.Minimum(), rb.Maximum()
	}
	it := rb.Iterator()
	for i := 0; i < 64 && it.HasNext(); i++ {
		_ = it.Next()
	}
	_ = roaring.And(rb, roaring.BitmapOf(0, 1, 2, 3, 1000, 70000)).GetCardinality()
	_ = rb.AndCardinality(roaring.BitmapOf(0, 5, 65536))
}

// fuzzSeeds is FuzzOpen's seed corpus: valid segments of every shape the tests know,
// plus one crafted file for each N4 finding (each a panic or a huge allocation before
// its fix), plus the dictionary term-count wrap found while fixing them.
func fuzzSeeds(tb testing.TB) [][]byte {
	numbers := &schema.Mapping{Fields: map[string]schema.FieldType{"n": schema.Number}}
	var numberDocs []schema.Doc
	for i := range 130 { // just over one 128-entry point block
		numberDocs = append(numberDocs, mustAnalyze(tb, numbers, fmt.Sprintf("n%d", i), fmt.Sprintf(`{"n": %d}`, i%97)))
	}
	seeds := [][]byte{
		buildFile(tb, testDocs(tb)),
		buildFile(tb, sparseDocs(tb, 24)),
		buildFile(tb, emptyTermDocs(tb)),
		buildFile(tb, numberDocs),
		buildFile(tb, nil),
	}
	for _, craft := range craftedCases() {
		seeds = append(seeds, craft.make(tb))
	}
	return seeds
}

// craftedCase is one hand-damaged segment: a specific way a checksum-valid file used to
// panic or allocate without bound.
type craftedCase struct {
	name string
	make func(tb testing.TB) []byte
}

func craftedCases() []craftedCase {
	return []craftedCase{
		{"stored block count", func(tb testing.TB) []byte {
			data := craftStoredBlockCount(tb, buildFile(tb, testDocs(tb)))
			fixChecksums(data)
			return data
		}},
		{"term shares more than the previous term has", craftSharedPrefix},
		{"point block outside the file", craftPointBlockOffset},
		{"multi column offsets past its ordinals", craftMultiOffsets},
		{"dictionary term count wraps uint32", craftDictTermCount},
		{"ids ordinal past the documents", craftIDsOrdinal},
		{"ids trailer outside the section", func(tb testing.TB) []byte {
			data := buildFile(tb, testDocs(tb))
			binary.LittleEndian.PutUint64(data[idsTrailer(tb, data):], math.MaxUint64)
			fixChecksums(data)
			return data
		}},
	}
}

// craftIDsOrdinal: the first id's inline ordinal is 200, past the segment's documents.
func craftIDsOrdinal(tb testing.TB) []byte {
	data := buildFile(tb, testDocs(tb))
	dict := openValid(tb, data).ids
	c := dict.cursor(0)
	if _, _, ok := c.next(); !ok || c.entry.docFreq != 1 {
		tb.Fatal("craftIDsOrdinal: no first id")
	}
	// The entry ends with its ordinal: a one-byte uvarint for these small segments.
	if c.entry.single > 0x7f {
		tb.Fatal("craftIDsOrdinal: the ordinal is not one byte")
	}
	data[c.d.pos-1] = 0x48 // 72: past testDocs' documents, still one byte
	fixChecksums(data)
	return data
}

// openValid opens a valid segment held in data, for locating the structure to damage.
func openValid(tb testing.TB, data []byte) *Reader {
	tb.Helper()
	r, err := openData("craft", data)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = r.Close() })
	return r
}

// offsetIn returns where sub (a view into data) starts within data.
func offsetIn(data, sub []byte) uint64 {
	return uint64(uintptr(unsafe.Pointer(unsafe.SliceData(sub))) - uintptr(unsafe.Pointer(unsafe.SliceData(data))))
}

// craftSharedPrefix: the first entry of brand's first value block claims to share 5
// bytes with a previous term it does not have; Terms (termIter) then sliced its term
// buffer to [:5] of nothing.
func craftSharedPrefix(tb testing.TB) []byte {
	data := buildFile(tb, testDocs(tb))
	dict := openValid(tb, data).fields["brand"].dicts[KindValue]
	start := dict.blockOff(0)
	_, n := binary.Uvarint(data[start:]) // postingsLen
	if data[start+uint64(n)] != 0 {
		tb.Fatal("craftSharedPrefix: the first entry's shared prefix is not a one-byte 0")
	}
	data[start+uint64(n)] = 5
	fixChecksums(data)
	return data
}

// craftPointBlockOffset: the first point block's offset points a terabyte past the
// file; rangeDocs used to slice the mapping there unchecked.
func craftPointBlockOffset(tb testing.TB) []byte {
	numbers := &schema.Mapping{Fields: map[string]schema.FieldType{"n": schema.Number}}
	var docs []schema.Doc
	for i := range 200 {
		docs = append(docs, mustAnalyze(tb, numbers, fmt.Sprintf("n%d", i), fmt.Sprintf(`{"n": %d}`, i)))
	}
	data := buildFile(tb, docs)
	p := openValid(tb, data).fields["n"].numberCol.points
	binary.LittleEndian.PutUint64(data[p.base+pointsHeaderLen+20:], 1<<40)
	fixChecksums(data)
	return data
}

// craftMultiOffsets: document 0's span in tags' multi column becomes ordinals 0 to 7
// when the column holds 5, and the ordinal width becomes 56 bits, so the ordinals slice
// (sized for 5 such ordinals) ends well before ordinal 6; Ords used to unpack there
// unchecked and read past it.
func craftMultiOffsets(tb testing.TB) []byte {
	data := buildFile(tb, testDocs(tb))
	mc := openValid(tb, data).fields["tags"].multiCol
	if mc.offWidth != 3 || mc.total != 5 {
		tb.Fatalf("craftMultiOffsets: tags column is %d-bit offsets over %d ordinals, want 3 over 5", mc.offWidth, mc.total)
	}
	start := offsetIn(data, mc.offs)
	data[start-10+1] = 56            // header: u8 offWidth, u8 ordWidth, u64 total
	data[start] = 0<<0 | 7<<3 | 0<<6 // offs[0] = 0, offs[1] = 7, low bit of offs[2] = 0
	fixChecksums(data)
	return data
}

// craftDictTermCount: brand's value dictionary claims 2^32-1 terms in 0 blocks, which
// (2^32-1 + 31) / 32 computed in uint32 wraps to 0 to agree with; the keyword column
// then asked it for ordinals its (empty) block index does not have.
func craftDictTermCount(tb testing.TB) []byte {
	data := buildFile(tb, testDocs(tb))
	dict := openValid(tb, data).fields["brand"].dicts[KindValue]
	binary.LittleEndian.PutUint32(data[dict.base:], math.MaxUint32)
	binary.LittleEndian.PutUint32(data[dict.base+4:], 0)
	// firstOffs[0] now sits where blockOffs[0] was; make it 0 so the index still
	// parses (no first terms) and the open gets as far as the count check.
	binary.LittleEndian.PutUint32(data[dict.base+16:], 0)
	fixChecksums(data)
	return data
}

// TestCraftedSegmentsFailSafely runs each crafted case as a plain test (FuzzOpen's seed
// corpus also replays them), so a regression names the case: each must be refused at
// Open with a *CorruptError, or open and survive exerciseReader.
func TestCraftedSegmentsFailSafely(t *testing.T) {
	for _, c := range craftedCases() {
		t.Run(c.name, func(t *testing.T) {
			r, err := openData("crafted", c.make(t))
			if err != nil {
				var ce *CorruptError
				if !errors.As(err, &ce) {
					t.Fatalf("Open: %v (%T), want *CorruptError", err, err)
				}
				t.Logf("refused at Open: %v", err)
				return
			}
			exerciseReader(r)
			_ = r.Close()
		})
	}
}
