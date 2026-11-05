package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/schema"
)

func nestedMapping() *schema.Mapping {
	return &schema.Mapping{Fields: map[string]schema.FieldType{
		"name":        schema.Text,
		"_v":          schema.Nested,
		"_v._key":     schema.Keyword,
		"_v.size":     schema.Keyword,
		"_v.price":    schema.Number,
		"_v.in_stock": schema.Bool,
		"_v.tags":     schema.KeywordList,
		"_v.title":    schema.Text,
		"other":       schema.Nested,
		"other.n":     schema.Number,
	}}
}

// nestedDocs is n documents of nestedMapping: some with no _v, an empty one, members
// that are no object, objects missing sub-fields, and a second nested field.
func nestedDocs(t testing.TB, rng *rand.Rand, n int) []schema.Doc {
	t.Helper()
	m := nestedMapping()
	sizes := []string{"S", "M", "L", "XL"}
	docs := make([]schema.Doc, 0, n)
	for i := range n {
		parts := []string{fmt.Sprintf(`"name": "product %d"`, i)}
		switch rng.IntN(6) {
		case 0:
		case 1:
			parts = append(parts, `"_v": []`)
		default:
			var members []string
			for k := range rng.IntN(5) + 1 {
				if rng.IntN(7) == 0 {
					members = append(members, `"not an object"`)
					continue
				}
				var fields []string
				fields = append(fields, fmt.Sprintf(`"_key": "SKU-%d-%d"`, i, k))
				if rng.IntN(4) > 0 {
					fields = append(fields, fmt.Sprintf(`"size": %q`, sizes[rng.IntN(len(sizes))]))
				}
				if rng.IntN(4) > 0 {
					fields = append(fields, fmt.Sprintf(`"price": %d`, 1000+rng.IntN(20000)))
				}
				if rng.IntN(3) > 0 {
					fields = append(fields, fmt.Sprintf(`"in_stock": %t`, rng.IntN(2) == 0))
				}
				if rng.IntN(5) == 0 {
					fields = append(fields, `"tags": ["sale", "new"]`)
				}
				if rng.IntN(5) == 0 {
					fields = append(fields, fmt.Sprintf(`"title": "variant number %d"`, k))
				}
				members = append(members, "{"+strings.Join(fields, ", ")+"}")
			}
			parts = append(parts, `"_v": [`+strings.Join(members, ", ")+`]`)
		}
		if rng.IntN(5) == 0 {
			parts = append(parts, fmt.Sprintf(`"other": {"n": %d}`, i))
		}
		docs = append(docs, mustAnalyze(t, m, fmt.Sprintf("p%d", i), "{"+strings.Join(parts, ", ")+"}"))
	}
	return docs
}

// A nested field's objects are their own index, each document's a run of ordinals in
// array order: a member that is no object holds its ordinal and nothing else.
func TestNestedObjects(t *testing.T) {
	m := nestedMapping()
	docs := []schema.Doc{
		mustAnalyze(t, m, "a", `{"_v": [{"size": "L", "price": 10}, {"size": "M", "price": 20}]}`),
		mustAnalyze(t, m, "b", `{"_v": ["x", {"size": "L"}]}`),
		mustAnalyze(t, m, "c", `{"name": "none"}`),
		mustAnalyze(t, m, "d", `{"_v": []}`),
		mustAnalyze(t, m, "e", `{"_v": {"size": "S"}}`),
	}
	meta, err := Build(t.TempDir(), docs, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	n := r.Nested("_v")
	if n == nil || r.Nested("other") != nil || n.NumObjects() != 5 {
		t.Fatalf("nested _v %v, other %v", n, r.Nested("other"))
	}
	for doc, want := range [][2]uint32{{0, 2}, {2, 4}, {4, 4}, {4, 4}, {4, 5}} {
		if lo, hi := n.Range(uint32(doc)); lo != want[0] || hi != want[1] {
			t.Fatalf("doc %d: objects [%d, %d), want %v", doc, lo, hi, want)
		}
	}
	o := n.Objects()
	if got := o.Present("_v").ToArray(); !slices.Equal(got, []uint32{0, 1, 3, 4}) {
		t.Fatalf("objects %v", got)
	}
	if got := o.Postings("_v.size", KindValue, "l").ToArray(); !slices.Equal(got, []uint32{0, 3}) {
		t.Fatalf("size L: %v", got)
	}
	if v, ok := o.Numbers("_v.price").Value(1); !ok || v != 20 {
		t.Fatalf("price of object 1: %v %v", v, ok)
	}
	if got := n.Parents(roaring.BitmapOf(1, 3, 4)).ToArray(); !slices.Equal(got, []uint32{0, 1, 4}) {
		t.Fatalf("parents %v", got)
	}
	if got := n.ObjectsOf(roaring.BitmapOf(1, 2, 3)).ToArray(); !slices.Equal(got, []uint32{2, 3}) {
		t.Fatalf("objects of b, c, d: %v", got)
	}
	if got := n.ObjectsOf(roaring.BitmapOf(0, 1, 2, 3, 4)).GetCardinality(); got != 5 {
		t.Fatalf("objects of every document: %d", got)
	}
	for obj, want := range []uint32{0, 0, 1, 1, 4} {
		if got := n.Parent(uint32(obj)); got != want {
			t.Fatalf("parent of %d: %d, want %d", obj, got, want)
		}
	}
	if !r.Present("_v").Contains(3) || r.Keywords("_v.size").Exists() {
		t.Fatal("the documents' own index holds the nested field's presence alone")
	}
	data, err := os.ReadFile(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint16(data[10:]); got != minorNested {
		t.Fatalf("minor %d, want %d", got, minorNested)
	}
}

// A segment without a nested field's members writes no NESTED section, and its minor
// is the one before.
func TestNestedMinorVersion(t *testing.T) {
	m := nestedMapping()
	for name, body := range map[string]string{"empty": `{"_v": []}`, "absent": `{"name": "x"}`, "scalar": `{"_v": "x"}`} {
		meta, err := Build(t.TempDir(), []schema.Doc{mustAnalyze(t, m, "a", body)}, BuildOptions{})
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(meta.Path)
		if err != nil {
			t.Fatal(err)
		}
		if got := binary.LittleEndian.Uint16(data[10:]); got != 0 {
			t.Fatalf("%s: minor %d", name, got)
		}
	}
}

// Merge keeps the objects of live documents, in document order, byte for byte as a
// build of those documents; a field no live document holds a member of is dropped.
func TestNestedMergeDropsDeleted(t *testing.T) {
	m := nestedMapping()
	dir := t.TempDir()
	a := []schema.Doc{
		mustAnalyze(t, m, "a", `{"_v": [{"size": "L"}], "other": {"n": 1}}`),
		mustAnalyze(t, m, "b", `{"_v": [{"size": "M"}, {"size": "S"}]}`),
	}
	metaA, err := Build(dir, a, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ra, err := Open(metaA.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ra.Close() }()
	merged, err := Merge(dir, []*Reader{ra}, []*roaring.Bitmap{roaring.BitmapOf(0)}, MergeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(merged.Path)
	if err != nil {
		t.Fatal(err)
	}
	want := buildBytes(t, dir, "rebuild", a[1:], 1)
	if !bytes.Equal(got, want) {
		t.Fatal("a merge dropping a document differs from a build of the rest")
	}
	r, err := Open(merged.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if r.Nested("other") != nil || r.Nested("_v").NumObjects() != 2 {
		t.Fatal("the merge kept a deleted document's objects")
	}
}

// Damage to a NESTED section is refused at Open, by the file's checksum or the section's
// own checks.
func TestNestedCorruptRefused(t *testing.T) {
	meta, err := Build(t.TempDir(), nestedDocs(t, rand.New(rand.NewPCG(3, 4)), 50), BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	footer, err := verifyFile(meta.Path, data)
	if err != nil {
		t.Fatal(err)
	}
	sec, ok := footer.sections[sectionNested]
	if !ok {
		t.Fatal("no NESTED section")
	}
	for _, at := range []uint64{sec.off, sec.off + sec.n/2, sec.off + sec.n - 9} {
		bad := bytes.Clone(data)
		bad[at] ^= 0x5a
		if _, err := openData(meta.Path, bad); err == nil {
			t.Fatalf("a flipped byte at %d of the NESTED section opened", at)
		}
	}
}
