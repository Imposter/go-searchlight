package segment

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"testing"

	"github.com/Imposter/go-searchlight/bench/datasets"
	"github.com/Imposter/go-searchlight/internal/schema"
)

func productsMapping() *schema.Mapping {
	m := &schema.Mapping{Dynamic: schema.DynamicStrict, Fields: map[string]schema.FieldType{}}
	for _, f := range datasets.Products {
		t, err := schema.ParseFieldType(string(f.Type))
		if err != nil {
			panic(err)
		}
		m.Fields[f.Name] = t
	}
	return m
}

func productRange(tb testing.TB, from, to int) []schema.Doc {
	tb.Helper()
	m := productsMapping()
	docs := make([]schema.Doc, 0, to-from)
	var buf []byte
	for i := from; i < to; i++ {
		buf = datasets.AppendProduct(buf[:0], 1, int64(i))
		d, _, err := schema.Analyze(m, datasets.ProductID(int64(i)), buf)
		if err != nil {
			tb.Fatal(err)
		}
		docs = append(docs, d)
	}
	return docs
}

// buildProducts builds n products as one segment: built in parts of at most 200k
// documents and merged, as a shard would end up after its merges.
func buildProducts(tb testing.TB, dir string, n int) *Reader {
	tb.Helper()
	const part = 200_000
	var readers []*Reader
	for from := 0; from < n; from += part {
		meta, err := Build(dir, productRange(tb, from, min(from+part, n)), BuildOptions{Threads: runtime.GOMAXPROCS(0), NoSync: true})
		if err != nil {
			tb.Fatal(err)
		}
		r, err := Open(meta.Path)
		if err != nil {
			tb.Fatal(err)
		}
		readers = append(readers, r)
	}
	if len(readers) == 1 {
		return readers[0]
	}
	meta, err := Merge(dir, readers, nil, MergeOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	for _, r := range readers {
		_ = r.Close()
	}
	r, err := Open(meta.Path)
	if err != nil {
		tb.Fatal(err)
	}
	return r
}

// TestDiskBreakdown prints bytes per section, field and structure of one segment built
// from the bench's product dataset. Set SL_BREAKDOWN_DOCS (e.g. 200000) to run it.
func TestDiskBreakdown(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("SL_BREAKDOWN_DOCS"))
	if n <= 0 {
		t.Skip("SL_BREAKDOWN_DOCS unset")
	}
	r := buildProducts(t, t.TempDir(), n)
	defer r.Close()
	u := r.DiskUsage()
	perM := func(b int64) string {
		return fmt.Sprintf("%8.1f MiB/M %6.0f B/doc", float64(b)*1e6/float64(n)/(1<<20), float64(b)/float64(n))
	}
	t.Logf("file %d bytes: %s", u.File, perM(u.File))
	for _, s := range u.Sections {
		t.Logf("  section %-10s %12d %s", s.Name, s.Bytes, perM(s.Bytes))
	}
	parts := append([]DiskPart(nil), u.Parts...)
	sort.Slice(parts, func(i, j int) bool { return parts[i].Bytes > parts[j].Bytes })
	for _, p := range parts {
		if p.Bytes*1000 < u.File {
			continue
		}
		t.Logf("  %-12s %-22s %12d %s", p.Field, p.Part, p.Bytes, perM(p.Bytes))
	}
}
