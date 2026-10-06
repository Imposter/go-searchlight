package segment

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// compatCorpora are the documents the format-3 files in testdata/v3 were built from, by
// Build of format 3.0, the commit before format 4. markedDocs marks untyped values as
// the replica did under format 3: GramsTruncated on a present value with no text
// (currentDocs turns that into Untyped).
func compatCorpora(t testing.TB) map[string][]schema.Doc {
	return map[string][]schema.Doc{
		"dense":  genCorpus(250),
		"sparse": sparseDocs(t, 400),
		"marked": markedDocs(t, 300),
		"points": pointDocs(t, 3000),
	}
}

// pointDocs are many documents of number and date fields, some missing, ties
// included: enough for dozens of point blocks per field.
func pointDocs(t testing.TB, n int) []schema.Doc {
	t.Helper()
	m := &schema.Mapping{Fields: map[string]schema.FieldType{
		"price":   schema.Number,
		"rating":  schema.Number,
		"created": schema.Date,
	}}
	state := uint64(424242)
	next := func(k uint64) uint64 {
		state = state*6364136223846793005 + 1442695040888963407
		return (state >> 33) % k
	}
	docs := make([]schema.Doc, 0, n)
	for i := range n {
		parts := []string{fmt.Sprintf(`"price": %d.%02d`, next(5000), next(100))}
		if next(5) != 0 {
			parts = append(parts, fmt.Sprintf(`"rating": %d.%d`, 1+next(4), next(10)))
		}
		parts = append(parts, fmt.Sprintf(`"created": %d`, 1_700_000_000_000+next(40)*86_400_000))
		doc, _, err := schema.Analyze(m, fmt.Sprintf("p%05d", i), []byte("{"+strings.Join(parts, ", ")+"}"))
		if err != nil {
			t.Fatal(err)
		}
		docs = append(docs, doc)
	}
	return docs
}

// markedDocs maps title and brand only; extra (typeable) and obj (not) are unmapped,
// and every eleventh title is too long to have grams.
func markedDocs(t testing.TB, n int) []schema.Doc {
	t.Helper()
	m := &schema.Mapping{Dynamic: schema.DynamicTrue, Fields: map[string]schema.FieldType{
		"title": schema.Text,
		"brand": schema.Keyword,
	}}
	long := strings.Repeat("long title words ", 80)
	docs := make([]schema.Doc, 0, n)
	for i := range n {
		var parts []string
		title := fmt.Sprintf("title %d", i)
		if i%11 == 0 {
			title = long + fmt.Sprint(i)
		}
		parts = append(parts, fmt.Sprintf(`"title": %q`, title), fmt.Sprintf(`"brand": "b%d"`, i%5))
		switch i % 4 {
		case 0:
			parts = append(parts, `"extra": "typed text"`)
		case 1:
			parts = append(parts, `"obj": {}`)
		case 2:
			parts = append(parts, `"extra": [], "obj": {"a": 1}`)
		}
		doc, upd, err := schema.Analyze(m, fmt.Sprintf("m%d", i), []byte("{"+strings.Join(parts, ", ")+"}"))
		if err != nil {
			t.Fatal(err)
		}
		for f := range upd.Fields {
			doc.Fields[f] = markUntyped()
		}
		docs = append(docs, doc)
	}
	return docs
}

// markUntyped is a format-3 untyped mark.
func markUntyped() schema.Value { return schema.Value{Present: true, GramsTruncated: true} }
