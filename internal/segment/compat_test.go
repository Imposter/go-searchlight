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
	}
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
