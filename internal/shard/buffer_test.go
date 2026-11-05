package shard

import (
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// A buffered document's points count toward the buffer's bytes: 40 for each, its
// value's and the build's.
func TestDocBytesCountsPoints(t *testing.T) {
	m := &schema.Mapping{Fields: map[string]schema.FieldType{"p": schema.PointType(schema.Number, schema.Number)}}
	doc := func(body string) *schema.Doc {
		d, _, err := schema.Analyze(m, "a", []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		return &d
	}
	none, many := doc(`{"p": [[1, 1]]}`), doc(`{"p": [[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]]}`)
	if got, want := docBytes("a", many)-docBytes("a", none), int64(len(many.Body)-len(none.Body))+4*pointBytes; got != want {
		t.Fatalf("four more points cost %d bytes, want %d", got, want)
	}
}
