package segment

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
)

func TestIDsFrom(t *testing.T) {
	dir := t.TempDir()
	ids := []string{"b", "a", "c", "ab", "B", "ß"}
	docs := make([]schema.Doc, len(ids))
	for i, id := range ids {
		d, _, err := schema.Analyze(nil, id, []byte(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		docs[i] = d
	}
	meta, err := Build(dir, docs, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(filepath.Join(dir, meta.ID+FileExt))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	walk := func(from string, limit int) []string {
		var out []string
		r.IDsFrom(from, func(id []byte, ord uint32) bool {
			if ids[ord] != string(id) {
				t.Errorf("id %q at ordinal %d, which holds %q", id, ord, ids[ord])
			}
			out = append(out, string(id))
			return len(out) < limit
		})
		return out
	}
	for _, tc := range []struct {
		from  string
		limit int
		want  []string
	}{
		{"", 10, []string{"B", "a", "ab", "b", "c", "ß"}},
		{"a", 10, []string{"a", "ab", "b", "c", "ß"}},
		{"aa", 2, []string{"ab", "b"}},
		{"c", 10, []string{"c", "ß"}},
		{"z", 10, []string{"ß"}},
		{"ß~", 10, nil},
	} {
		if got := walk(tc.from, tc.limit); !slices.Equal(got, tc.want) {
			t.Errorf("IDsFrom(%q): %q, want %q", tc.from, got, tc.want)
		}
	}
}
