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
	sorted := []string{"B", "a", "ab", "b", "c", "ß"}
	for i, want := range sorted {
		if got, ok := r.IDAt(uint32(i)); !ok || got != want {
			t.Errorf("IDAt(%d) = %q, %v; want %q", i, got, ok, want)
		}
	}
	if _, ok := r.IDAt(uint32(len(sorted))); ok {
		t.Error("IDAt past the last id")
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

func TestKeywordEachTerm(t *testing.T) {
	dir := t.TempDir()
	m := &schema.Mapping{Fields: map[string]schema.FieldType{"k": schema.Keyword}}
	var docs []schema.Doc
	for i, v := range []string{"pear", "apple", "fig", "apple", "kiwi"} {
		d, _, err := schema.Analyze(m, string(rune('a'+i)), []byte(`{"k":"`+v+`"}`))
		if err != nil {
			t.Fatal(err)
		}
		docs = append(docs, d)
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
	kc := r.Keywords("k")
	var got []string
	kc.EachTerm(1, func(ord uint32, term []byte) bool {
		if kc.Term(ord) != string(term) {
			t.Errorf("ordinal %d: %q, Term says %q", ord, term, kc.Term(ord))
		}
		got = append(got, string(term))
		return true
	})
	if want := []string{"fig", "kiwi", "pear"}; !slices.Equal(got, want) {
		t.Errorf("EachTerm(1): %q, want %q", got, want)
	}
	kc.EachTerm(kc.NumTerms(), func(uint32, []byte) bool { t.Error("past the last term"); return false })
	if r.Closed() {
		t.Error("open reader reports closed")
	}
	_ = r.Close()
	if !r.Closed() {
		t.Error("closed reader reports open")
	}
}
