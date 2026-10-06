package percolate

import (
	"bytes"
	"encoding/json"
	"math/rand/v2"
	"slices"
	"testing"
)

var idAlphabet = []string{"a", "b", "z", "0", "9", "-", "_", " ", "!", "\"", "\\", "<", ">", "&", "\t", "\x01", "é", "日", " ", " ", "�", "😀", "q0000"}

func randomID(r *rand.Rand) string {
	var b []byte
	for range 1 + r.IntN(10) {
		b = append(b, idAlphabet[r.IntN(len(idAlphabet))]...)
	}
	return string(b)
}

// An id's literal is encoding/json's, and recognized as its own.
func TestIDLiteralIsEncodingJSON(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 20000 {
		id := randomID(r)
		want, err := json.Marshal(id)
		if err != nil {
			t.Fatal(err)
		}
		lit := idLiteral(id)
		if !bytes.Equal(lit, want) {
			t.Fatalf("%q: literal %s, encoding/json %s", id, lit, want)
		}
		if !isLiteralOf(lit, []byte(id)) || isLiteralOf(lit, []byte(id+"x")) {
			t.Fatalf("%q: isLiteralOf disagrees", id)
		}
		raw, ok := plainID(lit)
		if !ok {
			raw = decodeLiteral(lit)
		}
		if string(raw) != id {
			t.Fatalf("%q decodes as %q", id, raw)
		}
	}
}

// MergeIDs is the sorted union of its lists, each id once, whatever the ids hold.
func TestMergeIDsIsSortedUnion(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for range 3000 {
		var lists []IDs
		var all []string
		for range r.IntN(6) {
			var ids []string
			for range r.IntN(8) {
				ids = append(ids, randomID(r))
			}
			slices.Sort(ids)
			ids = slices.Compact(ids)
			all = append(all, ids...)
			if len(ids) == 0 {
				lists = append(lists, nil)
				continue
			}
			raw, err := json.Marshal(ids)
			if err != nil {
				t.Fatal(err)
			}
			lists = append(lists, raw)
		}
		slices.Sort(all)
		all = slices.Compact(all)
		merged, err := MergeIDs(lists)
		if err != nil {
			t.Fatal(err)
		}
		got, err := merged.Strings()
		if err != nil {
			t.Fatalf("%s: %v", merged, err)
		}
		if !slices.Equal(got, all) {
			t.Fatalf("merged %q, want %q", got, all)
		}
		if len(all) > 0 {
			want, _ := json.Marshal(all)
			if !bytes.Equal(merged, want) {
				t.Fatalf("merged %s, encoding/json %s", merged, want)
			}
		}
	}
	for _, bad := range []string{`x`, `[`, `["a"`, `["a" "b"]`, `[1]`, `["a\"]`} {
		if _, err := MergeIDs([]IDs{IDs(bad), IDs(`["b"]`)}); err == nil {
			t.Fatalf("%s merged", bad)
		}
	}
}
