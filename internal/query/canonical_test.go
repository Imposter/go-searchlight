package query

import (
	"bytes"
	"testing"
)

func canon(t *testing.T, raw string) []byte {
	t.Helper()
	return Canonical(mustParse(t, raw))
}

func TestCanonicalEqualTreesEqualBytes(t *testing.T) {
	cases := []struct {
		name string
		a, b string
	}{
		{"in list reordered", `{"field": "f", "op": "in", "value": [1, 2, 3]}`, `{"field": "f", "op": "in", "value": [3, 1, 2]}`},
		{"in list with a duplicate", `{"field": "f", "op": "in", "value": [1, 2]}`, `{"field": "f", "op": "in", "value": [2, 1, 2]}`},
		{"contains_any reordered", `{"field": "f", "op": "contains_any", "value": ["a", "b"]}`, `{"field": "f", "op": "contains_any", "value": ["b", "a"]}`},
		{"has_all reordered", `{"field": "f", "op": "has_all", "value": ["a", "b"]}`, `{"field": "f", "op": "has_all", "value": ["b", "a"]}`},
		{"words_any reordered", `{"field": "f", "op": "words_any", "value": ["a", "b"]}`, `{"field": "f", "op": "words_any", "value": ["b", "a"]}`},
		{"text normalized", `{"field": "f", "op": "eq", "value": "  Acme  Corp "}`, `{"field": "f", "op": "eq", "value": "acme corp"}`},
		{"exists defaults to true", `{"field": "f", "op": "exists"}`, `{"field": "f", "op": "exists", "value": true}`},
		{"number literal forms", `{"field": "f", "op": "eq", "value": 1}`, `{"field": "f", "op": "eq", "value": 1.0}`},
		{"non-finite numbers alike", `{"field": "f", "op": "eq", "value": 1e400}`, `{"field": "f", "op": "eq", "value": 2e400}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := canon(t, tc.a), canon(t, tc.b)
			if !bytes.Equal(a, b) {
				t.Errorf("Canonical(%s) = %x\nCanonical(%s) = %x\nwant equal", tc.a, a, tc.b, b)
			}
		})
	}
}

func TestCanonicalDifferentTreesDifferentBytes(t *testing.T) {
	cases := []struct {
		name string
		a, b string
	}{
		{"different field", `{"field": "a", "op": "eq", "value": 1}`, `{"field": "b", "op": "eq", "value": 1}`},
		{"different op", `{"field": "f", "op": "eq", "value": 1}`, `{"field": "f", "op": "ne", "value": 1}`},
		{"different value", `{"field": "f", "op": "eq", "value": 1}`, `{"field": "f", "op": "eq", "value": 2}`},
		{"between keeps order", `{"field": "f", "op": "between", "value": [1, 5]}`, `{"field": "f", "op": "between", "value": [5, 1]}`},
		{"all vs any", `{"all": [{"field": "f", "op": "eq", "value": 1}]}`, `{"any": [{"field": "f", "op": "eq", "value": 1}]}`},
		{"different child count", `{"all": [{"field": "f", "op": "eq", "value": 1}]}`, `{"all": [{"field": "f", "op": "eq", "value": 1}, {"field": "g", "op": "eq", "value": 1}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := canon(t, tc.a), canon(t, tc.b)
			if bytes.Equal(a, b) {
				t.Errorf("Canonical(%s) == Canonical(%s) = %x, want different", tc.a, tc.b, a)
			}
		})
	}
}

func TestCanonicalNilAndRoot(t *testing.T) {
	if Canonical(nil) == nil {
		t.Fatal("Canonical(nil) returned nil")
	}
	if !bytes.Equal(Canonical(nil), Canonical((*Leaf)(nil))) {
		t.Error("Canonical(nil) and Canonical((*Leaf)(nil)) differ")
	}
	root := canon(t, `{"all": []}`)
	if len(root) == 0 {
		t.Fatal("Canonical(root) is empty")
	}
}

func TestCanonicalDeterministic(t *testing.T) {
	raw := `{"all": [{"field": "a", "op": "in", "value": [3, 1, 2]}, {"any": [{"field": "b", "op": "similar", "value": {"text": "x", "min": 0.5}}]}]}`
	a, b := canon(t, raw), canon(t, raw)
	if !bytes.Equal(a, b) {
		t.Fatalf("Canonical is not deterministic: %x vs %x", a, b)
	}
}
