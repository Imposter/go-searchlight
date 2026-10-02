package analysis

import (
	"slices"
	"testing"
)

func TestTrigramKeys(t *testing.T) {
	keys := TrigramKeys("Cat cat CAT!")
	if len(keys) != 4 || !slices.IsSorted(keys) {
		t.Fatalf("TrigramKeys = %x", keys)
	}
	if got := Trigrams("Cat"); !slices.Equal(got, []string{"  c", " ca", "at ", "cat"}) {
		t.Fatalf("Trigrams = %q", got)
	}
	if TrigramKeys("!! __") != nil {
		t.Fatal("keys of a text with no word")
	}
	a, b := TrigramKeys("nike air max 90"), TrigramKeys("nike air max 270")
	if got, want := SimilarityKeys(a, b), Similarity("nike air max 90", "nike air max 270"); got != want || got == 0 {
		t.Fatalf("SimilarityKeys = %v, Similarity = %v", got, want)
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = SimilarityKeys(a, b) }); allocs != 0 {
		t.Fatalf("SimilarityKeys allocates %v times", allocs)
	}
}

func BenchmarkSimilarityKeys(b *testing.B) {
	left, right := TrigramKeys("nike air max 90"), TrigramKeys("nike air max 270")
	b.ReportAllocs()
	for b.Loop() {
		_ = SimilarityKeys(left, right)
	}
}

func BenchmarkTrigramKeys(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = TrigramKeys("Nike Air Max 90 Running Shoe")
	}
}
