package analysis

// Similarity returns how alike a and b are, as Postgres pg_trgm's similarity measures
// it and scrape-bot's trigram_similarity computes it: the trigrams the two texts share
// over the trigrams either has (0 when either has none), rounded to a float32 as pg_trgm
// returns it. See [Trigrams] for how a text's trigrams are taken.
//
// That is pg_trgm exactly for ASCII text. Beyond ASCII it follows Python (Unicode
// lowercase, \w letters), where Postgres would follow its locale.
func Similarity(a, b string) float64 {
	left, right := trigramSet(a), trigramSet(b)
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	if len(left) > len(right) {
		left, right = right, left
	}
	shared := 0
	for gram := range left {
		if _, ok := right[gram]; ok {
			shared++
		}
	}
	ratio := float64(shared) / float64(len(left)+len(right)-shared)
	return float64(float32(ratio))
}
