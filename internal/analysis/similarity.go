package analysis

// Similarity returns how alike a and b are, as Postgres pg_trgm's similarity measures
// it and scrape-bot's trigram_similarity computes it: the trigrams the two texts share
// over the trigrams either has (0 when either has none), rounded to a float32 as pg_trgm
// returns it. See [Trigrams] for how a text's trigrams are taken.
//
// That is pg_trgm exactly for ASCII text. Beyond ASCII it follows Python (Unicode
// lowercase, \w letters), where Postgres would follow its locale.
func Similarity(a, b string) float64 {
	return SimilarityKeys(TrigramKeys(a), TrigramKeys(b))
}

// SimilarityKeys is [Similarity] over two texts' [TrigramKeys] (sorted and distinct), with
// no allocation: a merge count of the shared keys.
func SimilarityKeys(a, b []uint64) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	shared := 0
	for i, j := 0, 0; i < len(a) && j < len(b); {
		switch {
		case a[i] == b[j]:
			shared++
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	ratio := float64(shared) / float64(len(a)+len(b)-shared)
	return float64(float32(ratio))
}
