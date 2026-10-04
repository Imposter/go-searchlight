package es

// Caveat is one place where Searchlight and the Elasticsearch translation are not
// identical, or where the translation picks one Elasticsearch structure over another.
type Caveat struct {
	// Area is what it concerns: an operator, the mapping, aggregations, measurement.
	Area string `json:"area"`
	// Difference is what differs between the engines.
	Difference string `json:"difference"`
	// Choice is what the translation does, and why that is the fairest option.
	Choice string `json:"choice"`
	// Benchmark says how the benchmark's data and checks are affected.
	Benchmark string `json:"benchmark"`
}

// Caveats lists every known difference. The report prints them; the cross-check
// tolerates nothing outside them.
var Caveats = []Caveat{
	{
		Area:       "text normalization",
		Difference: "Searchlight folds text with Python's str.casefold (ß→ss, ligatures, Cherokee), its str.isspace whitespace set, and NFKC before words; Elasticsearch's lowercase filter is Java's per-code-point toLowerCase and stock Elasticsearch has no NFKC or case folding without the ICU plugin.",
		Choice:     "A custom normalizer: two pattern_replace char filters (trim, then fold runs of Python's whitespace set to one space) and lowercase. It equals Searchlight on every ASCII text and on all whitespace.",
		Benchmark:  "Generated data is ASCII, so the normalizations agree exactly; the cross-check would flag any disagreement.",
	},
	{
		Area:       "eq, ne, in, starts_with on keyword and text",
		Difference: "None for ASCII: both compare the whole normalized value.",
		Choice:     "term / terms / prefix on the normalized keyword (the field itself); ne is bool.must_not, which like Searchlight matches documents missing the field.",
		Benchmark:  "Exact.",
	},
	{
		Area:       "contains, contains_any, contains_all",
		Difference: "Searchlight tests substrings of the normalized value with a 3-gram prefilter it builds for every keyword and text field. Elasticsearch has no substring index by default; the options are a wildcard query on a keyword (exact, but scans the term dictionary per query), the wildcard field type (an n-gram index, but it takes no normalizer, so case-insensitive matching would need case_insensitive queries that it serves less well), or an n-gram subfield.",
		Choice:     "A 3-gram subfield (ngram tokenizer, min=max=3, every character kept, after the same trim/whitespace char filters, lowercased, positions indexed, no norms). A needle of three or more characters becomes a match_phrase of its 3-grams, which is exact: consecutive windows at consecutive positions are precisely the needle's occurrences. A shorter needle falls back to an exact wildcard *needle* on the normalized keyword. Only fields the workloads query with contains (the title) get the subfield, so Elasticsearch does not pay disk for 3-grams of fields nobody queries that way, while Searchlight indexes 3-grams of every keyword and text field.",
		Benchmark:  "Exact. Disk comparisons favour Elasticsearch here (fewer 3-gram fields).",
	},
	{
		Area:       "words_all, words_any",
		Difference: "Searchlight's words are runs of Python \\w characters after NFKC and case folding, matched as a contiguous padded substring; Elasticsearch's pattern tokenizer [^\\p{L}\\p{N}_]+ is Java's Unicode classes, which differ from Python's str.isalnum on a few code points (combining marks, some numerics).",
		Choice:     "A words subfield (pattern tokenizer [^\\p{L}\\p{N}_]+, lowercase, positions, no norms); each phrase is a match_phrase (slop 0), which is the contiguous-words test.",
		Benchmark:  "Exact on ASCII data.",
	},
	{
		Area:       "similar",
		Difference: "pg_trgm similarity has no Elasticsearch query. The fuzzy and more_like_this queries score differently and cannot reproduce a similarity threshold.",
		Choice:     "A painless script query computing pg_trgm similarity over the normalized keyword's doc value (Character.isLetterOrDigit runs, two spaces before and one after, distinct 3-grams, rounded to float like pg_trgm). It is exact for ASCII but evaluates every candidate document, so it is not benchmarked; it needs doc values on the field.",
		Benchmark:  "Cross-checked on the brand field; not a timed workload (Searchlight's trigram index would make the comparison one-sided).",
	},
	{
		Area:       "exists, empty and nonempty on lists",
		Difference: "Searchlight counts an empty list ([]) or a list of blank entries as present (exists: true) with no entries (empty: true); Elasticsearch indexes nothing for [] (exists: false) and indexes a blank entry as the term \"\".",
		Choice:     "exists → exists; exists:false and empty → must_not exists; nonempty → exists.",
		Benchmark:  "The generator never writes an empty list or a blank entry (a listing without tags omits the field), so all three agree.",
	},
	{
		Area:       "_id",
		Difference: "Searchlight queries the id normalized (casefolded) and sorts on the exact id; Elasticsearch 8 queries _id exactly and does not sort on it.",
		Choice:     "eq/ne/in on _id become ids queries with the normalized id; every Elasticsearch document carries sl_id, a doc-values-only keyword copy of the id, for sorting and the tie-breaker. contains, starts_with and similar on _id are refused (Elasticsearch's _id field does not support them).",
		Benchmark:  "Generated ids are lowercase (p000000123), so normalized and exact agree. sl_id costs Elasticsearch a little disk.",
	},
	{
		Area:       "sort and search_after",
		Difference: "Both sort missing values last in both directions and break ties on the id. Elasticsearch's search_after cursor for a missing value is a sentinel, Searchlight's is null.",
		Choice:     "sort entries get missing: _last and a final sl_id ascending; each engine pages with its own cursor.",
		Benchmark:  "Exact order, compared hit by hit.",
	},
	{
		Area:       "number fields",
		Difference: "Searchlight stores every number as float64. Elasticsearch could store whole-number fields as long or scaled_float (smaller), but a term query with a fraction on a long field does not behave as Searchlight's float equality does.",
		Choice:     "double for every number field, so eq, in and ranges compare exactly as Searchlight does.",
		Benchmark:  "Exact. Elasticsearch might save some disk with narrower types.",
	},
	{
		Area:       "dates",
		Difference: "None: both store Unix milliseconds, parse RFC 3339 and bare dates, and bucket calendar intervals in UTC with Monday weeks.",
		Choice:     "date with format strict_date_optional_time||epoch_millis.",
		Benchmark:  "Exact.",
	},
	{
		Area:       "terms aggregation",
		Difference: "Both order buckets by count, then key; both use shard_size = size*1.5+10 and report doc_count_error_upper_bound. With several shards both may be approximate in the same way, not necessarily on the same buckets. A bool terms key is true/false in Searchlight and 1/0 (key_as_string \"true\") in Elasticsearch.",
		Choice:     "terms with the same size, shard_size and min_doc_count; keys are compared through key_as_string for bools.",
		Benchmark:  "The cross-check compares buckets exactly, and tolerates a difference only when an engine reports a nonzero error bound.",
	},
	{
		Area:       "cardinality",
		Difference: "Both are HyperLogLog++ but with different parameters: Searchlight's precision p (default 14, exact below 2^(p-2) values), Elasticsearch's precision_threshold (exact-ish below it, at most 40000).",
		Choice:     "precision_threshold = min(2^(p-2), 40000).",
		Benchmark:  "The cross-check allows 3% relative difference.",
	},
	{
		Area:       "stats",
		Difference: "Sums are added in a different order (per segment, per shard), so the last bits of sum and avg can differ.",
		Choice:     "stats.",
		Benchmark:  "The cross-check allows 1e-9 relative difference on sum and avg; count, min and max are exact.",
	},
	{
		Area:       "percolation",
		Difference: "Searchlight's _percolate and _bulk?percolate return each document's matching saved-query ids; Elasticsearch's percolate query returns each matching query once with the slots of the documents it matched, limited to max_result_window hits per request. _bulk?percolate (write and percolate in one request) has no Elasticsearch equivalent.",
		Choice:     "A percolator index with the same field mappings; each batch is one percolate query in filter context (no scoring), paged by qid when more than 10000 queries match; the bulk-and-percolate workload sends Elasticsearch a _bulk then a percolate of the same documents.",
		Benchmark:  "Match sets are cross-checked per document.",
	},
	{
		Area:       "caching",
		Difference: "Elasticsearch's shard request cache returns a whole cached response for a repeated size-0 request (aggregations); Searchlight has no response cache, only a filter cache like Elasticsearch's node query cache.",
		Choice:     "Every Elasticsearch search is sent with request_cache=false; both filter caches stay on. Each workload also cycles through many query variants.",
		Benchmark:  "Aggregation latencies measure computing the aggregation, on both engines.",
	},
	{
		Area:       "durability and refresh",
		Difference: "Searchlight acknowledges a write once its SQL transaction commits (SQLite with synchronous=FULL in the bench); Elasticsearch once its translog is fsynced (index.translog.durability=request, its default).",
		Choice:     "Both acknowledged-means-durable; both refresh every 1 s (set explicitly on Elasticsearch, which also disables its search-idle refresh skipping); one replica-less node each.",
		Benchmark:  "Bulk throughput is compared at equal durability, as spec section 1 requires.",
	},
	{
		Area:       "scoring",
		Difference: "Searchlight does not score (v1 non-goal); Elasticsearch scores unless a query is in filter context.",
		Choice:     "Every translated search and percolation runs in constant_score filter context, so Elasticsearch skips scoring and can cache filters.",
		Benchmark:  "No engine pays for relevance.",
	},
}
