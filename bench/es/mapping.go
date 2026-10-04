// Package es translates the benchmark's Searchlight requests to Elasticsearch 8.x:
// index mappings and analysis that reproduce Searchlight's normalization, the query DSL
// with exactly matching semantics, aggregations, sorting and paging, and percolator
// documents for saved queries. Where the two engines cannot be made identical the
// difference is listed in [Caveats], and the benchmark's data avoids it.
package es

import (
	"github.com/Imposter/go-searchlight/bench/datasets"
)

// Elasticsearch names the translation uses.
const (
	// Normalizer is the keyword normalizer that reproduces Searchlight's text
	// normalization (whitespace runs folded to one space, trimmed, lowercased).
	Normalizer = "sl_norm"
	// WordsAnalyzer splits text into words as Searchlight's Words does (runs of
	// letters, digits and '_'), lowercased; phrases match on its positions.
	WordsAnalyzer = "sl_words"
	// TrigramAnalyzer emits every 3-character window of the normalized text, with
	// consecutive positions, so a match_phrase of a needle's windows is a substring test.
	TrigramAnalyzer = "sl_tri"
	// WordsSuffix and TrigramSuffix name a text field's subfields.
	WordsSuffix   = ".words"
	TrigramSuffix = ".tri"
	// IDField is a doc-values-only copy of the document id: Elasticsearch 8 does not
	// sort on _id, and Searchlight breaks every sort's ties on the exact id.
	IDField = "sl_id"
	// QueryField and QueryIDField are the percolator index's query and id fields.
	QueryField   = "query"
	QueryIDField = "qid"
	// DateFormat accepts what Searchlight's date fields accept: RFC 3339, a bare date,
	// or Unix milliseconds.
	DateFormat = "strict_date_optional_time||epoch_millis"
)

// pySpace is Python's str.isspace set (what Searchlight's normalization folds), as a
// Java character class.
const pySpace = `[\s\x1c-\x1f\x85\xa0\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}]`

// analysisSettings is the index analysis settings every translated index shares.
func analysisSettings() map[string]any {
	return map[string]any{
		"char_filter": map[string]any{
			"sl_trim": map[string]any{"type": "pattern_replace", "pattern": "^" + pySpace + "+|" + pySpace + "+$", "replacement": ""},
			"sl_ws":   map[string]any{"type": "pattern_replace", "pattern": pySpace + "+", "replacement": " "},
		},
		"normalizer": map[string]any{
			Normalizer: map[string]any{"type": "custom", "char_filter": []string{"sl_trim", "sl_ws"}, "filter": []string{"lowercase"}},
		},
		"tokenizer": map[string]any{
			WordsAnalyzer:   map[string]any{"type": "pattern", "pattern": `[^\p{L}\p{N}_]+`},
			TrigramAnalyzer: map[string]any{"type": "ngram", "min_gram": 3, "max_gram": 3},
		},
		"analyzer": map[string]any{
			WordsAnalyzer:   map[string]any{"type": "custom", "tokenizer": WordsAnalyzer, "filter": []string{"lowercase"}},
			TrigramAnalyzer: map[string]any{"type": "custom", "char_filter": []string{"sl_trim", "sl_ws"}, "tokenizer": TrigramAnalyzer, "filter": []string{"lowercase"}},
		},
	}
}

// properties maps each field to its cheapest Elasticsearch structures that still
// answer every operator the benchmark sends it (see the Caveats for each choice).
func properties(fields []datasets.Field) map[string]any {
	props := make(map[string]any, len(fields)+1)
	for _, f := range fields {
		var p map[string]any
		switch f.Type {
		case datasets.Keyword, datasets.Text, datasets.KeywordList:
			p = map[string]any{"type": "keyword", "normalizer": Normalizer, "doc_values": f.DocValues}
			sub := map[string]any{}
			if f.Type == datasets.Text {
				sub["words"] = map[string]any{"type": "text", "analyzer": WordsAnalyzer, "norms": false, "index_options": "positions"}
			}
			if f.Substring && f.Type != datasets.KeywordList {
				sub["tri"] = map[string]any{"type": "text", "analyzer": TrigramAnalyzer, "norms": false, "index_options": "positions"}
			}
			if len(sub) > 0 {
				p["fields"] = sub
			}
		case datasets.Number:
			p = map[string]any{"type": "double"}
		case datasets.Bool:
			p = map[string]any{"type": "boolean"}
		case datasets.Date:
			p = map[string]any{"type": "date", "format": DateFormat}
		}
		props[f.Name] = p
	}
	return props
}

// settings is an index's settings: shards, no replicas (a single node), the same 1 s
// refresh as Searchlight's default (set explicitly, which also turns off search-idle
// skipping of refreshes), and a translog fsynced on every request, so an
// acknowledged write is durable as Searchlight's is.
func settings(shards int) map[string]any {
	return map[string]any{
		"number_of_shards":    max(shards, 1),
		"number_of_replicas":  0,
		"refresh_interval":    "1s",
		"translog.durability": "request",
		"analysis":            analysisSettings(),
	}
}

// IndexBody is the body of PUT /{index} for documents with fields.
func IndexBody(fields []datasets.Field, shards int) map[string]any {
	props := properties(fields)
	props[IDField] = map[string]any{"type": "keyword", "index": false, "doc_values": true}
	return map[string]any{
		"settings": settings(shards),
		"mappings": map[string]any{"dynamic": "strict", "properties": props},
	}
}

// PercolatorIndexBody is the body of PUT /{index} for saved queries over documents
// with fields: the fields (so the percolator analyzes a document as the document index
// would), the percolator field, the query id and the opaque meta.
func PercolatorIndexBody(fields []datasets.Field, shards int) map[string]any {
	props := properties(fields)
	props[QueryField] = map[string]any{"type": "percolator"}
	props[QueryIDField] = map[string]any{"type": "keyword"}
	props[IDField] = map[string]any{"type": "keyword", "index": false, "doc_values": true}
	props["meta"] = map[string]any{"type": "object", "enabled": false}
	return map[string]any{
		"settings": settings(shards),
		"mappings": map[string]any{"dynamic": "strict", "properties": props},
	}
}
