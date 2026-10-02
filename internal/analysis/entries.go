package analysis

import (
	"slices"
	"strings"
)

// ListSeparator joins a list field's entries in one string.
const ListSeparator = ", "

// Entries returns a list string's entries as written: s split on ", ", each stripped of
// surrounding whitespace, blanks dropped (scrape-bot's list_entries). Order and
// duplicates are kept.
func Entries(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ListSeparator)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if entry := strip(part); entry != "" {
			out = append(out, entry)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// EntryTerms returns entries as a list field indexes and compares them: each one
// normalized and cleaned, blanks dropped, distinct, sorted by code point.
func EntryTerms(entries []string) []string {
	if len(entries) == 0 {
		return nil
	}
	terms := make([]string, 0, len(entries))
	for _, entry := range entries {
		if term := Clean(Normalize(entry)); term != "" {
			terms = append(terms, term)
		}
	}
	if len(terms) == 0 {
		return nil
	}
	slices.Sort(terms)
	return slices.Compact(terms)
}
