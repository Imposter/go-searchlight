package query

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// TestParityMatch evaluates every fixture query from testdata/parity/match.json over
// the fixture documents, through this package's own Parse and Compile/Match, and
// compares with scrape-bot's verdicts. It is Task 2's TestParityMatch moved here: the
// reference matcher it used to compare against scrape-bot has been retired now that
// the production matcher stands in for it.
func TestParityMatch(t *testing.T) {
	f := loadMatchFixture(t)
	queries := 0
	for g, group := range f.Groups {
		docs := make([]schema.Doc, len(group.Docs))
		for i, fd := range group.Docs {
			doc, _, err := schema.Analyze(&f.Mapping, fd.ID, []byte(fd.Body))
			if err != nil {
				t.Fatalf("Analyze(%q): %v", fd.ID, err)
			}
			docs[i] = doc
		}
		for q, fq := range group.Queries {
			node, problems := Parse(fq.Query)
			if len(problems) != 0 {
				t.Fatalf("group %d query %d %s: Parse: %v", g, q, fq.Query, problems)
			}
			compiled := Compile(node)
			var got []string
			for i := range docs {
				if compiled.Match(&docs[i]) {
					got = append(got, docs[i].ID)
				}
			}
			if !slices.Equal(got, fq.Matches) {
				t.Errorf("group %d query %d %s:\n got  %q\n want %q", g, q, fq.Query, got, fq.Matches)
			}
			queries++
		}
	}
	if queries < 300 {
		t.Fatalf("only %d fixture queries", queries)
	}
}

// matchFixture is testdata/parity/match.json's shape (tools/parity/gen.py).
type matchFixture struct {
	Mapping schema.Mapping `json:"mapping"`
	Groups  []struct {
		Docs []struct {
			ID   string `json:"id"`
			Body string `json:"body"`
		} `json:"docs"`
		Queries []struct {
			Query   json.RawMessage `json:"query"`
			Matches []string        `json:"matches"`
		} `json:"queries"`
	} `json:"groups"`
}

func loadMatchFixture(t *testing.T) matchFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "parity", "match.json"))
	if err != nil {
		t.Fatalf("read fixture: %v (regenerate with make parity)", err)
	}
	var f struct {
		Data matchFixture `json:"data"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&f); err != nil {
		t.Fatalf("decode match.json: %v", err)
	}
	if len(f.Data.Groups) == 0 {
		t.Fatal("match.json holds no groups")
	}
	return f.Data
}
