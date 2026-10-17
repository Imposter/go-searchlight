package api_test

import (
	"cmp"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/config"
)

// TestSearchAfterPastTheWindow: search_after walks a sorted result over three shards of
// several segments each far past max_result_window, by date, number, keyword and text,
// ascending and descending, on several keys and with missing values (last either way),
// visiting every match once and in order, with the whole search's total on every page;
// documents written between pages leave the unchanged ones visited once; and a cursor
// of the wrong length or types is a 400 at its loc.
func TestSearchAfterPastTheWindow(t *testing.T) {
	e := newEnv(t, envOpts{cfg: func(c *config.Config) { c.MaxResultWindow = 20 }})
	e.createIndex("sa", `{"mapping": {"dynamic": "strict", "fields": {"seen": "date", "price": "number", "brand": "keyword", "title": "text"}}, "settings": {"shards": 3}}`)
	const n = 120
	for batch := range 4 {
		var lines []string
		for i := batch; i < n; i += 4 {
			body := []string{fmt.Sprintf(`"seen": "2026-10-%02dT%02d:00:00Z"`, 1+i%9, i%24)}
			if i%7 != 0 {
				body = append(body, fmt.Sprintf(`"price": %d`, i%13))
			}
			if i%5 != 0 {
				body = append(body, fmt.Sprintf(`"brand": "b%d"`, i%6))
			}
			body = append(body, fmt.Sprintf(`"title": "%s %d"`, []string{"Red Cup", "blue cup", "RED pot"}[i%3], i%4))
			lines = append(lines, fmt.Sprintf(`{"upsert": {"id": "d%03d"}}`, i), "{"+strings.Join(body, ", ")+"}")
		}
		e.must(http.StatusOK, "POST", "/indexes/sa/_bulk?refresh=wait_for", ndjson(lines...))
	}
	added := 0
	for _, sort := range []string{
		`[{"seen": "desc"}]`,
		`[{"price": "asc"}]`,
		`[{"price": "desc"}, {"_id": "desc"}]`,
		`[{"brand": "asc"}, {"seen": "asc"}]`,
		`[{"brand": "desc"}, {"price": "desc"}]`,
		`[{"title": "desc"}]`,
		`[{"title": "asc"}, {"price": "asc"}, {"seen": "desc"}]`,
	} {
		var order []bool
		var keys []map[string]string
		if err := json.Unmarshal([]byte(sort), &keys); err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			for _, dir := range k {
				order = append(order, dir == "desc")
			}
		}
		if len(order) == len(keys) && !strings.Contains(sort, "_id") {
			order = append(order, false)
		}
		seen := map[string]int{}
		moved := map[string]bool{}
		var prev []any
		var after []byte
		for page := 0; ; page++ {
			body := fmt.Sprintf(`{"sort": %s, "size": 7, "track_total": true}`, sort)
			if after != nil {
				body = fmt.Sprintf(`{"sort": %s, "size": 7, "track_total": true, "search_after": %s}`, sort, after)
			}
			r := e.must(http.StatusOK, "POST", "/indexes/sa/_search", body)
			if totalOf(r) != n+added {
				t.Fatalf("sort %s page %d: total %d, want %d", sort, page, totalOf(r), n+added)
			}
			for _, h := range r["hits"].([]any) { //nolint:forcetypeassert,errcheck // the shape
				hit := h.(map[string]any)   //nolint:forcetypeassert,errcheck // the shape
				vals := hit["sort"].([]any) //nolint:forcetypeassert,errcheck // the shape
				if prev != nil && cmpSortValues(prev, vals, order) >= 0 {
					t.Fatalf("sort %s page %d: %v does not follow %v", sort, page, vals, prev)
				}
				prev = vals
				seen[hit["id"].(string)]++ //nolint:forcetypeassert,errcheck // the shape
			}
			if r["next"] == nil {
				break
			}
			var err error
			if after, err = json.Marshal(r["next"]); err != nil {
				t.Fatal(err)
			}
			if page%5 == 2 {
				// Writes between pages: a new document, and one changed to sort elsewhere.
				moved[fmt.Sprintf("d%03d", (page*37)%n)] = true
				e.must(http.StatusOK, "POST", "/indexes/sa/_bulk?refresh=wait_for", ndjson(
					fmt.Sprintf(`{"upsert": {"id": "x%03d"}}`, added),
					`{"seen": "2026-10-05T00:00:00Z", "price": 6, "brand": "b3", "title": "red cup 9"}`,
					fmt.Sprintf(`{"upsert": {"id": "d%03d"}}`, (page*37)%n),
					fmt.Sprintf(`{"seen": "2026-10-0%dT12:00:00Z", "price": %d, "title": "Blue pot"}`, 1+page%9, page%17),
				))
				added++
			}
		}
		for i := range n {
			if id := fmt.Sprintf("d%03d", i); !moved[id] && seen[id] != 1 {
				t.Errorf("sort %s: %s visited %d times", sort, id, seen[id])
			}
		}
	}
	for _, tc := range []struct{ body, loc string }{
		{`{"sort": [{"price": "asc"}], "search_after": [3]}`, "search_after"},
		{`{"sort": [{"price": "asc"}], "search_after": [3, "d001", "x"]}`, "search_after"},
		{`{"sort": [{"price": "asc"}], "search_after": ["cheap", "d001"]}`, "search_after.0"},
		{`{"sort": [{"brand": "asc"}], "search_after": [7, "d001"]}`, "search_after.0"},
		{`{"sort": [{"seen": "desc"}], "search_after": [true, "d001"]}`, "search_after.0"},
		{`{"sort": [{"price": "asc"}], "search_after": [3, null]}`, "search_after.1"},
		{`{"sort": [{"price": "asc"}], "search_after": [3, "d001"], "from": 7}`, "from"},
	} {
		p := e.problem(e.do("POST", "/indexes/sa/_search", tc.body), http.StatusBadRequest, "invalid_request")
		if !slices.Contains(locs(p), tc.loc) {
			t.Errorf("%s: %v, want a problem at %s", tc.body, p, tc.loc)
		}
	}
}

// cmpSortValues orders two hits' sort values as the engine does: per key, a missing
// value (null) after every other, then numbers or strings, reversed where desc says.
func cmpSortValues(a, b []any, desc []bool) int {
	for i := range a {
		var c int
		switch {
		case a[i] == nil && b[i] == nil:
		case a[i] == nil:
			c = 1
		case b[i] == nil:
			c = -1
		default:
			switch x := a[i].(type) {
			case float64:
				c = cmp.Compare(x, b[i].(float64)) //nolint:forcetypeassert,errcheck // one key, one type
			case string:
				c = cmp.Compare(x, b[i].(string)) //nolint:forcetypeassert,errcheck // one key, one type
			}
			if desc[i] {
				c = -c
			}
		}
		if c != 0 {
			return c
		}
	}
	return 0
}
