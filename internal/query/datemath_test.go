package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// testNow is a Friday, 2026-10-16T13:45:30.123Z.
var testNow = time.Date(2026, 10, 16, 13, 45, 30, 123e6, time.UTC)

func ms(t *testing.T, rfc3339 string) int64 {
	t.Helper()
	at, err := time.Parse(time.RFC3339Nano, rfc3339)
	if err != nil {
		t.Fatal(err)
	}
	return at.UnixMilli()
}

func resolveText(t *testing.T, expr string, roundUp bool) int64 {
	t.Helper()
	d, why := ParseDateMath(expr)
	if why != "" {
		t.Fatalf("ParseDateMath(%q): %s", expr, why)
	}
	got, ok := d.Resolve(testNow.UnixMilli(), roundUp)
	if !ok {
		t.Fatalf("%q does not resolve", expr)
	}
	return got
}

// Every unit, added and rounded down (gte, lt, between's lo) and up (gt, lte,
// between's hi), as Elasticsearch's JavaDateMathParser resolves them in UTC.
func TestDateMathUnits(t *testing.T) {
	t.Parallel()
	cases := []struct {
		expr       string
		down, up   string
		roundUpToo bool // up differs from down
	}{
		{"now", "2026-10-16T13:45:30.123Z", "", false},
		{"now-7d", "2026-10-09T13:45:30.123Z", "", false},
		{"now+d", "2026-10-17T13:45:30.123Z", "", false},
		{"now+1d", "2026-10-17T13:45:30.123Z", "", false},
		{"now+2w", "2026-10-30T13:45:30.123Z", "", false},
		{"now-1M", "2026-09-16T13:45:30.123Z", "", false},
		{"now+1y", "2027-10-16T13:45:30.123Z", "", false},
		{"now+1h", "2026-10-16T14:45:30.123Z", "", false},
		{"now+1H", "2026-10-16T14:45:30.123Z", "", false},
		{"now-30m", "2026-10-16T13:15:30.123Z", "", false},
		{"now+90s", "2026-10-16T13:47:00.123Z", "", false},
		{"now/y", "2026-01-01T00:00:00Z", "2026-12-31T23:59:59.999Z", true},
		{"now/M", "2026-10-01T00:00:00Z", "2026-10-31T23:59:59.999Z", true},
		{"now/w", "2026-10-12T00:00:00Z", "2026-10-18T23:59:59.999Z", true},
		{"now/d", "2026-10-16T00:00:00Z", "2026-10-16T23:59:59.999Z", true},
		{"now/1d", "2026-10-16T00:00:00Z", "2026-10-16T23:59:59.999Z", true},
		{"now/h", "2026-10-16T13:00:00Z", "2026-10-16T13:59:59.999Z", true},
		{"now/H", "2026-10-16T13:00:00Z", "2026-10-16T13:59:59.999Z", true},
		{"now/m", "2026-10-16T13:45:00Z", "2026-10-16T13:45:59.999Z", true},
		{"now/s", "2026-10-16T13:45:30Z", "2026-10-16T13:45:30.999Z", true},
		{"now-1d/d", "2026-10-15T00:00:00Z", "2026-10-15T23:59:59.999Z", true},
		{"now/d+1h", "2026-10-16T01:00:00Z", "2026-10-17T00:59:59.999Z", true},
		{"now/d/M", "2026-10-01T00:00:00Z", "2026-10-31T23:59:59.999Z", true},
		{"2026-10-01||+1M/d", "2026-11-01T00:00:00Z", "2026-11-01T23:59:59.999Z", true},
		{"2026-10-01||", "2026-10-01T00:00:00Z", "", false},
		// A date with no math is filled out when rounding up: a bare date to its last
		// millisecond, a time with no fraction to its second's last.
		{"2026-10-01", "2026-10-01T00:00:00Z", "2026-10-01T23:59:59.999Z", true},
		{"2026-10-01T09:30:00Z", "2026-10-01T09:30:00Z", "2026-10-01T09:30:00.999Z", true},
		{"2026-10-01T09:30:00.250Z", "2026-10-01T09:30:00.250Z", "", false},
		{"2026-10-01T09:30:00,5Z", "2026-10-01T09:30:00.500Z", "", false},
		{"2026-10-01T09:30:00.000Z", "2026-10-01T09:30:00Z", "", false},
		{"2026-10-01T11:30:00+02:00", "2026-10-01T09:30:00Z", "2026-10-01T09:30:00.999Z", true},
		// Before 1970, rounding still goes down.
		{"1969-12-31T23:59:59.500Z||/s", "1969-12-31T23:59:59Z", "1969-12-31T23:59:59.999Z", true},
		{"1969-12-31||/w", "1969-12-29T00:00:00Z", "1970-01-04T23:59:59.999Z", true},
		{"1955-07-20T10:00:00Z||/M", "1955-07-01T00:00:00Z", "1955-07-31T23:59:59.999Z", true},
		{"1955-07-20||/y", "1955-01-01T00:00:00Z", "1955-12-31T23:59:59.999Z", true},
		{"1969-12-31T23:59:59.999Z||+1s", "1970-01-01T00:00:00.999Z", "", false},
		// Rounding after month arithmetic at a month's end, and in a leap year.
		{"2026-01-31||+1M/M", "2026-02-01T00:00:00Z", "2026-02-28T23:59:59.999Z", true},
		{"2024-01-31||+1M/M", "2024-02-01T00:00:00Z", "2024-02-29T23:59:59.999Z", true},
		{"2024-03-15||-1M/M", "2024-02-01T00:00:00Z", "2024-02-29T23:59:59.999Z", true},
		{"2024-02-29||/y", "2024-01-01T00:00:00Z", "2024-12-31T23:59:59.999Z", true},
		{"2024-02-29||+1y/y", "2025-01-01T00:00:00Z", "2025-12-31T23:59:59.999Z", true},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			t.Parallel()
			up := tc.down
			if tc.roundUpToo {
				up = tc.up
			}
			if got, want := resolveText(t, tc.expr, false), ms(t, tc.down); got != want {
				t.Errorf("down: %s, want %s", time.UnixMilli(got).UTC().Format(time.RFC3339Nano), tc.down)
			}
			if got, want := resolveText(t, tc.expr, true), ms(t, up); got != want {
				t.Errorf("up: %s, want %s", time.UnixMilli(got).UTC().Format(time.RFC3339Nano), up)
			}
		})
	}
}

// Months and years add as java.time's plusMonths and plusYears do: the day clamped to
// the new month's last, leap years included, one step at a time.
func TestDateMathMonthEnds(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"2026-01-31||+1M":           "2026-02-28T00:00:00Z",
		"2024-01-31||+1M":           "2024-02-29T00:00:00Z",
		"2026-03-31||-1M":           "2026-02-28T00:00:00Z",
		"2026-01-31||+1M+1M":        "2026-03-28T00:00:00Z",
		"2026-01-31||+2M":           "2026-03-31T00:00:00Z",
		"2026-05-31||+1M":           "2026-06-30T00:00:00Z",
		"2026-12-31||+1M":           "2027-01-31T00:00:00Z",
		"2026-01-15||-13M":          "2024-12-15T00:00:00Z",
		"2024-02-29||+1y":           "2025-02-28T00:00:00Z",
		"2024-02-29||+4y":           "2028-02-29T00:00:00Z",
		"2024-02-29||-4y":           "2020-02-29T00:00:00Z",
		"2024-02-29||+12M":          "2025-02-28T00:00:00Z",
		"2024-02-29T10:00:00Z||+1y": "2025-02-28T10:00:00Z",
		"1900-03-01||-1d":           "1900-02-28T00:00:00Z",
		"1960-02-29||+1y":           "1961-02-28T00:00:00Z",
		"1960-03-31||-1M":           "1960-02-29T00:00:00Z",
		"1901-01-31||-2M":           "1900-11-30T00:00:00Z",
	}
	for expr, want := range cases {
		if got := resolveText(t, expr, false); got != ms(t, want) {
			t.Errorf("%s = %s, want %s", expr, time.UnixMilli(got).UTC().Format(time.RFC3339Nano), want)
		}
	}
}

func TestDateMathRefused(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"":                  "neither a date",
		"yesterday":         "neither a date",
		"Now-1d":            "neither a date",
		"2026-13-01||+1d":   "neither a date",
		"2026-10-01 ||+1d":  "neither a date",
		"now-7x":            `unknown unit 'x'`,
		"now*1d":            `unknown operator '*'`,
		"now||-1d":          `unknown operator '|'`,
		"now -1d":           `unknown operator ' '`,
		"now-":              "ends early",
		"now-7":             "ends early",
		"now/":              "ends early",
		"now/2d":            "one unit",
		"now-99999999999d":  "too large a number",
		"2026-10-01||+1d/x": `unknown unit 'x'`,
	}
	for expr, want := range cases {
		_, why := ParseDateMath(expr)
		if !strings.Contains(why, want) {
			t.Errorf("ParseDateMath(%q) = %q, want it to say %q", expr, why, want)
		}
	}
	d, why := ParseDateMath("now+300000y")
	if why != "" {
		t.Fatal(why)
	}
	if _, ok := d.Resolve(testNow.UnixMilli(), false); ok {
		t.Error("now+300000y resolves")
	}
}

func dateMapping() *schema.Mapping {
	return &schema.Mapping{Fields: map[string]schema.FieldType{
		"seen": schema.Date, "price": schema.Number, "brand": schema.Keyword,
	}}
}

func TestValidateDateMath(t *testing.T) {
	t.Parallel()
	valid := []string{
		`{"field": "seen", "op": "gte", "value": "now-7d/d"}`,
		`{"field": "seen", "op": "lt", "value": "2026-10-01"}`,
		`{"field": "seen", "op": "lte", "value": "2026-10-01T09:30:00Z||+1h"}`,
		`{"field": "seen", "op": "gt", "value": 1760000000000}`,
		`{"field": "seen", "op": "between", "value": ["now-1M/M", "now/M"]}`,
		`{"field": "seen", "op": "between", "value": [0, "now"]}`,
		`{"field": "seen", "op": "between", "value": ["2026-10-01", "2026-10-01"]}`,
		`{"field": "seen", "op": "between", "value": ["now", "2020-01-01"]}`,
		`{"field": "seen", "op": "between", "value": ["now-1d", "now"]}`,
		`{"field": "seen", "op": "between", "value": ["now", "now"]}`,
		`{"field": "seen", "op": "between", "value": ["now/d", "now-1h"]}`,
		`{"field": "seen", "op": "between", "value": ["now+1M", "now+30d"]}`,
	}
	for _, raw := range valid {
		if ps := Validate(mustParse(t, raw), dateMapping()); len(ps) != 0 {
			t.Errorf("%s: %v", raw, ps)
		}
	}
	refused := []struct{ raw, loc, says string }{
		{`{"field": "seen", "op": "gte", "value": "now-7x"}`, "query.value", "unknown unit"},
		{`{"all": [{"field": "brand", "op": "eq", "value": "a"}, {"field": "seen", "op": "lt", "value": "now+"}]}`, "query.all.1.value", "ends early"},
		{`{"field": "seen", "op": "between", "value": ["now", "bad"]}`, "query.value.1", "neither a date"},
		{`{"field": "seen", "op": "between", "value": ["now"]}`, "query.value", "[lo, hi]"},
		{`{"field": "seen", "op": "between", "value": [true, "now"]}`, "query.value", "two numbers or date math"},
		{`{"field": "seen", "op": "between", "value": ["2026-10-02", "2026-10-01"]}`, "query.value", "lo <= hi"},
		{`{"field": "seen", "op": "between", "value": ["now", "now-1d"]}`, "query.value", "lo <= hi"},
		{`{"field": "seen", "op": "between", "value": ["now+2h", "now+90m"]}`, "query.value", "lo <= hi"},
		{`{"field": "seen", "op": "eq", "value": "2026-10-01"}`, "query.value", "Unix milliseconds: date strings are for lt"},
		{`{"field": "seen", "op": "in", "value": ["2026-10-01"]}`, "query.value", "Unix milliseconds"},
		{`{"field": "seen", "op": "gte", "value": "2026-01-01||+300000y"}`, "query.value", "falls outside"},
		{`{"field": "seen", "op": "gte", "value": true}`, "query.value", "a number or date math"},
		{`{"field": "seen", "op": "eq", "value": "now/d"}`, "query.value", "date math is for lt, lte, gt, gte and between"},
		{`{"field": "seen", "op": "ne", "value": "now"}`, "query.value", "date math is for lt, lte, gt, gte and between"},
		{`{"field": "seen", "op": "in", "value": ["now"]}`, "query.value", "date math is for lt, lte, gt, gte and between"},
		{`{"field": "price", "op": "lt", "value": "now"}`, "query.value", "date math is for date fields"},
		{`{"field": "price", "op": "between", "value": [0, "now"]}`, "query.value", "date math is for date fields"},
		{`{"field": "brand", "op": "lt", "value": "now"}`, "query.op", "brand"},
	}
	for _, tc := range refused {
		ps := Validate(mustParse(t, tc.raw), dateMapping())
		if len(ps) != 1 || ps[0].Loc != tc.loc || !strings.Contains(ps[0].Message, tc.says) {
			t.Errorf("%s: %v, want one problem at %s saying %q", tc.raw, ps, tc.loc, tc.says)
		}
	}
}

func TestResolve(t *testing.T) {
	t.Parallel()
	plain := mustParse(t, `{"all": [{"field": "brand", "op": "eq", "value": "a"}, {"field": "seen", "op": "gte", "value": 5}]}`)
	if got, ps := Resolve(plain, testNow); got != plain || ps != nil {
		t.Fatalf("a query with no date math: %v, %v", got, ps)
	}

	raw := `{"all": [{"field": "brand", "op": "eq", "value": "a"},` +
		`{"field": "seen", "op": "gte", "value": "now-7d"},` +
		`{"not": {"field": "seen", "op": "lte", "value": "now/d"}},` +
		`{"any": [{"field": "seen", "op": "between", "value": [5, "now/M"]}]}]}`
	n := mustParse(t, raw)
	before := string(Canonical(n))
	got, ps := Resolve(n, testNow)
	if len(ps) != 0 {
		t.Fatal(ps)
	}
	if string(Canonical(n)) != before {
		t.Fatal("Resolve changed its argument")
	}
	if HasDateMath(got) {
		t.Fatal("the resolved tree still has date math")
	}
	back, ps := Parse(mustEncode(t, got))
	if len(ps) != 0 {
		t.Fatal(ps)
	}
	want := mustParse(t, fmt.Sprintf(`{"all": [{"field": "brand", "op": "eq", "value": "a"},`+
		`{"field": "seen", "op": "gte", "value": %d},`+
		`{"not": {"field": "seen", "op": "lte", "value": %d}},`+
		`{"any": [{"field": "seen", "op": "between", "value": [5, %d]}]}]}`,
		ms(t, "2026-10-09T13:45:30.123Z"), ms(t, "2026-10-16T23:59:59.999Z"), ms(t, "2026-10-31T23:59:59.999Z")))
	if !bytes.Equal(Canonical(back), Canonical(want)) || !bytes.Equal(Canonical(got), Canonical(want)) {
		t.Fatalf("resolved = %s", mustEncode(t, got))
	}

	far := mustParse(t, `{"all": [{"field": "brand", "op": "eq", "value": "a"}, {"field": "seen", "op": "between", "value": [0, "now+300000y"]}]}`)
	got, ps = Resolve(far, testNow)
	if len(ps) != 1 || ps[0].Loc != "query.all.1.value.1" {
		t.Fatalf("out of range: %v", ps)
	}
	d := doc(map[string]schema.Value{"brand": {Present: true, Text: strp("a")}, "seen": {Present: true, Number: nump(10)}})
	if Match(got, d) {
		t.Fatal("a bound out of range matches")
	}
}

// The matcher reads a resolved tree: lte now/d holds through the day's end, gt now/d
// not until the next day, and a query left unresolved matches nothing.
func TestMatchResolvedDateMath(t *testing.T) {
	t.Parallel()
	dated := func(at string) *schema.Doc {
		day, err := time.Parse(time.DateOnly, at)
		if err != nil {
			day, err = time.Parse(time.RFC3339Nano, at)
		}
		if err != nil {
			t.Fatal(err)
		}
		return doc(map[string]schema.Value{"seen": {Present: true, Number: nump(float64(day.UnixMilli()))}})
	}
	cases := []struct {
		raw  string
		at   string
		want bool
	}{
		{`{"field": "seen", "op": "lte", "value": "now/d"}`, "2026-10-16T23:59:59.999Z", true},
		{`{"field": "seen", "op": "lt", "value": "now/d"}`, "2026-10-16T00:00:00Z", false},
		{`{"field": "seen", "op": "lt", "value": "now/d"}`, "2026-10-15T23:59:59.999Z", true},
		{`{"field": "seen", "op": "gt", "value": "now/d"}`, "2026-10-16T23:59:59.999Z", false},
		{`{"field": "seen", "op": "gt", "value": "now/d"}`, "2026-10-17", true},
		{`{"field": "seen", "op": "gte", "value": "now/d"}`, "2026-10-16", true},
		{`{"field": "seen", "op": "gte", "value": "now-7d"}`, "2026-10-10", true},
		{`{"field": "seen", "op": "gte", "value": "now-7d"}`, "2026-10-08", false},
		{`{"field": "seen", "op": "between", "value": ["now/M", "now/M"]}`, "2026-10-31T23:59:59.999Z", true},
		{`{"field": "seen", "op": "between", "value": ["now/M", "now/M"]}`, "2026-11-01", false},
		{`{"field": "seen", "op": "lte", "value": "2026-10-16"}`, "2026-10-16T18:00:00Z", true},
		{`{"field": "seen", "op": "gt", "value": "2026-10-16"}`, "2026-10-16T18:00:00Z", false},
	}
	for _, tc := range cases {
		n := mustParse(t, tc.raw)
		resolved, ps := Resolve(n, testNow)
		if len(ps) != 0 {
			t.Fatal(ps)
		}
		if got := Match(resolved, dated(tc.at)); got != tc.want {
			t.Errorf("%s at %s: %v, want %v", tc.raw, tc.at, got, tc.want)
		}
		if Match(n, dated(tc.at)) {
			t.Errorf("%s unresolved matches", tc.raw)
		}
	}
}

func TestCanonicalDateMath(t *testing.T) {
	t.Parallel()
	same := [][2]string{
		{`{"field": "f", "op": "gte", "value": "now-7d"}`, `{"field": "f", "op": "gte", "value": "now-07d"}`},
		{`{"field": "f", "op": "gte", "value": "now+d"}`, `{"field": "f", "op": "gte", "value": "now+1d"}`},
		{`{"field": "f", "op": "gte", "value": "now/H"}`, `{"field": "f", "op": "gte", "value": "now/h"}`},
	}
	for _, p := range same {
		if !bytes.Equal(canon(t, p[0]), canon(t, p[1])) {
			t.Errorf("%s and %s differ", p[0], p[1])
		}
	}
	differ := [][2]string{
		{`{"field": "f", "op": "gte", "value": "now-1M"}`, `{"field": "f", "op": "gte", "value": "now-1m"}`},
		{`{"field": "f", "op": "gte", "value": "now/M"}`, `{"field": "f", "op": "gte", "value": "now/m"}`},
		{`{"field": "f", "op": "lte", "value": "2026-10-01"}`, `{"field": "f", "op": "lte", "value": "2026-10-01T00:00:00Z"}`},
		{`{"field": "f", "op": "between", "value": ["now-1d", "now"]}`, `{"field": "f", "op": "between", "value": ["now", "now-1d"]}`},
		{`{"field": "f", "op": "gte", "value": "x"}`, `{"field": "f", "op": "gte", "value": "X"}`},
	}
	for _, p := range differ {
		if bytes.Equal(canon(t, p[0]), canon(t, p[1])) {
			t.Errorf("%s and %s encode alike", p[0], p[1])
		}
	}
}

func TestWithNow(t *testing.T) {
	t.Parallel()
	if _, ok := NowFrom(context.Background()); ok {
		t.Fatal("a bare context carries a now")
	}
	got, ok := NowFrom(WithNow(context.Background(), testNow))
	if !ok || !got.Equal(testNow) {
		t.Fatalf("NowFrom = %v, %v", got, ok)
	}
}

// mustEncode writes n as query JSON, its values as they are held.
func mustEncode(t *testing.T, n Node) []byte {
	t.Helper()
	var b strings.Builder
	var write func(n Node)
	group := func(key string, children []Node) {
		b.WriteString(`{"` + key + `":[`)
		for i, c := range children {
			if i > 0 {
				b.WriteByte(',')
			}
			write(c)
		}
		b.WriteString("]}")
	}
	write = func(n Node) {
		switch x := n.(type) {
		case *All:
			group("all", x.Children)
		case *Any:
			group("any", x.Children)
		case *Not:
			b.WriteString(`{"not":`)
			write(x.Child)
			b.WriteByte('}')
		case *Leaf:
			field, _ := json.Marshal(x.Field)
			op, _ := json.Marshal(x.Op)
			b.WriteString(`{"field":` + string(field) + `,"op":` + string(op) + `,"value":` + string(x.Value) + "}")
		default:
			t.Fatalf("node %T", n)
		}
	}
	write(n)
	return []byte(b.String())
}

// ResolveFixed resolves what reads no now and keeps what does, so a saved query's
// absolute dates index as plain ranges.
func TestResolveFixed(t *testing.T) {
	t.Parallel()
	relative := mustParse(t, `{"field": "seen", "op": "gte", "value": "now-7d"}`)
	if got := ResolveFixed(relative); got != relative {
		t.Fatal("a now-relative condition was rewritten")
	}
	raw := `{"all": [{"field": "seen", "op": "lte", "value": "2026-10-01"},` +
		`{"field": "seen", "op": "between", "value": ["2026-10-01||+1M", "now"]},` +
		`{"field": "seen", "op": "gt", "value": "now/d"}]}`
	got := ResolveFixed(mustParse(t, raw))
	want := mustParse(t, fmt.Sprintf(`{"all": [{"field": "seen", "op": "lte", "value": %d},`+
		`{"field": "seen", "op": "between", "value": [%d, "now"]},`+
		`{"field": "seen", "op": "gt", "value": "now/d"}]}`, ms(t, "2026-10-01T23:59:59.999Z"), ms(t, "2026-11-01T00:00:00Z")))
	if !bytes.Equal(Canonical(got), Canonical(want)) {
		t.Fatalf("ResolveFixed = %s", mustEncode(t, got))
	}
	back, ps := Parse(mustEncode(t, got))
	if len(ps) != 0 || !bytes.Equal(Canonical(back), Canonical(want)) {
		t.Fatalf("round trip: %v", ps)
	}
	for _, now := range []time.Time{testNow, testNow.Add(400 * 24 * time.Hour)} {
		a, _ := Resolve(got, now)
		b, _ := Resolve(mustParse(t, raw), now)
		if !bytes.Equal(Canonical(a), Canonical(b)) {
			t.Fatalf("at %s, resolving after ResolveFixed differs", now)
		}
	}
}
