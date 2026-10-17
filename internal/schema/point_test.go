package schema

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
)

// A point type is written as an object naming its axes, round trips through a
// mapping's JSON, and keeps its axes apart: point(number, date) is not
// point(date, number), and a mapping cannot change one into the other.
func TestPointTypeJSON(t *testing.T) {
	var m Mapping
	err := json.Unmarshal([]byte(`{"fields": {"at": {"type": "point", "x": "date", "y": "number"}, "xy": {"y": "number", "x": "number", "type": "point"}, "n": "number"}}`), &m)
	if err != nil {
		t.Fatal(err)
	}
	if m.Fields["at"] != PointType(Date, Number) || m.Fields["xy"] != PointType(Number, Number) || m.Fields["n"] != Number {
		t.Fatalf("parsed %v", m.Fields)
	}
	if x, y := m.Fields["at"].Axes(); x != Date || y != Number {
		t.Fatalf("axes %v, %v", x, y)
	}
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"dynamic":true,"fields":{"at":{"type":"point","x":"date","y":"number"},"n":"number","xy":{"type":"point","x":"number","y":"number"}}}`
	if string(out) != want {
		t.Fatalf("marshalled %s\nwant %s", out, want)
	}
	for x := Number; x <= Date; x += Date - Number {
		for y := Number; y <= Date; y += Date - Number {
			p := PointType(x, y)
			if !p.Valid() || !p.IsPoint() || p.String() != "point("+x.String()+", "+y.String()+")" {
				t.Fatalf("PointType(%s, %s) = %v", x, y, p)
			}
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			var back FieldType
			if err := json.Unmarshal(raw, &back); err != nil || back != p {
				t.Fatalf("%s round trips to %v, %v", raw, back, err)
			}
			if _, err := p.MarshalText(); err == nil {
				t.Fatalf("%s has a text form", p)
			}
		}
	}
	if PointType(Keyword, Number) != 0 || PointType(Number, Bool) != 0 || Number.IsPoint() {
		t.Fatal("a point of a non-numeric axis")
	}
	if _, err := m.Merge(MappingUpdate{Fields: map[string]FieldType{"at": PointType(Number, Date)}}); err == nil ||
		!strings.Contains(err.Error(), "point(date, number) and cannot become point(number, date)") {
		t.Fatalf("retyping a point's axes: %v", err)
	}
}

func TestPointTypeRefused(t *testing.T) {
	for raw, want := range map[string]string{
		`"point"`:                          "names its axes",
		`{"type": "point", "x": "number"}`: "needs its y axis",
		`{"type": "point", "x": "text", "y": "number"}`:             `x axis is "number" or "date"`,
		`{"type": "point", "x": "number", "y": "number", "z": "a"}`: "unknown field",
		`{"type": "shape", "x": "number", "y": "number"}`:           "is a point",
		`{"type": "point", "x": "number", "y": "bool"}`:             `y axis is "number" or "date"`,
		`3`: "a field type is a name",
	} {
		var typ FieldType
		err := json.Unmarshal([]byte(raw), &typ)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want an error saying %q", raw, err, want)
		}
	}
}

// A point field's value is a list of [x, y] pairs or one pair; date axes take what a
// date field takes; a pair that is not two finite coordinates is no point, and leaves
// the field present.
func TestAnalyzePoints(t *testing.T) {
	m := &Mapping{Fields: map[string]FieldType{"p": PointType(Number, Number), "d": PointType(Date, Number)}}
	for body, want := range map[string][]Point{
		`{"p": [3, 4]}`:   {{3, 4}},
		`{"p": [[3, 4]]}`: {{3, 4}},
		`{"p": [[5, 1], [3, 4], [5, 0], [3, 4]]}`:               {{3, 4}, {5, 0}, {5, 1}},
		`{"p": [[-0, -0.0]]}`:                                   {{0, 0}},
		`{"p": [[1e400, 1], [1, "2"], [1], [1, 2, 3], [2, 2]]}`: {{2, 2}},
		`{"p": [1, [2, 3]]}`:                                    nil,
		`{"p": [[2, 3], 7]}`:                                    nil,
		`{"p": [1, 2, 3]}`:                                      nil,
		`{"p": []}`:                                             nil,
		`{"p": "here"}`:                                         nil,
		`{"p": [1e400, 1]}`:                                     nil,
		`{"d": [["2026-10-01", 1], [1767225600000, 2], ["2026-10-01T00:00:00.5Z", 3]]}`: {{1767225600000, 2}, {1790812800000, 1}, {1790812800500, 3}},
	} {
		doc, _, err := Analyze(m, "a", []byte(body))
		if err != nil {
			t.Fatalf("%s: %v", body, err)
		}
		var v Value
		for _, f := range []string{"p", "d"} {
			if got, ok := doc.Fields[f]; ok {
				v = got
			}
		}
		if !v.Present || !slices.Equal(v.Points, want) {
			t.Errorf("%s: present %v, points %v, want %v", body, v.Present, v.Points, want)
		}
		for _, p := range v.Points {
			if math.Signbit(p.X) || math.Signbit(p.Y) && p.Y == 0 {
				t.Errorf("%s: a -0 coordinate in %v", body, p)
			}
		}
	}
	// The cap counts valid pairs, after the invalid ones are passed over.
	pairs := []string{"[1e400, 0]", `["x", 0]`}
	for i := range MaxPoints + 5 {
		pairs = append(pairs, fmt.Sprintf("[%d, 0]", i))
	}
	doc, _, err := Analyze(m, "a", []byte(`{"p": [`+strings.Join(pairs, ",")+`]}`))
	if got := doc.Fields["p"].Points; err != nil || len(got) != MaxPoints || got[MaxPoints-1].X != MaxPoints-1 {
		t.Fatalf("past the cap: %d points, %v", len(got), err)
	}
}

// Dynamic typing never types a point: a list of pairs is a keyword_list, as before.
func TestPointsNotInferred(t *testing.T) {
	_, u, err := Analyze(&Mapping{}, "a", []byte(`{"p": [[1, 2]]}`))
	if err != nil {
		t.Fatal(err)
	}
	if u.Fields["p"] != KeywordList {
		t.Fatalf("inferred %v", u.Fields["p"])
	}
}
