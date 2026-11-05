package replica

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
	"github.com/Imposter/go-searchlight/internal/testtier"
)

var hoTypes = []schema.FieldType{schema.Keyword, schema.Text, schema.KeywordList, schema.Number, schema.Bool, schema.Date}

// hoValue is a random JSON value: of any kind, often not the one its field is mapped as.
func hoValue(r *rand.Rand, depth int) string {
	switch r.IntN(12) {
	case 0:
		return "null"
	case 1:
		return strconv.FormatBool(r.IntN(2) == 0)
	case 2:
		return strconv.Itoa(r.IntN(1000) - 10)
	case 3:
		return strconv.FormatFloat(r.Float64()*100, 'g', -1, 64)
	case 4:
		return `"2026-0` + strconv.Itoa(1+r.IntN(9)) + `-1` + strconv.Itoa(r.IntN(10)) + `"`
	case 5:
		return "[]"
	case 6:
		if depth > 0 {
			return `{"x": ` + hoValue(r, depth-1) + `}`
		}
		return "{}"
	case 7:
		n := r.IntN(4)
		parts := make([]string, n)
		for i := range parts {
			parts[i] = hoValue(r, 0)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case 8:
		return strconv.Quote(strings.Repeat("Straße ", 1+r.IntN(3)) + "Oak  CHAIR")
	default:
		words := []string{"oak", "chair", "Ünïcode", "a", "b-c", "x,y", " spaced "}
		return strconv.Quote(words[r.IntN(len(words))] + " " + words[r.IntN(len(words))])
	}
}

// hoDoc is a random document over names (the first mapped of them often, the rest now
// and then), a nested field _v among them when nested.
func hoDoc(r *rand.Rand, names []string, mapped int, subs []string) string {
	var members []string
	for i, n := range names {
		if (i < mapped && r.IntN(3) > 0) || (i >= mapped && r.IntN(8) == 0) {
			members = append(members, strconv.Quote(n)+": "+hoValue(r, 1))
		}
	}
	if r.IntN(3) > 0 {
		var objs []string
		for range r.IntN(4) {
			if r.IntN(6) == 0 {
				objs = append(objs, hoValue(r, 0))
				continue
			}
			var fields []string
			for _, s := range subs {
				if r.IntN(2) == 0 {
					fields = append(fields, strconv.Quote(s)+": "+hoValue(r, 0))
				}
			}
			objs = append(objs, "{"+strings.Join(fields, ", ")+"}")
		}
		members = append(members, `"_v": [`+strings.Join(objs, ", ")+`]`)
	}
	r.Shuffle(len(members), func(i, j int) { members[i], members[j] = members[j], members[i] })
	return "{" + strings.Join(members, ", ") + "}"
}

// The property: a document a writer analyzed under its mapping (schema.Analyze, the
// mapping then grown by its new fields, as a write commits it), when the writer's
// mapping types every field (dynamic true or strict) and fitsView accepts it under a
// copy's mapping at or after the write's (grown since by other writes, over fields the
// document holds untyped or dotted members of it too), is exactly the document the
// copy's own analysis of the committed body makes (analyzeDoc), with nothing unmapped. So a copy
// applying the handed-off document indexes what it would have indexed itself. The
// checks run over random mappings, documents with every kind of value under every
// type, nested objects, fields added after the write, and NUL-named members.
func TestHandedOffAnalysisIsTheCopys(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	names := []string{"title", "brand", "price", "tags", "when", "flag", "x\x00y", "new1", "new2", "dot.x"}
	subs := []string{"size", "price", "color", "fresh"}
	trials, accepted := testtier.Pick(3000, 30000), 0
	for trial := range trials {
		m := &schema.Mapping{Fields: map[string]schema.FieldType{}, Dynamic: []schema.DynamicMode{schema.DynamicTrue, schema.DynamicStrict}[r.IntN(2)]}
		for _, n := range names[:6] {
			if r.IntN(3) > 0 {
				m.Fields[n] = hoTypes[r.IntN(len(hoTypes))]
			}
		}
		if r.IntN(3) > 0 {
			m.Fields["_v"] = schema.Nested
			for _, s := range subs[:3] {
				if r.IntN(2) == 0 {
					m.Fields["_v."+s] = hoTypes[r.IntN(len(hoTypes))]
				}
			}
		}
		body := hoDoc(r, names, 6, subs)
		id := fmt.Sprintf("d%d", trial)
		doc, upd, err := schema.Analyze(m, id, []byte(body))
		if err != nil {
			continue
		}
		written := m
		if !upd.Empty() {
			if written, err = m.Merge(upd); err != nil {
				continue
			}
		}
		// Mappings that grew after the write: an unrelated field, a field the document
		// holds unmapped (a value no type was inferred from, such as {}), possibly as a
		// nested field, and a nested field over one of its dotted members.
		later := written.Clone()
		if r.IntN(3) == 0 {
			later.Fields["added_later"] = hoTypes[r.IntN(len(hoTypes))]
		}
		for _, n := range []string{"new1", "new2"} {
			if _, ok := later.Fields[n]; !ok && r.IntN(3) == 0 {
				if r.IntN(2) == 0 {
					later.Fields[n] = schema.Nested
				} else {
					later.Fields[n] = hoTypes[r.IntN(len(hoTypes))]
				}
			}
		}
		if _, ok := later.Fields["dot.x"]; !ok && r.IntN(3) == 0 {
			later.Fields["dot"] = schema.Nested
			later.Fields["dot.x"] = schema.Keyword
		}
		view := analysisView(later)
		if !fitsView(&doc, written, view) {
			continue
		}
		accepted++
		own, unmapped, err := analyzeDoc(view, later.Dynamic, id, doc.Body)
		if err != nil {
			t.Fatalf("trial %d: the copy refuses %s: %v", trial, body, err)
		}
		if unmapped || !reflect.DeepEqual(doc, own) {
			t.Fatalf("trial %d: under %v\n%s\nthe writer's analysis\n%+v\nis not the copy's (unmapped %v)\n%+v", trial, later.Fields, body, doc, unmapped, own)
		}
	}
	if accepted < trials/4 {
		t.Fatalf("only %d of %d trials handed off: the property is barely checked", accepted, trials)
	}
	t.Logf("%d of %d handed off", accepted, trials)
}

// A tailer applies the documents Options.Analyzed hands it, and analyzes the body itself
// for any that is not its change's (another id or other body bytes): the copy ends
// exactly as the database holds the shard, and the handed-off documents are counted (a
// copy loading the shard's records takes them by their seqs too).
func TestHandedOffDocumentsAreApplied(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "ho", testMapping)
		id := ShardID{Index: "ho", Shard: 0}
		m, err := parseMapping([]byte(testMapping))
		if err != nil {
			t.Fatal(err)
		}
		batch := make([]store.Change, 20)
		for i := range batch {
			batch[i] = upsert("ho", 0, fmt.Sprintf("d%d", i), docBody(fmt.Sprintf("d%d", i), i))
		}
		mustApply(t, st, batch...)
		handed := map[int64]*schema.Doc{}
		for i := range batch {
			body := batch[i].Payload
			docID := batch[i].ID
			switch i {
			case 3:
				body = []byte(docBody(docID, 99))
			case 5:
				docID = "someone else"
			}
			doc, _, err := schema.Analyze(m, docID, body)
			if err != nil {
				t.Fatal(err)
			}
			handed[batch[i].Seq] = &doc
		}
		reader := sdkmetric.NewManualReader()
		opts := testOptions()
		opts.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
		opts.Analyzed = func(seq int64) (*schema.Doc, *schema.Mapping) { return handed[seq], m }
		sh := openShard(t, t.TempDir(), testShardOptions(id))
		t.Cleanup(func() { _ = sh.Close(context.Background()) })
		tl := NewTailer(st, sh, id, opts)
		// Recover the empty copy from the changelog alone, then tail the rest.
		if err := tl.handle(ctx, tl.start(ctx)); err != nil {
			t.Fatal(err)
		}
		if err := tl.recoverIfNeeded(ctx); err != nil {
			t.Fatal(err)
		}
		more := make([]store.Change, 10)
		for i := range more {
			more[i] = upsert("ho", 0, fmt.Sprintf("e%d", i), docBody(fmt.Sprintf("e%d", i), 100+i))
		}
		head := mustApply(t, st, more...)
		for i := range more {
			doc, _, err := schema.Analyze(m, more[i].ID, more[i].Payload)
			if err != nil {
				t.Fatal(err)
			}
			if i == 4 {
				doc.Body = []byte(`{}`)
			}
			handed[more[i].Seq] = &doc
		}
		for caughtUp := false; !caughtUp; {
			if caughtUp, err = tl.step(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if err := sh.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
		if dd := diff(viewOf(t, sh), truthOf(t, st, id), false); dd != "" {
			t.Fatalf("the copy differs from the database:\n%s", dd)
		}
		if got := counterSum(t, reader, telemetry.MetricReplicaHandedOff, "", ""); got != 18+9 || tl.Applied() != head {
			t.Fatalf("handed off %d documents, applied %d; want %d and %d", got, tl.Applied(), 18+9, head)
		}
	})
}
