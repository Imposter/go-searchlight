package replica

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// view is what a copy holds, in a form two copies can be compared by: every live
// document's body, every live saved query (seq, canonical tree, meta), and the index
// itself: for every mapped field, which documents each value, entry and word term
// and each number belong to.
type view struct {
	seq     int64
	docs    map[string]string
	queries map[string]string
	terms   map[string]string
	mapping string
}

// viewOf reads sh's current generation.
func viewOf(t testing.TB, sh *shard.Shard) view {
	t.Helper()
	g := sh.Acquire()
	if g == nil {
		t.Fatal("the shard is closed")
	}
	defer g.Release()
	v := view{seq: g.Seq(), docs: map[string]string{}, queries: map[string]string{}, terms: map[string]string{}}
	m := g.Mapping()
	if m != nil {
		v.mapping = mappingString(m)
	}
	postings := map[string][]string{}
	for _, sv := range g.Segments {
		ids := make([]string, sv.NumDocs)
		for ord := range sv.NumDocs {
			if sv.Deletes.Contains(ord) {
				continue
			}
			id, err := sv.Reader.ID(ord)
			if err != nil {
				t.Fatal(err)
			}
			body, err := sv.Reader.Stored(ord)
			if err != nil {
				t.Fatal(err)
			}
			if _, dup := v.docs[id]; dup {
				t.Fatalf("document %q is live twice", id)
			}
			v.docs[id] = string(body)
			ids[ord] = id
		}
		if m == nil {
			continue
		}
		for field, ft := range m.Fields {
			for _, kind := range []segment.TermKind{segment.KindValue, segment.KindEntry, segment.KindWord} {
				sv.Reader.Terms(field, kind, "", func(term string, _ uint32) bool {
					it := sv.Reader.Postings(field, kind, term).Iterator()
					for it.HasNext() {
						if id := ids[it.Next()]; id != "" {
							k := fmt.Sprintf("%s/%d/%s", field, kind, term)
							postings[k] = append(postings[k], id)
						}
					}
					return true
				})
			}
			if ft == schema.Number || ft == schema.Date || ft == schema.Bool {
				col := sv.Reader.Numbers(field)
				for ord, id := range ids {
					if id == "" {
						continue
					}
					if x, ok := col.Value(uint32(ord)); ok {
						k := "num/" + field
						postings[k] = append(postings[k], fmt.Sprintf("%s=%g", id, x))
					}
				}
			}
		}
	}
	for k, ids := range postings {
		sort.Strings(ids)
		v.terms[k] = strings.Join(ids, ",")
	}
	for _, qv := range g.QuerySegments {
		for ord := range qv.NumQueries {
			if qv.Deletes.Contains(ord) {
				continue
			}
			q, err := qv.Segment.Query(ord)
			if err != nil {
				t.Fatal(err)
			}
			if _, dup := v.queries[q.ID]; dup {
				t.Fatalf("query %q is live twice", q.ID)
			}
			v.queries[q.ID] = fmt.Sprintf("%d %s %s", q.Seq, query.Canonical(q.Query), q.Meta)
		}
	}
	return v
}

func mappingString(m *schema.Mapping) string {
	var b strings.Builder
	fmt.Fprintf(&b, "dynamic=%s", m.Dynamic)
	for _, f := range slices.Sorted(maps.Keys(m.Fields)) {
		fmt.Fprintf(&b, " %s:%s", f, m.Fields[f])
	}
	return b.String()
}

// truthOf is what the store holds for the shard, in a view's terms (docs and queries).
func truthOf(t testing.TB, st store.Store, id ShardID) view {
	t.Helper()
	v := view{docs: map[string]string{}, queries: map[string]string{}}
	asOf, err := st.ScanShard(context.Background(), id, func(r store.Record) error {
		switch r.Kind {
		case store.RecordDocument:
			v.docs[r.ID] = string(r.Body)
		case store.RecordQuery:
			n, problems := query.Parse(r.Body)
			if len(problems) > 0 {
				return fmt.Errorf("query %s: %v", r.ID, problems)
			}
			v.queries[r.ID] = fmt.Sprintf("%d %s %s", r.Seq, query.Canonical(n), normalizeMeta(r.Meta))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	v.seq = asOf
	meta, err := st.Indexes().Get(context.Background(), id.Index)
	if err != nil {
		t.Fatal(err)
	}
	m, err := parseMapping(meta.Mapping)
	if err != nil {
		t.Fatal(err)
	}
	v.mapping = mappingString(m)
	return v
}

// diff describes how two views differ, "" when they do not. withTerms compares the
// index too.
func diff(a, b view, withTerms bool) string {
	var out []string
	cmp := func(what string, x, y map[string]string) {
		for _, k := range slices.Sorted(maps.Keys(x)) {
			if yv, ok := y[k]; !ok {
				out = append(out, fmt.Sprintf("%s %q only in the first: %s", what, k, x[k]))
			} else if yv != x[k] {
				out = append(out, fmt.Sprintf("%s %q: %s vs %s", what, k, x[k], yv))
			}
		}
		for _, k := range slices.Sorted(maps.Keys(y)) {
			if _, ok := x[k]; !ok {
				out = append(out, fmt.Sprintf("%s %q only in the second: %s", what, k, y[k]))
			}
		}
	}
	cmp("document", a.docs, b.docs)
	cmp("query", a.queries, b.queries)
	if withTerms {
		cmp("postings", a.terms, b.terms)
	}
	if len(out) > 10 {
		out = append(out[:10], fmt.Sprintf("... and %d more", len(out)-10))
	}
	return strings.Join(out, "\n")
}
