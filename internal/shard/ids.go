package shard

import (
	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
)

// Finding a document by id in a segment.
//
// schema.Analyze indexes a document's id under [schema.IDField] as a keyword, that is
// normalized: "SKU-1" and "sku-1" are two documents but one _id term. segment.Reader.Ord
// looks the raw id up in that dictionary, so on its own it misses a mixed-case id and
// cannot tell two such documents apart. The shard therefore stores, for every document
// whose normalized id differs from its id, the exact id under a hidden keyword field,
// uidField, whose name holds a NUL (which no mapped or queried field name can). Then:
//
//   - a segment with no uidField at all (every id is already normalized: the common
//     case) answers with one Ord call;
//   - an id that normalizes to something else can only be under uidField;
//   - an already-normalized id is the _id posting that is not under uidField (any
//     others are documents whose exact ids differ only in case or spacing).
//
// Within one segment an id has at most one copy: a refresh writes each id once, and a
// merge keeps only live copies, of which the shard has at most one per id.

// uidField is the hidden exact-id field.
const uidField = "\x00uid"

// idTerm is how schema.Analyze indexes id under schema.IDField.
func idTerm(id string) string { return analysis.Clean(analysis.Normalize(id)) }

// prepareDoc returns d as segment.Build should store it: with its _id term (which a
// hand-built Doc may lack) and, when the id is not already normalized, the hidden
// exact id. d itself is never changed: a document needing either gets a copy of its
// field map.
func prepareDoc(d *schema.Doc) schema.Doc {
	out := *d
	term := idTerm(d.ID)
	v, ok := d.Fields[schema.IDField]
	needID := !ok || v.Text == nil || *v.Text != term
	needUID := term != d.ID
	if !needID && !needUID {
		return out
	}
	fields := make(map[string]schema.Value, len(d.Fields)+2)
	for name, v := range d.Fields {
		fields[name] = v
	}
	if needID {
		fields[schema.IDField] = schema.Value{Present: true, Text: &term}
	}
	if needUID {
		id := d.ID
		fields[uidField] = schema.Value{Present: true, Text: &id}
	}
	out.Fields = fields
	return out
}

// docIDs is a doc segment's id index: its reader, and the documents that carry the
// hidden exact id (a view over the reader's mapping, valid as long as the reader).
type docIDs struct {
	r          *segment.Reader
	uidPresent *roaring.Bitmap // nil when no document in the segment has uidField
}

func newDocIDs(r *segment.Reader) docIDs {
	ids := docIDs{r: r}
	if p := r.Present(uidField); !p.IsEmpty() {
		ids.uidPresent = p
	}
	return ids
}

// lookup returns id's ordinal in the segment, deleted or not.
func (x docIDs) lookup(id string) (uint32, bool) {
	if x.uidPresent == nil {
		// Every term in _id is its document's exact id, and an id that is not
		// normalized matches no term at all.
		return x.r.Ord(id)
	}
	if idTerm(id) != id {
		p := x.r.Postings(uidField, segment.KindValue, id)
		if p.IsEmpty() {
			return 0, false
		}
		return p.Minimum(), true
	}
	if x.r.TermFreq(schema.IDField, segment.KindValue, id) == 1 {
		ord, ok := x.r.Ord(id)
		if !ok || x.uidPresent.Contains(ord) {
			return 0, false
		}
		return ord, true
	}
	it := x.r.Postings(schema.IDField, segment.KindValue, id).Iterator()
	for it.HasNext() {
		if ord := it.Next(); !x.uidPresent.Contains(ord) {
			return ord, true
		}
	}
	return 0, false
}
