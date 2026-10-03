package segment

import (
	"bytes"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
)

// fieldInfo is one field's parsed structures, offsets resolved into live views over the
// mapping.
type fieldInfo struct {
	presRegion  region
	truncRegion region // zero (n == 0) when the field has no truncated documents
	dicts       [numKinds]*termDict
	keywordCol  *keywordColumn
	multiCol    *multiColumn
	numberCol   *numberColumn
}

// Reader is an open, immutable segment, read through mmap. Its methods never allocate
// more than the result they return, and are safe to call from many goroutines at once,
// including concurrently with [Reader.Close] (see [Reader.Retain]).
type Reader struct {
	path      string
	m         *mapping
	data      []byte
	numDocs   uint32
	fields    map[string]*fieldInfo
	stored    storedIndex
	cache     *storedCache
	ownsCache bool
	closed    atomic.Bool
}

// Open opens the segment file at path through mmap and verifies its checksums. Open
// refuses a file whose format major version it does not know ([VersionError]) or whose
// checksums do not match ([CorruptError]).
func Open(path string) (*Reader, error) {
	m, err := openFileMapping(path)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = m.release()
		}
	}()
	footer, err := verifyFile(path, m.data)
	if err != nil {
		return nil, err
	}
	r := &Reader{path: path, m: m, data: m.data, fields: map[string]*fieldInfo{}}
	metaSec := footer.sections[sectionMeta]
	metaBytes, sliceOK := region{off: metaSec.off, n: metaSec.n}.slice(m.data)
	if !sliceOK {
		return nil, &CorruptError{Path: path, Section: "meta", Reason: "section runs outside the file"}
	}
	if err := r.parseMeta(metaBytes); err != nil {
		return nil, err
	}
	cache, err := newStoredCache()
	if err != nil {
		return nil, err
	}
	r.cache = cache
	r.ownsCache = true
	ok = true
	return r, nil
}

func (r *Reader) parseMeta(b []byte) error {
	corrupt := func() error { return &CorruptError{Path: r.path, Section: "meta", Reason: "truncated or malformed"} }
	d := decoder{b: b}
	r.numDocs = d.u32()
	storedIndexOff := d.u64()
	numFields := d.u32()
	for range numFields {
		name := string(d.bytes(d.uvarint()))
		fi := &fieldInfo{}
		fi.presRegion = region{off: d.u64(), n: d.u64()}
		fi.truncRegion = region{off: d.u64(), n: d.u64()}
		var dictOffs [numKinds]uint64
		for k := range numKinds {
			dictOffs[k] = d.u64()
		}
		keywordColOff := d.u64()
		multiColOff := d.u64()
		numberColOff := d.u64()
		pointsOff := d.u64()
		if d.err != nil {
			return corrupt()
		}
		for k := range numKinds {
			if dictOffs[k] == 0 {
				continue
			}
			dict, err := openDict(r.data, dictOffs[k])
			if err != nil {
				return &CorruptError{Path: r.path, Section: "terms", Reason: err.Error()}
			}
			fi.dicts[k] = dict
		}
		if keywordColOff != 0 {
			kc, err := openKeywordColumn(r.data, keywordColOff, r.numDocs, fi.dicts[KindValue])
			if err != nil {
				return &CorruptError{Path: r.path, Section: "docvalues", Reason: err.Error()}
			}
			fi.keywordCol = kc
		}
		if multiColOff != 0 {
			mc, err := openMultiColumn(r.data, multiColOff, r.numDocs, fi.dicts[KindEntry])
			if err != nil {
				return &CorruptError{Path: r.path, Section: "docvalues", Reason: err.Error()}
			}
			fi.multiCol = mc
		}
		if numberColOff != 0 {
			nc, err := openNumberColumn(r.data, numberColOff, r.numDocs)
			if err != nil {
				return &CorruptError{Path: r.path, Section: "docvalues", Reason: err.Error()}
			}
			if pointsOff != 0 {
				p, err := openPoints(r.data, pointsOff, nc.enc)
				if err != nil {
					return &CorruptError{Path: r.path, Section: "points", Reason: err.Error()}
				}
				nc.points = p
			}
			fi.numberCol = nc
		}
		r.fields[name] = fi
	}
	if d.err != nil {
		return corrupt()
	}
	stored, err := openStoredIndex(r.data, storedIndexOff)
	if err != nil {
		return &CorruptError{Path: r.path, Section: "stored", Reason: err.Error()}
	}
	r.stored = stored
	return nil
}

// NumDocs returns how many document ordinals the segment holds, live and deleted.
func (r *Reader) NumDocs() uint32 { return r.numDocs }

// Postings returns term's documents, or an empty bitmap when field has no such
// dictionary or term is not in it.
func (r *Reader) Postings(field string, kind TermKind, term string) *roaring.Bitmap {
	info, ok := r.lookup(field, kind, term)
	if !ok {
		return roaring.New()
	}
	return bitmapAt(r.data, info)
}

// TermFreq returns how many documents hold term, 0 when there are none.
func (r *Reader) TermFreq(field string, kind TermKind, term string) uint32 {
	info, ok := r.lookup(field, kind, term)
	if !ok {
		return 0
	}
	return info.docFreq
}

func (r *Reader) lookup(field string, kind TermKind, term string) (termInfo, bool) {
	if !kind.Valid() {
		return termInfo{}, false
	}
	fi := r.fields[field]
	if fi == nil {
		return termInfo{}, false
	}
	dict := fi.dicts[kind]
	if dict == nil {
		return termInfo{}, false
	}
	return dict.lookup(stringBytes(term))
}

// Terms calls fn with every term of field's kind dictionary that starts with prefix, in
// ascending order, and its document frequency, until fn returns false or the terms run
// out.
func (r *Reader) Terms(field string, kind TermKind, prefix string, fn func(term string, docFreq uint32) bool) {
	if !kind.Valid() {
		return
	}
	fi := r.fields[field]
	if fi == nil {
		return
	}
	dict := fi.dicts[kind]
	if dict == nil {
		return
	}
	pb := stringBytes(prefix)
	block, ok := dict.blockFor(pb)
	if !ok {
		block = 0
	}
	it := dict.iter(block)
	for it.next() {
		if !bytes.HasPrefix(it.term, pb) {
			if bytes.Compare(it.term, pb) > 0 {
				return
			}
			continue
		}
		if !fn(string(it.term), it.info.docFreq) {
			return
		}
	}
}

// Numbers returns field's number column (a number, date or bool field; a bool is 0 or
// 1). The zero [NumericColumn] when field has none.
func (r *Reader) Numbers(field string) NumericColumn {
	fi := r.fields[field]
	if fi == nil {
		return NumericColumn{}
	}
	return NumericColumn{c: fi.numberCol}
}

// Keywords returns field's keyword column: ordinals into its sorted KindValue
// dictionary, for sort and terms aggregations. The zero [KeywordColumn] when field has
// none (a bool field has a KindValue dictionary but no column, since there is nothing to
// sort).
func (r *Reader) Keywords(field string) KeywordColumn {
	fi := r.fields[field]
	if fi == nil {
		return KeywordColumn{}
	}
	return KeywordColumn{c: fi.keywordCol}
}

// Entries returns field's multi-valued column: ordinals into its sorted KindEntry
// dictionary. The zero [MultiColumn] when field has none.
func (r *Reader) Entries(field string) MultiColumn {
	fi := r.fields[field]
	if fi == nil {
		return MultiColumn{}
	}
	return MultiColumn{c: fi.multiCol}
}

// Present returns field's present documents, or an empty bitmap when no document ever
// had it. A present field is recorded even when it is untyped or does not fit its
// mapped type.
func (r *Reader) Present(field string) *roaring.Bitmap {
	fi := r.fields[field]
	if fi == nil {
		return roaring.New()
	}
	return viewBitmap(r.data, fi.presRegion)
}

// Truncated returns the documents whose field value was too long to have grams
// ([schema.Value.GramsTruncated]): candidates for any contains or starts_with needle
// that shares no gram with the index.
func (r *Reader) Truncated(field string) *roaring.Bitmap {
	fi := r.fields[field]
	if fi == nil || fi.truncRegion.n == 0 {
		return roaring.New()
	}
	return viewBitmap(r.data, fi.truncRegion)
}

// Stored returns document ord's original body.
func (r *Reader) Stored(ord uint32) ([]byte, error) {
	_, body, err := r.storedRecord(ord)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(body), nil
}

// ID returns document ord's id.
func (r *Reader) ID(ord uint32) (string, error) {
	id, _, err := r.storedRecord(ord)
	return id, err
}

func (r *Reader) storedRecord(ord uint32) (string, []byte, error) {
	b, ok := r.stored.blockFor(ord)
	if !ok {
		return "", nil, &CorruptError{Path: r.path, Section: "stored", Reason: "no block holds that ordinal"}
	}
	return r.cache.record(r.data, b, ord)
}

// Ord returns id's document ordinal, through the "_id" field's term dictionary. False
// when no document has that id.
func (r *Reader) Ord(id string) (uint32, bool) {
	info, ok := r.lookup(idFieldName, KindValue, id)
	if !ok {
		return 0, false
	}
	if info.docFreq == 1 {
		return info.single, true
	}
	// Duplicate ids should never reach a segment; fall back to the first document.
	docs := appendDocs(nil, r.data, info)
	if len(docs) == 0 {
		return 0, false
	}
	return docs[0], true
}

// idFieldName is the pseudo-field [schema.IDField] holds a document's id under. segment
// does not import schema to avoid a dependency cycle risk; the name is fixed by
// schema.Analyze.
const idFieldName = "_id"

// Retain returns a second handle on the same open segment, so it stays mapped even
// after the original is Closed. The returned *Reader is independent: Close it when
// done, separately from the one it was retained from.
func (r *Reader) Retain() *Reader {
	r.m.retain()
	nr := &Reader{
		path:    r.path,
		m:       r.m,
		data:    r.data,
		numDocs: r.numDocs,
		fields:  r.fields,
		stored:  r.stored,
	}
	if cache, err := newStoredCache(); err == nil {
		nr.cache = cache
		nr.ownsCache = true
	} else {
		// A second zstd decoder failing to allocate is as unexpected as it is
		// harmless to ignore: fall back to sharing the original's cache.
		nr.cache = r.cache
		nr.ownsCache = false
	}
	return nr
}

// Close unmaps the segment, once every handle from [Reader.Retain] (and the one Open
// returned) has also been closed. Idempotent, and safe to call concurrently with any
// other method.
func (r *Reader) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	if r.ownsCache {
		r.cache.close()
	}
	return r.m.release()
}
