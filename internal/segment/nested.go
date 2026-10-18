package segment

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// Nested fields.
//
// A segment whose documents hold a nested field's objects (the members of its array)
// keeps them in its NESTED section, one entry per field, in name order:
//
//	object segment  a whole segment image (header to footer, its own checksums) over
//	                object ordinals, with flagObjects set and no STORED and no IDS
//	                section: its fields are the nested field's own name, present for
//	                every member that is an object, and the sub-fields by their full
//	                names, built by the builders a segment's documents are and read by
//	                the same Reader
//	starts          each document's first object ordinal, then the object count:
//	                numDocs+1 values, bit-packed, ascending
//
// then the directory: the field count (u32), per field its name (uvarint length,
// bytes), the object segment's offset and length, the starts' offset (u64 each) and
// width (u8), offsets relative to the section; then the directory's offset (u64).
//
// Document d's objects are the ordinals [starts[d], starts[d+1]), in array order; a
// member that is no object holds its ordinal with nothing in it. The documents
// themselves are untouched: their ordinals, ids, stored bodies and every structure of
// their own fields are what they would be without the section, which is written only
// when some document holds a member. An object is live while its document is: a
// merge keeps the live documents' objects, in document order, as a build of those
// documents numbers them.

// nestedPart is one nested field's objects, as a build or a merge writes them: the
// starts, and the object segment's image but its object count.
type nestedPart struct {
	path   string
	starts []uint32
	names  []string
	parts  []map[string]*fieldBuilder
	dicts  dictSources
	flags  uint32
}

// nestedStarts returns, sorted by path, a nestedPart for each nested field some
// document holds members of, with its starts.
func nestedStarts(docs []schema.Doc) ([]nestedPart, error) {
	paths := map[string]bool{}
	for d := range docs {
		for name := range docs[d].Fields {
			if len(docs[d].Fields[name].Objects) > 0 {
				paths[name] = true
			}
		}
	}
	out := make([]nestedPart, 0, len(paths))
	for path := range paths {
		starts := make([]uint32, len(docs)+1)
		var next uint64
		for d := range docs {
			starts[d] = uint32(next)
			next += uint64(len(docs[d].Fields[path].Objects)) //nolint:gosec // a length
			if next > math.MaxUint32 {
				return nil, fmt.Errorf("segment: nested field %q holds over %d objects", path, uint32(math.MaxUint32))
			}
		}
		starts[len(docs)] = uint32(next)
		out = append(out, nestedPart{path: path, starts: starts})
	}
	slices.SortFunc(out, func(a, b nestedPart) int { return cmp.Compare(a.path, b.path) })
	return out, nil
}

// writeNestedSection writes img's NESTED section: each nested field's object segment
// and starts, then the directory.
func writeNestedSection(w *fileWriter, spill string, throttle func(n int) error, img *image, threads int) error {
	w.beginSection(sectionNested)
	start := w.off
	var dir encoder
	dir.u32(uint32(len(img.nested))) //nolint:gosec // a segment holds far fewer than 4 billion fields
	for i := range img.nested {
		np := &img.nested[i]
		numObjects := np.starts[img.numDocs]
		off := w.off - start
		iw := newFileWriter(sectionSink{w})
		iw.abort = w.abort
		obj := &image{numDocs: numObjects, flags: np.flags, names: np.names, parts: np.parts, dicts: np.dicts}
		if err := writeImage(iw, spill, throttle, obj, threads); err != nil {
			return err
		}
		if w.err != nil {
			return w.err
		}
		size := iw.off
		startsOff := w.off - start
		width := packedWidth(bitsFor(uint64(numObjects)))
		p := newPacker(w, width)
		for _, s := range np.starts {
			p.add(uint64(s))
		}
		p.finish()
		dir.bytes([]byte(np.path))
		dir.u64(off)
		dir.u64(size)
		dir.u64(startsOff)
		dir.u8(width)
	}
	dirOff := w.off - start
	w.write(dir.b)
	w.u64(dirOff)
	w.endSection()
	return w.err
}

// mergeNested is [Merge]'s nestedPart of each nested field an input holds objects
// of, with live documents' objects (remaps, total as Merge computed them): each
// input's object segment merged as Merge merges segments, an object at its document's
// new place in the order of its old ordinal. A field no live document holds a member
// of is left out, as a build of the live documents leaves it.
func mergeNested(inputs []*Reader, remaps [][]int32, total uint64, threads int, check func(int) error, flags uint32) ([]nestedPart, error) {
	paths := map[string]bool{}
	for _, r := range inputs {
		for path := range r.nested {
			paths[path] = true
		}
	}
	var out []nestedPart
	for _, path := range slices.Sorted(maps.Keys(paths)) {
		starts := make([]uint32, total+1)
		var next uint64
		var readers []*Reader
		var objRemaps [][]int32
		for i, r := range inputs {
			nv := r.nested[path]
			var objRemap []int32
			if nv != nil {
				objRemap = make([]int32, nv.NumObjects())
			}
			for old, newOrd := range remaps[i] {
				var lo, hi uint32
				if nv != nil {
					lo, hi = nv.Range(uint32(old))
				}
				if newOrd < 0 {
					for o := lo; o < hi; o++ {
						objRemap[o] = -1
					}
					continue
				}
				if next+uint64(hi-lo) > math.MaxInt32 {
					return nil, fmt.Errorf("segment: merge produces over %d objects of nested field %q", math.MaxInt32, path)
				}
				starts[newOrd] = uint32(next)
				for o := lo; o < hi; o++ {
					objRemap[o] = int32(next)
					next++
				}
			}
			if nv != nil {
				readers = append(readers, nv.objects)
				objRemaps = append(objRemaps, objRemap)
			}
		}
		if next == 0 {
			continue
		}
		starts[total] = uint32(next)
		groups := splitReaderRanges(len(readers), threads)
		parts := make([]map[string]*fieldBuilder, len(groups))
		errs := make([]error, len(groups))
		mergeGroups(readers, objRemaps, groups, parts, errs, check)
		if err := errors.Join(errs...); err != nil {
			return nil, err
		}
		out = append(out, nestedPart{
			path: path, starts: starts, names: liveFieldNames(parts), parts: parts,
			dicts: readersDicts(readers, objRemaps), flags: flags | flagObjects,
		})
	}
	return out, nil
}

// Nested is one nested field's objects in a segment: their own index, a Reader over
// object ordinals (its fields the nested field's name, present for each member that is
// an object, and the sub-fields by their full names), and each document's objects, a
// run of ordinals in array order. Its Reader shares the segment's mapping: it is valid
// while the segment's Reader is, and is never closed itself.
type Nested struct {
	objects *Reader
	starts  []byte
	width   uint8
	numDocs uint32
}

// Nested returns nested field path's objects, nil when no document of the segment holds
// a member of it.
func (r *Reader) Nested(path string) *Nested {
	return r.nested[path]
}

// Objects returns the objects' index: a Reader whose documents are the objects.
func (n *Nested) Objects() *Reader { return n.objects }

// NumObjects returns how many members the segment's documents hold, objects or not.
func (n *Nested) NumObjects() uint32 { return n.objects.numDocs }

func (n *Nested) start(doc uint32) uint32 {
	return uint32(unpack(n.starts, uint64(doc), n.width)) //nolint:gosec // validated at Open: at most the object count
}

// Range returns document doc's objects, the ordinals [lo, hi).
func (n *Nested) Range(doc uint32) (lo, hi uint32) {
	if doc >= n.numDocs {
		return 0, 0
	}
	return n.start(doc), n.start(doc + 1)
}

// Parent returns the document holding object obj; the document count for an obj past
// the last object.
func (n *Nested) Parent(obj uint32) uint32 {
	lo, hi := uint32(0), n.numDocs
	for lo < hi {
		mid := lo + (hi-lo)/2
		if n.start(mid+1) <= obj {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Parents returns the documents holding an object of objs. The documents ascend with
// the objects, so each is sought from the last one found, at steps that double.
func (n *Nested) Parents(objs *roaring.Bitmap) *roaring.Bitmap {
	out := roaring.New()
	it := objs.Iterator()
	buf := make([]uint32, 0, 256)
	d := uint32(0)
	for it.HasNext() {
		obj := it.Next()
		if d = n.ParentFrom(d, obj); d >= n.numDocs {
			break
		}
		if buf = append(buf, d); len(buf) == cap(buf) {
			out.AddMany(buf)
			buf = buf[:0]
		}
		it.AdvanceIfNeeded(n.start(d + 1))
	}
	out.AddMany(buf)
	return out
}

// ParentFrom is Parent of obj, sought from document from on, at steps that double:
// obj must not be before from's objects. Ascending objects find their documents
// ascending, each from the last.
func (n *Nested) ParentFrom(from, obj uint32) uint32 {
	lo, step := from, uint32(1)
	hi := lo
	for hi < n.numDocs && n.start(hi+1) <= obj {
		lo = hi + 1
		hi = min(n.numDocs, lo+step)
		step *= 2
	}
	for lo < hi {
		mid := lo + (hi-lo)/2
		if n.start(mid+1) <= obj {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// ObjectsOf returns the objects of docs' documents.
func (n *Nested) ObjectsOf(docs *roaring.Bitmap) *roaring.Bitmap {
	out := roaring.New()
	if docs.GetCardinality() >= uint64(n.numDocs) && n.numDocs > 0 && docs.Maximum() < n.numDocs {
		out.AddRange(0, uint64(n.NumObjects()))
		return out
	}
	it := docs.ManyIterator()
	buf := make([]uint32, 512)
	for {
		k := it.NextMany(buf)
		if k == 0 {
			return out
		}
		for _, d := range buf[:k] {
			if lo, hi := n.Range(d); hi > lo {
				out.AddRange(uint64(lo), uint64(hi))
			}
		}
	}
}

// openNestedSection reads the NESTED section's directory and opens every nested
// field's object segment and starts. An entry names a field of META, at most once, in
// name order; its starts ascend from 0 to its object count.
func (r *Reader) openNestedSection(sec sectionEntry) error {
	if sec.n < 8 {
		return errShort
	}
	dirOff := binary.LittleEndian.Uint64(r.data[sec.off+sec.n-8:])
	if dirOff > sec.n-8 {
		return errShort
	}
	d := decoder{b: r.data[sec.off+dirOff : sec.off+sec.n-8]}
	count := d.u32()
	prev := ""
	for i := range count {
		name := string(d.bytes(d.uvarint()))
		off, size, startsOff := d.u64(), d.u64(), d.u64()
		width := d.u8()
		if d.err != nil {
			return d.err
		}
		startsLen := packedSize(uint64(r.numDocs)+1, width)
		switch {
		case i > 0 && name <= prev, r.fields[name] == nil:
			return errors.New("the directory names a field twice, out of order, or one META does not")
		case off > dirOff || size > dirOff-off || startsOff > dirOff || startsLen > dirOff-startsOff || !validWidth(width):
			return errShort
		}
		prev = name
		objects, err := openObjects(r.path, r.data[sec.off+off:sec.off+off+size])
		if err != nil {
			return fmt.Errorf("nested field %q: %w", name, err)
		}
		n := &Nested{objects: objects, starts: r.data[sec.off+startsOff : sec.off+startsOff+startsLen], width: width, numDocs: r.numDocs}
		last := uint32(0)
		for doc := uint32(0); doc <= r.numDocs; doc++ {
			s := n.start(doc)
			if (doc == 0 && s != 0) || s < last {
				return fmt.Errorf("nested field %q: starts do not ascend from 0", name)
			}
			last = s
		}
		if last != objects.numDocs {
			return fmt.Errorf("nested field %q: starts end at %d, for %d objects", name, last, objects.numDocs)
		}
		if r.nested == nil {
			r.nested = make(map[string]*Nested, count)
		}
		r.nested[name] = n
	}
	if d.err != nil || d.pos != len(d.b) {
		return errShort
	}
	return nil
}

// openObjects opens an object segment over data, a part of its segment's mapping:
// verified and parsed as a segment is, with no stored records and no ids.
func openObjects(path string, data []byte) (*Reader, error) {
	footer, err := verifyFileSections(path, data, objectSections[:])
	if err != nil {
		return nil, err
	}
	if footer.major != FormatMajor {
		return nil, errors.New("an object segment of another major")
	}
	r := &Reader{path: path, data: data, major: footer.major, fields: map[string]*fieldInfo{}}
	metaSec := footer.sections[sectionMeta]
	metaBytes, ok := region{off: metaSec.off, n: metaSec.n}.slice(data)
	if !ok {
		return nil, errShort
	}
	if err := r.parseMeta(metaBytes, footer); err != nil {
		return nil, err
	}
	if r.flags&flagObjects == 0 {
		return nil, errors.New("an object segment without its flag")
	}
	return r, nil
}
