package segment

// DiskUsage is where a segment file's bytes go: per section, and per field and
// structure inside the sections.
type DiskUsage struct {
	File     int64
	Sections []DiskSection
	Parts    []DiskPart
}

// DiskSection is one section's size.
type DiskSection struct {
	Name  string
	Bytes int64
}

// DiskPart is one structure's size: Part names it ("value.postings", "gram.terms",
// "keyword column", "points", "stored blocks", ...), and Field is its field, or "" for a
// segment-wide one.
type DiskPart struct {
	Field string
	Part  string
	Bytes int64
}

// DiskUsage returns the segment's byte breakdown, for footprint reports. It walks every
// term dictionary, so it is for tools, not for a hot path.
func (r *Reader) DiskUsage() DiskUsage {
	u := DiskUsage{File: int64(len(r.data))}
	footer, err := verifyFileSections(r.path, r.data, nil)
	if err == nil {
		for _, k := range sectionOrder {
			if s, ok := footer.sections[k]; ok {
				u.Sections = append(u.Sections, DiskSection{Name: k.String(), Bytes: int64(s.n)}) //nolint:gosec // a section of a mapped file
			}
		}
	}
	add := func(field, part string, n uint64) {
		if n > 0 {
			u.Parts = append(u.Parts, DiskPart{Field: field, Part: part, Bytes: int64(n)}) //nolint:gosec // within a mapped file
		}
	}
	for name, fi := range r.fields {
		for k := range numKinds {
			if d := fi.dicts[k]; d != nil {
				post, total := d.usage()
				add(name, TermKind(k).String()+".postings", post)
				add(name, TermKind(k).String()+".terms", total-post)
			}
		}
		if c := fi.keywordCol; c != nil {
			add(name, "keyword column", uint64(len(c.packed))+1)
		}
		if c := fi.multiCol; c != nil {
			add(name, "multi column", uint64(len(c.offs)+len(c.ords))+10)
		}
		if c := fi.numberCol; c != nil {
			add(name, "number column", uint64(len(c.packed)+len(c.present))+44)
			if p := c.points; p != nil {
				n := pointsHeaderLen(r.major) + uint64(len(p.table))
				for i := range p.numBlocks {
					_, _, count, _ := p.blockEntry(i)
					n += p.blockSize(count)
				}
				add(name, "points", n)
			}
		}
		add(name, "presence", fi.presRegion.n)
		add(name, "truncated", fi.truncRegion.n)
		add(name, "untyped", fi.untypedRegion.n)
	}
	if r.ids != nil {
		_, total := r.ids.usage()
		add("", "ids", total)
	}
	if s, ok := footer.sections[sectionStored]; ok {
		dict := uint64(len(r.stored.dict))
		table := uint64(len(r.stored.firstOrds)+len(r.stored.offs)+len(r.stored.raws)) + storedIndexHeaderLen
		if r.stored.v3 != nil {
			table = 4 + uint64(len(r.stored.v3))*storedEntryLenV3
		}
		add("", "stored dictionary", dict)
		add("", "stored table", table)
		add("", "stored blocks", s.n-dict-min(table, s.n-dict))
	}
	return u
}

// usage returns the bytes of the dictionary's serialized postings, and of the whole
// dictionary (postings, term blocks and index).
func (t *termDict) usage() (postings, total uint64) {
	if t.numBlocks == 0 {
		return 0, 0
	}
	c := t.cursor(0)
	start := c.post
	it := t.iter(0)
	for it.next() {
		if it.info.docFreq > 1 {
			postings += it.info.post.n
		}
	}
	end := t.base + 16 + uint64(t.numBlocks)*8 + uint64(t.numBlocks+1)*4 + uint64(t.keyOff(t.numBlocks))
	return postings, end - start
}
