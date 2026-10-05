package percolate

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// The query segment format.
//
// A query segment is one file, NAME.perc, memory-mapped at Open and checked against a
// CRC32C over everything before its last four bytes, then validated structurally
// (every offset, length, ordinal, tree link and program in range), so that a damaged or
// crafted file is refused with a *CorruptError and every accessor of an opened segment
// is safe. Nothing per query is decoded onto the heap: a segment of a million queries
// costs its mapped pages, which the OS keeps or drops like any file cache. Its parts,
// each a length-prefixed section after the header (magic, version, section count):
//
//	meta       queries, fields, dictionary entries, hash slots, always-check, tree nodes, tree records, pairs (u32 each)
//	fields     per field a query names: name, the atom kinds it has terms of (a bit mask), its interval tree's root, node and record ranges
//	offsets    u64 per query + 1: each query's record in records
//	records    per query: id, seq (varint), query JSON, meta (id, JSON and meta uvarint-length-prefixed)
//	verify     32 bytes per rank: the first rank of its verification class (see below; all ones when no
//	           other query shares it), its class's program's length, and the program itself when it fits
//	           in the other 24 bytes, else its offset in programs
//	ids        8 bytes per rank: its ordinal, and the end of its id in idtext
//	idtext     the ids, concatenated in rank order (what a match returns, read front to back)
//	programs   the verification classes' programs too long to inline (prog.go)
//	slots      u32 per hash slot (a power of two): 0 empty, else a dictionary entry + 1
//	entries    32 bytes per dictionary entry: hash, field<<8|kind, term offset and length, postings offset
//	           and count, filtered offset and count
//	terms      the dictionary terms
//	postings   u32 ranks, ascending per entry and per pair
//	filtered   24 bytes per filtered posting (see Filter), ascending per entry: its rank,
//	           field<<1|bool, and the range (lo, hi) the field's value must lie in
//	partners   8 bytes per dictionary entry: the first pair record it owns and how many (a member's only)
//	pairs      12 bytes per pair: its partner (a member entry), postings offset and count
//	always     u32 ranks of the always-check list, ascending
//	nodes      24 bytes per interval tree node: center, left, right, record start and count
//	bylo       20 bytes per tree record (lo, hi, rank), each node's sorted by lo ascending
//	byhi       the same records, each node's sorted by hi descending
//
// The dictionary is an open-addressing hash table over (kind, field, term) with a
// stable hash (64-bit FNV-1a), so a probe is a hash and one or two term compares with
// nothing decoded or allocated. The interval tree is a centered interval tree per
// numeric field, its center the median of the field's endpoints, so a value finds the
// ranges holding it in O(log n + k).
//
// Ranks: a query's ordinal is its position in the segment as the shard numbers it (its
// deletes, Query and Ord use it), its rank its position sorted by id. Everything
// percolation reads is numbered by rank, so the candidates, taken in ascending order,
// are already in the order their ids are returned in.
//
// Pair anchors (two atoms a query needs together) are stored once each, under one of
// their halves, both of which are AtomMember entries (whose postings are every query
// either is half of). The owner is the half with fewer pairs, so a frequent atom
// paired with many rare ones keeps a short list. A document collects the members it
// holds, then walks their partner lists, keeping the pairs whose partner it holds
// too: work linear in those lists, never quadratic in the members (see probePairs).
//
// Verification classes: queries whose canonical form ([query.Canonical]) is the same
// match the same documents, so they share one program and, per document, one
// verification. A query with a words_* condition is its own class (keyed by its exact
// JSON), because the matcher reads a phrase's words from the text as written, which the
// canonical form does not keep.
//
// Proven leaves: a query whose only anchor is one atom that a leaf at its root (the
// root itself, or a child of an all there) holds exactly when a document holds the atom
// (eq on a string, a bool or a number, has of one entry, exists, lte, gte, between) is
// a candidate only for documents holding that atom, so that leaf always holds on a
// candidate and its program leaves it out. The proven atom is part of the class key, so
// a class's members are all proven on the same atom.

const (
	// FormatName is the format [Index] builds; shards record it per query segment.
	FormatName = "percolate/3"
	// FileExt is a query segment file's extension.
	FileExt = ".perc"

	formatVersion = 3
	numSections   = 19
	entrySize     = 32
	filteredSize  = 24
	nodeSize      = 24
	recordSize    = 20
	metaSize      = 8 * 4
	pairSize      = 12
	verifySize    = 32
	inlineProg    = verifySize - 8
	idSize        = 8
	// noClass is a verify record's class when no other query shares it.
	noClass      = math.MaxUint32
	minFieldSize = 1 + 6*4
)

var fileMagic = [8]byte{'S', 'L', 'P', 'E', 'R', 'C', '\r', '\n'}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Sections, in file order.
const (
	secMeta = iota
	secFields
	secOffsets
	secRecords
	secVerify
	secIDs
	secIDText
	secPrograms
	secSlots
	secEntries
	secTerms
	secPostings
	secFiltered
	secPartners
	secPairs
	secAlways
	secNodes
	secByLo
	secByHi
)

var sectionNames = [numSections]string{
	"meta", "fields", "offsets", "records", "verify", "ids", "idtext", "programs", "slots", "entries",
	"terms", "postings", "filtered", "partners", "pairs", "always", "nodes", "bylo", "byhi",
}

// CorruptError is a query segment file that fails its checksum or does not parse.
type CorruptError struct {
	Path string
	Why  string
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("percolate: query segment %s: corrupt: %s", e.Path, e.Why)
}

// Unwrap makes a CorruptError match segment.ErrCorrupt.
func (e *CorruptError) Unwrap() error { return segment.ErrCorrupt }

// Index is the percolator's [shard.QueryIndexBuilder]: set it as the shard's
// Options.QueryIndex. Its zero value is ready to use.
type Index struct{}

var _ shard.QueryIndexBuilder = Index{}

// Format implements [shard.QueryIndexBuilder].
func (Index) Format() string { return FormatName }

// Check implements [shard.QueryIndexBuilder]: the query must survive a round trip
// through its JSON form unchanged, which is how the segment stores it.
func (Index) Check(q *shard.StoredQuery) error {
	raw, err := encodeQuery(q.Query)
	if err != nil {
		return err
	}
	back, problems := query.Parse(raw)
	if len(problems) > 0 {
		return fmt.Errorf("query does not round-trip: %s: %s", problems[0].Loc, problems[0].Message)
	}
	if !bytes.Equal(query.Canonical(back), query.Canonical(q.Query)) {
		return errors.New("query does not round-trip through JSON")
	}
	return nil
}

// Build implements [shard.QueryIndexBuilder]: it extracts every query's anchors under
// stats and writes the segment name in dir.
func (Index) Build(ctx context.Context, dir, name string, queries []shard.StoredQuery, stats shard.TermStats) (int64, error) {
	data, err := encodeSegment(ctx, queries, stats)
	if err != nil {
		return 0, err
	}
	if err := writeFile(filepath.Join(dir, name+FileExt), data); err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

// Open implements [shard.QueryIndexBuilder]: it maps the file and validates it.
func (Index) Open(dir, name string) (shard.QuerySegment, error) {
	path := filepath.Join(dir, name+FileExt)
	m, err := mapFile(path)
	if err != nil {
		return nil, err
	}
	seg, err := openData(path, m.data)
	if err != nil {
		_ = m.close()
		return nil, err
	}
	seg.mapped = m
	return seg, nil
}

// ---- building ----

// interval is one Range of one query, as the tree holds it.
type interval struct {
	lo, hi float64
	rank   uint32
}

type dictEntry struct {
	kind     AtomKind
	field    uint32
	term     string
	posts    []uint32
	filtered []filteredPost
}

// filteredPost is one filtered posting, as built.
type filteredPost struct {
	rank, field uint32
	isBool      bool
	lo, hi      float64
}

type fieldBuild struct {
	name   string
	kinds  uint32
	ranges []interval
}

// segmentBuilder accumulates a query segment.
type segmentBuilder struct {
	ex      *extractor
	fields  []fieldBuild
	fieldAt map[string]uint32
	dict    map[string]*dictEntry
	key     []byte
	always  []uint32
	classes []uint32
	classOf map[string]uint32
	// progs are the classes' programs too long to inline; prog is each rank's class's
	// program as its verify record holds it.
	pc    progCompiler
	progs []byte
	prog  []progRef

	// pairs are the pair anchors, keyed by their members' dictionary keys in order;
	// degree counts each member's pairs and memberCost its estimated frequency.
	pairs      map[[2]string]*pairBuild
	degree     map[string]int
	memberCost map[string]float64
}

// progRef is a program as a verify record holds it: its length, and the program itself
// when it fits, else its offset in programs.
type progRef struct {
	n  uint32
	at [inlineProg]byte
}

type pairBuild struct {
	posts []uint32
}

func encodeSegment(ctx context.Context, queries []shard.StoredQuery, stats shard.TermStats) ([]byte, error) {
	if len(queries) >= math.MaxUint32 {
		return nil, fmt.Errorf("percolate: %d queries is too many for one segment", len(queries))
	}
	b := &segmentBuilder{
		ex:      newExtractor(stats),
		fieldAt: map[string]uint32{},
		dict:    map[string]*dictEntry{},
		classes: make([]uint32, len(queries)),
		classOf: map[string]uint32{},
		pairs:   map[[2]string]*pairBuild{},
		degree:  map[string]int{},

		memberCost: map[string]float64{},
		prog:       make([]progRef, len(queries)),
	}
	b.pc.field = b.field
	byID := make([]uint32, len(queries))
	for i := range byID {
		byID[i] = count32(i)
	}
	slices.SortFunc(byID, func(x, y uint32) int { return strings.Compare(queries[x].ID, queries[y].ID) })
	for r := 1; r < len(byID); r++ {
		if queries[byID[r]].ID == queries[byID[r-1]].ID {
			return nil, fmt.Errorf("percolate: query %q twice in one segment", queries[byID[r]].ID)
		}
	}
	// Records by ordinal; anchors, classes and programs by rank.
	offsets := make([]uint64, 0, len(queries)+1)
	var records []byte
	srcs := make([][]byte, len(queries))
	for i := range queries {
		if i%1024 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		q := &queries[i]
		src, err := encodeQuery(q.Query)
		if err != nil {
			return nil, fmt.Errorf("percolate: query %q: %w", q.ID, err)
		}
		srcs[i] = src
		offsets = append(offsets, uint64(len(records)))
		records = appendBytes(records, []byte(q.ID))
		records = binary.AppendVarint(records, q.Seq)
		records = appendBytes(records, src)
		records = appendBytes(records, q.Meta)
	}
	offsets = append(offsets, uint64(len(records)))
	for r, ord := range byID {
		if r%1024 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err := b.add(count32(r), srcs[ord]); err != nil {
			return nil, fmt.Errorf("percolate: query %q: %w", queries[ord].ID, err)
		}
	}
	return b.encode(queries, byID, offsets, records)
}

// add anchors the query at rank, src its stored JSON.
func (b *segmentBuilder) add(rank uint32, src []byte) error {
	// Anchors and the class come from the stored JSON parsed back, the very tree
	// verification compiles, so the two can never disagree.
	stored, problems := query.Parse(src)
	if len(problems) > 0 {
		return fmt.Errorf("query does not round-trip: %s: %s", problems[0].Loc, problems[0].Message)
	}
	set := b.ex.node(stored)
	filter, filtered := postingFilter(stored, &set)
	rest, proven := provenRest(stored, &set, filter, filtered)
	key := classKey(stored, src) + "\x00" + proven
	if rep, ok := b.classOf[key]; ok {
		b.classes[rank] = rep
		b.prog[rank] = b.prog[rep]
	} else {
		b.classOf[key] = rank
		b.classes[rank] = rank
		prog := b.pc.compile(rest)
		var ref progRef
		switch {
		case len(prog) == 1 && prog[0] == pTrue: // the empty program holds
		case len(prog) <= inlineProg:
			ref.n = count32(len(prog))
			copy(ref.at[:], prog)
		default:
			if len(b.progs) > math.MaxUint32-len(prog) {
				return errors.New("query segment programs over 4 GiB")
			}
			ref.n = count32(len(prog))
			binary.LittleEndian.PutUint32(ref.at[:], count32(len(b.progs)))
			b.progs = append(b.progs, prog...)
		}
		b.prog[rank] = ref
	}

	if !set.ok {
		b.always = append(b.always, rank)
		return nil
	}
	for _, t := range set.terms {
		if filtered {
			b.postFiltered(t.Kind, b.field(t.Field), t.Term, filteredPost{
				rank: rank, field: b.field(filter.Field), isBool: filter.Bool, lo: filter.Lo, hi: filter.Hi,
			})
		} else {
			b.post(t.Kind, b.field(t.Field), t.Term, rank)
		}
	}
	for _, p := range set.pairs {
		// Each half is a member of its field (postings: every query it is half of);
		// the pair itself is stored once, under one of its halves (see encode).
		var k [2]string
		for i, t := range p {
			fi := b.field(t.Field)
			k[i] = b.post(AtomMember, fi, memberTerm(t.Kind, t.Term), rank)
			b.memberCost[k[i]] = b.ex.termCost(t.Kind, t.Field, t.Term)
		}
		if k[0] > k[1] {
			k[0], k[1] = k[1], k[0]
		}
		pb := b.pairs[k]
		if pb == nil {
			pb = &pairBuild{}
			b.pairs[k] = pb
			b.degree[k[0]]++
			b.degree[k[1]]++
		}
		if n := len(pb.posts); n == 0 || pb.posts[n-1] != rank {
			pb.posts = append(pb.posts, rank)
		}
	}
	for _, r := range set.ranges {
		fi := b.field(r.Field)
		b.fields[fi].ranges = append(b.fields[fi].ranges, interval{lo: r.Lo, hi: r.Hi, rank: rank})
	}
	return nil
}

// classKey is a query's verification class: its canonical form, unless it holds a
// words_* condition (see the format notes), then its exact JSON.
func classKey(n query.Node, src []byte) string {
	words := false
	query.Walk(n, func(_ string, node query.Node) bool {
		if l, ok := node.(*query.Leaf); ok && (l.Op == query.OpWordsAll || l.Op == query.OpWordsAny) {
			words = true
		}
		return !words
	})
	if words {
		return "j" + string(src)
	}
	return "c" + string(query.Canonical(n))
}

func (b *segmentBuilder) field(name string) uint32 {
	if fi, ok := b.fieldAt[name]; ok {
		return fi
	}
	fi := count32(len(b.fields))
	b.fieldAt[name] = fi
	b.fields = append(b.fields, fieldBuild{name: name})
	return fi
}

// treeNode is an interval tree node, as written.
type treeNode struct {
	center       float64
	left, right  int32
	start, count uint32
}

func (b *segmentBuilder) encode(queries []shard.StoredQuery, byID []uint32, offsets []uint64, records []byte) ([]byte, error) {
	n := len(queries)

	// The dictionary, in key order (so a build is deterministic), and its hash table.
	keys := make([]string, 0, len(b.dict))
	for k := range b.dict {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	tableSize := uint32(1)
	for uint64(tableSize) <= 2*uint64(len(keys)) {
		tableSize <<= 1
	}
	slots := make([]uint32, tableSize)
	var entries, terms, postings, filtered []byte
	for i, k := range keys {
		e := b.dict[k]
		if len(terms) > math.MaxUint32-len(e.term) || len(postings)/4 > math.MaxUint32-len(e.posts) ||
			len(filtered)/filteredSize > math.MaxUint32-len(e.filtered) {
			return nil, errors.New("percolate: query segment dictionary over 4 GiB")
		}
		h := hashTerm(e.kind, e.field, e.term)
		entries = binary.LittleEndian.AppendUint32(entries, hi32(h))
		entries = binary.LittleEndian.AppendUint32(entries, e.field<<8|uint32(e.kind))
		entries = binary.LittleEndian.AppendUint32(entries, count32(len(terms)))
		entries = binary.LittleEndian.AppendUint32(entries, count32(len(e.term)))
		entries = binary.LittleEndian.AppendUint32(entries, count32(len(postings)/4))
		entries = binary.LittleEndian.AppendUint32(entries, count32(len(e.posts)))
		entries = binary.LittleEndian.AppendUint32(entries, count32(len(filtered)/filteredSize))
		entries = binary.LittleEndian.AppendUint32(entries, count32(len(e.filtered)))
		terms = append(terms, e.term...)
		for _, p := range e.posts {
			postings = binary.LittleEndian.AppendUint32(postings, p)
		}
		for _, f := range e.filtered {
			filtered = binary.LittleEndian.AppendUint32(filtered, f.rank)
			filtered = binary.LittleEndian.AppendUint32(filtered, f.field<<1|uint32(boolByte(f.isBool)))
			filtered = appendF64(filtered, f.lo)
			filtered = appendF64(filtered, f.hi)
		}
		mask := tableSize - 1
		for slot := lo32(h) & mask; ; slot = (slot + 1) & mask {
			if slots[slot] == 0 {
				slots[slot] = count32(i) + 1
				break
			}
		}
	}

	// Pairs, each stored once under its owner, with its partner and postings. The
	// owner is the half with fewer pairs (then the rarer, then the smaller key), so a
	// frequent atom paired with many rare ones (a store with every model) keeps a short
	// partner list, and a document walks about one record per member it holds.
	entryOf := make(map[string]uint32, len(keys))
	for i, k := range keys {
		entryOf[k] = count32(i)
	}
	owned := make([][]pairRecord, len(keys))
	for k, pb := range b.pairs {
		owner, partner := k[0], k[1]
		if b.ownsAfter(owner, partner) {
			owner, partner = partner, owner
		}
		owned[entryOf[owner]] = append(owned[entryOf[owner]], pairRecord{partner: entryOf[partner], posts: pb.posts})
	}
	var partners, pairs []byte
	numPairs := 0
	for _, list := range owned {
		slices.SortFunc(list, func(a, b pairRecord) int { return cmp.Compare(a.partner, b.partner) })
		partners = binary.LittleEndian.AppendUint32(partners, count32(numPairs))
		partners = binary.LittleEndian.AppendUint32(partners, count32(len(list)))
		for _, r := range list {
			if len(postings)/4 > math.MaxUint32-len(r.posts) {
				return nil, errors.New("percolate: query segment postings over 4 GiB")
			}
			pairs = binary.LittleEndian.AppendUint32(pairs, r.partner)
			pairs = binary.LittleEndian.AppendUint32(pairs, count32(len(postings)/4))
			pairs = binary.LittleEndian.AppendUint32(pairs, count32(len(r.posts)))
			for _, p := range r.posts {
				postings = binary.LittleEndian.AppendUint32(postings, p)
			}
		}
		numPairs += len(list)
	}

	// Fields and their interval trees.
	var fields []byte
	var nodes []treeNode
	var byLo, byHi []interval
	for i := range b.fields {
		f := &b.fields[i]
		nodeStart, recStart := len(nodes), len(byLo)
		root := buildTree(f.ranges, &nodes, &byLo, &byHi)
		fields = appendBytes(fields, []byte(f.name))
		fields = binary.LittleEndian.AppendUint32(fields, f.kinds)
		fields = binary.LittleEndian.AppendUint32(fields, unsigned(root))
		fields = binary.LittleEndian.AppendUint32(fields, count32(nodeStart))
		fields = binary.LittleEndian.AppendUint32(fields, count32(len(nodes)))
		fields = binary.LittleEndian.AppendUint32(fields, count32(recStart))
		fields = binary.LittleEndian.AppendUint32(fields, count32(len(byLo)))
	}

	var sec [numSections][]byte
	meta := make([]byte, 0, metaSize)
	for _, v := range []int{n, len(b.fields), len(keys), int(tableSize), len(b.always), len(nodes), len(byLo), numPairs} {
		meta = binary.LittleEndian.AppendUint32(meta, count32(v))
	}
	sec[secMeta] = meta
	sec[secFields] = fields
	for _, off := range offsets {
		sec[secOffsets] = binary.LittleEndian.AppendUint64(sec[secOffsets], off)
	}
	sec[secRecords] = records
	members := make(map[uint32]int, len(b.classOf))
	for _, rep := range b.classes {
		members[rep]++
	}
	sec[secVerify] = make([]byte, 0, verifySize*n)
	for r := range n {
		class := b.classes[r]
		if members[class] == 1 {
			class = noClass
		}
		sec[secVerify] = binary.LittleEndian.AppendUint32(sec[secVerify], class)
		sec[secVerify] = binary.LittleEndian.AppendUint32(sec[secVerify], b.prog[r].n)
		sec[secVerify] = append(sec[secVerify], b.prog[r].at[:]...)
	}
	for _, ord := range byID {
		if len(sec[secIDText]) > math.MaxUint32-len(queries[ord].ID) {
			return nil, errors.New("percolate: query segment ids over 4 GiB")
		}
		sec[secIDText] = append(sec[secIDText], queries[ord].ID...)
		sec[secIDs] = binary.LittleEndian.AppendUint32(sec[secIDs], ord)
		sec[secIDs] = binary.LittleEndian.AppendUint32(sec[secIDs], count32(len(sec[secIDText])))
	}
	sec[secPrograms] = b.progs
	sec[secSlots] = appendU32s(slots)
	sec[secEntries] = entries
	sec[secTerms] = terms
	sec[secPostings] = postings
	sec[secFiltered] = filtered
	sec[secPartners] = partners
	sec[secPairs] = pairs
	sec[secAlways] = appendU32s(b.always)
	for _, nd := range nodes {
		sec[secNodes] = binary.LittleEndian.AppendUint64(sec[secNodes], math.Float64bits(nd.center))
		sec[secNodes] = binary.LittleEndian.AppendUint32(sec[secNodes], unsigned(nd.left))
		sec[secNodes] = binary.LittleEndian.AppendUint32(sec[secNodes], unsigned(nd.right))
		sec[secNodes] = binary.LittleEndian.AppendUint32(sec[secNodes], nd.start)
		sec[secNodes] = binary.LittleEndian.AppendUint32(sec[secNodes], nd.count)
	}
	sec[secByLo] = appendIntervals(byLo)
	sec[secByHi] = appendIntervals(byHi)

	size := len(fileMagic) + 8 + 4
	for _, s := range sec {
		size += 8 + len(s)
	}
	out := make([]byte, 0, size)
	out = append(out, fileMagic[:]...)
	out = binary.LittleEndian.AppendUint32(out, formatVersion)
	out = binary.LittleEndian.AppendUint32(out, numSections)
	for _, s := range sec {
		out = binary.LittleEndian.AppendUint64(out, uint64(len(s)))
		out = append(out, s...)
	}
	return binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, castagnoli)), nil
}

func appendU32s(vs []uint32) []byte {
	var dst []byte
	for _, v := range vs {
		dst = binary.LittleEndian.AppendUint32(dst, v)
	}
	return dst
}

func appendIntervals(ivs []interval) []byte {
	var dst []byte
	for _, iv := range ivs {
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(iv.lo))
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(iv.hi))
		dst = binary.LittleEndian.AppendUint32(dst, iv.rank)
	}
	return dst
}

// buildTree appends a centered interval tree over ivs (each lo <= hi) to nodes, in
// preorder (so every child follows its parent), with each node's intervals appended to
// byLo (by lo ascending) and byHi (by hi descending), and returns its root, -1 for no
// interval. The center is the median of every endpoint, infinities included, so each
// subtree holds at most half the intervals (an interval wholly left of the center has
// both endpoints below it), and the node itself at least one (the interval the median
// endpoint belongs to contains it).
func buildTree(ivs []interval, nodes *[]treeNode, byLo, byHi *[]interval) int32 {
	if len(ivs) == 0 {
		return -1
	}
	ends := make([]float64, 0, 2*len(ivs))
	for _, iv := range ivs {
		ends = append(ends, iv.lo, iv.hi)
	}
	slices.Sort(ends)
	center := ends[len(ends)/2]
	var left, right, mid []interval
	for _, iv := range ivs {
		switch {
		case iv.hi < center:
			left = append(left, iv)
		case iv.lo > center:
			right = append(right, iv)
		default:
			mid = append(mid, iv)
		}
	}
	at := len(*nodes)
	*nodes = append(*nodes, treeNode{center: center, start: count32(len(*byLo)), count: count32(len(mid))})
	slices.SortStableFunc(mid, func(a, b interval) int { return cmpFloat(a.lo, b.lo) })
	*byLo = append(*byLo, mid...)
	slices.SortStableFunc(mid, func(a, b interval) int { return cmpFloat(b.hi, a.hi) })
	*byHi = append(*byHi, mid...)
	l := buildTree(left, nodes, byLo, byHi)
	r := buildTree(right, nodes, byLo, byHi)
	(*nodes)[at].left, (*nodes)[at].right = l, r
	return int32(at) //nolint:gosec // a node per interval at most, and intervals are bounded by the query count
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// hashTerm is the dictionary's stable hash of (kind, field, term): FNV-1a, 64 bits.
func hashTerm(kind AtomKind, field uint32, term string) uint64 {
	h := hashPrefix(kind, field)
	for i := 0; i < len(term); i++ {
		h ^= uint64(term[i])
		h *= fnvPrime
	}
	return h
}

// hashKey is hashTerm of an AtomSimKey term (the key's 8 bytes big-endian).
func hashKey(field uint32, key uint64) uint64 {
	h := hashPrefix(AtomSimKey, field)
	for shift := 56; shift >= 0; shift -= 8 {
		h ^= key >> uint(shift) & 0xff
		h *= fnvPrime
	}
	return h
}

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

func hashPrefix(kind AtomKind, field uint32) uint64 {
	h := uint64(fnvOffset)
	h ^= uint64(kind)
	h *= fnvPrime
	for shift := 0; shift < 32; shift += 8 {
		h ^= uint64(field >> uint(shift) & 0xff)
		h *= fnvPrime
	}
	return h
}

// ---- reading ----

// fieldInfo is one field of an open segment.
type fieldInfo struct {
	name               string
	kinds              uint32
	root               int32
	nodeStart, nodeEnd uint32
	recStart, recEnd   uint32
	// grams is a prefilter of the field's AtomGram terms, 16 bits per term rounded up
	// to a power of two (bit: the entry's stored hash half, masked by gramMask), built
	// at Open: a document's window whose bit is clear has no entry, so it skips the
	// table. Nil when the field has no gram terms.
	grams    []uint64
	gramMask uint32
}

// Segment is an open query segment: a [shard.QuerySegment] that also answers the
// percolator's probes. Its methods are safe for concurrent use.
type Segment struct {
	path      string
	n         uint32
	tableMask uint32
	fields    []fieldInfo
	offsets   []byte
	records   []byte
	verifies  []byte
	ids       []byte
	idText    []byte
	programs  []byte
	slots     []byte
	entries   []byte
	terms     []byte
	postings  []byte
	filtered  []byte
	partners  []byte
	pairs     []byte
	always    []byte
	nodes     []byte
	byLo      []byte
	byHi      []byte

	// mapped is the file mapping every section views (nil for a segment opened from
	// bytes).
	mapped *mapping
}

var _ shard.QuerySegment = (*Segment)(nil)

func u32(b []byte, i int) uint32 { return binary.LittleEndian.Uint32(b[i:]) }

func u64(b []byte, i int) uint64 { return binary.LittleEndian.Uint64(b[i:]) }

func f64(b []byte, i int) float64 { return math.Float64frombits(u64(b, i)) }

// openData validates data as a query segment file named path.
func openData(path string, data []byte) (*Segment, error) {
	corrupt := func(format string, args ...any) error {
		return &CorruptError{Path: path, Why: fmt.Sprintf(format, args...)}
	}
	head := len(fileMagic) + 8
	if len(data) < head+4 || !bytes.Equal(data[:len(fileMagic)], fileMagic[:]) {
		return nil, corrupt("bad magic")
	}
	body := data[:len(data)-4]
	if crc32.Checksum(body, castagnoli) != u32(data, len(data)-4) {
		return nil, corrupt("checksum mismatch")
	}
	return parseBody(path, body)
}

// parseBody parses and validates a checksummed body.
func parseBody(path string, body []byte) (*Segment, error) {
	corrupt := func(format string, args ...any) error {
		return &CorruptError{Path: path, Why: fmt.Sprintf(format, args...)}
	}
	if len(body) < len(fileMagic)+8 || !bytes.Equal(body[:len(fileMagic)], fileMagic[:]) {
		return nil, corrupt("bad magic")
	}
	if v := u32(body, len(fileMagic)); v != formatVersion {
		return nil, fmt.Errorf("percolate: query segment %s: format version %d, this build reads %d: %w",
			path, v, formatVersion, segment.FormatError(uint64(v), formatVersion))
	}
	if c := u32(body, len(fileMagic)+4); c != numSections {
		return nil, corrupt("%d sections, want %d", c, numSections)
	}
	var sec [numSections][]byte
	rest := body[len(fileMagic)+8:]
	for i := range sec {
		if len(rest) < 8 {
			return nil, corrupt("section %s: truncated", sectionNames[i])
		}
		n := u64(rest, 0)
		rest = rest[8:]
		if n > uint64(len(rest)) {
			return nil, corrupt("section %s: %d bytes, %d left", sectionNames[i], n, len(rest))
		}
		sec[i], rest = rest[:n], rest[n:]
	}
	if len(rest) != 0 {
		return nil, corrupt("%d trailing bytes", len(rest))
	}
	if len(sec[secMeta]) != metaSize {
		return nil, corrupt("meta: %d bytes", len(sec[secMeta]))
	}
	var meta [8]uint32
	for i := range meta {
		meta[i] = u32(sec[secMeta], 4*i)
	}
	n, numFields, numEntries, tableSize, numAlways, numNodes, numRecs, numPairs := meta[0], meta[1], meta[2], meta[3], meta[4], meta[5], meta[6], meta[7]
	s := &Segment{
		path: path, n: n,
		offsets: sec[secOffsets], records: sec[secRecords], verifies: sec[secVerify], ids: sec[secIDs], idText: sec[secIDText], programs: sec[secPrograms],
		slots: sec[secSlots], entries: sec[secEntries], terms: sec[secTerms], postings: sec[secPostings], filtered: sec[secFiltered],
		partners: sec[secPartners], pairs: sec[secPairs],
		always: sec[secAlways], nodes: sec[secNodes], byLo: sec[secByLo], byHi: sec[secByHi],
	}
	sizes := []struct {
		sec  int
		want uint64
	}{
		{secOffsets, 8 * (uint64(n) + 1)},
		{secVerify, verifySize * uint64(n)},
		{secIDs, idSize * uint64(n)},
		{secSlots, 4 * uint64(tableSize)},
		{secEntries, entrySize * uint64(numEntries)},
		{secAlways, 4 * uint64(numAlways)},
		{secNodes, nodeSize * uint64(numNodes)},
		{secByLo, recordSize * uint64(numRecs)},
		{secByHi, recordSize * uint64(numRecs)},
		{secPartners, 8 * uint64(numEntries)},
		{secPairs, pairSize * uint64(numPairs)},
	}
	for _, sz := range sizes {
		if uint64(len(sec[sz.sec])) != sz.want {
			return nil, corrupt("section %s: %d bytes, want %d", sectionNames[sz.sec], len(sec[sz.sec]), sz.want)
		}
	}
	if len(s.postings)%4 != 0 || len(s.filtered)%filteredSize != 0 {
		return nil, corrupt("postings: %d and %d bytes", len(s.postings), len(s.filtered))
	}
	if tableSize == 0 || tableSize&(tableSize-1) != 0 || tableSize <= numEntries {
		return nil, corrupt("hash table of %d slots for %d entries", tableSize, numEntries)
	}
	s.tableMask = tableSize - 1

	if err := s.checkRecords(); err != nil {
		return nil, corrupt("%v", err)
	}
	if err := s.checkDictionary(numEntries, numFields); err != nil {
		return nil, corrupt("%v", err)
	}
	if err := s.parseFields(sec[secFields], numFields, numNodes, numRecs); err != nil {
		return nil, corrupt("%v", err)
	}
	if err := s.checkRanks(); err != nil {
		return nil, corrupt("%v", err)
	}
	s.buildGramFilters()
	return s, nil
}

// checkRecords checks every query record, and that ids and idtext hold every ordinal
// and its id, sorted by distinct ids.
func (s *Segment) checkRecords() error {
	var prev uint64
	for i := 0; i <= int(s.n); i++ {
		off := u64(s.offsets, 8*i)
		if off < prev || off > uint64(len(s.records)) {
			return fmt.Errorf("record offset %d out of order or range", i)
		}
		prev = off
	}
	if prev != uint64(len(s.records)) {
		return errors.New("records: trailing bytes")
	}
	for ord := range s.n {
		if _, _, _, _, err := s.record(ord); err != nil {
			return fmt.Errorf("query %d: %w", ord, err)
		}
	}
	var last []byte
	var end uint32
	seen := make([]bool, s.n)
	for i := range s.n {
		ord := s.ordAt(i)
		if ord >= s.n || seen[ord] {
			return fmt.Errorf("ids: ordinal %d out of range or repeated", ord)
		}
		seen[ord] = true
		next := u32(s.ids, idSize*int(i)+4)
		if next < end || uint64(next) > uint64(len(s.idText)) {
			return fmt.Errorf("ids: id %d out of range", i)
		}
		end = next
		_, id, _, _, _ := s.record(ord)
		if !bytes.Equal(id, s.idAt(i)) {
			return fmt.Errorf("ids: id %d is not query %d's", i, ord)
		}
		if i > 0 && bytes.Compare(last, id) >= 0 {
			return errors.New("ids: not sorted, or an id repeated")
		}
		last = id
	}
	if uint64(end) != uint64(len(s.idText)) {
		return errors.New("idtext: trailing bytes")
	}
	return nil
}

// checkRanks checks the classes and their programs (each class's once, at its
// first rank; the others must hold the same), the always-check list and the tree
// records.
func (s *Segment) checkRanks() error {
	numFields := uint32(len(s.fields)) //nolint:gosec // bounded by a u32 count
	for r := range s.n {
		rep := s.class(r)
		if rep == noClass {
			rep = r
		} else if rep > r || s.class(rep) != rep {
			return fmt.Errorf("classes: rank %d names %d", r, rep)
		}
		o := verifySize * int(r)
		if rep != r {
			if !bytes.Equal(s.verifies[o+4:o+verifySize], s.verifies[verifySize*int(rep)+4:verifySize*int(rep+1)]) {
				return fmt.Errorf("rank %d: not its class's program", r)
			}
			continue
		}
		n := u32(s.verifies, o+4)
		if n > inlineProg && uint64(u32(s.verifies, o+8))+uint64(n) > uint64(len(s.programs)) {
			return fmt.Errorf("rank %d: program out of range", r)
		}
		if p := s.program(r); len(p) > 0 {
			if err := checkProg(p, numFields); err != nil {
				return fmt.Errorf("rank %d: %w", r, err)
			}
		}
	}
	for i := 0; i < len(s.always); i += 4 {
		ord := u32(s.always, i)
		if ord >= s.n || i > 0 && ord <= u32(s.always, i-4) {
			return errors.New("always: a rank out of range or order")
		}
	}
	for _, recs := range [][]byte{s.byLo, s.byHi} {
		for i := 0; i < len(recs); i += recordSize {
			lo, hi := f64(recs, i), f64(recs, i+8)
			if !(lo <= hi) || u32(recs, i+16) >= s.n { // NaN fails lo <= hi
				return errors.New("interval records: a bad interval or rank")
			}
		}
	}
	return nil
}

// checkDictionary checks every entry's term and postings (at least one, plain or
// filtered), and every slot.
func (s *Segment) checkDictionary(numEntries, numFields uint32) error {
	for i := range numEntries {
		e := int(i) * entrySize
		fk := u32(s.entries, e+4)
		kind := AtomKind(fk & 0xff)
		if kind == 0 || kind >= numAtomKinds {
			return fmt.Errorf("entry %d: atom kind %d", i, kind)
		}
		termOff, termLen := uint64(u32(s.entries, e+8)), uint64(u32(s.entries, e+12))
		if termOff+termLen > uint64(len(s.terms)) {
			return fmt.Errorf("entry %d: term out of range", i)
		}
		if kind == AtomSimKey && termLen != 8 {
			return fmt.Errorf("entry %d: a trigram key of %d bytes", i, termLen)
		}
		postOff, postN := uint64(u32(s.entries, e+16)), uint64(u32(s.entries, e+20))
		filtOff, filtN := uint64(u32(s.entries, e+24)), uint64(u32(s.entries, e+28))
		if postN+filtN == 0 || postOff+postN > uint64(len(s.postings)/4) || filtOff+filtN > uint64(len(s.filtered)/filteredSize) ||
			postN > 0 && !s.validPostings(postOff, postN) ||
			filtN > 0 && (kind == AtomMember || !s.validFiltered(filtOff, filtN, numFields)) {
			return fmt.Errorf("entry %d: postings out of range or order", i)
		}
		// Partners: only a member lists any.
		pOff, pN := uint64(u32(s.partners, int(i)*8)), uint64(u32(s.partners, int(i)*8+4))
		if pOff+pN > uint64(len(s.pairs)/pairSize) || pN > 0 && kind != AtomMember {
			return fmt.Errorf("entry %d: partners out of range", i)
		}
	}
	// Each pair names a member entry as its partner, with valid postings.
	for r := 0; r < len(s.pairs); r += pairSize {
		partner := u32(s.pairs, r)
		if partner >= numEntries || AtomKind(u32(s.entries, int(partner)*entrySize+4)&0xff) != AtomMember ||
			!s.validPostings(uint64(u32(s.pairs, r+4)), uint64(u32(s.pairs, r+8))) {
			return fmt.Errorf("pair %d: bad partner or postings", r/pairSize)
		}
	}
	for i := 0; i < len(s.slots); i += 4 {
		if u32(s.slots, i) > numEntries {
			return fmt.Errorf("slot %d: entry out of range", i/4)
		}
	}
	return nil
}

// parseFields reads the fields section, checking each field's tree: its nodes lie in
// its own range, every child follows its parent (so a walk ends), and every node's
// records lie in the field's record range.
func (s *Segment) parseFields(b []byte, numFields, numNodes, numRecs uint32) error {
	r := byteReader{b: b}
	// Each field takes at least 25 bytes (a length byte and six u32s): bound the count
	// by the section before allocating for it.
	if uint64(numFields)*minFieldSize > uint64(len(b)) {
		return errors.New("fields: count out of range")
	}
	s.fields = make([]fieldInfo, 0, numFields)
	for i := range numFields {
		name := r.bytes()
		var vals [6]uint32
		for j := range vals {
			vals[j] = r.u32()
		}
		if r.err != nil {
			return fmt.Errorf("field %d: %w", i, r.err)
		}
		f := fieldInfo{
			name: string(name), kinds: vals[0], root: signed(vals[1]),
			nodeStart: vals[2], nodeEnd: vals[3], recStart: vals[4], recEnd: vals[5],
		}
		if f.nodeStart > f.nodeEnd || f.nodeEnd > numNodes || f.recStart > f.recEnd || f.recEnd > numRecs {
			return fmt.Errorf("field %d: node or record range out of range", i)
		}
		if f.nodeStart == f.nodeEnd && f.root != -1 || f.nodeStart != f.nodeEnd && int64(f.root) != int64(f.nodeStart) {
			return fmt.Errorf("field %d: bad tree root", i)
		}
		for nd := f.nodeStart; nd < f.nodeEnd; nd++ {
			o := int(nd) * nodeSize
			if math.IsNaN(f64(s.nodes, o)) {
				return fmt.Errorf("field %d: a NaN center", i)
			}
			for _, child := range []int32{signed(u32(s.nodes, o+8)), signed(u32(s.nodes, o+12))} {
				if child != -1 && (child <= signed(nd) || unsigned(child) >= f.nodeEnd) {
					return fmt.Errorf("field %d: a bad child link", i)
				}
			}
			start, count := uint64(u32(s.nodes, o+16)), uint64(u32(s.nodes, o+20))
			if start < uint64(f.recStart) || start+count > uint64(f.recEnd) {
				return fmt.Errorf("field %d: node records out of range", i)
			}
		}
		s.fields = append(s.fields, f)
	}
	if len(r.b) != 0 {
		return errors.New("fields: trailing bytes")
	}
	return nil
}

// record returns query ord's seq, id, JSON and meta.
func (s *Segment) record(ord uint32) (seq int64, id, src, meta []byte, err error) {
	lo, hi := u64(s.offsets, 8*int(ord)), u64(s.offsets, 8*int(ord)+8)
	r := byteReader{b: s.records[lo:hi]}
	id = r.bytes()
	seq = r.varint()
	src = r.bytes()
	meta = r.bytes()
	if r.err == nil && len(r.b) != 0 {
		r.err = errors.New("trailing bytes")
	}
	return seq, id, src, meta, r.err
}

// NumQueries implements [shard.QuerySegment].
func (s *Segment) NumQueries() uint32 { return s.n }

// NumAlways is how many of the segment's queries are on its always-check list.
func (s *Segment) NumAlways() int { return len(s.always) / 4 }

// Ord implements [shard.QuerySegment].
func (s *Segment) Ord(id string) (uint32, bool) {
	lo, hi := uint32(0), s.n
	for lo < hi {
		mid := lo + (hi-lo)/2
		switch c := compareBytesString(s.idAt(mid), id); {
		case c == 0:
			return s.ordAt(mid), true
		case c < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return 0, false
}

// ordAt returns the ordinal of the query at rank r (its position sorted by id).
func (s *Segment) ordAt(r uint32) uint32 { return u32(s.ids, idSize*int(r)) }

// idAt returns the id of the query at rank r, a view into the segment.
func (s *Segment) idAt(r uint32) []byte {
	var start uint32
	if r > 0 {
		start = u32(s.ids, idSize*int(r)-4)
	}
	return s.idText[start:u32(s.ids, idSize*int(r)+4)]
}

func compareBytesString(b []byte, s string) int {
	n := min(len(b), len(s))
	for i := range n {
		if b[i] != s[i] {
			if b[i] < s[i] {
				return -1
			}
			return 1
		}
	}
	return len(b) - len(s)
}

// Query implements [shard.QuerySegment]: the query parsed from its stored JSON.
func (s *Segment) Query(ord uint32) (shard.StoredQuery, error) {
	if ord >= s.n {
		return shard.StoredQuery{}, fmt.Errorf("query ordinal %d out of range", ord)
	}
	seq, id, src, meta, err := s.record(ord)
	if err != nil {
		return shard.StoredQuery{}, &CorruptError{Path: s.path, Why: err.Error()}
	}
	n, problems := query.Parse(src)
	if len(problems) > 0 {
		return shard.StoredQuery{}, &CorruptError{Path: s.path, Why: fmt.Sprintf("query %q: %s", id, problems[0].Message)}
	}
	var m []byte
	if len(meta) > 0 {
		m = bytes.Clone(meta)
	}
	return shard.StoredQuery{ID: string(id), Seq: seq, Query: n, Meta: m}, nil
}

// Close implements [shard.QuerySegment]: it unmaps the file. Nothing the segment
// handed out views it (Query and Percolate return copies).
func (s *Segment) Close() error {
	if s.mapped == nil {
		return nil
	}
	return s.mapped.close()
}

// class returns the first rank of rank r's verification class, noClass when no other
// query shares it.
func (s *Segment) class(r uint32) uint32 { return u32(s.verifies, verifySize*int(r)) }

// program returns rank r's class's program (empty: it holds).
func (s *Segment) program(r uint32) []byte {
	o := verifySize * int(r)
	n := u32(s.verifies, o+4)
	if n <= inlineProg {
		return s.verifies[o+8 : o+8+int(n)]
	}
	off := u32(s.verifies, o+8)
	return s.programs[off : off+n]
}

// lookup returns the entry (its offset in entries) of (kind, field, term), -1 when the
// dictionary has none.
func (s *Segment) lookup(kind AtomKind, field uint32, term string) int {
	return s.lookupHashed(kind, field, term, hashTerm(kind, field, term))
}

// Gram prefilter sizes: gramFilterBitsPerTerm bits per gram term (a false positive
// rate under 1 in 16 per window), as a power of two from 512 bits up to 2^26 (8 MiB).
const (
	gramFilterBitsPerTerm = 16
	minGramFilterBits     = 1 << 9
	maxGramFilterBits     = 1 << 26
)

// buildGramFilters sizes each field's gram prefilter to its gram terms and sets each
// one's bit. It runs after the entries and fields are validated.
func (s *Segment) buildGramFilters() {
	counts := make([]uint64, len(s.fields))
	for o := 0; o < len(s.entries); o += entrySize {
		if fk := u32(s.entries, o+4); AtomKind(fk&0xff) == AtomGram && int(fk>>8) < len(s.fields) {
			counts[fk>>8]++
		}
	}
	for fi, c := range counts {
		if c == 0 {
			continue
		}
		bits := uint64(minGramFilterBits)
		for bits < gramFilterBitsPerTerm*c && bits < maxGramFilterBits {
			bits <<= 1
		}
		s.fields[fi].grams = make([]uint64, bits/64)
		s.fields[fi].gramMask = uint32(bits - 1)
	}
	for o := 0; o < len(s.entries); o += entrySize {
		fk := u32(s.entries, o+4)
		if AtomKind(fk&0xff) != AtomGram || int(fk>>8) >= len(s.fields) {
			continue
		}
		f := &s.fields[fk>>8]
		bit := u32(s.entries, o) & f.gramMask
		f.grams[bit>>6] |= 1 << (bit & 63)
	}
}

// lookupGram is lookup of an AtomGram term of field f, through its prefilter.
func (s *Segment) lookupGram(f *fieldInfo, field uint32, gram string) int {
	h := hashTerm(AtomGram, field, gram)
	if bit := hi32(h) & f.gramMask; len(f.grams) == 0 || f.grams[bit>>6]&(1<<(bit&63)) == 0 {
		return -1
	}
	return s.lookupHashed(AtomGram, field, gram, h)
}

// lookupHashed is lookup with the term's hash computed.
func (s *Segment) lookupHashed(kind AtomKind, field uint32, term string, h uint64) int {
	fk := field<<8 | uint32(kind)
	for slot, probes := lo32(h)&s.tableMask, uint32(0); probes <= s.tableMask; slot, probes = (slot+1)&s.tableMask, probes+1 {
		e := u32(s.slots, 4*int(slot))
		if e == 0 {
			return -1
		}
		o := int(e-1) * entrySize
		if u32(s.entries, o) != hi32(h) || u32(s.entries, o+4) != fk {
			continue
		}
		off, n := u32(s.entries, o+8), u32(s.entries, o+12)
		if string(s.terms[off:off+n]) == term {
			return o
		}
	}
	return -1
}

// lookupKey is lookup of an AtomSimKey term.
func (s *Segment) lookupKey(field uint32, key uint64) int {
	h := hashKey(field, key)
	fk := field<<8 | uint32(AtomSimKey)
	for slot, probes := lo32(h)&s.tableMask, uint32(0); probes <= s.tableMask; slot, probes = (slot+1)&s.tableMask, probes+1 {
		e := u32(s.slots, 4*int(slot))
		if e == 0 {
			return -1
		}
		o := int(e-1) * entrySize
		if u32(s.entries, o) != hi32(h) || u32(s.entries, o+4) != fk {
			continue
		}
		off := u32(s.entries, o+8) // validated: 8 bytes
		if binary.BigEndian.Uint64(s.terms[off:]) == key {
			return o
		}
	}
	return -1
}

func (s *Segment) posts(entry int) []byte {
	off, n := u32(s.entries, entry+16), u32(s.entries, entry+20)
	return s.postings[4*int(off) : 4*int(off+n)]
}

// filteredOf returns the entry's filtered postings.
func (s *Segment) filteredOf(entry int) []byte {
	off, n := u32(s.entries, entry+24), u32(s.entries, entry+28)
	return s.filtered[filteredSize*int(off) : filteredSize*int(off+n)]
}

// ---- small helpers ----

func appendBytes(buf, p []byte) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(p)))
	return append(buf, p...)
}

var errShortRead = errors.New("truncated")

type byteReader struct {
	b   []byte
	err error
}

func (r *byteReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errShortRead
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *byteReader) varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.err = errShortRead
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *byteReader) u32() uint32 {
	if r.err != nil {
		return 0
	}
	if len(r.b) < 4 {
		r.err = errShortRead
		return 0
	}
	v := binary.LittleEndian.Uint32(r.b)
	r.b = r.b[4:]
	return v
}

func (r *byteReader) bytes() []byte {
	n := r.uvarint()
	if r.err != nil {
		return nil
	}
	if n > uint64(len(r.b)) {
		r.err = errShortRead
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

// encodeQuery writes n as the query language's JSON, which [query.Parse] reads back.
func encodeQuery(n query.Node) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeQuery(&buf, n); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeQuery(buf *bytes.Buffer, n query.Node) error {
	switch x := n.(type) {
	case *query.All:
		if x == nil {
			return errors.New("a nil all group cannot be stored")
		}
		return writeGroup(buf, "all", x.Children)
	case *query.Any:
		if x == nil {
			return errors.New("a nil any group cannot be stored")
		}
		return writeGroup(buf, "any", x.Children)
	case *query.Not:
		if x == nil {
			return errors.New("a nil not cannot be stored")
		}
		buf.WriteString(`{"not":`)
		if err := writeQuery(buf, x.Child); err != nil {
			return err
		}
		buf.WriteByte('}')
		return nil
	case *query.Leaf:
		if x == nil {
			return errors.New("a nil condition cannot be stored")
		}
		field, err := json.Marshal(x.Field)
		if err != nil {
			return err
		}
		op, err := json.Marshal(x.Op)
		if err != nil {
			return err
		}
		buf.WriteString(`{"field":`)
		buf.Write(field)
		buf.WriteString(`,"op":`)
		buf.Write(op)
		if len(x.Value) > 0 {
			if !json.Valid(x.Value) {
				return fmt.Errorf("condition on %q: value is not JSON", x.Field)
			}
			buf.WriteString(`,"value":`)
			buf.Write(x.Value)
		}
		buf.WriteByte('}')
		return nil
	default:
		return fmt.Errorf("query node %T cannot be stored", n)
	}
}

func writeGroup(buf *bytes.Buffer, key string, children []query.Node) error {
	buf.WriteString(`{"` + key + `":[`)
	for i, c := range children {
		if i > 0 {
			buf.WriteByte(',')
		}
		if err := writeQuery(buf, c); err != nil {
			return err
		}
	}
	buf.WriteString("]}")
	return nil
}

func writeFile(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// post adds rank to the postings of (kind, field fi, term), marking the field as having
// terms of the kind, and returns the entry's dictionary key.
func (b *segmentBuilder) post(kind AtomKind, fi uint32, term string, rank uint32) string {
	b.fields[fi].kinds |= 1 << kind
	b.key = append(b.key[:0], byte(kind))
	b.key = binary.LittleEndian.AppendUint32(b.key, fi)
	b.key = append(b.key, term...)
	e := b.dict[string(b.key)]
	if e == nil {
		e = &dictEntry{kind: kind, field: fi, term: term}
		b.dict[string(b.key)] = e
	}
	if n := len(e.posts); n == 0 || e.posts[n-1] != rank {
		e.posts = append(e.posts, rank)
	}
	return string(b.key)
}

// postFiltered adds a filtered posting to (kind, field fi, term), as post does.
func (b *segmentBuilder) postFiltered(kind AtomKind, fi uint32, term string, fp filteredPost) {
	b.fields[fi].kinds |= 1 << kind
	b.key = append(b.key[:0], byte(kind))
	b.key = binary.LittleEndian.AppendUint32(b.key, fi)
	b.key = append(b.key, term...)
	e := b.dict[string(b.key)]
	if e == nil {
		e = &dictEntry{kind: kind, field: fi, term: term}
		b.dict[string(b.key)] = e
	}
	if n := len(e.filtered); n == 0 || e.filtered[n-1].rank != fp.rank {
		e.filtered = append(e.filtered, fp)
	}
}

// memberTerm is an AtomMember term: the atom's kind, then its term.
func memberTerm(kind AtomKind, term string) string {
	return string(rune(kind)) + term
}

// findEntry returns the index of the dictionary entry (kind, field, term), -1 when
// there is none.
func (s *Segment) findEntry(kind AtomKind, field uint32, term []byte) int {
	h := hashPrefix(kind, field)
	for _, c := range term {
		h ^= uint64(c)
		h *= fnvPrime
	}
	fk := field<<8 | uint32(kind)
	for slot, probes := lo32(h)&s.tableMask, uint32(0); probes <= s.tableMask; slot, probes = (slot+1)&s.tableMask, probes+1 {
		e := u32(s.slots, 4*int(slot))
		if e == 0 {
			return -1
		}
		o := int(e-1) * entrySize
		if u32(s.entries, o) != hi32(h) || u32(s.entries, o+4) != fk {
			continue
		}
		off, n := u32(s.entries, o+8), u32(s.entries, o+12)
		if bytes.Equal(s.terms[off:off+n], term) {
			return int(e - 1)
		}
	}
	return -1
}

// lo32 and hi32 are a 64-bit hash's halves: the slot and the stored check.
func lo32(h uint64) uint32 { return uint32(h) } //nolint:gosec // truncation is the point

func hi32(h uint64) uint32 { return uint32(h >> 32) }

// signed and unsigned convert a tree link (-1: none) to and from its stored form, its
// two's complement.
func signed(v uint32) int32 { return int32(v) } //nolint:gosec // two's complement on purpose

func unsigned(v int32) uint32 { return uint32(v) } //nolint:gosec // two's complement on purpose

// count32 is a count or index the builder writes as a u32: every one is bounded by the
// query count (checked below 2^32 at the start of a build) or by the dictionary's size
// (checked below 4 GiB as it is written).
func count32(n int) uint32 { return uint32(n) } //nolint:gosec // see above

// pairRecord is one pair as its owner lists it.
type pairRecord struct {
	partner uint32
	posts   []uint32
}

// ownsAfter reports whether member a should leave the pair to b: a has more pairs, or
// as many and is more frequent, or ties on both and has the larger key.
func (b *segmentBuilder) ownsAfter(a, other string) bool {
	if da, db := b.degree[a], b.degree[other]; da != db {
		return da > db
	}
	if ca, cb := b.memberCost[a], b.memberCost[other]; ca != cb {
		return ca > cb
	}
	return a > other
}

// validPostings reports whether postings [off, off+n) lie in the section, are not
// empty, and hold ascending ranks below the query count.
func (s *Segment) validPostings(off, n uint64) bool {
	if n == 0 || off+n > uint64(len(s.postings)/4) {
		return false
	}
	list := s.postings[4*off : 4*(off+n)]
	prev := uint32(0)
	for i := 0; i < len(list); i += 4 {
		ord := binary.LittleEndian.Uint32(list[i:])
		if ord >= s.n || i > 0 && ord <= prev {
			return false
		}
		prev = ord
	}
	return true
}

// validFiltered reports whether filtered postings [off, off+n) lie in the section and
// hold ascending ranks below the query count, fields below numFields, and ranges
// (a bool's 0 or 1).
func (s *Segment) validFiltered(off, n uint64, numFields uint32) bool {
	if off+n > uint64(len(s.filtered)/filteredSize) {
		return false
	}
	for i := range int(n) { //nolint:gosec // n is a u32 count
		r := (int(off) + i) * filteredSize //nolint:gosec // off is a u32 offset
		ord, fb := u32(s.filtered, r), u32(s.filtered, r+4)
		lo, hi := f64(s.filtered, r+8), f64(s.filtered, r+16)
		if ord >= s.n || i > 0 && ord <= u32(s.filtered, r-filteredSize) || fb>>1 >= numFields ||
			!(lo <= hi) || fb&1 == 1 && (lo != hi || lo != 0 && lo != 1) {
			return false
		}
	}
	return true
}

// entryPosts returns entry i's postings.
func (s *Segment) entryPosts(i int) []byte { return s.posts(i * entrySize) }

// partnersOf returns the first pair record entry i owns and how many.
func (s *Segment) partnersOf(i int) (first, n int) {
	return int(u32(s.partners, 8*i)), int(u32(s.partners, 8*i+4))
}

// pairAt returns pair record r: its partner entry and its postings.
func (s *Segment) pairAt(r int) (partner uint32, posts []byte) {
	o := r * pairSize
	off, n := u32(s.pairs, o+4), u32(s.pairs, o+8)
	return u32(s.pairs, o), s.postings[4*int(off) : 4*int(off+n)]
}

// NumEntries is how many terms the segment's dictionary holds.
func (s *Segment) NumEntries() uint32 { return uint32(len(s.entries) / entrySize) } //nolint:gosec // validated against a u32 count
