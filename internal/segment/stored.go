package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// Stored fields are each document's id and original body, packed id-then-body
// (uvarint length prefixes) into blocks of a contiguous run of document ordinals, each
// compressed. Looking one up is a binary search over block start ordinals, then one
// decompress and a short linear scan. Blocks are compressed in parallel, a bounded
// batch at a time, and placed by one sequential pass in ordinal order. Every offset is
// relative to the STORED section's own start, as every other section's internal
// offsets are, so relocating the section does not break it.
//
// Format 4 (writeStored):
//
//	dict    dictLen bytes: an s2 dictionary (uvarint repeat offset, then the
//	        segment's first records, raw) every block is compressed against, or none
//	        for a small segment
//	blocks  s2 blocks, back to back, from dictLen on
//	index   u32 numBlocks, u32 dictLen, u8 ordWidth, u8 offWidth, u8 rawWidth, then
//	        bit-packed: each block's first ordinal (numBlocks), each block's offset
//	        and the blocks' end (numBlocks+1), each block's uncompressed length
//	        (numBlocks)
//
// A block is flushed once it holds storedBlockTarget bytes: small, and s2, so fetching
// a hit decompresses little, and fast; the dictionary - the documents' shared shape and
// their common values - keeps small blocks compressing about as well as format 3's
// larger zstd ones.
//
// Format 3:
//
//	index   u32 numBlocks, per block: u64 offset, u32 compressedLen, u32 rawLen,
//	        u32 firstOrd, u32 count
//	blocks  zstd frames, back to back, about 16 KB raw each, no dictionary
//
// rawLen is a block's uncompressed length. A read decompresses into a buffer of exactly
// that size and refuses a block whose own header disagrees, instead of trusting that
// header, which in a crafted file could claim gigabytes. Blocks start at ordinal 0 and
// run contiguously up to NumDocs, each holding at least one document; openStoredIndex
// checks all of that, so a damaged table is refused at Open instead of misread later.

// storedBlockTarget is the uncompressed payload size a block is flushed at.
const storedBlockTarget = 4 << 10

// storedBlockTargetV3 is the block size format 3 was written with.
const storedBlockTargetV3 = 16 << 10

const (
	// storedDictLen is how many of the segment's first record bytes its dictionary
	// holds: s2's largest.
	storedDictLen = s2.MaxDictSize
	// storedBatchBlocks is how many blocks each compressing goroutine takes per batch:
	// what bounds a write's memory, with the dictionary sample, whatever the
	// segment's size.
	storedBatchBlocks = 64
)

// storedDictSample is the fewest record bytes a segment has a dictionary for. A
// smaller one (a refresh's, which merges soon replace) is compressed without: there
// a dictionary costs more to load into the encoders than it saves. A variable so a
// test can give a small segment one.
var storedDictSample = 1 << 20

// MaxStoredBytes is the largest document Build and Merge store: its id and body
// together, in bytes. A larger one fails the build with [ErrDocTooLarge]. The limit
// gives a stored block's uncompressed size a format-wide ceiling (maxStoredBlockRaw)
// that a reader refuses anything above, which bounds what decompressing one block of
// a damaged or crafted file can allocate. The API's request body limit must stay
// below it.
const MaxStoredBytes = 32 << 20

// maxStoredBlockRaw is the largest uncompressed block a reader accepts: a block is
// flushed once it reaches its target, so it holds less than that before its last
// document, plus that document (at most MaxStoredBytes, with two length prefixes).
const maxStoredBlockRaw = storedBlockTargetV3 + MaxStoredBytes + 2*binary.MaxVarintLen64

// storedEntryLenV3 is one format-3 block table entry's size: offset, compressedLen,
// rawLen, firstOrd, count.
const storedEntryLenV3 = 8 + 4 + 4 + 4 + 4

// storedIndexHeaderLen is the format-4 block table's fixed header.
const storedIndexHeaderLen = 4 + 4 + 1 + 1 + 1

// ErrDocTooLarge is a document whose id and body together exceed [MaxStoredBytes].
var ErrDocTooLarge = errors.New("segment: document exceeds MaxStoredBytes")

type storedBlockInfo struct {
	off      uint64
	clen     uint32
	rawLen   uint32
	firstOrd uint32
	count    uint32
}

// storedPayload is one block's raw (not yet compressed) bytes.
type storedPayload struct {
	firstOrd uint32
	count    uint32
	data     []byte
}

// compressBlocksParallel compresses every payload with encode into its own result slot
// (in payloads' order, whatever order they finish in), using up to threads goroutines
// that claim payloads from a shared counter.
func compressBlocksParallel(payloads []storedPayload, threads int, encode func(src []byte) []byte) [][]byte {
	compressed := make([][]byte, len(payloads))
	threads = min(threads, len(payloads))
	if threads < 2 {
		for i, p := range payloads {
			compressed[i] = encode(p.data)
		}
		return compressed
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for range threads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(payloads) {
					return
				}
				compressed[i] = encode(payloads[i].data)
			}
		}()
	}
	wg.Wait()
	return compressed
}

// storedWriter writes the STORED section from a stream of records: it holds back the
// first storedDictSample bytes to choose the dictionary, then cuts blocks and
// compresses them a bounded batch at a time.
type storedWriter struct {
	w            *fileWriter
	sectionStart uint64
	threads      int

	chosen   bool
	head     []byte
	headRecs []headRecord
	encode   func(src []byte) []byte

	cur      []byte
	curFirst uint32
	curCount uint32
	batch    []storedPayload

	firstOrds []uint32
	offs      []uint64
	raws      []uint32
}

// headRecord is one record held back while the dictionary is chosen: its ordinal and
// where it ends in head.
type headRecord struct {
	ord uint32
	end int
}

// writeStored writes src's records as the STORED section and returns the block table's
// offset, relative to the section's start. w must be at the section's start.
func writeStored(w *fileWriter, src storedSource, threads int) (uint64, error) {
	sw := &storedWriter{w: w, sectionStart: w.off, threads: max(threads, 1)}
	err := src(func(ord uint32, id string, body []byte) error {
		if size := uint64(len(id)) + uint64(len(body)); size > MaxStoredBytes {
			return fmt.Errorf("%w: ordinal %d holds %d bytes, over %d", ErrDocTooLarge, ord, size, MaxStoredBytes)
		}
		return sw.add(ord, id, body)
	})
	if err != nil {
		return 0, err
	}
	return sw.finish()
}

func appendRecord(dst []byte, id string, body []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(id)))
	dst = append(dst, id...)
	dst = binary.AppendUvarint(dst, uint64(len(body)))
	return append(dst, body...)
}

func (sw *storedWriter) add(ord uint32, id string, body []byte) error {
	if sw.w.err != nil {
		return sw.w.err
	}
	if !sw.chosen {
		sw.head = appendRecord(sw.head, id, body)
		sw.headRecs = append(sw.headRecs, headRecord{ord: ord, end: len(sw.head)})
		if len(sw.head) < storedDictSample {
			return nil
		}
		return sw.choose()
	}
	sw.cur = appendRecord(sw.cur, id, body)
	return sw.added(ord)
}

func (sw *storedWriter) added(ord uint32) error {
	if sw.curCount == 0 {
		sw.curFirst = ord
	}
	sw.curCount++
	if len(sw.cur) < storedBlockTarget {
		return nil
	}
	sw.batch = append(sw.batch, storedPayload{firstOrd: sw.curFirst, count: sw.curCount, data: sw.cur})
	sw.cur, sw.curCount = nil, 0
	if len(sw.batch) >= sw.threads*storedBatchBlocks {
		return sw.flushBatch()
	}
	return nil
}

func (sw *storedWriter) choose() error {
	sw.chosen = true
	sw.encode = func(src []byte) []byte { return s2.EncodeBetter(nil, src) }
	if len(sw.head) >= storedDictSample {
		dict := s2.MakeDict(sw.head[:min(storedDictLen, len(sw.head))], nil)
		sw.w.write(dict.Bytes())
		sw.encode = func(src []byte) []byte { return dict.EncodeBetter(nil, src) }
	}
	start := 0
	for _, r := range sw.headRecs {
		sw.cur = append(sw.cur, sw.head[start:r.end]...)
		start = r.end
		if err := sw.added(r.ord); err != nil {
			return err
		}
	}
	sw.head, sw.headRecs = nil, nil
	return nil
}

func (sw *storedWriter) flushBatch() error {
	if len(sw.batch) == 0 {
		return nil
	}
	compressed := compressBlocksParallel(sw.batch, sw.threads, sw.encode)
	for i, p := range sw.batch {
		sw.firstOrds = append(sw.firstOrds, p.firstOrd)
		sw.offs = append(sw.offs, sw.w.off-sw.sectionStart)
		sw.raws = append(sw.raws, uint32(len(p.data))) //nolint:gosec // at most maxStoredBlockRaw
		sw.w.write(compressed[i])
	}
	sw.batch = sw.batch[:0]
	return nil
}

func (sw *storedWriter) finish() (uint64, error) {
	if !sw.chosen {
		if err := sw.choose(); err != nil {
			return 0, err
		}
	}
	if sw.curCount > 0 {
		sw.batch = append(sw.batch, storedPayload{firstOrd: sw.curFirst, count: sw.curCount, data: sw.cur})
	}
	if err := sw.flushBatch(); err != nil {
		return 0, err
	}
	end := sw.w.off - sw.sectionStart
	dictLen := end
	if len(sw.offs) > 0 {
		dictLen = sw.offs[0]
	}
	var maxOrd uint32
	var maxRaw uint32
	for i := range sw.firstOrds {
		maxOrd = max(maxOrd, sw.firstOrds[i])
		maxRaw = max(maxRaw, sw.raws[i])
	}
	ordWidth := packedWidth(bitsFor(uint64(maxOrd)))
	offWidth := packedWidth(bitsFor(end))
	rawWidth := packedWidth(bitsFor(uint64(maxRaw)))
	var h encoder
	h.u32(uint32(len(sw.firstOrds))) //nolint:gosec // a segment holds far fewer than 4 billion blocks
	h.u32(uint32(dictLen))           //nolint:gosec // at most storedDictLen
	h.u8(ordWidth)
	h.u8(offWidth)
	h.u8(rawWidth)
	sw.w.write(h.b)
	p := newPacker(sw.w, ordWidth)
	for _, o := range sw.firstOrds {
		p.add(uint64(o))
	}
	p.finish()
	p = newPacker(sw.w, offWidth)
	for _, o := range sw.offs {
		p.add(o)
	}
	p.add(end)
	p.finish()
	p = newPacker(sw.w, rawWidth)
	for _, r := range sw.raws {
		p.add(uint64(r))
	}
	p.finish()
	return end, nil
}

// storedIndex is the parsed block table: format 3's parsed whole, format 4's read in
// place.
type storedIndex struct {
	data []byte
	v3   []storedBlockInfo

	base                         uint64 // the STORED section's absolute start
	numDocs                      uint32
	numBlocks                    uint32
	ordWidth, offWidth, rawWidth uint8
	firstOrds, offs, raws        []byte
	dict                         []byte
	s2dict                       *s2.Dict
}

// openStoredIndex parses the block table at the absolute position off of a STORED
// section starting at base and sectionLen long, in a file of the given major.
//
// It refuses a table that does not describe numDocs documents exactly: a block count
// the remaining bytes cannot hold (checked before allocating anything for it), a block
// outside the STORED section, an empty block, a gap or overlap in the ordinals, an
// uncompressed length over maxStoredBlockRaw or too short for its documents' two
// length prefixes each, or a total other than numDocs.
func openStoredIndex(data []byte, off, base, sectionLen uint64, numDocs uint32, major uint16) (storedIndex, error) {
	if off > uint64(len(data)) || off < base || off-base > sectionLen {
		return storedIndex{}, errShort
	}
	if major < 4 {
		return openStoredIndexV3(data, off, base, sectionLen, numDocs)
	}
	d := decoder{b: data[:base+sectionLen], pos: int(off)} //nolint:gosec // bounded by len(data)
	si := storedIndex{data: data, base: base, numDocs: numDocs}
	si.numBlocks = d.u32()
	dictLen := uint64(d.u32())
	si.ordWidth, si.offWidth, si.rawWidth = d.u8(), d.u8(), d.u8()
	if d.err != nil || !validWidth(si.ordWidth) || !validWidth(si.offWidth) || !validWidth(si.rawWidth) {
		return storedIndex{}, errShort
	}
	si.firstOrds = d.bytes(packedSize(uint64(si.numBlocks), si.ordWidth))
	si.offs = d.bytes(packedSize(uint64(si.numBlocks)+1, si.offWidth))
	si.raws = d.bytes(packedSize(uint64(si.numBlocks), si.rawWidth))
	tableRel := off - base
	if d.err != nil || dictLen > tableRel || (numDocs == 0) != (si.numBlocks == 0) {
		return storedIndex{}, errShort
	}
	si.dict = data[base : base+dictLen]
	if dictLen > 0 {
		if si.s2dict = s2.NewDict(si.dict); si.s2dict == nil {
			return storedIndex{}, errShort
		}
	}
	if unpack(si.offs, 0, si.offWidth) != dictLen || unpack(si.offs, uint64(si.numBlocks), si.offWidth) != tableRel {
		return storedIndex{}, errShort
	}
	for i := range uint64(si.numBlocks) {
		first := unpack(si.firstOrds, i, si.ordWidth)
		next := uint64(numDocs)
		if i+1 < uint64(si.numBlocks) {
			next = unpack(si.firstOrds, i+1, si.ordWidth)
		}
		raw := unpack(si.raws, i, si.rawWidth)
		switch {
		case (i == 0 && first != 0) || next <= first || next > uint64(numDocs),
			unpack(si.offs, i, si.offWidth) >= unpack(si.offs, i+1, si.offWidth),
			raw > maxStoredBlockRaw || raw < 2*(next-first):
			return storedIndex{}, errShort
		}
	}
	return si, nil
}

func validWidth(w uint8) bool { return w <= 56 || w == 64 }

func openStoredIndexV3(data []byte, off, base, sectionLen uint64, numDocs uint32) (storedIndex, error) {
	d := decoder{b: data, pos: int(off)} //nolint:gosec // bounded by len(data)
	n := uint64(d.u32())
	if d.err != nil || n > uint64(len(data)-d.pos)/storedEntryLenV3 { //nolint:gosec // d.pos <= len(data): the u32 just read fit
		return storedIndex{}, errShort
	}
	blocks := make([]storedBlockInfo, n)
	var next uint64 // the ordinal the next block must start at
	for i := range blocks {
		rel, clen, rawLen := d.u64(), d.u32(), d.u32()
		firstOrd, count := d.u32(), d.u32()
		switch {
		case rel > sectionLen || uint64(clen) > sectionLen-rel,
			count == 0 || uint64(firstOrd) != next,
			rawLen > maxStoredBlockRaw || uint64(rawLen) < 2*uint64(count):
			return storedIndex{}, errShort
		}
		next += uint64(count)
		blocks[i] = storedBlockInfo{off: base + rel, clen: clen, rawLen: rawLen, firstOrd: firstOrd, count: count}
	}
	if d.err != nil || next != uint64(numDocs) {
		return storedIndex{}, errShort
	}
	return storedIndex{data: data, v3: blocks, numDocs: numDocs}, nil
}

// blockFor returns the block holding ord, if any.
func (si *storedIndex) blockFor(ord uint32) (storedBlockInfo, bool) {
	if ord >= si.numDocs {
		return storedBlockInfo{}, false
	}
	if si.v3 != nil {
		lo, hi := 0, len(si.v3)
		for lo < hi {
			mid := lo + (hi-lo)/2
			if si.v3[mid].firstOrd <= ord {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo == 0 {
			return storedBlockInfo{}, false
		}
		return si.v3[lo-1], true
	}
	lo, hi := uint64(0), uint64(si.numBlocks)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if unpack(si.firstOrds, mid, si.ordWidth) <= uint64(ord) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return storedBlockInfo{}, false
	}
	b := lo - 1
	first := unpack(si.firstOrds, b, si.ordWidth)
	next := uint64(si.numDocs)
	if b+1 < uint64(si.numBlocks) {
		next = unpack(si.firstOrds, b+1, si.ordWidth)
	}
	start, end := unpack(si.offs, b, si.offWidth), unpack(si.offs, b+1, si.offWidth)
	return storedBlockInfo{
		off:      si.base + start,
		clen:     uint32(end - start),                     //nolint:gosec // a block within the section, checked at open
		rawLen:   uint32(unpack(si.raws, b, si.rawWidth)), //nolint:gosec // at most maxStoredBlockRaw, checked at open
		firstOrd: uint32(first),                           //nolint:gosec // below numDocs, checked at open
		count:    uint32(next - first),                    //nolint:gosec // below numDocs, checked at open
	}, true
}

// storedCacheShards is how many independent cache slots [storedCache] keeps. Concurrent
// hit fetches hit many different blocks at once; a single shared slot (and the one
// mutex guarding it) meant every one of those serialized on the same lock and then, as
// likely as not, evicted the block the next concurrent call wanted, decompressing it
// all over again. Picking a slot by block offset modulo this spreads unrelated blocks
// across independent locks, so only two fetches that land on the very same slot
// (whether or not they're the very same block) ever wait on each other.
const storedCacheShards = 8

// storedCache decompresses blocks, keeping the last few decompressed ones (one per
// shard) around on the assumption that nearby ordinals are fetched together, as hits
// usually are - within one shard; across shards, unrelated blocks simply don't
// contend.
//
// A format-4 segment's blocks decode with s2 (against dict, when the segment has one),
// which needs no state of its own. A format-3 segment's zstd frames decode with one
// *zstd.Decoder shared by every shard with no lock of its own: DecodeAll is safe for
// concurrent use.
type storedCache struct {
	dict   *s2.Dict
	zdec   *zstd.Decoder
	shards [storedCacheShards]storedShard
}

// storedShard is one cache slot: the one block it currently holds decompressed (valid
// false: none yet), and its own lock, independent of every other shard's.
type storedShard struct {
	mu    sync.Mutex
	valid bool
	block uint64
	data  []byte
}

// newStoredCache makes the cache for si's blocks. A format-3 decoder gets two caps on
// what DecodeAll may allocate: a format-wide ceiling (maxStoredBlockRaw, which also
// bounds the window it will accept), and the cap of the buffer record hands it -
// exactly the block's recorded rawLen - past which it fails instead of growing it.
func newStoredCache(si *storedIndex) (*storedCache, error) {
	if si.v3 == nil {
		return &storedCache{dict: si.s2dict}, nil
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxStoredBlockRaw), zstd.WithDecodeAllCapLimit(true))
	if err != nil {
		return nil, err
	}
	return &storedCache{zdec: dec}, nil
}

func (c *storedCache) close() {
	if c.zdec != nil {
		c.zdec.Close()
	}
}

// inflate decompresses one block into exactly rawLen bytes, refusing one whose own
// header claims another length before allocating anything.
func (c *storedCache) inflate(compressed []byte, rawLen uint32) ([]byte, error) {
	if c.zdec != nil {
		var h zstd.Header
		if h.Decode(compressed) != nil || (h.HasFCS && h.FrameContentSize != uint64(rawLen)) {
			return nil, errShort
		}
		payload, err := c.zdec.DecodeAll(compressed, make([]byte, 0, rawLen))
		if err != nil {
			return nil, err
		}
		if len(payload) != int(rawLen) {
			return nil, errShort
		}
		return payload, nil
	}
	if n, err := s2.DecodedLen(compressed); err != nil || n != int(rawLen) {
		return nil, errShort
	}
	dst := make([]byte, rawLen)
	var out []byte
	var err error
	if c.dict != nil {
		out, err = c.dict.Decode(dst, compressed)
	} else {
		out, err = s2.Decode(dst, compressed)
	}
	if err != nil || len(out) != int(rawLen) {
		return nil, errShort
	}
	return out, nil
}

// record decompresses b (reusing its shard's cache when b is already cached) and
// returns document ord's id and body.
//
// The body is a view of the shard's decompressed block, which is never written again
// once decompressed: replacing a shard's block always decompresses into a new buffer
// and swaps the slice under the shard's lock. So a caller may keep reading (or copy)
// the body after record returns and the lock is released, even while another call on
// the same shard replaces the block - the old buffer simply stays alive, unchanged,
// for as long as the caller holds the view. Reader.Stored and Merge both copy it.
func (c *storedCache) record(data []byte, b storedBlockInfo, ord uint32) (string, []byte, error) {
	shard := &c.shards[b.off%storedCacheShards]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if !shard.valid || shard.block != b.off {
		region := region{off: b.off, n: uint64(b.clen)}
		compressed, ok := region.slice(data)
		if !ok {
			return "", nil, errShort
		}
		payload, err := c.inflate(compressed, b.rawLen)
		if err != nil {
			return "", nil, err
		}
		shard.data, shard.block, shard.valid = payload, b.off, true
	}
	d := decoder{b: shard.data}
	var id string
	var body []byte
	for i := uint32(0); i <= ord-b.firstOrd; i++ {
		id = string(d.bytes(d.uvarint()))
		body = d.bytes(d.uvarint())
		if d.err != nil {
			return "", nil, errShort
		}
	}
	return id, body, nil
}
