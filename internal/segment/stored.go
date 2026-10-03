package segment

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

// Stored fields are each document's id and original body, packed id-then-body
// (uvarint length prefixes) into ~16 KB blocks and zstd-compressed. A block holds a
// contiguous run of document ordinals, so looking one up is a binary search over
// block start ordinals, then one decompress and a short linear scan. Blocks are
// compressed in parallel (compressBlocksParallel) but always placed by one
// sequential, already-ordered pass (writeStoredParallel): unlike a field's own
// structures (format.go), there is only ever one of these per segment, not one per
// field built independently in parallel, so there is no "self-contained in its own
// buffer" requirement driving the offset scheme here. They are still relative to the
// STORED section's own start, though (not plain absolute file offsets), for the same
// reason every other section's internal offsets are: so relocating the section - as
// Open never does on its own, but TestSectionOffsetsAreRelocatable does, standing in
// for anything that might someday need to - does not break it.
//
//	index   u32 numBlocks, per block: u64 offset, u32 compressedLen, u32 firstOrd, u32 count
//	blocks  compressed bytes, back to back (offsets relative to the STORED section's
//	        own absolute start, which Reader.parseMeta resolves once)

// storedBlockTarget is the uncompressed payload size a block is flushed at.
const storedBlockTarget = 16 << 10

// newBlockEncoder makes one stored-block compressor. A variable only so a test can make
// it fail ([TestCompressBlocksParallelEncoderFailureNoLeak]).
var newBlockEncoder = func() (*zstd.Encoder, error) { return zstd.NewWriter(nil) }

type storedBlockInfo struct {
	off      uint64
	clen     uint32
	firstOrd uint32
	count    uint32
}

// storedPayload is one block's raw (not yet compressed) bytes, collected by
// collectStoredPayloads and compressed by compressBlocksParallel - split out as its
// own step so compression, the expensive part, can run in parallel across blocks,
// while collecting (just concatenating each document's id and body) and placing (just
// writing already-compressed bytes one after another) stay simple, sequential passes.
type storedPayload struct {
	firstOrd uint32
	count    uint32
	data     []byte
}

// collectStoredPayloads runs src, splitting every live document's id and body into
// ~16 KB raw payload blocks, in ascending ordinal order.
func collectStoredPayloads(src storedSource) ([]storedPayload, error) {
	var payloads []storedPayload
	var cur []byte
	var firstOrd, count uint32
	flush := func() {
		if count == 0 {
			return
		}
		payloads = append(payloads, storedPayload{firstOrd: firstOrd, count: count, data: cur})
		cur, count = nil, 0
	}
	err := src(func(ord uint32, id string, body []byte) error {
		if count == 0 {
			firstOrd = ord
		}
		var e encoder
		e.bytes([]byte(id))
		e.bytes(body)
		cur = append(cur, e.b...)
		count++
		if len(cur) >= storedBlockTarget {
			flush()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	flush()
	return payloads, nil
}

// compressBlocksParallel zstd-compresses every payload into its own result slot (in
// payloads' order, whatever order they finish in), using up to threads goroutines,
// each with its own encoder reused across every block it takes (rather than one
// encoder per block, or one shared encoder guarded by a lock).
//
// Workers claim block indices from a shared atomic counter instead of a channel fed
// by a separate goroutine: a worker that fails to make its encoder simply returns, and
// once every worker has returned there is nothing left running and nothing left
// blocked - a feeder goroutine, by contrast, leaked forever when every worker failed
// before reading from its channel. wg.Wait() is the only synchronization the results
// need: each slot of compressed is written by exactly one worker (the one whose Add
// claimed that index), and read only after Wait.
func compressBlocksParallel(payloads []storedPayload, threads int) ([][]byte, error) {
	compressed := make([][]byte, len(payloads))
	if threads < 2 || len(payloads) < 2 {
		enc, err := newBlockEncoder()
		if err != nil {
			return nil, err
		}
		defer enc.Close()
		for i, p := range payloads {
			compressed[i] = enc.EncodeAll(p.data, nil)
		}
		return compressed, nil
	}
	threads = min(threads, len(payloads))
	var next atomic.Int64
	errs := make([]error, threads)
	var wg sync.WaitGroup
	for t := range threads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			enc, err := newBlockEncoder()
			if err != nil {
				errs[t] = err
				return
			}
			defer enc.Close()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(payloads) {
					return
				}
				compressed[i] = enc.EncodeAll(payloads[i].data, nil)
			}
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return compressed, nil
}

// writeStoredParallel collects src's documents into blocks, compresses them in
// parallel (compressBlocksParallel) and writes the compressed blocks and the block
// table into w, returning the table's offset. Unlike a field's structures (format.go),
// stored blocks are not written per field and are always placed by one continuous,
// already-sequential pass here - only compressing them has anything to parallelize.
// Every offset recorded (each block's, and the table's own, which is returned) is
// relative to the STORED section's start, not the file's; Reader.parseMeta resolves
// them against the section table once.
func writeStoredParallel(w *fileWriter, src storedSource, threads int) (uint64, error) {
	// sectionStart: w.off is already at the STORED section's own absolute start
	// (writeSegmentParts calls this right after beginSection(sectionStored)), so
	// every offset recorded below, relative to it, is relative to the section - the
	// same scheme format.go documents for a field's own structures, just with one
	// shared section start instead of one private buffer per field.
	sectionStart := w.off
	payloads, err := collectStoredPayloads(src)
	if err != nil {
		return 0, err
	}
	compressed, err := compressBlocksParallel(payloads, threads)
	if err != nil {
		return 0, err
	}
	var h encoder
	h.u32(uint32(len(payloads))) //nolint:gosec // a segment holds far fewer than 4 billion blocks
	for i, p := range payloads {
		blockOff := w.off - sectionStart
		w.write(compressed[i])
		h.u64(blockOff)
		h.u32(uint32(len(compressed[i]))) //nolint:gosec // one compressed block stays far below 4 GiB
		h.u32(p.firstOrd)
		h.u32(p.count)
	}
	// The table's own offset, returned to the caller for META: captured only now,
	// after every block above has actually been written, so it correctly points
	// past them, at the table itself - not before them, where it would read the
	// first block's bytes as if they were the table. Relative to sectionStart, like
	// the block offsets above.
	off := w.off - sectionStart
	w.write(h.b)
	return off, nil
}

// storedIndex is the parsed block table, read in place.
type storedIndex struct {
	data   []byte
	blocks []storedBlockInfo
}

// openStoredIndex parses the block table at the absolute position off (base +
// META's section-relative storedIndexOff), materializing each block's own offset
// (also stored relative to base) as an absolute position immediately, once, here -
// so every other reader of a storedBlockInfo.off (blockFor, storedCache.record) needs
// no further change.
func openStoredIndex(data []byte, off, base uint64) (storedIndex, error) {
	if off > uint64(len(data)) {
		return storedIndex{}, errShort
	}
	d := decoder{b: data, pos: int(off)} //nolint:gosec // bounded by len(data)
	n := d.u32()
	blocks := make([]storedBlockInfo, n)
	for i := range blocks {
		blocks[i] = storedBlockInfo{off: base + d.u64(), clen: d.u32(), firstOrd: d.u32(), count: d.u32()}
	}
	if d.err != nil {
		return storedIndex{}, d.err
	}
	return storedIndex{data: data, blocks: blocks}, nil
}

// blockFor returns the block holding ord, if any.
func (si storedIndex) blockFor(ord uint32) (storedBlockInfo, bool) {
	lo, hi := 0, len(si.blocks)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if si.blocks[mid].firstOrd <= ord {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return storedBlockInfo{}, false
	}
	b := si.blocks[lo-1]
	if ord >= b.firstOrd+b.count {
		return storedBlockInfo{}, false
	}
	return b, true
}

// storedCacheShards is how many independent cache slots [storedCache] keeps. Task 6's
// concurrent hit fetches will hit many different blocks at once; a single shared slot
// (and the one mutex guarding it) meant every one of those serialized on the same
// lock and then, as likely as not, evicted the block the next concurrent call wanted,
// decompressing it all over again. Picking a slot by block offset modulo this spreads
// unrelated blocks across independent locks, so only two fetches that land on the
// very same slot (whether or not they're the very same block) ever wait on each
// other.
const storedCacheShards = 8

// storedCache decompresses blocks, keeping the last few decompressed ones (one per
// shard) around on the assumption that nearby ordinals are fetched together, as hits
// usually are - within one shard; across shards, unrelated blocks simply don't
// contend.
//
// Its one *zstd.Decoder is shared by every shard with no lock of its own:
// Decoder.DecodeAll is documented safe for concurrent use (it hands out one of the
// decoder's own pooled block decoders per call, up to its configured concurrency,
// instead of mutating shared state), so decompressing two different blocks for two
// different shards already proceeds in parallel without a sync.Pool of decoders on
// top - which would only add redundant block-decoder pools fighting over the same
// cores.
type storedCache struct {
	dec    *zstd.Decoder
	shards [storedCacheShards]storedShard
}

// storedShard is one cache slot: the one block it currently holds decompressed (or 0,
// i.e. never, since block offset 0 is inside the header and never a real block), and
// its own lock, independent of every other shard's.
type storedShard struct {
	mu    sync.Mutex
	block uint64
	data  []byte
}

func newStoredCache() (*storedCache, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	return &storedCache{dec: dec}, nil
}

func (c *storedCache) close() { c.dec.Close() }

// record decompresses b (reusing its shard's cache when b is already cached) and
// returns document ord's id and body. The returned slices are views of the cache and
// are only valid until the next call that lands on the same shard.
func (c *storedCache) record(data []byte, b storedBlockInfo, ord uint32) (string, []byte, error) {
	shard := &c.shards[b.off%storedCacheShards]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.block != b.off {
		region := region{off: b.off, n: uint64(b.clen)}
		compressed, ok := region.slice(data)
		if !ok {
			return "", nil, errShort
		}
		payload, err := c.dec.DecodeAll(compressed, nil)
		if err != nil {
			return "", nil, err
		}
		shard.data = payload
		shard.block = b.off
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
