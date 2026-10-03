package segment

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Stored fields are each document's id and original body, packed id-then-body
// (uvarint length prefixes) into ~16 KB blocks and zstd-compressed one block at a time.
// A block holds a contiguous run of document ordinals, so looking one up is a binary
// search over block start ordinals, then one decompress and a short linear scan.
//
//	index   u32 numBlocks, per block: u64 offset, u32 compressedLen, u32 firstOrd, u32 count
//	blocks  compressed bytes, back to back (their offsets are absolute file offsets)

// storedBlockTarget is the uncompressed payload size a block is flushed at.
const storedBlockTarget = 16 << 10

type storedWriter struct {
	w        *fileWriter
	enc      *zstd.Encoder
	payload  []byte
	blocks   []storedBlockInfo
	firstOrd uint32
	count    uint32
}

type storedBlockInfo struct {
	off      uint64
	clen     uint32
	firstOrd uint32
	count    uint32
}

func newStoredWriter(w *fileWriter) (*storedWriter, error) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	return &storedWriter{w: w, enc: enc}, nil
}

// add appends document ord's id and body; ords must ascend.
func (s *storedWriter) add(ord uint32, id string, body []byte) {
	if s.count == 0 {
		s.firstOrd = ord
	}
	var e encoder
	e.bytes([]byte(id))
	e.bytes(body)
	s.payload = append(s.payload, e.b...)
	s.count++
	if len(s.payload) >= storedBlockTarget {
		s.flush()
	}
}

func (s *storedWriter) flush() {
	if s.count == 0 {
		return
	}
	compressed := s.enc.EncodeAll(s.payload, nil)
	off := s.w.off
	s.w.write(compressed)
	s.blocks = append(s.blocks, storedBlockInfo{
		off: off, clen: uint32(len(compressed)), firstOrd: s.firstOrd, count: s.count, //nolint:gosec // segment files stay far below 4 GiB
	})
	s.payload = s.payload[:0]
	s.count = 0
}

// finish flushes the last block, writes the index and returns its offset. The caller
// must also Close the encoder.
func (s *storedWriter) finish() uint64 {
	s.flush()
	off := s.w.off
	var h encoder
	h.u32(uint32(len(s.blocks))) //nolint:gosec // a segment holds far fewer than 4 billion blocks
	for _, b := range s.blocks {
		h.u64(b.off)
		h.u32(b.clen)
		h.u32(b.firstOrd)
		h.u32(b.count)
	}
	s.w.write(h.b)
	return off
}

func (s *storedWriter) close() error { return s.enc.Close() }

// storedIndex is the parsed block table, read in place.
type storedIndex struct {
	data   []byte
	blocks []storedBlockInfo
}

func openStoredIndex(data []byte, off uint64) (storedIndex, error) {
	if off > uint64(len(data)) {
		return storedIndex{}, errShort
	}
	d := decoder{b: data, pos: int(off)} //nolint:gosec // bounded by len(data)
	n := d.u32()
	blocks := make([]storedBlockInfo, n)
	for i := range blocks {
		blocks[i] = storedBlockInfo{off: d.u64(), clen: d.u32(), firstOrd: d.u32(), count: d.u32()}
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

// storedCache decompresses one block at a time, reused across calls on the assumption
// that nearby ordinals are fetched together (as hits usually are).
type storedCache struct {
	dec   *zstd.Decoder
	mu    sync.Mutex
	block uint64 // the cached block's offset, or 0 for none
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

// record decompresses b (reusing the cache when b is already cached) and returns
// document ord's id and body. The returned slices are views of the cache and are only
// valid until the next call.
func (c *storedCache) record(data []byte, b storedBlockInfo, ord uint32) (string, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.block != b.off {
		region := region{off: b.off, n: uint64(b.clen)}
		compressed, ok := region.slice(data)
		if !ok {
			return "", nil, errShort
		}
		payload, err := c.dec.DecodeAll(compressed, nil)
		if err != nil {
			return "", nil, err
		}
		c.data = payload
		c.block = b.off
	}
	d := decoder{b: c.data}
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
