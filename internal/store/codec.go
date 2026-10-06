package store

import (
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Stored bodies. A document's body in sl_documents and a change's payload in
// sl_changes are kept either as the JSON they are or as one codec byte followed by
// their encoding. JSON text never starts with a byte below 0x09 (it starts with
// whitespace or a value), so a value whose first byte is a codec byte is encoded and
// any other value is plain: rows written before codecs existed read unchanged, and a
// body too small or too random to gain from compression stays readable JSON.
const (
	// codecZstd is a zstd frame of the JSON, without a dictionary.
	codecZstd byte = 0x01
	// codecLast is the highest codec byte; bytes up to it and above codecZstd are
	// reserved for codecs to come (a trained dictionary, say).
	codecLast byte = 0x08
)

// compressMin is the smallest body worth compressing: below it a zstd frame's own
// header outweighs what it saves.
const compressMin = 128

// zstdEncoder and zstdDecoder are shared by every store: EncodeAll and DecodeAll are
// safe for concurrent use. The decoder refuses a frame that would decode past the
// largest payload Apply accepts, so a damaged row cannot make it allocate more.
var (
	zstdEncoder = sync.OnceValue(func() *zstd.Encoder {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(false),
			zstd.WithSingleSegment(true))
		if err != nil {
			panic(err)
		}
		return enc
	})
	zstdDecoder = sync.OnceValue(func() *zstd.Decoder {
		dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(MaxPayloadBytes),
			zstd.WithDecoderConcurrency(0))
		if err != nil {
			panic(err)
		}
		return dec
	})
)

// encodeBody returns the stored form of a JSON body: its zstd encoding behind
// codecZstd when that saves at least an eighth, and the body itself otherwise.
func encodeBody(body []byte) []byte {
	if len(body) < compressMin {
		return body
	}
	out := make([]byte, 1, 1+len(body)/2)
	out[0] = codecZstd
	out = zstdEncoder().EncodeAll(body, out)
	if len(out) > len(body)-len(body)/8 {
		return body
	}
	return out
}

// errUnknownCodec is a stored value encoded by a codec this binary does not know.
var errUnknownCodec = errors.New("store: a stored body uses a codec this binary does not know")

// decodeBody returns the JSON of a stored body (see encodeBody). A plain body is
// returned as it is.
func decodeBody(stored []byte) ([]byte, error) {
	if len(stored) == 0 || stored[0] > codecLast {
		return stored, nil
	}
	if stored[0] != codecZstd {
		return nil, fmt.Errorf("%w (codec %d)", errUnknownCodec, stored[0])
	}
	body, err := zstdDecoder().DecodeAll(stored[1:], nil)
	if err != nil {
		return nil, fmt.Errorf("store: a stored body does not decode: %w", err)
	}
	return body, nil
}
