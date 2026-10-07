package store

import (
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Stored bodies. A document's body in sl_documents and an upsert's payload in
// sl_changes each have two columns: body (payload), the JSON as text, and body_z
// (payload_z), one codec byte followed by the JSON's encoding. A row holds its value
// in exactly one of them: the text when it is not empty, the encoding otherwise.
// Binaries from before codecs read and write the text column alone, so a store holds
// encoded values only once every node reads them: Apply encodes only after the
// cluster has turned FeatureZstdBodies on (see [Store.CompressBodies]). A body too
// small or too random to gain from compression stays text either way.
//
// codecZstd is a zstd frame of the JSON, without a dictionary. Codec bytes 0x02 to
// 0x08 are reserved for codecs to come (a trained dictionary, say); a binary refuses
// a value whose codec it does not know.
const codecZstd byte = 0x01

// BodyCodecs is the stored-body codecs this binary reads, as a level its nodes
// heartbeat (sl_nodes.body_codecs): 1 reads zstd. A node from before codecs is at 0.
const BodyCodecs = 1

// FeatureZstdBodies is the sl_features row that lets Apply store zstd bodies: the
// cluster's leader records it once every registered node reads them (or has been
// dead past the fence time), and it is never removed.
const FeatureZstdBodies = "zstd_bodies"

// compressMin is the smallest body worth compressing: below it a zstd frame's own
// header outweighs what it saves.
const compressMin = 128

// zstdWindow bounds the encoder's window, and with it the memory each concurrent
// encoding of a large body holds: a JSON document repeats itself over far less.
const zstdWindow = 1 << 20

// zstdEncoder and zstdDecoder are shared by every store: EncodeAll and DecodeAll are
// safe for concurrent use. Frames carry a checksum, so a damaged row is reported
// rather than read as other JSON. The decoder refuses a frame that would decode past
// the largest payload Apply accepts, so a damaged row cannot make it allocate more.
var (
	zstdEncoder = sync.OnceValue(func() *zstd.Encoder {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(true),
			zstd.WithWindowSize(zstdWindow), zstd.WithLowerEncoderMem(true))
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

// storedBody is a body as Apply writes it: in plain (the text column) or, encoded,
// in z (the codec column).
type storedBody struct {
	plain string
	z     []byte
}

// size is the stored body's bytes.
func (b storedBody) size() int { return len(b.plain) + len(b.z) }

// encodeBody returns the stored form of a JSON body: encoded when compress is set
// and zstd saves at least an eighth, and the JSON itself otherwise.
func encodeBody(body []byte, compress bool) storedBody {
	if !compress || len(body) < compressMin {
		return storedBody{plain: string(body)}
	}
	out := make([]byte, 1, 1+len(body)/2)
	out[0] = codecZstd
	out = zstdEncoder().EncodeAll(body, out)
	if len(out) > len(body)-len(body)/8 {
		return storedBody{plain: string(body)}
	}
	return storedBody{z: out}
}

// errUnknownCodec is a stored value encoded by a codec this binary does not know.
var errUnknownCodec = errors.New("a stored body uses a codec this binary does not know")

// readBody returns the JSON of a row's two body columns: the text when it is not
// empty, else the decoded encoding (a delete's payload has neither).
func readBody(plain, z []byte) ([]byte, error) {
	if len(plain) > 0 || len(z) == 0 {
		return plain, nil
	}
	return decodeBody(z)
}

// decodeBody returns the JSON of an encoded body: a codec byte and its encoding.
func decodeBody(z []byte) ([]byte, error) {
	if z[0] != codecZstd {
		return nil, fmt.Errorf("%w (codec %d)", errUnknownCodec, z[0])
	}
	body, err := zstdDecoder().DecodeAll(z[1:], nil)
	if err != nil {
		return nil, fmt.Errorf("a stored body does not decode: %w", err)
	}
	return body, nil
}
