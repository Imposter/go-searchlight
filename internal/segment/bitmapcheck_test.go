package segment

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

func serialize(t *testing.T, rb *roaring.Bitmap, runOptimize bool) []byte {
	t.Helper()
	if runOptimize {
		rb.RunOptimize()
	}
	var buf bytes.Buffer
	if _, err := rb.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestCheckBitmapAcceptsWhatRoaringWrites: every container kind roaring serializes,
// with and without run containers and the offset table, checks out with its true
// cardinality.
func TestCheckBitmapAcceptsWhatRoaringWrites(t *testing.T) {
	shapes := map[string]*roaring.Bitmap{
		"empty": roaring.New(),
		"array": roaring.BitmapOf(0, 3, 70000, 1<<31),
		"bitmap": func() *roaring.Bitmap {
			rb := roaring.New()
			for i := uint32(0); i < 20000; i += 3 {
				rb.Add(i)
			}
			return rb
		}(),
		"runs": func() *roaring.Bitmap {
			rb := roaring.New()
			rb.AddRange(10, 50000)
			rb.AddRange(65530, 65536*3+7)
			rb.Add(0xFFFFFFFF)
			return rb
		}(),
	}
	for name, rb := range shapes {
		for _, opt := range []bool{false, true} {
			b := serialize(t, rb.Clone(), opt)
			card, ok := checkBitmap(b, 1<<32)
			if !ok || card != rb.GetCardinality() {
				t.Fatalf("%s (runOptimize %v): checkBitmap = %d, %v, want %d, true", name, opt, card, ok, rb.GetCardinality())
			}
		}
	}
}

// TestCheckBitmapRefusesMalformed damages a valid serialization in each way FromBuffer
// lets through (or that would let a bitmap claim documents a segment lacks).
func TestCheckBitmapRefusesMalformed(t *testing.T) {
	// One run container, key 0, runs [5,9] and [20,29]: cardinality 15.
	runs := func() []byte {
		var e encoder
		e.u32(serialCookie)                             // 1 container, with run flags
		e.u8(1)                                         // container 0 is a run container
		e.b = binary.LittleEndian.AppendUint16(e.b, 0)  // key
		e.b = binary.LittleEndian.AppendUint16(e.b, 14) // cardinality - 1
		e.b = binary.LittleEndian.AppendUint16(e.b, 2)  // runs
		for _, r := range [][2]uint16{{5, 4}, {20, 9}} {
			e.b = binary.LittleEndian.AppendUint16(e.b, r[0])
			e.b = binary.LittleEndian.AppendUint16(e.b, r[1])
		}
		return e.b
	}
	if card, ok := checkBitmap(runs(), 1000); !ok || card != 15 {
		t.Fatalf("hand-built run container: %d, %v, want 15, true", card, ok)
	}
	array := serialize(t, roaring.BitmapOf(1, 2, 3, 4), false) // no-run cookie, 1 array container
	arrayBody := len(array) - 8
	bitmapRB := roaring.New()
	bitmapRB.AddRange(0, 5000)
	bitmapRB.Remove(4000)
	bitmapSer := serialize(t, bitmapRB, false)

	cases := map[string]struct {
		b     []byte
		limit uint64
	}{
		"empty input":       {nil, 1 << 32},
		"bad cookie":        {[]byte{1, 2, 3, 4}, 1 << 32},
		"truncated":         {array[:len(array)-1], 1 << 32},
		"trailing bytes":    {append(append([]byte(nil), array...), 0), 1 << 32},
		"value past limit":  {array, 4},
		"run past limit":    {runs(), 29},
		"no runs":           {func() []byte { b := runs(); binary.LittleEndian.PutUint16(b[9:], 0); return b[:11] }(), 1000},
		"overlapping runs":  {func() []byte { b := runs(); binary.LittleEndian.PutUint16(b[15:], 8); return b }(), 1000},
		"run past 0xFFFF":   {func() []byte { b := runs(); binary.LittleEndian.PutUint16(b[15:], 0xFFF8); return b }(), 1 << 32}, // ends at 0x10001
		"run card mismatch": {func() []byte { b := runs(); binary.LittleEndian.PutUint16(b[7:], 3); return b }(), 1000},
		"unsorted array":    {func() []byte { b := append([]byte(nil), array...); b[arrayBody+2] = 9; return b }(), 1 << 32},
		"bitmap card wrong": {func() []byte { b := append([]byte(nil), bitmapSer...); b[len(b)-1] ^= 0x80; return b }(), 1 << 32},
		"too many containers": {func() []byte {
			b := append([]byte(nil), array...)
			binary.LittleEndian.PutUint32(b[4:], 1<<17)
			return b
		}(), 1 << 32},
	}
	for name, c := range cases {
		if _, ok := checkBitmap(c.b, c.limit); ok {
			t.Errorf("%s: checkBitmap accepted it", name)
		}
	}
}

// TestLoadDeletesRefusesMalformedBitmap: the fuzz crasher's shape (a run container with
// no runs) in a checksum-valid deletes sidecar is a CorruptError, not a bitmap that
// panics on first use.
func TestLoadDeletesRefusesMalformedBitmap(t *testing.T) {
	dir := t.TempDir()
	if err := WriteDeletes(dir, "seg", 1, roaring.BitmapOf(3, 4, 5, 6, 7, 8)); err != nil {
		t.Fatal(err)
	}
	path := deletesPath(dir, "seg", 1)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	footer, err := verifyFileSections(path, data, []sectionKind{sectionPresence})
	if err != nil {
		t.Fatal(err)
	}
	sec := footer.sections[sectionPresence]
	// Replace the bitmap with one run container holding zero runs, same length.
	var e encoder
	e.u32(serialCookie)
	e.u8(1)
	e.b = binary.LittleEndian.AppendUint16(e.b, 0)
	e.b = binary.LittleEndian.AppendUint16(e.b, 0)
	e.b = binary.LittleEndian.AppendUint16(e.b, 0)
	bad := append(append([]byte(nil), data[:sec.off]...), e.b...)
	bad = append(bad, data[sec.off+sec.n:]...)
	tableStart := len(bad) - tailSize - sectionEntrySize
	binary.LittleEndian.PutUint64(bad[tableStart+12:], uint64(len(e.b)))
	fixChecksums(bad)
	if err := os.WriteFile(filepath.Join(dir, filepath.Base(path)), bad, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = LoadDeletes(dir, "seg", 1)
	wantCorrupt(t, err, "presence")
}

func BenchmarkCheckBitmap(b *testing.B) {
	for name, rb := range map[string]*roaring.Bitmap{
		"array400": func() *roaring.Bitmap {
			rb := roaring.New()
			for i := uint32(0); i < 20000; i += 50 {
				rb.Add(i)
			}
			return rb
		}(),
		"bitmap": func() *roaring.Bitmap {
			rb := roaring.New()
			for i := uint32(0); i < 65536; i += 3 {
				rb.Add(i)
			}
			return rb
		}(),
	} {
		var buf bytes.Buffer
		if _, err := rb.WriteTo(&buf); err != nil {
			b.Fatal(err)
		}
		data := buf.Bytes()
		b.Run(name, func(b *testing.B) {
			for range b.N {
				if _, ok := checkBitmap(data, 1<<32); !ok {
					b.Fatal("refused")
				}
			}
		})
	}
}
