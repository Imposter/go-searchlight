package percolate

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// FuzzOpenQuerySegment feeds arbitrary bytes to the query segment reader. The input is
// the file body (everything before the checksum): it is stamped with the current magic,
// version and section count and given a valid checksum, so the fuzzer reaches the
// structural checks rather than stopping at the checksum. Every input must be refused
// with a *CorruptError, or open and survive every accessor without a panic or a hang.
//
// Run it with: go test -run '^$' -fuzz FuzzOpenQuerySegment ./internal/percolate/
func FuzzOpenQuerySegment(f *testing.F) {
	full, err := encodeSegment(context.Background(), sampleStored(f), nil)
	if err != nil {
		f.Fatal(err)
	}
	empty, err := encodeSegment(context.Background(), nil, nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(full[:len(full)-4])
	f.Add(empty[:len(empty)-4])
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, body []byte) {
		openOrRefuse(t, restamp(body))
	})
}

// FuzzQuerySegmentSection overwrites bytes inside one section of a valid segment,
// keeping every section length and the checksum valid, so each mutation reaches the
// checks of that section's contents (offsets, ranks, filters, programs, id literals)
// rather than the framing. Every result must be refused with a *CorruptError, or open
// and survive every accessor.
//
// Run it with: go test -run '^$' -fuzz FuzzQuerySegmentSection ./internal/percolate/
func FuzzQuerySegmentSection(f *testing.F) {
	full, err := encodeSegment(context.Background(), sampleStored(f), nil)
	if err != nil {
		f.Fatal(err)
	}
	body := full[:len(full)-4]
	var starts, lengths [numSections]int
	at := len(fileMagic) + 8
	for i := range numSections {
		n := int(binary.LittleEndian.Uint64(body[at:]))
		starts[i], lengths[i] = at+8, n
		at += 8 + n
	}
	for sec := range numSections {
		f.Add(uint8(sec), uint32(0), []byte{0xff})
		f.Add(uint8(sec), uint32(3), []byte{0, 0, 0, 0})
	}
	f.Fuzz(func(t *testing.T, sec uint8, offset uint32, patch []byte) {
		i := int(sec) % numSections
		if lengths[i] == 0 || len(patch) == 0 {
			return
		}
		mutated := append([]byte(nil), body...)
		start := int(offset) % lengths[i]
		copy(mutated[starts[i]+start:starts[i]+lengths[i]], patch)
		openOrRefuse(t, restamp(mutated))
	})
}

// FuzzProgram evaluates arbitrary programs that pass checkProg over documents of every
// shape: a program the validator accepts never reads out of range.
//
// Run it with: go test -run '^$' -fuzz FuzzProgram ./internal/percolate/
func FuzzProgram(f *testing.F) {
	ft := &fieldTable{at: map[string]uint32{}}
	pc := progCompiler{field: ft.field}
	for _, raw := range []string{
		`{"all":[{"field":"title","op":"contains","value":["ab","rtx"]},{"field":"price","op":"between","value":[1,2]}]}`,
		`{"any":[{"not":{"field":"brand","op":"in","value":["a",1,true]}},{"field":"title","op":"similar","value":{"text":"rtx 4090","min":0.3}}]}`,
		`{"all":[{"field":"tags","op":"has_all","value":["a","b"]},{"field":"title","op":"starts_with","value":"rt"},{"field":"title","op":"words_any","value":["new york"]}]}`,
		`{"all":[{"field":"stock","op":"eq","value":true},{"field":"price","op":"ne","value":3},{"field":"tags","op":"empty"},{"field":"brand","op":"exists","value":false}]}`,
	} {
		n, problems := query.Parse([]byte(raw))
		if len(problems) > 0 {
			f.Fatal(problems)
		}
		f.Add(pc.compile(n))
	}
	nf := uint32(len(ft.names))
	var docs []schema.Doc
	for _, body := range []string{
		`{"brand":"a","title":"rtx 4090 new york","tags":["a","b"],"price":1.5,"stock":true}`,
		`{"brand":1,"title":["x"],"price":"x","tags":"a, b"}`,
		`{}`,
	} {
		d, _, err := schema.Analyze(testMapping(), "x", []byte(body))
		if err != nil {
			f.Fatal(err)
		}
		docs = append(docs, d)
	}
	f.Fuzz(func(_ *testing.T, p []byte) {
		if checkProg(p, nf) != nil {
			return
		}
		sc := new(scratch)
		for i := range docs {
			_ = evalOn(ft, sc, p, &docs[i])
		}
	})
}

// openOrRefuse opens data as a query segment: it must be refused with a *CorruptError,
// or open and survive every accessor.
func openOrRefuse(t *testing.T, data []byte) {
	t.Helper()
	seg, err := openData("fuzz", data)
	if err != nil {
		if !isCorrupt(err) {
			t.Fatalf("refused with %T (%v), want *CorruptError", err, err)
		}
		return
	}
	exercise(seg)
}

// A query segment file that is empty, truncated or missing is refused at Open, the
// first two as corrupt.
func TestOpenRefusesEmptyAndTruncatedFiles(t *testing.T) {
	dir := t.TempDir()
	full, err := encodeSegment(context.Background(), sampleStored(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"empty": {}, "short": full[:7], "truncated": full[:len(full)/2]} {
		if err := os.WriteFile(filepath.Join(dir, name+FileExt), data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := (Index{}).Open(dir, name); !isCorrupt(err) {
			t.Fatalf("%s: %v, want *CorruptError", name, err)
		}
		if err := os.Remove(filepath.Join(dir, name+FileExt)); err != nil {
			t.Fatalf("%s: the refused file is still held: %v", name, err)
		}
	}
	if _, err := (Index{}).Open(dir, "missing"); err == nil || isCorrupt(err) {
		t.Fatalf("a missing file: %v", err)
	}
}
