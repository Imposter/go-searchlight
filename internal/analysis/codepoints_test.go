package analysis

import (
	"crypto/sha256"
	"encoding/hex"
	"runtime"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

// TestParityCodepoints recomputes, for every code point in every fixture context, the
// line scrape-bot hashed (normalize, words_of, lower, trigrams, list_entries) and
// compares each context's per-block SHA-256 with codepoints.json. A failure names the
// block and the context; tools/parity/gen.py's _codepoints_fixture spells the line.
func TestParityCodepoints(t *testing.T) {
	data := loadFixture[struct {
		Contexts []string   `json:"contexts"`
		Block    int        `json:"block"`
		Hashes   [][]string `json:"hashes"`
	}](t, "codepoints.json")
	blocks := (utf8.MaxRune + 1) / data.Block
	if len(data.Contexts) == 0 || len(data.Hashes) != len(data.Contexts) || len(data.Hashes[0]) != blocks {
		t.Fatalf("codepoints.json: %d contexts, %d hash columns, want %d blocks each",
			len(data.Contexts), len(data.Hashes), blocks)
	}

	type failure struct{ block, context int }
	var (
		mu       sync.Mutex
		failures []failure
		next     = make(chan int)
		wg       sync.WaitGroup
	)
	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			var line strings.Builder
			for block := range next {
				for c, context := range data.Contexts {
					digest := sha256.New()
					for r := rune(block * data.Block); r < rune((block+1)*data.Block); r++ {
						if r >= 0xD800 && r <= 0xDFFF {
							continue
						}
						text := strings.ReplaceAll(context, "{}", string(r))
						line.Reset()
						line.WriteString(Normalize(text))
						line.WriteByte(1)
						line.WriteString(Words(text))
						line.WriteByte(1)
						line.WriteString(Lower(text))
						line.WriteByte(1)
						line.WriteString(strings.Join(Trigrams(text), "\x03"))
						line.WriteByte(1)
						line.WriteString(strings.Join(Entries(text), "\x03"))
						line.WriteByte(2)
						digest.Write([]byte(line.String()))
					}
					if hex.EncodeToString(digest.Sum(nil)) != data.Hashes[c][block] {
						mu.Lock()
						failures = append(failures, failure{block, c})
						mu.Unlock()
					}
				}
			}
		})
	}
	for block := range blocks {
		next <- block
	}
	close(next)
	wg.Wait()
	for i, f := range failures {
		if i == 40 {
			t.Errorf("... and %d more", len(failures)-i)
			break
		}
		lo := f.block * data.Block
		t.Errorf("block U+%04X..U+%04X differs in context %+q", lo, lo+data.Block-1, data.Contexts[f.context])
	}
}
