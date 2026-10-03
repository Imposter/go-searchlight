package query

import (
	"bytes"
	"encoding/binary"
	"maps"
	"math"
	"slices"

	"github.com/Imposter/go-searchlight/internal/analysis"
)

// Canonical returns a deterministic byte encoding of n: semantically equivalent trees
// encode identically, for dedup keys and caches (never for storage or display; the
// format may change between versions).
//
//   - Leaf values are normalized as [Compiled.Match] compares them: texts normalized
//     and cleaned, numbers by their float64 bits (so 1, 1.0 and 1e0 agree), and every
//     non-finite number (one no float64 holds) alike, since they behave alike: never
//     equal to anything.
//   - exists with no value and exists: true canonicalize the same way, as they mean
//     the same thing; so do the other shapes [Validate] holds equivalent.
//   - words_all and words_any encode their phrases' words (analysis.Words of each text
//     as written, sorted and distinct), which is what the matcher compares: two texts
//     with one normalized form can still have different words (a combining sequence
//     NFKC composes differently from case folding).
//   - A list value whose order does not change the condition (in, has_any, has_all,
//     contains_any, contains_all, words_any, words_all: each is a set) is sorted and
//     deduplicated after normalizing. between's [lo, hi] is ordered and keeps its
//     shape, and so does similar's {text, min}, whose members are encoded by name.
func Canonical(n Node) []byte {
	return appendNode(nil, n)
}

// Node tags.
const (
	tagNil byte = iota
	tagAll
	tagAny
	tagNot
	tagLeaf
)

func appendNode(buf []byte, n Node) []byte {
	switch x := n.(type) {
	case *All:
		if x == nil {
			return append(buf, tagNil)
		}
		return appendGroup(buf, tagAll, x.Children)
	case *Any:
		if x == nil {
			return append(buf, tagNil)
		}
		return appendGroup(buf, tagAny, x.Children)
	case *Not:
		if x == nil {
			return append(buf, tagNil)
		}
		buf = append(buf, tagNot)
		return appendNode(buf, x.Child)
	case *Leaf:
		if x == nil {
			return append(buf, tagNil)
		}
		return appendLeaf(buf, x)
	default: // nil
		return append(buf, tagNil)
	}
}

func appendGroup(buf []byte, tag byte, children []Node) []byte {
	buf = append(buf, tag)
	buf = binary.AppendUvarint(buf, uint64(len(children)))
	for _, c := range children {
		buf = appendNode(buf, c)
	}
	return buf
}

func appendLeaf(buf []byte, l *Leaf) []byte {
	buf = append(buf, tagLeaf)
	buf = appendString(buf, l.Field)
	buf = appendString(buf, l.Op)
	return appendLeafValue(buf, l)
}

func appendString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

// Value tags, inside a leaf and inside a list or object entry.
const (
	valNone byte = iota
	valBool
	valNumber
	valString
	valList
	valObject
	valWords // words_*: the phrases as the matcher reads them
	valNever // a condition that never holds
)

func appendLeafValue(buf []byte, l *Leaf) []byte {
	a := l.arg()
	if l.Op == OpExists {
		// {exists} and {exists: true} mean the same thing: canonicalize to the bool
		// wanted, exactly as compileLeaf derives it.
		want := a.Kind != ArgBool || a.Scalar.Bool
		return appendScalar(buf, Scalar{Kind: ArgBool, Bool: want})
	}
	if l.Op == OpWordsAll || l.Op == OpWordsAny {
		return appendWords(buf, &a, l.Op == OpWordsAll)
	}
	switch a.Kind {
	case ArgBool, ArgNumber, ArgString:
		return appendScalar(buf, a.Scalar)
	case ArgList:
		return appendList(buf, a.List, unorderedList(l.Op))
	case ArgObject:
		return appendObject(buf, a.Object)
	default: // ArgNone
		return append(buf, valNone)
	}
}

// unorderedList reports whether op's list value is a set: reordering it does not
// change what the condition holds (between's [lo, hi] is the one list value that is
// ordered, and is not one of these).
func unorderedList(op string) bool {
	switch op {
	case OpIn, OpHasAny, OpHasAll, OpContainsAny, OpContainsAll, OpWordsAny, OpWordsAll:
		return true
	default:
		return false
	}
}

func appendScalar(buf []byte, s Scalar) []byte {
	switch s.Kind {
	case ArgBool:
		buf = append(buf, valBool)
		if s.Bool {
			return append(buf, 1)
		}
		return append(buf, 0)
	case ArgNumber:
		buf = append(buf, valNumber)
		if !s.Finite {
			return append(buf, 0) // every such number behaves alike: never equal
		}
		buf = append(buf, 1)
		var bits [8]byte
		binary.BigEndian.PutUint64(bits[:], math.Float64bits(s.Number))
		return append(buf, bits[:]...)
	case ArgString:
		buf = append(buf, valString)
		return appendString(buf, s.Norm)
	default:
		return append(buf, valNone)
	}
}

func appendList(buf []byte, list []Scalar, unordered bool) []byte {
	entries := make([][]byte, len(list))
	for i := range list {
		entries[i] = appendScalar(nil, list[i])
	}
	if unordered {
		slices.SortFunc(entries, bytes.Compare)
		entries = slices.CompactFunc(entries, bytes.Equal)
	}
	buf = append(buf, valList)
	buf = binary.AppendUvarint(buf, uint64(len(entries)))
	for _, e := range entries {
		buf = append(buf, e...)
	}
	return buf
}

func appendObject(buf []byte, object map[string]Scalar) []byte {
	keys := slices.Sorted(maps.Keys(object))
	buf = append(buf, valObject)
	buf = binary.AppendUvarint(buf, uint64(len(keys)))
	for _, k := range keys {
		buf = appendString(buf, k)
		buf = appendScalar(buf, object[k])
	}
	return buf
}

// appendWords encodes words_all's or words_any's phrases as the matcher compiles them
// (setWords): each string's words (analysis.Words of the text as written, not of its
// normalized form, which can differ on combining sequences), sorted and distinct; a
// phrase with no word drops out of words_any and makes words_all never hold.
func appendWords(buf []byte, a *Arg, every bool) []byte {
	var phrases []string
	for _, s := range a.texts() {
		w := analysis.Words(s.Text)
		if w == analysis.NoWords {
			if every {
				return append(buf, valNever)
			}
			continue
		}
		phrases = append(phrases, w)
	}
	if len(phrases) == 0 {
		return append(buf, valNever)
	}
	slices.Sort(phrases)
	phrases = slices.Compact(phrases)
	buf = append(buf, valWords)
	buf = binary.AppendUvarint(buf, uint64(len(phrases)))
	for _, p := range phrases {
		buf = appendString(buf, p)
	}
	return buf
}
