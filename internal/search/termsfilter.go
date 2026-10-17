package search

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"regexp/syntax"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// Terms include and exclude.
//
// A terms aggregation's include and exclude (Elasticsearch's) choose the values it
// buckets: only included values that are not excluded are counted, before any shard
// cuts its list, so size, shard_size, sum_other_doc_count and the error bounds are
// those of the chosen values alone. Each is an array of exact values, or a pattern
// over the whole key.
//
// Values compare as the field's values do: a keyword, text, keyword_list or _id value
// normalized as the field indexes it (casefolded, whitespace folded; a number or bool
// as its text); a number exactly (-0 is 0); a date in Unix milliseconds or as a date
// string the field takes; a bool as true or false.
//
// A pattern applies to text keys only, and to the normalized key: the subset of
// Lucene's regular expressions in [compilePattern]'s doc, folded as values are,
// always matching the whole key, run as RE2 (linear in the key's length) and
// compiled once per parsed or decoded request.
//
// Per segment, a keyword or list field's filter is a bitmap over the dictionary's
// ordinals, decided once per term (exact values looked up, only the terms after a
// pattern's literal prefix read), or, with few documents collected for the
// dictionary (lazyTermsRatio), decided per ordinal as it is collected; so it costs the
// distinct values or the hits, whichever is less. A number field's values are
// checked directly.

// Bounds of include and exclude.
const (
	// MaxTermsValues is the most exact values an include or exclude holds.
	MaxTermsValues = 1_000
	// MaxPatternChars is the longest pattern, in characters.
	MaxPatternChars = 1_000
	// maxPatternInsts bounds a compiled pattern's program, which a counted repetition
	// multiplies: past it the pattern is refused as too complex.
	maxPatternInsts = 10_000
	// maxPatternRepeat is the largest count of a {n,m} repetition.
	maxPatternRepeat = 1_000
	// maxPatternDepth bounds a pattern's group nesting.
	maxPatternDepth = 100
)

// TermsFilter is a terms aggregation's include or exclude: exact values when Values
// is not nil (an empty list then chooses none), else Pattern. It is written as
// Elasticsearch writes it: an array of values, or the pattern as a string.
type TermsFilter struct {
	// Values are strings, numbers (float64, json.Number, any Go integer) and bools.
	Values []any
	// Pattern matches whole normalized keys (see compilePattern).
	Pattern string

	// compiled is Pattern compiled, once for every shard a parsed or decoded request
	// searches (nil for one built by hand, compiled at each use).
	compiled *patternCache
}

// patternCache compiles a TermsFilter's pattern once.
type patternCache struct {
	once sync.Once
	re   *compiledPattern
	err  error
}

// pattern is f's Pattern compiled.
func (f *TermsFilter) pattern() (*compiledPattern, error) {
	c := f.compiled
	if c == nil {
		return compilePattern(f.Pattern)
	}
	c.once.Do(func() { c.re, c.err = compilePattern(f.Pattern) })
	return c.re, c.err
}

// MarshalJSON writes f as an array of values or a pattern string. A float64 is
// written with its fraction (10.0), as a JSON literal keeps it, so a keyword's text of
// it is the same after a round trip.
func (f *TermsFilter) MarshalJSON() ([]byte, error) {
	if f.Values == nil {
		return json.Marshal(f.Pattern)
	}
	out := make([]any, len(f.Values))
	for i, v := range f.Values {
		if x, ok := v.(float64); ok && !math.IsNaN(x) && !math.IsInf(x, 0) {
			v = json.Number(analysis.FloatText(x))
		}
		out[i] = v
	}
	return json.Marshal(out)
}

// UnmarshalJSON reads an array of values (numbers as json.Number) or a pattern string.
func (f *TermsFilter) UnmarshalJSON(raw []byte) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '"' {
		*f = TermsFilter{compiled: &patternCache{}}
		return json.Unmarshal(raw, &f.Pattern)
	}
	var values []any
	if err := decodeNumbers(raw, &values); err != nil || values == nil {
		return errors.New("search: include and exclude are an array of values or a pattern")
	}
	*f = TermsFilter{Values: values}
	return nil
}

// parseTermsFilter reads an include or exclude at loc: an array of values or a
// pattern. Elasticsearch's partitions are refused.
func parseTermsFilter(raw json.RawMessage, loc string, ps *problems) *TermsFilter {
	trimmed := bytes.TrimSpace(raw)
	switch {
	case len(trimmed) > 0 && trimmed[0] == '"':
		var pattern string
		if json.Unmarshal(trimmed, &pattern) != nil {
			ps.add(loc, "a pattern is text")
			return nil
		}
		f := &TermsFilter{Pattern: pattern, compiled: &patternCache{}}
		if _, err := f.pattern(); err != nil {
			ps.add(loc, "%s", err.Error())
			return nil
		}
		return f
	case len(trimmed) > 0 && trimmed[0] == '[':
		var items []json.RawMessage
		if json.Unmarshal(trimmed, &items) != nil {
			ps.add(loc, "include and exclude are an array of values or a pattern")
			return nil
		}
		if len(items) > MaxTermsValues {
			ps.add(loc, "include and exclude hold at most %d values", MaxTermsValues)
			return nil
		}
		f := &TermsFilter{Values: make([]any, 0, len(items))}
		for i, item := range items {
			var v any
			if decodeNumbers(item, &v) != nil {
				ps.add(fmt.Sprintf("%s.%d", loc, i), "a value is text, a number or a boolean")
				continue
			}
			switch v.(type) {
			case string, json.Number, bool:
				f.Values = append(f.Values, v)
			default:
				ps.add(fmt.Sprintf("%s.%d", loc, i), "a value is text, a number or a boolean")
			}
		}
		return f
	case len(trimmed) > 0 && trimmed[0] == '{':
		var obj map[string]json.RawMessage
		if json.Unmarshal(trimmed, &obj) == nil {
			if _, ok := obj["partition"]; ok {
				ps.add(loc, "partitions (partition, num_partitions) are not supported: give an array of values or a pattern")
				return nil
			}
			if _, ok := obj["num_partitions"]; ok {
				ps.add(loc, "partitions (partition, num_partitions) are not supported: give an array of values or a pattern")
				return nil
			}
		}
	}
	ps.add(loc, "include and exclude are an array of values or a pattern")
	return nil
}

// termsFilter is a terms aggregation's include and exclude, compiled: either may be
// nil (everything included, nothing excluded).
type termsFilter struct {
	include, exclude *termSet
}

// termSet is one include or exclude, compiled for a field's type: a pattern (re, with
// the literal every match starts with), or exact values as the field's keys.
type termSet struct {
	re      *regexp.Regexp
	prefix  string
	texts   map[string]struct{}
	numbers map[float64]struct{}
	bools   [2]bool
}

func (t *termSet) hasText(term []byte) bool {
	if t.re != nil {
		return t.re.Match(term)
	}
	_, ok := t.texts[string(term)]
	return ok
}

func (t *termSet) hasNumber(v float64) bool {
	_, ok := t.numbers[positiveZero(v)]
	return ok
}

func (t *termSet) hasBool(b bool) bool { return t.bools[boolIndex(b)] }

func boolIndex(b bool) int {
	if b {
		return 1
	}
	return 0
}

// text reports whether the filter lets the text key term through.
func (f *termsFilter) text(term []byte) bool {
	return (f.include == nil || f.include.hasText(term)) && (f.exclude == nil || !f.exclude.hasText(term))
}

// number reports whether the filter lets the number key v through.
func (f *termsFilter) number(v float64) bool {
	return (f.include == nil || f.include.hasNumber(v)) && (f.exclude == nil || !f.exclude.hasNumber(v))
}

// boolean reports whether the filter lets the bool key b through.
func (f *termsFilter) boolean(b bool) bool {
	return (f.include == nil || f.include.hasBool(b)) && (f.exclude == nil || !f.exclude.hasBool(b))
}

// compileTermsFilter checks include and exclude against a terms aggregation's field
// type t (0: unmapped), at loc (the aggregation's "aggs.<name>.terms"), and compiles
// them; nil when neither is given.
func compileTermsFilter(include, exclude *TermsFilter, t schema.FieldType, loc string, ps *problems) *termsFilter {
	if include == nil && exclude == nil {
		return nil
	}
	f := &termsFilter{}
	if include != nil {
		f.include = compileTermSet(include, t, loc+".include", ps)
	}
	if exclude != nil {
		f.exclude = compileTermSet(exclude, t, loc+".exclude", ps)
	}
	return f
}

func compileTermSet(src *TermsFilter, t schema.FieldType, loc string, ps *problems) *termSet {
	set := &termSet{}
	if src.Values == nil {
		if t == schema.Number || t == schema.Date || t == schema.Bool {
			ps.add(loc, "a pattern chooses text keys, and this field is a %s: give an array of values", t)
			return set
		}
		re, err := src.pattern()
		if err != nil {
			ps.add(loc, "%s", err.Error())
			return set
		}
		set.re, set.prefix = re.re, re.prefix
		return set
	}
	if len(src.Values) > MaxTermsValues {
		ps.add(loc, "include and exclude hold at most %d values", MaxTermsValues)
		return set
	}
	for i, v := range src.Values {
		at := fmt.Sprintf("%s.%d", loc, i)
		switch t {
		case schema.Number:
			n, ok := toFloat(v)
			if !ok {
				ps.add(at, "the field is a number: a value is a finite number")
				continue
			}
			set.addNumber(n)
		case schema.Date:
			n, ok := schema.DateMillis(v)
			if !ok {
				ps.add(at, "the field is a date: a value is Unix milliseconds, an RFC 3339 time or a date (2026-10-02)")
				continue
			}
			set.addNumber(n)
		case schema.Bool:
			b, ok := v.(bool)
			if !ok {
				ps.add(at, "the field is a bool: a value is true or false")
				continue
			}
			set.bools[boolIndex(b)] = true
		default:
			text, ok := analysis.AsText(v)
			if !ok {
				ps.add(at, "a value is text, a number or a boolean")
				continue
			}
			if set.texts == nil {
				set.texts = make(map[string]struct{}, len(src.Values))
			}
			set.texts[analysis.Clean(text)] = struct{}{}
		}
	}
	return set
}

func (t *termSet) addNumber(v float64) {
	if t.numbers == nil {
		t.numbers = map[float64]struct{}{}
	}
	t.numbers[positiveZero(v)] = struct{}{}
}

// termMask is a terms filter over one segment's keys: bit o of bits set when ordinal
// o (a bool field's false 0 or true 1) is let through. A filter with an include also
// lists the ordinals it lets through (listed), with their document frequencies, so a
// dense count reads those alone.
type termMask struct {
	bits   []uint64
	listed bool
	ords   []uint32
	dfs    []uint32
}

func (m *termMask) has(o uint32) bool { return m.bits[o/64]&(1<<(o%64)) != 0 }

// ordMask is the filter over a keyword or list column's ordinals in fs's segment.
// Exact includes are looked up; a pattern include reads the terms from its literal
// prefix on, while they share it; an exclude alone reads every term once (exact
// values: none, they are looked up).
func (f *termsFilter) ordMask(fs *fieldSrc) *termMask {
	n := fs.numTerms()
	m := &termMask{bits: make([]uint64, (n+63)/64), listed: f.include != nil}
	kind := ordTermKind(fs.kind == srcList)
	lookup := fs.kc.Lookup
	if fs.kind == srcList {
		lookup = fs.mc.Lookup
	}
	set := func(o, df uint32) {
		m.bits[o/64] |= 1 << (o % 64)
		if m.listed {
			m.ords, m.dfs = append(m.ords, o), append(m.dfs, df)
		}
	}
	walk := func(prefix string) {
		p := []byte(prefix)
		fs.r.TermsFrom(fs.field, kind, prefix, func(o uint32, term []byte, df uint32) bool {
			if !bytes.HasPrefix(term, p) {
				return false
			}
			if o < n && f.text(term) {
				set(o, df)
			}
			return true
		})
	}
	switch {
	case f.include != nil && f.include.re == nil:
		for term := range f.include.texts {
			if o, ok := lookup(term); ok && o < n && (f.exclude == nil || !f.exclude.hasText([]byte(term))) {
				set(o, fs.r.TermFreq(fs.field, kind, term))
			}
		}
	case f.include != nil:
		walk(f.include.prefix)
	case f.exclude.re == nil:
		for o := range n {
			set(o, 0)
		}
		for term := range f.exclude.texts {
			if o, ok := lookup(term); ok && o < n {
				m.bits[o/64] &^= 1 << (o % 64)
			}
		}
	default:
		walk("")
	}
	return m
}

// boolMask is the filter over a bool field's two keys.
func (f *termsFilter) boolMask() *termMask {
	var w uint64
	if f.boolean(false) {
		w |= 1
	}
	if f.boolean(true) {
		w |= 2
	}
	return &termMask{bits: []uint64{w}}
}

// lazyTermsRatio: a pattern or exclude-only filter over a dictionary at least this many
// times the documents collected is decided ordinal by ordinal as they are collected,
// rather than over the whole dictionary. A variable so tests can force either way.
var lazyTermsRatio uint64 = 16

// lazyTerms reports whether spec's filter over src is decided lazily: a keyword or
// list field's pattern or exclude-only filter, with few documents collected for its
// dictionary. Exact includes are looked up, which costs the values, not the terms.
func (s *segExec) lazyTerms(spec *aggSpec, src *fieldSrc) bool {
	f := spec.terms
	if f == nil || !src.ordinal() || (f.include != nil && f.include.re == nil) {
		return false
	}
	return s.collecting*lazyTermsRatio < uint64(src.numTerms())
}

// lazyFilter decides a terms filter one ordinal at a time, each once.
type lazyFilter struct {
	f    *termsFilter
	src  *fieldSrc
	seen map[uint32]bool
	buf  []byte
}

func (l *lazyFilter) allows(o uint32) bool {
	if ok, done := l.seen[o]; done {
		return ok
	}
	if l.src.kind == srcList {
		l.buf = l.src.mc.AppendTerm(l.buf[:0], o)
	} else {
		l.buf = l.src.kc.AppendTerm(l.buf[:0], o)
	}
	ok := l.f.text(l.buf)
	l.seen[o] = ok
	return ok
}

// termMasks are a segment's terms filters by aggregation, made once and shared by the
// forks that collect parts of the segment apart.
type termMasks struct {
	mu    sync.Mutex
	masks map[*aggSpec]*termMask
}

// termMask is spec's filter over src's keys in this segment, or nil when spec has none
// or src is not counted by ordinal (nor a bool).
func (s *segExec) termMask(spec *aggSpec, src *fieldSrc) *termMask {
	if spec.terms == nil || (!src.ordinal() && src.kind != srcBool) {
		return nil
	}
	ms := s.masks
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if m, ok := ms.masks[spec]; ok {
		return m
	}
	var m *termMask
	if src.kind == srcBool {
		m = spec.terms.boolMask()
	} else {
		m = spec.terms.ordMask(src)
	}
	if ms.masks == nil {
		ms.masks = map[*aggSpec]*termMask{}
	}
	ms.masks[spec] = m
	return m
}

// compiledPattern is a pattern as RE2, anchored at both ends, with the literal every
// match starts with.
type compiledPattern struct {
	re     *regexp.Regexp
	prefix string
}

// patternError is a pattern compilePattern refuses, at a character (0-based).
type patternError struct {
	Pos     int
	Message string
}

func (e *patternError) Error() string {
	if e.Pos < 0 {
		return "the pattern " + e.Message
	}
	return fmt.Sprintf("the pattern at character %d: %s", e.Pos, e.Message)
}

// compilePattern compiles a terms include or exclude pattern: Lucene's regular
// expressions (as Elasticsearch takes them) in this subset, always matching the whole
// key:
//
//   - a character stands for itself; \ makes the next one literal (any but a letter or
//     digit: Lucene's \d, \w and \s are not taken), and "..." quotes a literal run;
//   - . is any character;
//   - [abc], [a-z] and [^a-z] are classes, \ escaping within them;
//   - x* x+ x? x{n} x{n,} x{n,m} repeat (n and m at most 1,000);
//   - x|y is either, and (x) groups.
//
// A pattern is folded as text is normalized, so it reads as an exact value does
// ("Nike.*" is "nike.*"): each literal character casefolded (ß is ss), whitespace runs
// one space and none at either end, and a class holds each member's folded form too
// ([A-C] takes a to c). What cannot fold faithfully is refused: a class member that
// folds to more than one character, and a negated class of characters casefolding
// changes (write them as they fold).
//
// Lucene's other operators (~ complement, & intersection, <n-m> numeric ranges, @ any
// string, # the empty language) are refused, as are ^ and $: a pattern is anchored
// already, so neither is an anchor. Escape any of them for the character itself. A
// pattern holds at most MaxPatternChars characters and compiles to at most
// maxPatternInsts instructions.
func compilePattern(pattern string) (*compiledPattern, error) {
	if pattern == "" {
		return nil, &patternError{Pos: -1, Message: "is empty: give at least one character"}
	}
	if !utf8.ValidString(pattern) {
		return nil, &patternError{Pos: -1, Message: "is not valid UTF-8"}
	}
	if n := utf8.RuneCountInString(pattern); n > MaxPatternChars {
		return nil, &patternError{Pos: -1, Message: fmt.Sprintf("holds %d characters, past the %d a pattern takes", n, MaxPatternChars)}
	}
	p := &patternParser{src: []rune(pattern)}
	if err := p.alternation(); err != nil {
		return nil, err
	}
	if p.pos < len(p.src) {
		return nil, p.fail("a ) that opens no group: escape it (\\)) for the character")
	}
	body := "(?s:" + string(p.out) + ")"
	tree, err := syntax.Parse(body, syntax.Perl)
	if err != nil {
		return nil, &patternError{Pos: -1, Message: "is too complex: its repetitions nest too deep or count too high"}
	}
	tree = tree.Simplify()
	prog, err := syntax.Compile(tree)
	if err != nil || len(prog.Inst) > maxPatternInsts {
		return nil, &patternError{Pos: -1, Message: fmt.Sprintf("is too complex: it compiles past %d instructions (fewer or smaller repetitions)", maxPatternInsts)}
	}
	re, err := regexp.Compile(`\A` + body + `\z`)
	if err != nil {
		return nil, &patternError{Pos: -1, Message: "is too complex: " + err.Error()}
	}
	return &compiledPattern{re: re, prefix: literalPrefix(tree)}, nil
}

// literalPrefix is the literal every match of re starts with ("" when none).
func literalPrefix(re *syntax.Regexp) string {
	literal := func(r *syntax.Regexp) bool { return r.Op == syntax.OpLiteral && r.Flags&syntax.FoldCase == 0 }
	switch {
	case literal(re):
		return string(re.Rune)
	case re.Op == syntax.OpConcat:
		var b strings.Builder
		for _, sub := range re.Sub {
			if !literal(sub) {
				break
			}
			b.WriteString(string(sub.Rune))
		}
		return b.String()
	}
	return ""
}

// patternParser translates a pattern into RE2 syntax, folded, every literal quoted.
type patternParser struct {
	src   []rune
	pos   int
	out   []byte
	depth int
	// space is set while out ends in a literal space a further one folds into, which
	// began at spaceAt; edge is set at the start of a top-level alternative, where
	// whitespace folds away.
	space   bool
	spaceAt int
	edge    bool
}

func (p *patternParser) fail(format string, args ...any) error {
	return &patternError{Pos: p.pos, Message: fmt.Sprintf(format, args...)}
}

func (p *patternParser) more() bool { return p.pos < len(p.src) }

func (p *patternParser) alternation() error {
	for first := true; first || (p.more() && p.src[p.pos] == '|'); first = false {
		if !first {
			p.pos++
			p.out = append(p.out, '|')
		}
		p.space, p.edge = false, p.depth == 0
		if err := p.concatenation(); err != nil {
			return err
		}
		if p.depth == 0 && p.space {
			p.out = p.out[:p.spaceAt]
		}
		p.space = false
	}
	return nil
}

// quantified reports whether a quantifier follows the atom ending before position at.
func (p *patternParser) quantified(at int) bool {
	return at < len(p.src) && strings.ContainsRune("*+?{", p.src[at])
}

func (p *patternParser) concatenation() error {
	for p.more() {
		if c := p.src[p.pos]; c == '|' || c == ')' {
			return nil
		}
		start := len(p.out)
		if err := p.atom(); err != nil {
			return err
		}
		for p.more() && strings.ContainsRune("*+?{", p.src[p.pos]) {
			q, err := p.quantifier()
			if err != nil {
				return err
			}
			atom := string(p.out[start:])
			p.out = append(append(append(p.out[:start], "(?:"...), atom...), ")"+q...)
		}
	}
	return nil
}

func (p *patternParser) atom() error {
	c := p.src[p.pos]
	switch c {
	case '(':
		p.depth++
		if p.depth > maxPatternDepth {
			return p.fail("groups nest past %d deep", maxPatternDepth)
		}
		open := p.pos
		p.pos++
		p.opaque("(?:")
		if err := p.alternation(); err != nil {
			return err
		}
		if !p.more() {
			p.pos = open
			return p.fail("a ( never closed")
		}
		p.pos++
		p.depth--
		p.opaque(")")
	case '[':
		return p.class()
	case '.':
		p.pos++
		p.opaque(".")
	case '"':
		end := p.pos + 1
		for end < len(p.src) && p.src[end] != '"' {
			end++
		}
		if end == len(p.src) {
			return p.fail(`a " never closed`)
		}
		q := p.quantified(end + 1)
		if q {
			p.opaque("")
		}
		for p.pos++; p.pos < end; p.pos++ {
			p.literal(p.src[p.pos], false)
		}
		if q {
			p.space = false
		}
		p.pos++
	case '\\':
		r, err := p.escaped()
		if err != nil {
			return err
		}
		p.literal(r, p.quantified(p.pos))
	case '*', '+', '?', '{':
		return p.fail("%c repeats nothing: escape it (\\%c) for the character", c, c)
	case '}', ']':
		return p.fail("a %c that closes nothing: escape it (\\%c) for the character", c, c)
	case '~', '&', '<', '>', '@', '#':
		return p.fail("%c is a Lucene operator Searchlight does not take (complement, intersection, numeric range, any string, empty language): escape it (\\%c) for the character", c, c)
	case '^', '$':
		return p.fail("a pattern matches the whole key already, so %c is no anchor: escape it (\\%c) for the character", c, c)
	default:
		p.pos++
		p.literal(c, p.quantified(p.pos))
	}
	return nil
}

// opaque appends s, after which no whitespace folds away.
func (p *patternParser) opaque(s string) {
	p.out = append(p.out, s...)
	p.space, p.edge = false, false
}

// escaped reads \x at p.pos: x, which must not be a letter or digit.
func (p *patternParser) escaped() (rune, error) {
	p.pos++
	if !p.more() {
		p.pos--
		return 0, p.fail(`a \ escapes nothing`)
	}
	r := p.src[p.pos]
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return 0, p.fail(`\%c: only punctuation is escaped (Lucene's \d, \w and \s are not taken: write [0-9] and the like)`, r)
	}
	p.pos++
	return r, nil
}

// literal appends r folded: casefolded, a NUL as U+FFFD (as text is cleaned), and
// whitespace as one space, dropped next to another (unless quantified) or at a
// top-level edge.
func (p *patternParser) literal(r rune, quantified bool) {
	switch {
	case analysis.IsSpace(r):
		if !quantified && (p.space || p.edge) {
			return
		}
		at := len(p.out)
		p.out = append(p.out, ' ')
		p.space, p.spaceAt, p.edge = !quantified, at, false
		return
	case r == 0:
		r = utf8.RuneError
	}
	p.opaque(regexp.QuoteMeta(analysis.FoldRune(r)))
}

// spaces are the code points Normalize folds to a space, ascending.
var spaces = sync.OnceValue(func() []rune {
	var out []rune
	for r := range rune(0x3001) {
		if analysis.IsSpace(r) {
			out = append(out, r)
		}
	}
	return out
})

// foldClassRange appends to the class being written what range lo-hi folds to beyond
// itself: each member casefolding changes as it folds, a space for whitespace, U+FFFD
// for a NUL. In a negated class a member that folds is refused, as is, anywhere, one
// that folds to more than one character.
func (p *patternParser) foldClassRange(lo, hi rune, negated bool, at int) error {
	changed := analysis.FoldChanged(lo, hi)
	i, _ := slices.BinarySearch(spaces(), lo)
	j, _ := slices.BinarySearch(spaces(), hi+1)
	ws := spaces()[i:max(i, j)]
	if negated {
		for _, r := range ws {
			if r != ' ' {
				changed = append(changed, r)
			}
		}
		if lo == 0 {
			changed = append(changed, 0)
		}
		if len(changed) > 0 {
			p.pos = at
			return p.fail("a negated class of characters that fold (%q) cannot be matched against casefolded keys: write them as they fold, in lowercase", changed[0])
		}
		return nil
	}
	for _, r := range changed {
		f := []rune(analysis.FoldRune(r))
		if len(f) != 1 {
			p.pos = at
			return p.fail("%q folds to %q, more than one character, which a class cannot hold: write it outside the class", r, string(f))
		}
		p.out = append(p.out, classRune(f[0])...)
	}
	if len(ws) > 0 {
		p.out = append(p.out, classRune(' ')...)
	}
	if lo == 0 {
		p.out = append(p.out, classRune(utf8.RuneError)...)
	}
	return nil
}

func classRune(r rune) string { return `\x{` + strconv.FormatInt(int64(r), 16) + `}` }

// class reads [...] at p.pos.
func (p *patternParser) class() error {
	open := p.pos
	p.pos++
	p.opaque("[")
	negated := p.more() && p.src[p.pos] == '^'
	if negated {
		p.pos++
		p.out = append(p.out, '^')
	}
	items := 0
	next := func() (rune, error) {
		switch c := p.src[p.pos]; c {
		case '\\':
			return p.escaped()
		case '[':
			return 0, p.fail(`a [ within a class: escape it (\[) for the character`)
		default:
			p.pos++
			return c, nil
		}
	}
	for p.more() && p.src[p.pos] != ']' {
		at := p.pos
		lo, err := next()
		if err != nil {
			return err
		}
		hi := lo
		if p.pos+1 < len(p.src) && p.src[p.pos] == '-' && p.src[p.pos+1] != ']' {
			p.pos++
			if hi, err = next(); err != nil {
				return err
			}
			if hi < lo {
				p.pos = at
				return p.fail("the range %c-%c runs backward", lo, hi)
			}
		}
		p.out = append(p.out, classRune(lo)...)
		if hi != lo {
			p.out = append(append(p.out, '-'), classRune(hi)...)
		}
		if err := p.foldClassRange(lo, hi, negated, at); err != nil {
			return err
		}
		items++
	}
	if !p.more() {
		p.pos = open
		return p.fail("a [ never closed")
	}
	if items == 0 {
		p.pos = open
		return p.fail("an empty class: [] matches nothing")
	}
	p.pos++
	p.out = append(p.out, ']')
	return nil
}

// quantifier reads * + ? or {n}, {n,}, {n,m} at p.pos, as RE2 writes it.
func (p *patternParser) quantifier() (string, error) {
	c := p.src[p.pos]
	if c != '{' {
		p.pos++
		return string(c), nil
	}
	open := p.pos
	p.pos++
	number := func() (int, bool) {
		start := p.pos
		for p.more() && p.src[p.pos] >= '0' && p.src[p.pos] <= '9' {
			p.pos++
		}
		if p.pos == start || p.pos-start > 4 {
			return 0, false
		}
		n, _ := strconv.Atoi(string(p.src[start:p.pos]))
		return n, true
	}
	bad := func() (string, error) {
		p.pos = open
		return "", p.fail("a repetition is {n}, {n,} or {n,m}, n and m at most %d and n at most m: escape { (\\{) for the character", maxPatternRepeat)
	}
	n, ok := number()
	if !ok || n > maxPatternRepeat || !p.more() {
		return bad()
	}
	if p.src[p.pos] == '}' {
		p.pos++
		return fmt.Sprintf("{%d}", n), nil
	}
	if p.src[p.pos] != ',' {
		return bad()
	}
	p.pos++
	if p.more() && p.src[p.pos] == '}' {
		p.pos++
		return fmt.Sprintf("{%d,}", n), nil
	}
	m, ok := number()
	if !ok || m > maxPatternRepeat || m < n || !p.more() || p.src[p.pos] != '}' {
		return bad()
	}
	p.pos++
	return fmt.Sprintf("{%d,%d}", n, m), nil
}
