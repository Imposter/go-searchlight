package query

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/Imposter/go-searchlight/internal/analysis"
)

// Parse reads a query from JSON. It checks the tree's shape (every node one of the five,
// with no other key; no empty group but the root {"all": []}, and a nested node's
// query), each condition's field, op and value as JSON (a value is a scalar, a list of
// at most MaxListItems scalars, or similar's {text, min}; no text longer than
// MaxValueChars characters or holding a NUL), a nested node's inner_hits, and the
// bounds MaxDepth, MaxLeaves and MaxNodes. Whether the fields exist and take the ops
// and values given is [Validate]'s, against a mapping.
//
// It returns the tree and no problem, or nil and every problem found (up to a limit), each
// with its loc below [RootLoc]. A key given twice keeps its last value, as Python's json
// reads it. The bounds are checked while descending: the first node past one stops the
// parse, and that bound is the one problem returned, so a query far past them costs
// little more than decoding it.
func Parse(raw []byte) (Node, []Problem) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, []Problem{{Loc: RootLoc, Message: "the query is not valid JSON: " + err.Error()}}
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, []Problem{{Loc: RootLoc, Message: "the query holds more than one JSON value"}}
	}
	if isEverything(v) {
		return &All{}, nil
	}
	var p parser
	n := p.node(v)
	if p.exceeded != "" {
		return nil, []Problem{{Loc: RootLoc, Message: p.exceeded}}
	}
	if len(p.problems) > 0 {
		return nil, p.problems
	}
	return n, nil
}

// isEverything reports whether v is the root {"all": []}: every document.
func isEverything(v any) bool {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return false
	}
	list, ok := m["all"].([]any)
	return ok && len(list) == 0
}

// parser reads a decoded query. path holds the keys and list indexes from the root to
// the node being read, from which a problem's loc is built only when one is recorded.
// depth, leaves and nodes are counted while descending (an entry that is not a node
// counts as one, so a group of scalars is not walked past MaxNodes); exceeded is the
// first bound passed, which stops the parse.
type parser struct {
	problems problems
	path     []string
	depth    int
	leaves   int
	nodes    int
	exceeded string
}

// at is the loc of the node being read, or of sub below it.
func (p *parser) at(sub ...string) string {
	return strings.Join(slices.Concat([]string{RootLoc}, p.path, sub), ".")
}

func (p *parser) push(key string) { p.path = append(p.path, key) }

func (p *parser) pop() { p.path = p.path[:len(p.path)-1] }

// enter counts one more node, a level deeper when nested (an all or any group), and
// reports whether the parse goes on; leave closes a group.
func (p *parser) enter(nested bool) bool {
	p.nodes++
	if nested {
		p.depth++
	}
	switch {
	case p.depth > MaxDepth:
		p.exceeded = msgDepth
	case p.nodes > MaxNodes:
		p.exceeded = msgNodes
	}
	return p.exceeded == ""
}

func (p *parser) leave() { p.depth-- }

func (p *parser) node(v any) Node {
	m, ok := v.(map[string]any)
	if !ok {
		if p.enter(false) {
			p.problems.add(p.at(), msgNode)
		}
		return nil
	}
	if _, ok := m["all"]; ok {
		return p.group(m, "all")
	}
	if _, ok := m["any"]; ok {
		return p.group(m, "any")
	}
	if child, ok := m["not"]; ok {
		return p.not(m, child)
	}
	if body, ok := m["nested"]; ok {
		return p.nested(m, body)
	}
	if !p.enter(false) {
		return nil
	}
	p.leaves++
	if p.leaves > MaxLeaves {
		p.exceeded = msgLeaves
		return nil
	}
	return p.leaf(m)
}

func (p *parser) not(m map[string]any, child any) Node {
	if !p.enter(false) {
		return nil
	}
	p.extra(m, `a not holds only "not"`, "not")
	p.push("not")
	defer p.pop()
	inner := p.node(child)
	if p.exceeded != "" {
		return nil
	}
	return &Not{Child: inner}
}

// nested reads {"nested": {"path": field, "query": node, "inner_hits": {...}}}: a
// level, as a group is. A missing query, or {"all": []}, holds of every object.
func (p *parser) nested(m map[string]any, body any) Node {
	if !p.enter(true) {
		return nil
	}
	defer p.leave()
	p.extra(m, `a nested node holds only "nested"`, "nested")
	p.push("nested")
	defer p.pop()
	b, ok := body.(map[string]any)
	if !ok {
		p.problems.add(p.at(), `nested is {"path": field, "query": node, "inner_hits": {...}}`)
		return nil
	}
	p.extra(b, "nested takes path, query and inner_hits", "path", "query", "inner_hits")
	n := &Nested{Query: &All{}}
	switch path := b["path"].(type) {
	case string:
		if msg := fieldProblem(path); msg != "" {
			p.problems.add(p.at("path"), msg)
		}
		n.Path = path
	case nil:
		p.problems.add(p.at("path"), "a nested node needs the path of its nested field")
	default:
		p.problems.add(p.at("path"), "a nested path is a field name")
	}
	if q, ok := b["query"]; ok && !isEverything(q) {
		p.push("query")
		n.Query = p.node(q)
		p.pop()
		if p.exceeded != "" {
			return nil
		}
	}
	if raw, ok := b["inner_hits"]; ok {
		p.push("inner_hits")
		n.InnerHits = p.innerHits(raw)
		p.pop()
	}
	return n
}

// innerHits reads inner_hits: {"name": text, "size": count, "fields": [sub-field,
// ...]}, every key optional.
func (p *parser) innerHits(raw any) *InnerHits {
	m, ok := raw.(map[string]any)
	if !ok {
		p.problems.add(p.at(), `inner_hits is {"name": text, "size": count, "fields": [sub-field, ...]}`)
		return nil
	}
	p.extra(m, "inner_hits takes name, size and fields", "name", "size", "fields")
	h := &InnerHits{Size: DefaultInnerHitsSize}
	if v, ok := m["name"]; ok {
		name, isText := v.(string)
		if !isText || name == "" {
			p.problems.add(p.at("name"), "an inner_hits name is a non-empty text")
		} else if msg := textProblem(name); msg != "" {
			p.problems.add(p.at("name"), msg)
		}
		h.Name = name
	}
	if v, ok := m["size"]; ok {
		n, isNumber := v.(json.Number)
		size, err := n.Int64()
		if !isNumber || err != nil || size < 0 || size > MaxInnerHitsSize {
			p.problems.addf(p.at("size"), "an inner_hits size is a whole number from 0 to %d", MaxInnerHitsSize)
		} else {
			h.Size = int(size)
		}
	}
	if v, ok := m["fields"]; ok {
		list, isList := v.([]any)
		if !isList || len(list) > MaxInnerHitsFields {
			p.problems.addf(p.at("fields"), "inner_hits fields is a list of at most %d sub-field names", MaxInnerHitsFields)
			return h
		}
		for i, f := range list {
			name, isText := f.(string)
			if !isText {
				p.problems.add(p.at("fields", strconv.Itoa(i)), "a field name is a string")
				continue
			}
			if msg := fieldProblem(name); msg != "" {
				p.problems.add(p.at("fields", strconv.Itoa(i)), msg)
			}
			h.Fields = append(h.Fields, name)
		}
	}
	return h
}

func (p *parser) group(m map[string]any, key string) Node {
	if !p.enter(true) {
		return nil
	}
	defer p.leave()
	p.extra(m, fmt.Sprintf("an %s group holds only %q", key, key), key)
	p.push(key)
	defer p.pop()
	list, ok := m[key].([]any)
	if !ok {
		p.problems.addf(p.at(), "%s needs a list of nodes", key)
		return nil
	}
	if len(list) == 0 {
		p.problems.add(p.at(), msgEmptyGroup)
		return nil
	}
	children := make([]Node, 0, min(len(list), MaxNodes))
	for i, child := range list {
		p.push(strconv.Itoa(i))
		children = append(children, p.node(child))
		p.pop()
		if p.exceeded != "" {
			return nil
		}
	}
	if key == "all" {
		return &All{Children: children}
	}
	return &Any{Children: children}
}

// extra reports each key of m but allowed, in order.
func (p *parser) extra(m map[string]any, message string, allowed ...string) {
	if len(m) <= len(allowed) {
		return
	}
	for _, key := range slices.Sorted(maps.Keys(m)) {
		if !slices.Contains(allowed, key) {
			p.problems.add(p.at(key), "unknown key: "+message)
		}
	}
}

func (p *parser) leaf(m map[string]any) Node {
	p.extra(m, "a condition holds only field, op and value", "field", "op", "value")
	l := &Leaf{}
	switch field := m["field"].(type) {
	case string:
		if msg := fieldProblem(field); msg != "" {
			p.problems.add(p.at("field"), msg)
		}
		l.Field = field
	case nil:
		p.problems.add(p.at("field"), "a condition needs a field")
	default:
		p.problems.add(p.at("field"), "a field name is a string")
	}
	switch op := m["op"].(type) {
	case string:
		if !IsOp(op) {
			p.problems.addf(p.at("op"), "unknown op %q; %s", op, msgUnknownOp)
		}
		l.Op = op
	case nil:
		p.problems.add(p.at("op"), "a condition needs an op; "+msgUnknownOp)
	default:
		p.problems.add(p.at("op"), "an op is a string; "+msgUnknownOp)
	}
	value := m["value"]
	l.Arg = decodeArg(value, l.Op, func(sub ...string) string { return p.at(slices.Concat([]string{"value"}, sub)...) }, &p.problems)
	if value != nil {
		l.Value = encodeValue(value)
	}
	return l
}

// decodeRaw decodes a Leaf's raw Value, as Parse would have.
func decodeRaw(raw json.RawMessage, op, loc string) (Arg, []Problem) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return Arg{}, []Problem{{Loc: loc, Message: "the value is not valid JSON: " + err.Error()}}
	}
	var ps problems
	a := decodeArg(v, op, func(sub ...string) string { return strings.Join(slices.Concat([]string{loc}, sub), ".") }, &ps)
	return a, ps
}

// decodeArg decodes a value (as encoding/json decodes it with UseNumber) for op,
// reporting what no op takes; at builds the loc of the value, or of sub below it.
func decodeArg(v any, op string, at func(sub ...string) string, ps *problems) Arg {
	switch x := v.(type) {
	case nil:
		return Arg{}
	case []any:
		if len(x) > MaxListItems {
			ps.add(at(), msgLongList)
			return Arg{Kind: ArgList}
		}
		list := make([]Scalar, 0, len(x))
		for i, entry := range x {
			s, ok := scalar(entry)
			if !ok {
				ps.add(at(strconv.Itoa(i)), "a list value holds text, numbers and booleans only")
				continue
			}
			if msg := s.problem(); msg != "" {
				ps.add(at(strconv.Itoa(i)), msg)
			}
			list = append(list, s)
		}
		return Arg{Kind: ArgList, List: list}
	case map[string]any:
		if op == OpBox {
			return decodeBox(x, at, ps)
		}
		keys := slices.Sorted(maps.Keys(x))
		switch {
		case op == OpSimilar:
			var unknown []string
			for _, key := range keys {
				if key != "text" && key != "min" {
					unknown = append(unknown, fmt.Sprintf("%q", key))
				}
			}
			if len(unknown) > 0 {
				ps.addf(at(), "similar takes {min, text}, not %s", strings.Join(unknown, ", "))
			}
		case IsOp(op):
			ps.addf(at(), "%s takes no object value", op)
		}
		object := make(map[string]Scalar, len(x))
		for _, key := range keys {
			s, ok := scalar(x[key])
			if !ok {
				ps.add(at(key), "an object value holds text, numbers and booleans only")
				continue
			}
			if msg := s.problem(); msg != "" {
				ps.add(at(key), msg)
			}
			object[key] = s
		}
		return Arg{Kind: ArgObject, Object: object}
	default:
		s, ok := scalar(v)
		if !ok {
			ps.add(at(), "a value is text, a number, a boolean or a list of them")
			return Arg{}
		}
		if msg := s.problem(); msg != "" {
			ps.add(at(), msg)
		}
		return Arg{Kind: s.Kind, Scalar: s}
	}
}

// decodeBox decodes box's value: {"x": axis, "y": axis}, either one optional, each
// axis an object of at most one lower bound (gt or gte) and one upper (lt or lte),
// each a number or a string.
func decodeBox(m map[string]any, at func(sub ...string) string, ps *problems) Arg {
	box := &Box{}
	for _, key := range slices.Sorted(maps.Keys(m)) {
		var iv *Interval
		switch key {
		case BoxX:
			iv = &box.X
		case BoxY:
			iv = &box.Y
		default:
			ps.addf(at(key), "unknown key %q: box takes {x, y}, each {gt|gte, lt|lte}", key)
			continue
		}
		axis, ok := m[key].(map[string]any)
		if !ok {
			ps.addf(at(key), "box's %s is an object of bounds: {gt|gte, lt|lte}", key)
			continue
		}
		if len(axis) == 0 {
			ps.addf(at(key), "box's %s needs a bound (gt, gte, lt or lte): leave %s out for no bound", key, key)
			continue
		}
		for _, side := range slices.Sorted(maps.Keys(axis)) {
			b, upper := &iv.Lo, false
			switch side {
			case BoxGt, BoxGte:
			case BoxLt, BoxLte:
				b, upper = &iv.Hi, true
			default:
				ps.addf(at(key, side), "unknown bound %q: an axis takes gt or gte, and lt or lte", side)
				continue
			}
			if b.Set {
				which := "gt or gte"
				if upper {
					which = "lt or lte"
				}
				ps.addf(at(key, side), "box's %s takes %s, not both", key, which)
				continue
			}
			s, ok := scalar(axis[side])
			if !ok || s.Kind == ArgBool {
				ps.add(at(key, side), "a box bound is a number, or on a date axis a date or date math")
				continue
			}
			if msg := s.problem(); msg != "" {
				ps.add(at(key, side), msg)
			}
			*b = Bound{Set: true, Inclusive: side == BoxGte || side == BoxLte, Value: s}
		}
	}
	return Arg{Kind: ArgBox, Box: box}
}

// scalar decodes one bool, number or string; false for anything else. A Go number, as
// a tree built by hand holds, is a number too.
func scalar(v any) (Scalar, bool) {
	switch x := v.(type) {
	case bool:
		return Scalar{Kind: ArgBool, Bool: x}, true
	case json.Number:
		n, finite := analysis.Number(x)
		return Scalar{Kind: ArgNumber, Number: n, Finite: finite}, true
	case string:
		return Scalar{Kind: ArgString, Text: x, Norm: analysis.Clean(analysis.Normalize(x))}, true
	default:
		if n, ok := analysis.Number(v); ok {
			return Scalar{Kind: ArgNumber, Number: n, Finite: true}, true
		}
		return Scalar{}, false
	}
}

// problem is why s's text cannot be in a value, or "".
func (s *Scalar) problem() string {
	if s.Kind != ArgString {
		return ""
	}
	return textProblem(s.Text)
}

// encodeValue writes a decoded value back as compact JSON (numbers as written). v came
// from encoding/json, so encoding it cannot fail; nil stands for the failure anyway.
func encodeValue(v any) json.RawMessage {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n"))
}

// size is how deep a tree's all/any groups nest, and how many leaves and nodes it holds.
type size struct {
	depth, leaves, nodes int
}

func sizeOf(n Node) size {
	switch x := n.(type) {
	case *Leaf:
		return size{leaves: 1, nodes: 1}
	case *Not:
		if x == nil {
			return size{}
		}
		inner := sizeOf(x.Child)
		inner.nodes++
		return inner
	case *Nested:
		if x == nil {
			return size{}
		}
		var inner size
		if !isEveryObject(x.Query) {
			inner = sizeOf(x.Query)
		}
		inner.nodes++
		inner.depth++
		return inner
	case *All:
		if x == nil {
			return size{}
		}
		return groupSize(x.Children)
	case *Any:
		if x == nil {
			return size{}
		}
		return groupSize(x.Children)
	default:
		return size{}
	}
}

func groupSize(children []Node) size {
	s := size{nodes: 1}
	deepest := 0
	for _, child := range children {
		c := sizeOf(child)
		deepest = max(deepest, c.depth)
		s.leaves += c.leaves
		s.nodes += c.nodes
	}
	s.depth = 1 + deepest
	return s
}

// boundsProblem is why n is too big a query (MaxDepth, MaxLeaves, MaxNodes), or "".
func boundsProblem(n Node) string {
	s := sizeOf(n)
	switch {
	case s.depth > MaxDepth:
		return msgDepth
	case s.leaves > MaxLeaves:
		return msgLeaves
	case s.nodes > MaxNodes:
		return msgNodes
	}
	return ""
}
