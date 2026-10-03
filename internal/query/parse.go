package query

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	"github.com/Imposter/go-searchlight/internal/analysis"
)

// Parse reads a query from JSON. It checks the tree's shape (every node one of the four,
// with no other key; no empty group but the root {"all": []}), each condition's field,
// op and value as JSON (a value is a scalar, a list of at most MaxListItems scalars, or
// similar's {text, min}; no text longer than MaxValueChars characters or holding a NUL),
// and the bounds MaxDepth, MaxLeaves and MaxNodes. Whether the fields exist and take the
// ops and values given is [Validate]'s, against a mapping.
//
// It returns the tree and no problem, or nil and every problem found (up to a limit), each
// with its loc below [RootLoc]. A key given twice keeps its last value, as Python's json
// reads it.
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
	n := p.node(v, RootLoc)
	if len(p.problems) > 0 {
		return nil, p.problems
	}
	if msg := boundsProblem(n); msg != "" {
		return nil, []Problem{{Loc: RootLoc, Message: msg}}
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

type parser struct {
	problems problems
}

func (p *parser) node(v any, loc string) Node {
	m, ok := v.(map[string]any)
	if !ok {
		p.problems.add(loc, msgNode)
		return nil
	}
	if _, ok := m["all"]; ok {
		return p.group(m, "all", loc)
	}
	if _, ok := m["any"]; ok {
		return p.group(m, "any", loc)
	}
	if child, ok := m["not"]; ok {
		p.extra(m, loc, `a not holds only "not"`, "not")
		return &Not{Child: p.node(child, join(loc, "not"))}
	}
	return p.leaf(m, loc)
}

func (p *parser) group(m map[string]any, key, loc string) Node {
	p.extra(m, loc, fmt.Sprintf("an %s group holds only %q", key, key), key)
	at := join(loc, key)
	list, ok := m[key].([]any)
	if !ok {
		p.problems.addf(at, "%s needs a list of nodes", key)
		return nil
	}
	if len(list) == 0 {
		p.problems.add(at, msgEmptyGroup)
		return nil
	}
	children := make([]Node, len(list))
	for i, child := range list {
		children[i] = p.node(child, index(at, i))
	}
	if key == "all" {
		return &All{Children: children}
	}
	return &Any{Children: children}
}

// extra reports each key of m but allowed, in order.
func (p *parser) extra(m map[string]any, loc, message string, allowed ...string) {
	if len(m) <= len(allowed) {
		return
	}
	for _, key := range slices.Sorted(maps.Keys(m)) {
		if !slices.Contains(allowed, key) {
			p.problems.add(join(loc, key), "unknown key: "+message)
		}
	}
}

func (p *parser) leaf(m map[string]any, loc string) Node {
	p.extra(m, loc, "a condition holds only field, op and value", "field", "op", "value")
	l := &Leaf{}
	switch field := m["field"].(type) {
	case string:
		if msg := fieldProblem(field); msg != "" {
			p.problems.add(join(loc, "field"), msg)
		}
		l.Field = field
	case nil:
		p.problems.add(join(loc, "field"), "a condition needs a field")
	default:
		p.problems.add(join(loc, "field"), "a field name is a string")
	}
	switch op := m["op"].(type) {
	case string:
		if !IsOp(op) {
			p.problems.addf(join(loc, "op"), "unknown op %q; %s", op, msgUnknownOp)
		}
		l.Op = op
	case nil:
		p.problems.add(join(loc, "op"), "a condition needs an op; "+msgUnknownOp)
	default:
		p.problems.add(join(loc, "op"), "an op is a string; "+msgUnknownOp)
	}
	value := m["value"]
	l.Arg = decodeArg(value, l.Op, join(loc, "value"), &p.problems)
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
	a := decodeArg(v, op, loc, &ps)
	return a, ps
}

// decodeArg decodes a value (as encoding/json decodes it with UseNumber) for op,
// reporting what no op takes.
func decodeArg(v any, op, loc string, ps *problems) Arg {
	switch x := v.(type) {
	case nil:
		return Arg{}
	case []any:
		if len(x) > MaxListItems {
			ps.add(loc, msgLongList)
			return Arg{Kind: ArgList}
		}
		list := make([]Scalar, 0, len(x))
		for i, entry := range x {
			s, ok := scalar(entry)
			if !ok {
				ps.add(index(loc, i), "a list value holds text, numbers and booleans only")
				continue
			}
			if msg := s.problem(); msg != "" {
				ps.add(index(loc, i), msg)
			}
			list = append(list, s)
		}
		return Arg{Kind: ArgList, List: list}
	case map[string]any:
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
				ps.addf(loc, "similar takes {min, text}, not %s", strings.Join(unknown, ", "))
			}
		case IsOp(op):
			ps.addf(loc, "%s takes no object value", op)
		}
		object := make(map[string]Scalar, len(x))
		for _, key := range keys {
			s, ok := scalar(x[key])
			if !ok {
				ps.add(join(loc, key), "an object value holds text, numbers and booleans only")
				continue
			}
			if msg := s.problem(); msg != "" {
				ps.add(join(loc, key), msg)
			}
			object[key] = s
		}
		return Arg{Kind: ArgObject, Object: object}
	default:
		s, ok := scalar(v)
		if !ok {
			ps.add(loc, "a value is text, a number, a boolean or a list of them")
			return Arg{}
		}
		if msg := s.problem(); msg != "" {
			ps.add(loc, msg)
		}
		return Arg{Kind: s.Kind, Scalar: s}
	}
}

// scalar decodes one bool, number or string; false for anything else.
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
		if n, ok := analysis.Number(v); ok { // a Go number, in a tree built by hand
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

// encodeValue writes a decoded value back as compact JSON (numbers as written).
func encodeValue(v any) json.RawMessage {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil // unreachable: v came from encoding/json
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
		return fmt.Sprintf("a query nests groups at most %d deep", MaxDepth)
	case s.leaves > MaxLeaves:
		return fmt.Sprintf("a query holds at most %d conditions", MaxLeaves)
	case s.nodes > MaxNodes:
		return fmt.Sprintf("a query holds at most %d nodes (groups and conditions)", MaxNodes)
	}
	return ""
}
