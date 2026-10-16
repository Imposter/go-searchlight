package query

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Date math is Elasticsearch's, in UTC (Searchlight has no time_zone): a string value
// of lt, lte, gt, gte or between on a date field is an anchor, `now` or a date (RFC
// 3339, or YYYY-MM-DD for midnight UTC), followed, after `||` for a date, by any
// number of steps: +N<unit> or -N<unit> (N defaults to 1) and /<unit>, which rounds
// down to the unit's start. The units are y (years), M (months), w (weeks, from
// Monday), d (days), h or H (hours), m (minutes) and s (seconds). Months and years
// add as java.time does, the day clamped to the month's last (2026-01-31||+1M is
// 2026-02-28).
//
// How a bound rounds depends on its op, as Elasticsearch's range query rounds: gte,
// lt and between's lo round down (now/d is the day's first millisecond), and gt, lte
// and between's hi round up (now/d is the day's last millisecond), so lte now/d takes
// in the whole day and gt now/d leaves it out. Rounding up also fills what a date
// with no `||` leaves out: a bare date stands for its last millisecond
// (23:59:59.999), a time without a fraction for its second's last (.999).
//
// now is one instant per request (see [WithNow]): [Resolve] turns every date math
// value of a query into its Unix milliseconds at that instant.

// MaxDateMillis bounds the Unix milliseconds date math resolves to: JavaScript's date
// range, 100,000,000 days either side of 1970.
const MaxDateMillis = 8.64e15

// Units in milliseconds, for the units of a fixed length.
const (
	msSecond = int64(1000)
	msMinute = 60 * msSecond
	msHour   = 60 * msMinute
	msDay    = 24 * msHour
	msWeek   = 7 * msDay
)

// DateMath is a parsed date math expression.
type DateMath struct {
	now    bool
	anchor int64 // Unix ms, when !now
	fill   int64 // ms rounding up adds to an anchor with no math: what its form leaves out
	steps  []dateStep
}

// dateStep is one +N, -N or /unit step.
type dateStep struct {
	round bool
	n     int64 // +N or -N, signed
	unit  byte
}

// Relative reports whether the expression reads now.
func (d *DateMath) Relative() bool { return d.now }

// ParseDateMath parses s as date math, returning why it is not one ("" when it is).
func ParseDateMath(s string) (DateMath, string) {
	var d DateMath
	var math string
	if rest, ok := strings.CutPrefix(s, "now"); ok {
		d.now, math = true, rest
	} else {
		anchor, rest, hasMath := strings.Cut(s, "||")
		ms, fill, ok := anchorMillis(anchor)
		if !ok {
			return DateMath{}, fmt.Sprintf("%q is neither a date (RFC 3339 or YYYY-MM-DD) nor date math (now-7d, 2026-10-01||+1M/d)", s)
		}
		d.anchor = ms
		if !hasMath {
			d.fill = fill
			return d, ""
		}
		math = rest
	}
	for i := 0; i < len(math); {
		var st dateStep
		switch math[i] {
		case '/':
			st.round = true
		case '+':
			st.n = 1
		case '-':
			st.n = -1
		default:
			return DateMath{}, fmt.Sprintf("date math %q: unknown operator %q; use +, - or /", s, math[i])
		}
		i++
		num := int64(1)
		if i < len(math) && isDigit(math[i]) {
			start := i
			for i < len(math) && isDigit(math[i]) {
				i++
			}
			n, err := strconv.ParseInt(math[start:i], 10, 32)
			if err != nil {
				return DateMath{}, fmt.Sprintf("date math %q: %s is too large a number", s, math[start:i])
			}
			num = n
		}
		if i >= len(math) {
			return DateMath{}, fmt.Sprintf("date math %q ends early: a step needs a unit (y, M, w, d, h, H, m or s)", s)
		}
		if st.round && num != 1 {
			return DateMath{}, fmt.Sprintf("date math %q: / rounds to one unit (/d), not %d", s, num)
		}
		st.unit = math[i]
		i++
		if !strings.ContainsRune("yMwdhHms", rune(st.unit)) {
			return DateMath{}, fmt.Sprintf("date math %q: unknown unit %q; use y, M, w, d, h, H, m or s", s, st.unit)
		}
		st.n *= num
		d.steps = append(d.steps, st)
	}
	return d, ""
}

func isDigit(c byte) bool { return '0' <= c && c <= '9' }

// anchorMillis reads a date as documents' date strings are read, with the ms a round
// up adds for what its form leaves out: a bare date its day's, a time with no
// fraction its second's.
func anchorMillis(s string) (ms, fill int64, ok bool) {
	if at, err := time.Parse(time.DateOnly, s); err == nil {
		return at.UnixMilli(), msDay - 1, inDateRange(at.UnixMilli())
	}
	if at, err := time.Parse(time.RFC3339Nano, s); err == nil {
		if !strings.Contains(s, ".") {
			fill = msSecond - 1
		}
		return at.UnixMilli(), fill, inDateRange(at.UnixMilli())
	}
	return 0, 0, false
}

func inDateRange(ms int64) bool { return ms >= -MaxDateMillis && ms <= MaxDateMillis }

// Resolve returns the expression's Unix milliseconds at now (Unix milliseconds),
// rounded up or down (see the package notes); false when it falls outside
// ±[MaxDateMillis].
func (d *DateMath) Resolve(now int64, roundUp bool) (int64, bool) {
	ms := d.anchor
	if d.now {
		ms = now
	}
	if !inDateRange(ms) {
		return 0, false
	}
	if roundUp {
		ms += d.fill
	}
	for _, st := range d.steps {
		var ok bool
		if ms, ok = st.apply(ms, roundUp); !ok {
			return 0, false
		}
	}
	return ms, true
}

// apply applies one step to ms, which is in range; false when the result is not.
func (st dateStep) apply(ms int64, roundUp bool) (int64, bool) {
	if !st.round {
		switch st.unit {
		case 'y':
			return addMonths(ms, 12*st.n)
		case 'M':
			return addMonths(ms, st.n)
		}
		out := ms + st.n*fixedUnit(st.unit)
		return out, inDateRange(out)
	}
	var start, next int64
	var ok bool
	switch st.unit {
	case 'y', 'M':
		t := time.UnixMilli(ms).UTC()
		month, months := t.Month(), int64(1)
		if st.unit == 'y' {
			month, months = time.January, 12
		}
		start = time.Date(t.Year(), month, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
		if next, ok = addMonths(start, months); !ok && roundUp {
			return 0, false
		}
	case 'w':
		days := floorDiv(ms, msDay)
		monday := days - ((days+3)%7+7)%7 // 1970-01-01 was a Thursday
		start = monday * msDay
		next = start + msWeek
	default:
		unit := fixedUnit(st.unit)
		start = floorDiv(ms, unit) * unit
		next = start + unit
	}
	if roundUp {
		return next - 1, inDateRange(next - 1)
	}
	return start, inDateRange(start)
}

// fixedUnit is a unit of a fixed length in milliseconds (d, w, h, H, m, s).
func fixedUnit(unit byte) int64 {
	switch unit {
	case 'w':
		return msWeek
	case 'd':
		return msDay
	case 'h', 'H':
		return msHour
	case 'm':
		return msMinute
	default:
		return msSecond
	}
}

// addMonths adds n months to ms as java.time's plusMonths does: the day of the month
// kept, clamped to the new month's last, and the time of day kept.
func addMonths(ms, n int64) (int64, bool) {
	t := time.UnixMilli(ms).UTC()
	total := int64(t.Year())*12 + int64(t.Month()-1) + n
	if total < -400_000*12 || total > 400_000*12 {
		return 0, false
	}
	year, month := int(floorDiv(total, 12)), time.Month(total-floorDiv(total, 12)*12+1)
	day := min(t.Day(), daysIn(year, month))
	ofDay := ms - floorDiv(ms, msDay)*msDay
	out := time.Date(year, month, day, 0, 0, 0, 0, time.UTC).UnixMilli() + ofDay
	return out, inDateRange(out)
}

func daysIn(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// appendCanonical appends the expression's canonical form: two expressions that
// resolve alike at every instant and in both directions encode alike when they are
// written alike up to their numbers (now-7d and now-07d; +d and +1d).
func (d *DateMath) appendCanonical(buf []byte) []byte {
	if d.now {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
		buf = binary.AppendVarint(buf, d.anchor)
		buf = binary.AppendVarint(buf, d.fill)
	}
	buf = binary.AppendUvarint(buf, uint64(len(d.steps)))
	for _, st := range d.steps {
		unit := st.unit
		if unit == 'H' {
			unit = 'h'
		}
		if st.round {
			buf = append(buf, '/', unit)
			continue
		}
		buf = append(buf, '+', unit)
		buf = binary.AppendVarint(buf, st.n)
	}
	return buf
}

// RoundsUp reports whether a date math bound of op rounds up: gt and lte do (and
// between's hi, which is lte's), gte and lt (and between's lo) round down.
func RoundsUp(op string) bool { return op == OpGt || op == OpLte }

// IsDateMathOp reports whether op takes date math on a date field: lt, lte, gt, gte
// and between.
func IsDateMathOp(op string) bool {
	switch op {
	case OpLt, OpLte, OpGt, OpGte, OpBetween:
		return true
	default:
		return false
	}
}

// hasDateText reports whether a's value holds a string: date math, for an op that
// takes it on a date field.
func hasDateText(a *Arg) bool {
	if a.Kind == ArgString {
		return true
	}
	if a.Kind == ArgList {
		for i := range a.List {
			if a.List[i].Kind == ArgString {
				return true
			}
		}
	}
	return false
}

// IsDateMath reports whether l is a condition with date math: an op that takes it,
// with a string in its value. [Validate] lets one through only on a date field.
func (l *Leaf) IsDateMath() bool {
	if !IsDateMathOp(l.Op) {
		return false
	}
	a := l.arg()
	return hasDateText(&a)
}

// HasDateMath reports whether any condition of n has date math.
func HasDateMath(n Node) bool {
	found := false
	Walk(n, func(_ string, node Node) bool {
		if l, ok := node.(*Leaf); ok && l.IsDateMath() {
			found = true
		}
		return !found
	})
	return found
}

// ResolveDate is a date math bound's Unix milliseconds at now (Unix milliseconds): a
// number as it is, a string resolved, rounding up or down; false for a value that is
// neither, or a date out of range.
func ResolveDate(s *Scalar, now int64, roundUp bool) (float64, bool) {
	switch s.Kind {
	case ArgNumber:
		return s.Number, s.Finite
	case ArgString:
		d, why := ParseDateMath(s.Text)
		if why != "" {
			return 0, false
		}
		ms, ok := d.Resolve(now, roundUp)
		return float64(ms), ok
	default:
		return 0, false
	}
}

// Resolve returns n with every date math value resolved at now: each such condition
// is replaced by one of the same field and op whose value is the Unix milliseconds
// it stands for (now itself is read to the millisecond). A tree with none is returned
// as it is; n is never changed. A bound out of range is a problem at its loc, and its
// condition is left holding a number no float64 holds (1e400), which compares with
// nothing; so is any other bound that cannot be resolved (a tree built by hand).
//
// Every condition of a request resolves at the same now, so that its shards, and its
// nodes, all answer the same query: the coordinator resolves it once, and sends the
// resolved tree, or the instant itself ([WithNow]) for percolation.
func Resolve(n Node, now time.Time) (Node, []Problem) {
	if !HasDateMath(n) {
		return n, nil
	}
	r := resolver{now: now.UnixMilli()}
	return r.node(n, RootLoc), r.problems
}

type resolver struct {
	now      int64
	problems problems
}

func (r *resolver) node(n Node, loc string) Node {
	switch x := n.(type) {
	case *All:
		if x == nil {
			return n
		}
		return &All{Children: r.children(x.Children, join(loc, "all"))}
	case *Any:
		if x == nil {
			return n
		}
		return &Any{Children: r.children(x.Children, join(loc, "any"))}
	case *Not:
		if x == nil {
			return n
		}
		return &Not{Child: r.node(x.Child, join(loc, "not"))}
	case *Leaf:
		if x == nil || !x.IsDateMath() {
			return x
		}
		return r.leaf(x, join(loc, "value"))
	default:
		return n
	}
}

func (r *resolver) children(nodes []Node, loc string) []Node {
	out := make([]Node, len(nodes))
	for i, c := range nodes {
		out[i] = r.node(c, index(loc, i))
	}
	return out
}

// unresolved is a bound that cannot be resolved: a number no float64 holds.
var unresolved = Scalar{Kind: ArgNumber}

func (r *resolver) leaf(l *Leaf, loc string) *Leaf {
	a := l.arg()
	out := &Leaf{Field: l.Field, Op: l.Op}
	if l.Op != OpBetween {
		s := r.bound(&a.Scalar, RoundsUp(l.Op), loc)
		out.Arg = Arg{Kind: ArgNumber, Scalar: s}
		out.Value = numberJSON(&s)
		return out
	}
	if a.Kind != ArgList || len(a.List) != 2 {
		out.Arg = Arg{Kind: ArgNumber, Scalar: unresolved}
		out.Value = numberJSON(&unresolved)
		return out
	}
	lo := r.bound(&a.List[0], false, index(loc, 0))
	hi := r.bound(&a.List[1], true, index(loc, 1))
	out.Arg = Arg{Kind: ArgList, List: []Scalar{lo, hi}}
	out.Value = append(append(append(append([]byte{'['}, numberJSON(&lo)...), ','), numberJSON(&hi)...), ']')
	return out
}

func (r *resolver) bound(s *Scalar, roundUp bool, loc string) Scalar {
	if s.Kind == ArgNumber {
		return *s
	}
	ms, ok := ResolveDate(s, r.now, roundUp)
	if !ok {
		if s.Kind == ArgString {
			if _, why := ParseDateMath(s.Text); why == "" {
				r.problems.addf(loc, "date math %q falls outside the dates a date field holds", s.Text)
			}
		}
		return unresolved
	}
	return Scalar{Kind: ArgNumber, Number: ms, Finite: true}
}

func numberJSON(s *Scalar) []byte {
	if !s.Finite {
		return []byte("1e400")
	}
	return strconv.AppendFloat(nil, s.Number, 'f', -1, 64)
}

type nowKey struct{}

// WithNow returns ctx carrying now as the instant date math resolves at, for the
// request ctx serves: the coordinator sets it once, and every shard (and every node
// it asks) reads it, rather than its own clock.
func WithNow(ctx context.Context, now time.Time) context.Context {
	return context.WithValue(ctx, nowKey{}, now)
}

// NowFrom returns the instant [WithNow] set on ctx, false when none is.
func NowFrom(ctx context.Context) (time.Time, bool) {
	now, ok := ctx.Value(nowKey{}).(time.Time)
	return now, ok
}
