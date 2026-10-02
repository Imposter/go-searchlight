package analysis

import (
	"encoding/json"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// Number returns v as a finite float64, as scrape-bot's number reads a JSON value.
//
// Accepted are json.Number (as encoding/json's UseNumber decodes), Go's integer and
// float types, and *big.Int. A bool is never a number, nor is a string, even a numeric
// one. The result is false for NaN, for infinities (a JSON literal like 1e400 parses to
// one, as Python's json does) and for an integer too large for a float64 after rounding
// (10**400), which Python's float() refuses with OverflowError.
func Number(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		return literalNumber(string(x))
	case float64:
		return x, isFinite(x)
	case float32:
		return float64(x), isFinite(float64(x))
	case int:
		return float64(x), true
	case int8:
		return float64(x), true
	case int16:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case *big.Int:
		if x == nil {
			return 0, false
		}
		return bigNumber(x)
	default:
		return 0, false
	}
}

func isFinite(f float64) bool {
	return !math.IsNaN(f) && !math.IsInf(f, 0)
}

// literalNumber parses a JSON number literal the way Python's json module and then
// number() read it: an integer literal is an exact integer, rounded half-to-even to a
// float (refused when that overflows); any other literal is a correctly rounded float.
func literalNumber(lit string) (float64, bool) {
	isInt, ok := jsonNumber(lit)
	if !ok {
		return 0, false
	}
	if isInt {
		if i, err := strconv.ParseInt(lit, 10, 64); err == nil {
			return float64(i), true
		}
		// JSON forbids leading zeros, so more than 309 digits is at least 1e309, past any
		// float64; refusing it here also keeps big.Int's superlinear parse off huge input.
		if len(strings.TrimPrefix(lit, "-")) > maxFloatDigits {
			return 0, false
		}
		n, ok := new(big.Int).SetString(lit, 10)
		if !ok {
			return 0, false
		}
		return bigNumber(n)
	}
	f, _ := strconv.ParseFloat(lit, 64) // a range error is ±Inf or ±0, as Python reads it
	return f, isFinite(f)
}

// maxFloatDigits is the most decimal digits an integer below math.MaxFloat64 can have.
const maxFloatDigits = 309

func bigNumber(n *big.Int) (float64, bool) {
	if n.BitLen() > 1024 { // at least 2**1024, past any float64
		return 0, false
	}
	f, _ := new(big.Float).SetInt(n).Float64() // exact, then rounded half-to-even
	return f, isFinite(f)
}

// jsonNumber reports whether lit is a JSON number literal, and whether it is an integer
// one (no fraction, no exponent), which Python's json decodes as an int.
func jsonNumber(lit string) (isInt, ok bool) {
	i := 0
	if i < len(lit) && lit[i] == '-' {
		i++
	}
	switch {
	case i < len(lit) && lit[i] == '0':
		i++
	case i < len(lit) && lit[i] >= '1' && lit[i] <= '9':
		for i < len(lit) && isDigit(lit[i]) {
			i++
		}
	default:
		return false, false
	}
	isInt = true
	if i < len(lit) && lit[i] == '.' {
		isInt = false
		i++
		start := i
		for i < len(lit) && isDigit(lit[i]) {
			i++
		}
		if i == start {
			return false, false
		}
	}
	if i < len(lit) && (lit[i] == 'e' || lit[i] == 'E') {
		isInt = false
		i++
		if i < len(lit) && (lit[i] == '+' || lit[i] == '-') {
			i++
		}
		start := i
		for i < len(lit) && isDigit(lit[i]) {
			i++
		}
		if i == start {
			return false, false
		}
	}
	return isInt, i == len(lit)
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// AsText returns v as comparable text (scrape-bot's as_text): a string normalized, a
// bool as "true" or "false", a number as Python's str() spells it (12, 12.5, 1e+16,
// inf), normalized. Anything else (null, an array, an object) has no text.
//
// A json.Number keeps its literal's kind, as Python's json does: "10" is the int 10 and
// reads "10"; "1E1" and "10.0" are the float 10.0 and read "10.0"; "1e400" is infinity.
func AsText(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return Normalize(x), true
	case bool:
		if x {
			return "true", true
		}
		return "false", true
	case json.Number:
		return literalText(string(x))
	case float64:
		return FloatText(x), true
	case float32:
		return FloatText(float64(x)), true
	case int:
		return strconv.FormatInt(int64(x), 10), true
	case int8:
		return strconv.FormatInt(int64(x), 10), true
	case int16:
		return strconv.FormatInt(int64(x), 10), true
	case int32:
		return strconv.FormatInt(int64(x), 10), true
	case int64:
		return strconv.FormatInt(x, 10), true
	case uint:
		return strconv.FormatUint(uint64(x), 10), true
	case uint8:
		return strconv.FormatUint(uint64(x), 10), true
	case uint16:
		return strconv.FormatUint(uint64(x), 10), true
	case uint32:
		return strconv.FormatUint(uint64(x), 10), true
	case uint64:
		return strconv.FormatUint(x, 10), true
	case *big.Int:
		if x == nil {
			return "", false
		}
		return x.String(), true
	default:
		return "", false
	}
}

func literalText(lit string) (string, bool) {
	isInt, ok := jsonNumber(lit)
	if !ok {
		return "", false
	}
	if isInt {
		// JSON forbids leading zeros and '+', so the literal is Python's str(int) but for
		// a negative zero, which an int has not.
		if lit == "-0" {
			return "0", true
		}
		return lit, true
	}
	f, _ := strconv.ParseFloat(lit, 64)
	return FloatText(f), true
}

// FloatText returns f as Python's repr (and str) spells a float: the shortest digits
// that round-trip, positional when the decimal exponent is from -4 to 15 with at least
// one fractional digit (1.0, 0.0001, 1234567890123456.0), scientific otherwise with a
// two-digit signed exponent (1e+16, 1e-05, 1.5e+300); and nan, inf, -inf.
func FloatText(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	sign := ""
	if math.Signbit(f) {
		sign = "-"
		f = -f
	}
	if f == 0 {
		return sign + "0.0"
	}
	// Shortest round-trip digits: d.ddddde±XX.
	sci := strconv.FormatFloat(f, 'e', -1, 64)
	mantissa, exponent, _ := strings.Cut(sci, "e")
	digits := strings.Replace(mantissa, ".", "", 1)
	exp, _ := strconv.Atoi(exponent)
	point := exp + 1 // digits before the decimal point
	var b strings.Builder
	b.WriteString(sign)
	if point > -4 && point <= 16 {
		switch {
		case point <= 0:
			b.WriteString("0.")
			b.WriteString(strings.Repeat("0", -point))
			b.WriteString(digits)
		case point >= len(digits):
			b.WriteString(digits)
			b.WriteString(strings.Repeat("0", point-len(digits)))
			b.WriteString(".0")
		default:
			b.WriteString(digits[:point])
			b.WriteByte('.')
			b.WriteString(digits[point:])
		}
		return b.String()
	}
	b.WriteByte(digits[0])
	if len(digits) > 1 {
		b.WriteByte('.')
		b.WriteString(digits[1:])
	}
	b.WriteByte('e')
	if exp < 0 {
		b.WriteByte('-')
		exp = -exp
	} else {
		b.WriteByte('+')
	}
	if exp < 10 {
		b.WriteByte('0')
	}
	b.WriteString(strconv.Itoa(exp))
	return b.String()
}
