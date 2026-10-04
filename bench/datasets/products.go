package datasets

import (
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// BaseTime is the start of the generated dates: listings were first seen within the
// two years after it.
var BaseTime = time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC)

// DateSpan is how far after BaseTime the dates run.
const DateSpan = 730 * 24 * time.Hour

// Generator-wide samplers (immutable once built).
var (
	samplersOnce                               sync.Once
	zWords, zBrands, zCats, zTags, zNouns, zAd *Zipf
	zSource                                    *Zipf
)

func samplers() {
	samplersOnce.Do(func() {
		buildVocab()
		zWords = NewZipf(NumWords, 1.05)
		zBrands = NewZipf(NumBrands, 1.0)
		zCats = NewZipf(NumCategories, 0.9)
		zTags = NewZipf(NumTags, 1.1)
		zNouns = NewZipf(len(titleNouns), 0.8)
		zAd = NewZipf(len(titleAdjectives), 0.7)
		zSource = NewZipf(len(Sources), 0.8)
	})
}

// streams separate the generators' random sources, so documents and searches with the
// same ordinal draw unrelated values.
const (
	streamProducts = 0x9e3779b97f4a7c15
	streamSearches = 0xbf58476d1ce4e5b9
)

// rng returns the random source of item i of a stream under seed.
func rng(seed, stream uint64, i int64) *rand.Rand {
	return rand.New(rand.NewPCG(seed^stream, uint64(i)*0x94d049bb133111eb+stream)) //nolint:gosec // reproducible benchmark data, not secrets
}

// ProductID returns document i's id: "p" and nine digits.
func ProductID(i int64) string {
	return string(appendID(nil, 'p', i))
}

func appendID(dst []byte, prefix byte, i int64) []byte {
	dst = append(dst, prefix)
	s := strconv.FormatInt(i, 10)
	for range 9 - len(s) {
		dst = append(dst, '0')
	}
	return append(dst, s...)
}

// AppendProduct appends document i's JSON object to dst.
func AppendProduct(dst []byte, seed uint64, i int64) []byte {
	samplers()
	r := rng(seed, streamProducts, i)

	brand := Brand(zBrands.Sample(r))
	noun := TitleNoun(zNouns.Sample(r))
	source := Sources[zSource.Sample(r)]

	dst = append(dst, `{"title":`...)
	title := make([]byte, 0, 96)
	title = append(title, brand...)
	title = append(title, ' ')
	title = append(title, TitleAdjective(zAd.Sample(r))...)
	title = append(title, ' ')
	title = append(title, noun...)
	title = append(title, ' ', 'A'+byte(r.IntN(26))) //nolint:gosec // r.IntN(26) is 0-25, always a safe byte
	title = strconv.AppendInt(title, int64(10+r.IntN(990)), 10)
	for range r.IntN(3) {
		title = append(title, ' ')
		title = append(title, Word(zWords.Sample(r)+40)...)
	}
	if r.IntN(3) == 0 {
		title = append(title, " - "...)
		c := colors[r.IntN(len(colors))]
		title = append(title, strings.ToUpper(c[:1])...)
		title = append(title, c[1:]...)
	}
	dst = appendString(dst, string(title))

	dst = append(dst, `,"description":`...)
	dst = appendString(dst, description(r))

	dst = append(dst, `,"brand":`...)
	dst = appendString(dst, brand)
	dst = append(dst, `,"category":`...)
	dst = appendString(dst, Category(zCats.Sample(r)))

	if r.IntN(20) != 0 { // 5% of listings have no tags
		dst = append(dst, `,"tags":[`...)
		n := 1 + min(r.IntN(4)+r.IntN(3), 5)
		var picked [6]int
		got := 0
		for tries := 0; got < n && tries < 20; tries++ {
			t := zTags.Sample(r)
			dup := false
			for _, p := range picked[:got] {
				dup = dup || p == t
			}
			if !dup {
				picked[got] = t
				got++
			}
		}
		for k, t := range picked[:got] {
			if k > 0 {
				dst = append(dst, ',')
			}
			dst = appendString(dst, Tag(t))
		}
		dst = append(dst, ']')
	}

	dst = append(dst, `,"source":`...)
	dst = appendString(dst, source)
	cond := Conditions[0]
	switch c := r.IntN(100); {
	case c >= 95:
		cond = Conditions[2]
	case c >= 85:
		cond = Conditions[1]
	}
	dst = append(dst, `,"condition":`...)
	dst = appendString(dst, cond)

	dst = append(dst, `,"sku":"`...)
	dst = append(dst, SKUPrefix(brand)...)
	dst = append(dst, '-')
	h := strconv.FormatUint(uint64(uint32(i)*2654435761), 16) //nolint:gosec // a bijection of the ordinal: unique SKUs
	for range 8 - len(h) {
		dst = append(dst, '0')
	}
	dst = append(dst, strings.ToUpper(h)...)
	dst = append(dst, '"')

	dst = append(dst, `,"url":"https://`...)
	dst = append(dst, source...)
	dst = append(dst, ".example.com/p/"...)
	dst = append(dst, strings.ReplaceAll(noun, " ", "-")...)
	dst = append(dst, '-')
	dst = strconv.AppendInt(dst, i, 10)
	dst = append(dst, '"')

	price := math.Round(math.Min(math.Max(math.Exp(3.6+1.2*r.NormFloat64()), 0.99), 9999.99)*100) / 100
	dst = append(dst, `,"price":`...)
	dst = strconv.AppendFloat(dst, price, 'f', -1, 64)
	onSale := false
	if r.IntN(10) < 7 {
		list := math.Round(price*(1+0.6*r.Float64())*100) / 100
		onSale = list > price*1.1
		dst = append(dst, `,"list_price":`...)
		dst = strconv.AppendFloat(dst, list, 'f', -1, 64)
	}
	if r.IntN(5) != 0 {
		rating := math.Round((1+4*math.Sqrt(r.Float64()))*10) / 10
		dst = append(dst, `,"rating":`...)
		dst = strconv.AppendFloat(dst, rating, 'f', -1, 64)
	}
	dst = append(dst, `,"reviews":`...)
	dst = strconv.AppendInt(dst, int64(math.Floor(math.Exp(r.Float64()*10)-1)), 10)
	stock := 0
	if r.IntN(5) != 0 {
		stock = 1 + r.IntN(500)
	}
	dst = append(dst, `,"stock":`...)
	dst = strconv.AppendInt(dst, int64(stock), 10)
	dst = append(dst, `,"in_stock":`...)
	dst = strconv.AppendBool(dst, stock > 0)
	dst = append(dst, `,"on_sale":`...)
	dst = strconv.AppendBool(dst, onSale)

	first := BaseTime.Add(time.Duration(r.Int64N(int64(DateSpan))))
	updated := first.Add(time.Duration(r.Int64N(int64(BaseTime.Add(DateSpan).Sub(first)) + 1)))
	dst = append(dst, `,"first_seen":"`...)
	dst = first.Truncate(time.Second).AppendFormat(dst, time.RFC3339)
	dst = append(dst, `","updated_at":"`...)
	dst = updated.Truncate(time.Second).AppendFormat(dst, time.RFC3339)
	return append(dst, `"}`...)
}

// description is about 60 words of Zipfian prose in sentences.
func description(r *rand.Rand) string {
	n := 45 + r.IntN(31)
	var b strings.Builder
	b.Grow(n * 8)
	sentence := 0
	for k := range n {
		w := Word(zWords.Sample(r))
		if sentence == 0 {
			if k > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(strings.ToUpper(w[:1]))
			b.WriteString(w[1:])
		} else {
			b.WriteByte(' ')
			b.WriteString(w)
		}
		sentence++
		if sentence >= 8+r.IntN(7) || k == n-1 {
			b.WriteByte('.')
			sentence = 0
		}
	}
	return b.String()
}

// appendString appends s as a JSON string.
func appendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c >= utf8.RuneSelf {
			_, size := utf8.DecodeRuneInString(s[i:])
			dst = append(dst, s[i:i+size]...)
			i += size
			continue
		}
		switch {
		case c == '"' || c == '\\':
			dst = append(dst, '\\', c)
		case c < 0x20:
			const hex = "0123456789abcdef"
			dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
		default:
			dst = append(dst, c)
		}
		i++
	}
	return append(dst, '"')
}

// AppendProductLine appends document i as one NDJSON line of a dataset file:
// {"id": ..., "doc": {...}} and a newline.
func AppendProductLine(dst []byte, seed uint64, i int64) []byte {
	dst = append(dst, `{"id":"`...)
	dst = appendID(dst, 'p', i)
	dst = append(dst, `","doc":`...)
	dst = AppendProduct(dst, seed, i)
	return append(dst, "}\n"...)
}

// SKUPrefix is the SKU prefix of a brand's listings: its first three letters (spaces
// dropped), uppercase.
func SKUPrefix(brand string) string {
	p := strings.ToUpper(strings.ReplaceAll(brand, " ", ""))
	if len(p) > 3 {
		p = p[:3]
	}
	return p
}
