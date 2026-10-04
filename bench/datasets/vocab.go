package datasets

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
)

// Zipf samples ranks 0..n-1 with P(k) proportional to 1/(k+1)^s from a precomputed
// cumulative table, so a sample is one uniform draw and a binary search: cheap enough
// per value, and a pure function of the random source.
type Zipf struct {
	cdf []float64
}

// NewZipf returns a sampler over n ranks with exponent s.
func NewZipf(n int, s float64) *Zipf {
	cdf := make([]float64, n)
	sum := 0.0
	for k := range n {
		sum += 1 / math.Pow(float64(k+1), s)
		cdf[k] = sum
	}
	for k := range cdf {
		cdf[k] /= sum
	}
	return &Zipf{cdf: cdf}
}

// N is how many ranks z samples.
func (z *Zipf) N() int { return len(z.cdf) }

// Sample returns a rank drawn from r.
func (z *Zipf) Sample(r *rand.Rand) int {
	u := r.Float64()
	i := sort.SearchFloat64s(z.cdf, u)
	return min(i, len(z.cdf)-1)
}

// Vocabulary sizes: realistic cardinalities for a scrape-bot catalogue.
const (
	NumWords      = 20_000
	NumBrands     = 2_000
	NumCategories = 300
	NumTags       = 1_000
)

// Head words lead the description vocabulary (the most frequent ranks); synthetic
// words follow.
var headWords = strings.Fields(`
the and with for this your from that our are its all new made high quality design
perfect easy use set includes features great durable black white steel wood premium
compact lightweight portable heavy duty professional home office kitchen outdoor indoor
power battery charger cordless electric manual adjustable size large small medium
model series edition version warranty year years day days free shipping fast secure
fit comfort soft hard strong light dark blue red green grey silver gold brown clear
modern classic vintage style stylish elegant simple smart digital wireless bluetooth
usb led screen display speed control system kit pack piece pieces inch inches mm cm
storage capacity performance efficient energy saving safe safety tested certified
ideal gift family kids adults men women unisex everyday travel work school sport
water resistant waterproof proof cleaning care maintenance replacement parts tool tools
drill saw driver bit bits blade blades motor engine pump filter cable cord plug socket
chair desk table lamp shelf cabinet sofa bed mattress pillow frame mirror rug curtain
jacket shoe shoes shirt pants hoodie hat bag backpack wallet watch ring necklace
phone laptop tablet monitor keyboard mouse headphones speaker camera lens printer router
coffee tea mug bottle plate bowl knife pan pot grill oven blender mixer toaster kettle
garden hose mower trimmer shovel rake planter seed soil fertilizer fence gate deck
`)

// Adjectives and nouns build titles.
var titleAdjectives = strings.Fields(`
cordless compact premium heavy-duty portable wireless smart classic modern ergonomic
adjustable professional lightweight stainless foldable rechargeable digital vintage
deluxe ultra mini pro max slim rugged waterproof insulated electric manual automatic
industrial outdoor indoor commercial universal magnetic brushless variable dual triple
`)

var titleNouns = strings.Fields(`
drill driver saw grinder sander router planer jigsaw impact wrench ratchet socket set
hammer level tape measure flashlight lantern battery charger vacuum blower trimmer mower
chain saw pressure washer compressor generator welder ladder workbench toolbox cabinet
chair desk lamp sofa table bookshelf dresser mattress pillow blanket rug curtain mirror
jacket hoodie sneaker boot backpack wallet watch headphones speaker monitor keyboard mouse
laptop tablet phone case camera tripod printer router kettle blender toaster coffee maker
grill smoker cooler tent sleeping bag kayak bicycle helmet scooter skateboard treadmill
`)

var colors = strings.Fields(`black white grey silver red blue green yellow orange navy beige brown`)

var syllables = strings.Fields(`ka lo mi ne ru ta vi so pe du ga fi ho ze ba ly mor tan rel
vok sim dar len pol kit wen jor fal mup ris bex nod cra ster tri`)

// synthWord spells synthetic word i from syllables: two or three of them, lowercase.
func synthWord(i int) string {
	n := len(syllables)
	var b strings.Builder
	parts := 2 + i%2
	for range parts {
		b.WriteString(syllables[i%n])
		i /= n
	}
	b.WriteString(syllables[(i+parts)%n])
	return b.String()
}

var (
	vocabOnce  sync.Once
	words      []string
	brands     []string
	categories []string
	tags       []string
)

func buildVocab() {
	vocabOnce.Do(func() {
		seen := map[string]bool{}
		words = make([]string, 0, NumWords)
		for _, w := range headWords {
			if !seen[w] {
				seen[w] = true
				words = append(words, w)
			}
		}
		for i := 0; len(words) < NumWords; i++ {
			w := synthWord(i)
			if !seen[w] {
				seen[w] = true
				words = append(words, w)
			}
		}

		head := strings.Fields(`Acme Globex Initech Umbrella Hooli Stark Wayne Tyrell Cyberdyne Soylent
Dewalt Makita Milwaukee Ryobi Bosch Craftsman Kobalt Ikea Herman Steelcase Sony Samsung Logitech
Anker Philips Braun Dyson Weber Coleman Yeti Patagonia Nike Adidas Puma Timex Casio Canon Nikon`)
		brands = make([]string, 0, NumBrands)
		bseen := map[string]bool{}
		for _, b := range head {
			bseen[strings.ToLower(b)] = true
			brands = append(brands, b)
		}
		for i := 0; len(brands) < NumBrands; i++ {
			w := synthWord(i*7 + 3)
			name := strings.ToUpper(w[:1]) + w[1:]
			if i%5 == 0 {
				name += " Co"
			}
			if !bseen[strings.ToLower(name)] {
				bseen[strings.ToLower(name)] = true
				brands = append(brands, name)
			}
		}

		tops := strings.Fields(`Tools Home Garden Kitchen Furniture Electronics Computers Phones Audio
Cameras Apparel Footwear Sports Outdoors Toys Automotive Office Health Beauty Pets`)
		categories = make([]string, 0, NumCategories)
		for i := 0; len(categories) < NumCategories; i++ {
			top := tops[i%len(tops)]
			sub := words[(i/len(tops))*13%400+50]
			categories = append(categories, fmt.Sprintf("%s > %s%s", top, strings.ToUpper(sub[:1]), sub[1:]))
		}

		headTags := strings.Fields(`sale clearance new bestseller cordless refurbished limited
online-only eco energy-star bundle gift free-shipping open-box pro outdoor kids wireless
smart-home rechargeable`)
		tags = make([]string, 0, NumTags)
		tseen := map[string]bool{}
		for _, t := range headTags {
			tseen[t] = true
			tags = append(tags, t)
		}
		for i := 0; len(tags) < NumTags; i++ {
			t := "t-" + words[i+100]
			if !tseen[t] {
				tseen[t] = true
				tags = append(tags, t)
			}
		}
	})
}

// Word returns description word rank k (0 is the most frequent).
func Word(k int) string { buildVocab(); return words[k%len(words)] }

// Brand returns brand rank k, as written (mixed case).
func Brand(k int) string { buildVocab(); return brands[k%len(brands)] }

// Category returns category rank k.
func Category(k int) string { buildVocab(); return categories[k%len(categories)] }

// Tag returns tag rank k.
func Tag(k int) string { buildVocab(); return tags[k%len(tags)] }

// TitleNoun returns title noun k.
func TitleNoun(k int) string { return titleNouns[k%len(titleNouns)] }

// TitleAdjective returns title adjective k.
func TitleAdjective(k int) string { return titleAdjectives[k%len(titleAdjectives)] }

// Sources are the scrape sources a listing came from.
var Sources = strings.Fields(`tesla-ca nocta herman-miller royal-distributing ovo staples-ca
princess-auto bestbuy-ca walmart-ca canadian-tire home-depot-ca lowes-ca costco-ca amazon-ca
ikea-ca wayfair-ca newegg-ca memory-express sport-chek marks`)

// Conditions are a listing's condition, with their weights.
var Conditions = []string{"new", "refurbished", "used"}
