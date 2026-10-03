package query

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Imposter/go-searchlight/internal/analysis"
)

// Problem is one reason a query cannot run: where it is and why. Loc is a dotted path of
// the keys and list indexes leading to it, from [RootLoc]: query.all.2.any.0.value is the
// value of the first child of the any group that is the third child of the root all
// group.
type Problem struct {
	Loc     string `json:"loc"`
	Message string `json:"message"`
}

// String returns "loc: message".
func (p Problem) String() string {
	return p.Loc + ": " + p.Message
}

// maxProblems is the most problems one call reports; a hostile query of thousands of bad
// conditions gets the first ones, not a response as big as itself.
const maxProblems = 50

// problems collects a call's problems, up to maxProblems.
type problems []Problem

func (ps *problems) add(loc, message string) {
	if len(*ps) < maxProblems {
		*ps = append(*ps, Problem{Loc: loc, Message: message})
	}
}

func (ps *problems) addf(loc, format string, args ...any) {
	ps.add(loc, fmt.Sprintf(format, args...))
}

// Messages shared by Parse and Validate.
const (
	msgNode       = "a node is {all: [...]}, {any: [...]}, {not: node} or a condition"
	msgEmptyGroup = "an empty group matches nothing: give it a condition or remove it"
	msgNUL        = "a value cannot hold a NUL character"
)

var (
	msgLongValue = fmt.Sprintf("a value is at most %d characters", MaxValueChars)
	msgLongList  = fmt.Sprintf("a list value holds at most %d entries", MaxListItems)
	msgLongField = fmt.Sprintf("a field name is at most %d characters", MaxFieldChars)
	msgUnknownOp = "use one of " + strings.Join(Ops, ", ")
)

// textProblem is why s cannot be a value's text, or "".
func textProblem(s string) string {
	if strings.IndexByte(s, 0) >= 0 {
		return msgNUL
	}
	if len(s) > MaxValueChars && utf8.RuneCountInString(s) > MaxValueChars {
		return msgLongValue
	}
	return ""
}

// fieldProblem is why name cannot be a condition's field, or "".
func fieldProblem(name string) string {
	switch {
	case name == "":
		return "a condition needs a field"
	case strings.IndexByte(name, 0) >= 0:
		return "a field name cannot hold a NUL character"
	case len(name) > MaxFieldChars && utf8.RuneCountInString(name) > MaxFieldChars:
		return msgLongField
	}
	return ""
}

func join(loc, key string) string {
	return loc + "." + key
}

func index(loc string, i int) string {
	return loc + "." + strconv.Itoa(i)
}

// blank reports whether s is empty or whitespace only, as Python's str.strip() sees it.
func blank(s string) bool {
	return strings.TrimFunc(s, analysis.IsSpace) == ""
}
