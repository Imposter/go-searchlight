package percolate

import "github.com/Imposter/go-searchlight/internal/query"

// EncodeQuery writes a parsed query back as its DSL JSON, exactly as a query segment
// stores it: query.Parse of the result gives the same tree.
func EncodeQuery(n query.Node) ([]byte, error) { return encodeQuery(n) }
