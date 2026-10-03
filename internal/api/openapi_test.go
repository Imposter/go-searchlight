package api_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/Imposter/go-searchlight/internal/api"
)

// openAPI is the part of api/openapi.yaml the test checks.
type openAPI struct {
	Security   []map[string][]string `yaml:"security"`
	Paths      map[string]pathItem   `yaml:"paths"`
	Components struct {
		Parameters map[string]parameter `yaml:"parameters"`
	} `yaml:"components"`
}

type pathItem struct {
	Parameters []parameter           `yaml:"parameters"`
	Get        *operation            `yaml:"get"`
	Put        *operation            `yaml:"put"`
	Post       *operation            `yaml:"post"`
	Delete     *operation            `yaml:"delete"`
	Patch      *operation            `yaml:"patch"`
	Extra      map[string]*operation `yaml:",inline"`
}

type operation struct {
	OperationID string                 `yaml:"operationId"`
	Parameters  []parameter            `yaml:"parameters"`
	Security    *[]map[string][]string `yaml:"security"`
	Responses   map[string]any         `yaml:"responses"`
}

type parameter struct {
	Ref  string `yaml:"$ref"`
	Name string `yaml:"name"`
	In   string `yaml:"in"`
}

func loadOpenAPI(t *testing.T) (*openAPI, []byte) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc openAPI
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("api/openapi.yaml: %v", err)
	}
	return &doc, raw
}

// TestOpenAPIMatchesRoutes checks api/openapi.yaml against the router: the same
// routes and methods, each operation's query parameters exactly the ones its route
// takes, auth on every route but the probes, and every $ref resolving.
func TestOpenAPIMatchesRoutes(t *testing.T) {
	doc, raw := loadOpenAPI(t)
	cfg := testConfig(t)
	srv, err := api.NewServer(nil, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	resolve := func(p parameter) parameter {
		if name, ok := strings.CutPrefix(p.Ref, "#/components/parameters/"); ok {
			r, found := doc.Components.Parameters[name]
			if !found {
				t.Errorf("unresolved parameter %s", p.Ref)
			}
			return r
		}
		return p
	}
	spec := map[string]*operation{}
	pathParams := map[string][]parameter{}
	for path, item := range doc.Paths {
		for method, op := range map[string]*operation{"GET": item.Get, "PUT": item.Put, "POST": item.Post, "DELETE": item.Delete, "PATCH": item.Patch} {
			if op != nil {
				spec[method+" "+path] = op
				pathParams[method+" "+path] = item.Parameters
			}
		}
		for k := range item.Extra {
			if k != "summary" && k != "description" {
				t.Errorf("%s: unsupported key %q", path, k)
			}
		}
	}
	routes := map[string]api.Route{}
	for _, rt := range srv.Routes() {
		routes[rt.Method+" "+rt.Path] = rt
	}
	for key := range routes {
		if spec[key] == nil {
			t.Errorf("route %s is not in api/openapi.yaml", key)
		}
	}
	for key, op := range spec {
		rt, ok := routes[key]
		if !ok {
			t.Errorf("api/openapi.yaml lists %s, which no route serves", key)
			continue
		}
		var query, path []string
		for _, p := range append(slices.Clone(pathParams[key]), op.Parameters...) {
			p = resolve(p)
			switch p.In {
			case "query":
				query = append(query, p.Name)
			case "path":
				path = append(path, p.Name)
			}
		}
		slices.Sort(query)
		want := slices.Sorted(slices.Values(rt.Params))
		if fmt.Sprint(query) != fmt.Sprint(want) {
			t.Errorf("%s: query parameters %v in the spec, %v in the router", key, query, want)
		}
		for _, seg := range strings.Split(rt.Path, "/") {
			if name, ok := strings.CutPrefix(seg, "{"); ok && !slices.Contains(path, strings.TrimSuffix(name, "}")) {
				t.Errorf("%s: path parameter %s is not described", key, seg)
			}
		}
		open := op.Security != nil && len(*op.Security) == 0
		if open == rt.Auth {
			t.Errorf("%s: the spec says auth %v, the router %v", key, !open, rt.Auth)
		}
		if op.OperationID == "" || len(op.Responses) == 0 {
			t.Errorf("%s: no operationId or responses", key)
		}
	}
	// Every local $ref resolves.
	var root yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil {
		t.Fatal(err)
	}
	var walk func(n *yaml.Node)
	walk = func(n *yaml.Node) {
		for i := 0; i+1 < len(n.Content); i++ {
			if n.Kind == yaml.MappingNode && n.Content[i].Value == "$ref" {
				if !refExists(&root, n.Content[i+1].Value) {
					t.Errorf("unresolved $ref %s", n.Content[i+1].Value)
				}
			}
		}
		for _, c := range n.Content {
			walk(c)
		}
	}
	walk(&root)
}

// refExists reports whether a local ref (#/a/b/c) names a node of doc.
func refExists(doc *yaml.Node, ref string) bool {
	path, ok := strings.CutPrefix(ref, "#/")
	if !ok {
		return false
	}
	n := doc
	if n.Kind == yaml.DocumentNode {
		n = n.Content[0]
	}
	for _, key := range strings.Split(path, "/") {
		var next *yaml.Node
		for i := 0; i+1 < len(n.Content); i += 2 {
			if n.Content[i].Value == key {
				next = n.Content[i+1]
				break
			}
		}
		if next == nil {
			return false
		}
		n = next
	}
	return true
}

// TestResponseValidatorCatchesMismatches proves the response check every env runs is
// live: an undocumented status and a body off its schema are both caught.
func TestResponseValidatorCatchesMismatches(t *testing.T) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://x/_cluster/health", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	h := http.Header{"Content-Type": []string{"application/json"}}
	ok := `{"status":"green","nodes":1,"indexes":0,"shards":0,"serving_shards":0,"unassigned":0}`
	if err := validateResponse(req, &http.Response{StatusCode: http.StatusOK, Header: h}, []byte(ok)); err != nil {
		t.Errorf("a valid response: %v", err)
	}
	if err := validateResponse(req, &http.Response{StatusCode: http.StatusTeapot, Header: h}, []byte(ok)); err == nil {
		t.Error("an undocumented status passed")
	}
	if err := validateResponse(req, &http.Response{StatusCode: http.StatusOK, Header: h}, []byte(`{"status":"purple"}`)); err == nil {
		t.Error("a body off its schema passed")
	}
	// A field the schema does not list (an internal ref leaking) fails too.
	if err := validateResponse(req, &http.Response{StatusCode: http.StatusOK, Header: h}, []byte(ok[:len(ok)-1]+`,"ref":{"segment":"x"}}`)); err == nil {
		t.Error("an unlisted field passed")
	}
	// A status the operation does not list fails, even with a default response.
	ph := http.Header{"Content-Type": []string{api.ProblemContentType}}
	problem := `{"type":"urn:searchlight:problem:internal","title":"x","status":502,"code":"internal"}`
	if err := validateResponse(req, &http.Response{StatusCode: http.StatusBadGateway, Header: ph}, []byte(problem)); err == nil {
		t.Error("a status left to default passed")
	}
}
