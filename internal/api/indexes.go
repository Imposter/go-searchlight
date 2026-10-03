package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// MaxIndexNameBytes is the longest index name.
const MaxIndexNameBytes = 255

// ValidIndexName refuses a name an index may not have: it is 1 to 255 bytes of
// lowercase letters, digits, '-', '_' and '.', starting with a letter or digit (so it
// never collides with an endpoint such as _bulk, and is safe in a path and a file
// name).
func ValidIndexName(name string) *Error {
	bad := func(msg string) *Error { return InvalidAt("params.index", "%s", msg) }
	switch {
	case name == "":
		return bad("an index name cannot be empty")
	case len(name) > MaxIndexNameBytes:
		return bad("an index name is at most 255 bytes")
	case !isAlnum(name[0]):
		return bad("an index name starts with a lowercase letter or a digit")
	}
	for i := 0; i < len(name); i++ {
		if c := name[i]; !isAlnum(c) && c != '-' && c != '_' && c != '.' {
			return bad("an index name holds only lowercase letters, digits, '-', '_' and '.'")
		}
	}
	return nil
}

func isAlnum(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') }

// createBody is PUT /indexes/{index}'s body.
type createBody struct {
	Mapping  json.RawMessage `json:"mapping"`
	Settings json.RawMessage `json:"settings"`
}

func (s *Server) createIndex(w http.ResponseWriter, r *http.Request, _ params) error {
	name := r.PathValue("index")
	if e := ValidIndexName(name); e != nil {
		return e
	}
	body, err := readBody(r)
	if err != nil {
		return err
	}
	var req createBody
	if err := decodeJSONObject(body, &req, true); err != nil {
		return err
	}
	spec := IndexSpec{Mapping: &schema.Mapping{Fields: map[string]schema.FieldType{}}, Settings: IndexSettings{Shards: DefaultShards}}
	if len(req.Mapping) > 0 {
		m, e := parseMapping(req.Mapping, "mapping")
		if e != nil {
			return e
		}
		spec.Mapping = m
	}
	if len(req.Settings) > 0 {
		if err := json.Unmarshal(req.Settings, &spec.Settings); err != nil {
			return settingsProblem(err)
		}
	}
	info, err := s.c.CreateIndex(r.Context(), name, spec)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusCreated, info)
}

// parseMapping reads a mapping, {"dynamic": ..., "fields": {...}}, naming the field a
// problem is about under loc.
func parseMapping(raw json.RawMessage, loc string) (*schema.Mapping, *Error) {
	var m schema.Mapping
	if err := json.Unmarshal(raw, &m); err != nil {
		var ve *schema.ValidationError
		if errors.As(err, &ve) {
			at := loc
			if ve.Field != "" {
				at += ".fields." + ve.Field
			}
			return nil, InvalidAt(at, "%s", ve.Message)
		}
		return nil, InvalidAt(loc, "%s", strings.TrimPrefix(err.Error(), "mapping: "))
	}
	return &m, nil
}

func settingsProblem(err error) *Error {
	var fe *FieldError
	if errors.As(err, &fe) {
		return InvalidAt("settings."+fe.Loc, "%s", fe.Message)
	}
	return InvalidAt("settings", "%s", err.Error())
}

func (s *Server) listIndexes(w http.ResponseWriter, r *http.Request, _ params) error {
	list, err := s.c.ListIndexes(r.Context())
	if err != nil {
		return err
	}
	if list == nil {
		list = []*IndexInfo{}
	}
	return writeJSON(w, http.StatusOK, map[string]any{"indexes": list})
}

func (s *Server) getIndex(w http.ResponseWriter, r *http.Request, _ params) error {
	info, err := s.c.GetIndex(r.Context(), r.PathValue("index"))
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, info)
}

func (s *Server) deleteIndex(w http.ResponseWriter, r *http.Request, _ params) error {
	if err := s.c.DeleteIndex(r.Context(), r.PathValue("index")); err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, map[string]bool{"acknowledged": true})
}

// mappingPatch is PATCH /indexes/{index}/mapping's body.
type mappingPatch struct {
	Fields map[string]json.RawMessage `json:"fields"`
}

func (s *Server) patchMapping(w http.ResponseWriter, r *http.Request, _ params) error {
	body, err := readBody(r)
	if err != nil {
		return err
	}
	var req mappingPatch
	if err := decodeJSONObject(body, &req, false); err != nil {
		return err
	}
	if len(req.Fields) == 0 {
		return InvalidAt("fields", "fields names at least one field to add")
	}
	fields := make(map[string]schema.FieldType, len(req.Fields))
	for name, raw := range req.Fields {
		var t schema.FieldType
		if err := json.Unmarshal(raw, &t); err != nil {
			return InvalidAt("fields."+name, "a field type is keyword, text, keyword_list, number, bool or date")
		}
		fields[name] = t
	}
	info, err := s.c.PatchMapping(r.Context(), r.PathValue("index"), fields)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, info)
}

// settingsPatchBody is PATCH /indexes/{index}/settings' body.
type settingsPatchBody struct {
	RefreshInterval  json.RawMessage `json:"refresh_interval"`
	ReplicasPerShard *int            `json:"replicas_per_shard"`
	Shards           json.RawMessage `json:"shards"`
}

func (s *Server) patchSettings(w http.ResponseWriter, r *http.Request, _ params) error {
	body, err := readBody(r)
	if err != nil {
		return err
	}
	var req settingsPatchBody
	if err := decodeJSONObject(body, &req, false); err != nil {
		return err
	}
	if req.Shards != nil {
		return InvalidAt("shards", "shards is fixed when the index is created")
	}
	var patch SettingsPatch
	if req.RefreshInterval != nil {
		d, err := parseRefreshInterval(req.RefreshInterval)
		if err != nil {
			return err
		}
		patch.RefreshInterval = &d
	}
	if req.ReplicasPerShard != nil {
		if n := *req.ReplicasPerShard; n < 0 || n > MaxReplicas {
			return InvalidAt("replicas_per_shard", "replicas_per_shard is from 0 (every node) to %d", MaxReplicas)
		}
		patch.ReplicasPerShard = req.ReplicasPerShard
	}
	if patch.RefreshInterval == nil && patch.ReplicasPerShard == nil {
		return InvalidAt("body", "give refresh_interval or replicas_per_shard")
	}
	info, err := s.c.PatchSettings(r.Context(), r.PathValue("index"), patch)
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, info)
}
