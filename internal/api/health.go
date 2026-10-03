package api

import (
	"net/http"
)

// healthz is liveness: 200 while the process serves.
func (s *Server) healthz(w http.ResponseWriter, _ *http.Request, _ params) error {
	return writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz is readiness: 200 when the node should take traffic, 503 while it drains for
// shutdown or its coordinator reports a reason not to (the database unreachable, a
// halted copy, a copy too far behind).
func (s *Server) readyz(w http.ResponseWriter, r *http.Request, _ params) error {
	if s.draining.Load() {
		return &Error{Status: http.StatusServiceUnavailable, Code: CodeUnavailable, Detail: "the node is shutting down"}
	}
	if err := s.c.Ready(r.Context()); err != nil {
		e := ProblemFor(err)
		if e.Status < 500 {
			e = Unavailable(err, "%s", e.Detail)
		}
		return e
	}
	return writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// metricsHandler serves the Prometheus exposition, as the admin listener does.
func (s *Server) metricsHandler(w http.ResponseWriter, r *http.Request, _ params) error {
	if s.metrics == nil {
		return NotFound(CodeNotFound, "metrics are served on the admin listener")
	}
	s.metrics.ServeHTTP(w, r)
	return nil
}

func (s *Server) clusterHealth(w http.ResponseWriter, r *http.Request, _ params) error {
	h, err := s.c.Health(r.Context())
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, h)
}

func (s *Server) clusterNodes(w http.ResponseWriter, r *http.Request, _ params) error {
	nodes, err := s.c.Nodes(r.Context())
	if err != nil {
		return err
	}
	if nodes == nil {
		nodes = []NodeInfo{}
	}
	return writeJSON(w, http.StatusOK, map[string]any{"nodes": nodes})
}

func (s *Server) clusterShards(w http.ResponseWriter, r *http.Request, _ params) error {
	shards, err := s.c.Shards(r.Context())
	if err != nil {
		return err
	}
	if shards == nil {
		shards = []ShardInfo{}
	}
	return writeJSON(w, http.StatusOK, map[string]any{"shards": shards})
}
