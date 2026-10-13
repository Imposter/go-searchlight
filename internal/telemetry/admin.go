package telemetry

import (
	"net/http"
	"net/http/pprof"
)

// newAdminHandler returns the admin listener's mux:
//
//	GET /healthz        200 while the process runs
//	GET /metrics        Prometheus exposition
//	/debug/pprof/*      runtime profiles, when pprofOn
//
// pprof's cmdline is not served: the command line can hold secrets (a store_url with
// its password), and the admin listener has no auth.
func newAdminHandler(metrics http.Handler, pprofOn bool) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}` + "\n"))
	})
	if pprofOn {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return mux
}
