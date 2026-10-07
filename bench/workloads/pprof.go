package workloads

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// defaultPprofCPUSeconds bounds a phase's CPU profile: real work can run longer or
// shorter than this, but the sample still describes it, just not necessarily its
// whole span for a long-running phase (visibility's write-to-visible loop, say).
const defaultPprofCPUSeconds = 10

// PprofCapture captures CPU, heap and mutex profiles from an engine's pprof endpoint
// (net/http/pprof on Searchlight's admin listener, enabled with the pprof=true
// setting) bracketing a workload phase, so a tuning PR for that phase carries
// evidence. Every capture is best-effort: a failure (pprof off, the engine briefly
// unreachable) is logged to Log and never fails the phase or the run. Mutex profiles
// come back empty unless the process itself has raised its mutex-profile fraction
// above the runtime's default of 0 — Searchlight does not yet (that is an engine
// change, out of this harness's scope) — so an empty mutex.pprof is expected, not a
// bug in this capture.
type PprofCapture struct {
	AdminURL string
	Dir      string
	// Engine names whose process this is, for the file names.
	Engine string
	// CPUSeconds bounds the CPU profile. 0: defaultPprofCPUSeconds.
	CPUSeconds int
	Log        io.Writer

	client *http.Client
}

// httpClient lazily builds p's client, long enough to outlast a CPU profile request.
func (p *PprofCapture) httpClient() *http.Client {
	if p.client == nil {
		p.client = &http.Client{Timeout: time.Duration(p.cpuSeconds()+30) * time.Second}
	}
	return p.client
}

func (p *PprofCapture) cpuSeconds() int {
	if p.CPUSeconds > 0 {
		return p.CPUSeconds
	}
	return defaultPprofCPUSeconds
}

func (p *PprofCapture) logf(format string, args ...any) {
	if p.Log == nil {
		return
	}
	fmt.Fprintf(p.Log, "%s pprof: "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
}

// capture fetches path from the admin listener and writes it to
// "<phase>.<engine>.<kind>.pprof" under Dir.
func (p *PprofCapture) capture(ctx context.Context, phase, kind, path string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.AdminURL+path, http.NoBody)
	if err != nil {
		p.logf("%s %s: %v", phase, kind, err)
		return
	}
	resp, err := p.httpClient().Do(req)
	if err != nil {
		p.logf("%s %s: %v", phase, kind, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		p.logf("%s %s: HTTP %d (pprof off, or the admin listener is remote: pass pprof=true)", phase, kind, resp.StatusCode)
		return
	}
	out := filepath.Join(p.Dir, fmt.Sprintf("%s.%s.%s.pprof", phase, p.Engine, kind))
	f, err := os.Create(out)
	if err != nil {
		p.logf("%s %s: %v", phase, kind, err)
		return
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		p.logf("%s %s: %v", phase, kind, err)
	}
}

// snapshot captures a non-CPU profile (heap or mutex) by its pprof lookup name.
func (p *PprofCapture) snapshot(ctx context.Context, phase, label, lookup string) {
	p.capture(ctx, phase, label, "/debug/pprof/"+lookup)
}

// Around runs phase while capturing phaseName's pprof evidence: a CPU profile for
// roughly CPUSeconds concurrently with it, and a heap and mutex snapshot immediately
// before and immediately after. p being nil, or AdminURL being empty, runs phase
// unchanged (no capture configured).
func (p *PprofCapture) Around(ctx context.Context, phaseName string, phase func(context.Context) error) error {
	if p == nil || p.AdminURL == "" {
		return phase(ctx)
	}
	if err := os.MkdirAll(p.Dir, 0o750); err != nil {
		p.logf("%s: %v", phaseName, err)
		return phase(ctx)
	}
	p.snapshot(ctx, phaseName, "heap_before", "heap")
	p.snapshot(ctx, phaseName, "mutex_before", "mutex")
	cpuDone := make(chan struct{})
	go func() {
		defer close(cpuDone)
		p.capture(ctx, phaseName, "cpu", fmt.Sprintf("/debug/pprof/profile?seconds=%d", p.cpuSeconds()))
	}()
	err := phase(ctx)
	<-cpuDone
	p.snapshot(ctx, phaseName, "heap_after", "heap")
	p.snapshot(ctx, phaseName, "mutex_after", "mutex")
	return err
}
