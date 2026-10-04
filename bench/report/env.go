package report

import (
	"os"
	"runtime"
	"strings"
)

// CaptureEnvironment describes this machine and toolchain.
func CaptureEnvironment() Environment {
	e := Environment{
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		CPUs:      runtime.NumCPU(),
		GoVersion: runtime.Version(),
	}
	e.Hostname, _ = os.Hostname()
	e.CPU, e.MemTotal, e.Kernel = platformInfo()
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		e.CI = strings.TrimSpace("GitHub Actions " + os.Getenv("RUNNER_OS") + " " + os.Getenv("RUNNER_ARCH") + " (" + os.Getenv("ImageOS") + ")")
		e.Commit = os.Getenv("GITHUB_SHA")
	}
	return e
}
