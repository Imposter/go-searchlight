//go:build !linux && !windows

package report

// platformInfo knows nothing beyond the runtime on other systems.
func platformInfo() (cpu string, mem int64, kernel string) { return "", 0, "" }
