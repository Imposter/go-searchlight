package report

import (
	"os"
	"strconv"
	"strings"
)

// platformInfo reads the CPU model and memory from /proc and the kernel release.
func platformInfo() (cpu string, mem int64, kernel string) {
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for line := range strings.SplitSeq(string(b), "\n") {
			if k, v, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(k) == "model name" {
				cpu = strings.TrimSpace(v)
				break
			}
		}
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for line := range strings.SplitSeq(string(b), "\n") {
			if rest, ok := strings.CutPrefix(line, "MemTotal:"); ok {
				if f := strings.Fields(rest); len(f) > 0 {
					if kb, err := strconv.ParseInt(f[0], 10, 64); err == nil {
						mem = kb << 10
					}
				}
			}
		}
	}
	if b, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		kernel = strings.TrimSpace(string(b))
	}
	return cpu, mem, kernel
}
