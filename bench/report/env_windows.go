package report

import (
	"golang.org/x/sys/windows/registry"
)

// platformInfo reads the CPU model and Windows build from the registry; memory is not
// read on Windows.
func platformInfo() (cpu string, mem int64, kernel string) {
	if k, err := registry.OpenKey(registry.LOCAL_MACHINE, `HARDWARE\DESCRIPTION\System\CentralProcessor\0`, registry.QUERY_VALUE); err == nil {
		cpu, _, _ = k.GetStringValue("ProcessorNameString")
		_ = k.Close()
	}
	if k, err := registry.OpenKey(registry.LOCAL_MACHINE, `SOFTWARE\Microsoft\Windows NT\CurrentVersion`, registry.QUERY_VALUE); err == nil {
		name, _, _ := k.GetStringValue("ProductName")
		build, _, _ := k.GetStringValue("CurrentBuild")
		kernel = name + " build " + build
		_ = k.Close()
	}
	return cpu, 0, kernel
}
