package slproc

import "syscall"

// orphanGuard has Linux kill a node (SIGKILL) when the process that started it dies,
// so a test or a benchmark that crashes leaves no node running.
func orphanGuard() *syscall.SysProcAttr { return &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL} }
