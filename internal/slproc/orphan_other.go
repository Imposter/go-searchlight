//go:build !windows && !linux

package slproc

import "syscall"

func orphanGuard() *syscall.SysProcAttr { return nil }
