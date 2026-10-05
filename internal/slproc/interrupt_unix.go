//go:build !windows

package slproc

import (
	"os/exec"
	"syscall"
)

func prepareInterrupt(cmd *exec.Cmd) { cmd.SysProcAttr = orphanGuard() }

func adoptChild(*exec.Cmd) error { return nil }

func interrupt(cmd *exec.Cmd) error { return cmd.Process.Signal(syscall.SIGTERM) }
