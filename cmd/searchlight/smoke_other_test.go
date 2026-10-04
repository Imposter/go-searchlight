//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

func prepareInterrupt(*exec.Cmd) {}

func interrupt(cmd *exec.Cmd) error { return cmd.Process.Signal(syscall.SIGTERM) }
