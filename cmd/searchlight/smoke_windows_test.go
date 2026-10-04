package main

import (
	"os/exec"
	"syscall"
)

// Windows has no SIGTERM to send. The node is started in a process group of its own,
// and interrupted with CTRL_BREAK_EVENT, which the Go runtime delivers as os.Interrupt:
// the same graceful path SIGINT and SIGTERM take elsewhere.
var generateConsoleCtrlEvent = syscall.NewLazyDLL("kernel32.dll").NewProc("GenerateConsoleCtrlEvent")

func prepareInterrupt(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

func interrupt(cmd *exec.Cmd) error {
	if ok, _, err := generateConsoleCtrlEvent.Call(syscall.CTRL_BREAK_EVENT, uintptr(cmd.Process.Pid)); ok == 0 {
		return err
	}
	return nil
}
