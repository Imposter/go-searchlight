package main

import (
	"os/exec"
	"syscall"
	"unsafe"
)

// Windows has no SIGTERM to send. The node is started in a process group of its own,
// and interrupted with CTRL_BREAK_EVENT, which the Go runtime delivers as os.Interrupt:
// the same graceful path SIGINT and SIGTERM take elsewhere. The event travels through
// the console the test shares with the node, so without one the test cannot run.
var (
	kernel32                 = syscall.NewLazyDLL("kernel32.dll")
	generateConsoleCtrlEvent = kernel32.NewProc("GenerateConsoleCtrlEvent")
	getConsoleProcessList    = kernel32.NewProc("GetConsoleProcessList")
)

func canInterrupt() (bool, string) {
	var pids [8]uint32
	if n, _, _ := getConsoleProcessList.Call(uintptr(unsafe.Pointer(&pids[0])), uintptr(len(pids))); n == 0 {
		return false, "no console to send the node CTRL_BREAK through"
	}
	return true, ""
}

func prepareInterrupt(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

func interrupt(cmd *exec.Cmd) error {
	if ok, _, err := generateConsoleCtrlEvent.Call(syscall.CTRL_BREAK_EVENT, uintptr(cmd.Process.Pid)); ok == 0 {
		return err
	}
	return nil
}
