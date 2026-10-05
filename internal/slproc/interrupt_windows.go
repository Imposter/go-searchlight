package slproc

import (
	"errors"
	"os/exec"
	"syscall"
	"unsafe"
)

// Windows has no SIGTERM to send. A node is started in a process group of its own and
// interrupted with CTRL_BREAK_EVENT, which the Go runtime delivers as os.Interrupt: the
// same graceful path SIGINT and SIGTERM take elsewhere. The event travels through the
// console this process shares with the node, so without one a node can only be killed.
var (
	kernel32                 = syscall.NewLazyDLL("kernel32.dll")
	generateConsoleCtrlEvent = kernel32.NewProc("GenerateConsoleCtrlEvent")
	getConsoleProcessList    = kernel32.NewProc("GetConsoleProcessList")
)

// errNoConsole is an interrupt with no console to send it through.
var errNoConsole = errors.New("slproc: no console to send the node CTRL_BREAK through")

func prepareInterrupt(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

func interrupt(cmd *exec.Cmd) error {
	var pids [8]uint32
	if n, _, _ := getConsoleProcessList.Call(uintptr(unsafe.Pointer(&pids[0])), uintptr(len(pids))); n == 0 {
		return errNoConsole
	}
	if ok, _, err := generateConsoleCtrlEvent.Call(syscall.CTRL_BREAK_EVENT, uintptr(cmd.Process.Pid)); ok == 0 {
		return err
	}
	return nil
}
