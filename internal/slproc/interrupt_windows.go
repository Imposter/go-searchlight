package slproc

import (
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows has no SIGTERM to send. A node is started in a process group of its own and
// interrupted with CTRL_BREAK_EVENT, which the Go runtime delivers as os.Interrupt: the
// same graceful path SIGINT and SIGTERM take elsewhere. The event travels through the
// console this process shares with the node, so without one a node can only be killed.
//
// Every node joins one job object, closed only when this process exits, whose limit
// kills its processes then: a test or a benchmark that crashes leaves no node running.
var (
	kernel32                 = syscall.NewLazyDLL("kernel32.dll")
	generateConsoleCtrlEvent = kernel32.NewProc("GenerateConsoleCtrlEvent")
	getConsoleProcessList    = kernel32.NewProc("GetConsoleProcessList")
)

// errNoConsole is an interrupt with no console to send it through.
var errNoConsole = errors.New("slproc: no console to send the node CTRL_BREAK through")

var killOnExit = sync.OnceValues(func() (windows.Handle, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return 0, err
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{
		BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE},
	}
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		_ = windows.CloseHandle(job)
		return 0, err
	}
	return job, nil
})

func prepareInterrupt(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

// adoptChild puts a started node into the kill-on-exit job.
func adoptChild(cmd *exec.Cmd) error {
	job, err := killOnExit()
	if err != nil {
		return fmt.Errorf("slproc: job object: %w", err)
	}
	h, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid)) //nolint:gosec // a process id
	if err != nil {
		return fmt.Errorf("slproc: open the node's process: %w", err)
	}
	defer windows.CloseHandle(h) //nolint:errcheck // a handle only used here
	if err := windows.AssignProcessToJobObject(job, h); err != nil {
		return fmt.Errorf("slproc: assign the node to the job object: %w", err)
	}
	return nil
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
