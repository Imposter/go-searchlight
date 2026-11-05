package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// golang/go#79249: on Windows the Go runtime can async-preempt a thread that is inside
// exception handling (a page fault in a mapped segment, a recovered nil dereference), and
// the thread resumes with corrupt registers: an access violation at a small address in
// code that is otherwise correct. The fix is in Go 1.28. Until go.mod's toolchain carries
// it, a Windows node runs with GODEBUG=asyncpreemptoff=1, unless the operator sets
// asyncpreemptoff in GODEBUG themselves.
//
// The release binaries and "make build" link it in as the runtime's default
// (tools/ldflags.sh), which also sets asyncPreemptLinked. A binary built without those
// flags, such as by "go install", cannot change the setting once running (the runtime
// reads it at startup only), so it starts itself again with it.

// asyncPreemptLinked is "1" when the binary was linked with asyncpreemptoff=1 as the
// runtime's default, set with -ldflags "-X main.asyncPreemptLinked=1".
var asyncPreemptLinked string

// ensureAsyncPreemptOff returns when this process runs with async preemption off, or
// with the operator's own asyncpreemptoff. Otherwise it runs the binary again, with the
// same arguments, standard streams and environment plus asyncpreemptoff=1 in GODEBUG,
// and exits with the child's exit code. Ctrl+C and Ctrl+Break reach the child through
// the console they share, and this process ignores them; with no console, neither has
// one, and a stop this process is sent goes on to the child (envParentStopEvent). The
// child is in a job object that ends it if this process is killed. If the child cannot be started, it says so on
// standard error and returns, and the node runs with async preemption on.
func ensureAsyncPreemptOff() {
	godebug := os.Getenv("GODEBUG")
	if asyncPreemptLinked == "1" || godebugSets(godebug, "asyncpreemptoff") {
		return
	}
	code, err := rerunWithAsyncPreemptOff(godebug)
	if err != nil {
		fmt.Fprintf(os.Stderr, "searchlight: running with async preemption on; set GODEBUG=asyncpreemptoff=1 (golang/go#79249): %v\n", err)
		return
	}
	os.Exit(code)
}

// godebugSets reports whether the GODEBUG value godebug sets key.
func godebugSets(godebug, key string) bool {
	for field := range strings.SplitSeq(godebug, ",") {
		if k, _, ok := strings.Cut(field, "="); ok && k == key {
			return true
		}
	}
	return false
}

// envParentStopEvent names the event a parent with no console sets to stop the node it
// started again (rerunWithAsyncPreemptOff); the node treats it as a signal to stop.
const envParentStopEvent = "SEARCHLIGHT_PARENT_STOP_EVENT"

var (
	modKernel32               = windows.NewLazySystemDLL("kernel32.dll")
	procGetConsoleProcessList = modKernel32.NewProc("GetConsoleProcessList")
)

// hasConsole reports whether this process is attached to a console.
func hasConsole() bool {
	var pids [4]uint32
	n, _, _ := procGetConsoleProcessList.Call(uintptr(unsafe.Pointer(&pids[0])), uintptr(len(pids)))
	return n != 0
}

func rerunWithAsyncPreemptOff(godebug string) (int, error) {
	exe, err := os.Executable()
	if err != nil {
		return 0, err
	}
	if godebug != "" {
		godebug += ","
	}
	// The child runs until it exits on its own: nothing here cancels it.
	cmd := exec.CommandContext(context.Background(), exe, os.Args[1:]...) //nolint:gosec // this binary, with its own arguments
	cmd.Args[0] = os.Args[0]
	cmd.Env = append(os.Environ(), "GODEBUG="+godebug+"asyncpreemptoff=1")
	if os.Stdin != nil {
		cmd.Stdin = os.Stdin
	}
	if os.Stdout != nil {
		cmd.Stdout = os.Stdout
	}
	if os.Stderr != nil {
		cmd.Stderr = os.Stderr
	}
	// The child starts suspended, and runs only once it is in the job that ends it with
	// this process: killed before then, this process leaves no node running.
	flags := uint32(windows.CREATE_SUSPENDED)
	// With a console, the child shares it, and its Ctrl+C, Ctrl+Break and close reach
	// the child directly. Without one (a detached process, a service host), the child
	// gets none either, rather than a console window of its own, and this process passes
	// a stop it is sent on through an event.
	var stopEvent windows.Handle
	if !hasConsole() {
		flags |= windows.DETACHED_PROCESS
		name := fmt.Sprintf(`Local\searchlight-stop-%d`, os.Getpid())
		stopEvent, err = windows.CreateEvent(nil, 1, 0, windows.StringToUTF16Ptr(name))
		if err != nil {
			return 0, fmt.Errorf("stop event: %w", err)
		}
		cmd.Env = append(cmd.Env, envParentStopEvent+"="+name)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: flags}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	if err := cmd.Start(); err != nil {
		signal.Stop(signals)
		return 0, err
	}
	if err := endWithThisProcess(cmd.Process.Pid); err != nil {
		fmt.Fprintf(os.Stderr, "searchlight: the node may outlive this process if it is killed: %v\n", err)
	}
	if err := resumeProcess(uint32(cmd.Process.Pid)); err != nil { //nolint:gosec // a process id
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		signal.Stop(signals)
		return 0, fmt.Errorf("resume the node: %w", err)
	}
	if stopEvent != 0 {
		go func() {
			for range signals {
				_ = windows.SetEvent(stopEvent)
			}
		}()
	}
	err = cmd.Wait()
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		return exit.ExitCode(), nil
	case err != nil:
		fmt.Fprintf(os.Stderr, "searchlight: %v\n", err)
		return 1, nil
	}
	return 0, nil
}

// endWithThisProcess puts the process pid into a job object that kills it when this
// process exits, which closes the job's only handle.
func endWithThisProcess(pid int) error {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return fmt.Errorf("job object: %w", err)
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{
		BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE},
	}
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		_ = windows.CloseHandle(job)
		return fmt.Errorf("job object: %w", err)
	}
	h, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(pid)) //nolint:gosec // a process id
	if err != nil {
		_ = windows.CloseHandle(job)
		return fmt.Errorf("open the node's process: %w", err)
	}
	defer windows.CloseHandle(h) //nolint:errcheck // a handle only used here
	if err := windows.AssignProcessToJobObject(job, h); err != nil {
		_ = windows.CloseHandle(job)
		return fmt.Errorf("assign the node to a job object: %w", err)
	}
	return nil
}

// resumeProcess resumes every thread of the suspended process pid: os/exec keeps no
// handle to the thread CreateProcess suspended, so it is found by its owner.
func resumeProcess(pid uint32) error {
	snap, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(snap) //nolint:errcheck // a handle only used here
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	resumed := 0
	for err = windows.Thread32First(snap, &entry); err == nil; err = windows.Thread32Next(snap, &entry) {
		if entry.OwnerProcessID != pid {
			continue
		}
		t, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
		if err != nil {
			return err
		}
		_, err = windows.ResumeThread(t)
		_ = windows.CloseHandle(t)
		if err != nil {
			return err
		}
		resumed++
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return err
	}
	if resumed == 0 {
		return fmt.Errorf("no thread of process %d", pid)
	}
	return nil
}

// parentStopped returns a channel closed when the parent that started this node again
// with async preemption off, having no console to pass signals through, asks it to stop;
// nil (never ready) when there is no such parent.
func parentStopped() <-chan struct{} {
	name := os.Getenv(envParentStopEvent)
	if name == "" {
		return nil
	}
	_ = os.Unsetenv(envParentStopEvent)
	event, err := windows.OpenEvent(windows.SYNCHRONIZE, false, windows.StringToUTF16Ptr(name))
	if err != nil {
		fmt.Fprintf(os.Stderr, "searchlight: open the parent's stop event: %v\n", err)
		return nil
	}
	stopped := make(chan struct{})
	go func() {
		defer windows.CloseHandle(event) //nolint:errcheck // a handle only used here
		if ev, err := windows.WaitForSingleObject(event, windows.INFINITE); err == nil && ev == windows.WAIT_OBJECT_0 {
			close(stopped)
		}
	}()
	return stopped
}
