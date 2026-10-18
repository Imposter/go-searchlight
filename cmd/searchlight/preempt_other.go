//go:build !windows

package main

// ensureAsyncPreemptOff does nothing outside Windows: golang/go#79249 is a Windows bug
// (preempt_windows.go).
func ensureAsyncPreemptOff() {}

// parentStopped returns nil: only a Windows node is started again by a parent.
func parentStopped() <-chan struct{} { return nil }
