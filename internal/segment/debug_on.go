//go:build searchlight_debug

package segment

// debugGuard is true in a build tagged searchlight_debug: [mapping.release] poisons a
// mapping's address range instead of unmapping it, so a zero-copy bitmap or byte slice
// kept past the last [Reader.Close] faults deterministically on its next access instead
// of reading whatever the OS happens to map at that address afterward. Never set in a
// release build: it leaks the mapping's address range (though not the file) for
// the rest of the process, which is a fine trade for a test run and a bad one for a server.
const debugGuard = true
