//go:build !searchlight_debug

package segment

// debugGuard is false by default (no searchlight_debug tag): [mapping.release] really
// unmaps and reclaims the segment's address space and file descriptor. See debug_on.go.
const debugGuard = false
