//go:build !linux && !windows

package segment

import "errors"

// residentBytes has no implementation here.
func residentBytes([]byte) (int64, error) { return 0, errors.ErrUnsupported }
