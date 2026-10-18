#!/bin/sh
# Prints the -ldflags the searchlight binary is built with for GOOS (the host's unless it
# is set): "-s -w -X main.version=<version>" ("dev" without one) and, for Windows, the
# flags below. With -godebug instead of a version, only the Windows flags (none
# elsewhere), for test binaries. Run it from the repository root. The Makefile,
# .github/workflows/release.yml and ci.yml's Windows job all use it.
#
# golang/go#79249: on Windows the runtime can async-preempt a thread that is inside
# exception handling (a page fault in a mapped segment, a recovered nil dereference), and
# the thread resumes with corrupt registers. Go 1.28 fixes it; no earlier release has the
# fix. So a Windows build links asyncpreemptoff=1 into runtime.godebugDefault, the
# defaults the runtime applies before $GODEBUG: async preemption is off unless the
# operator sets GODEBUG=asyncpreemptoff=0. This -X replaces the one the go command passes
# (the last -X of a symbol wins), so it repeats the go command's own defaults first.
# main.asyncPreemptLinked tells the binary it was built so; one built without it starts
# itself again with the setting (cmd/searchlight/preempt_windows.go).
#
# Drop the Windows flags once go.mod's toolchain is Go 1.28 or later.
set -eu

flags=
if [ "${1-}" != -godebug ]; then
  flags="-s -w -X main.version=${1:-dev}"
fi
if [ "$(go env GOOS)" = windows ]; then
  defaults=$(go list -f '{{.DefaultGODEBUG}}' ./cmd/searchlight)
  flags="$flags -X=runtime.godebugDefault=${defaults:+$defaults,}asyncpreemptoff=1 -X=main.asyncPreemptLinked=1"
fi
echo "${flags# }"
