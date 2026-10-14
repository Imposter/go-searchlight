#!/bin/sh
# Writes the third-party notices of the searchlight binary built for GOOS/GOARCH (the
# host's unless they are set) to the file named by $1: the Go distribution's licence,
# which covers the runtime and standard library linked into it, then the licence of every
# dependency, as go-licenses (pinned below) finds them. A dependency whose licence
# go-licenses cannot classify fails it, naming the dependency. Run it from the repository
# root. deploy/Dockerfile and .github/workflows/release.yml both run it, so the image and
# the release archives carry the same notices.
set -eu

out=${1:?usage: tools/notices.sh <output file>}
module=github.com/Imposter/go-searchlight

GOOS='' GOARCH='' go install github.com/google/go-licenses/v2@v2.0.1
bin=$(GOOS='' GOARCH='' go env GOBIN)
[ -n "$bin" ] || bin=$(GOOS='' GOARCH='' go env GOPATH)/bin
licenses=$bin/go-licenses

report=$("$licenses" report ./cmd/searchlight --ignore "$module")
unknown=$(echo "$report" | grep ',Unknown$' || true)
if [ -n "$unknown" ]; then
  echo "notices: go-licenses cannot classify these dependencies' licences:" >&2
  echo "$unknown" >&2
  exit 1
fi

{
  printf 'Third-party notices for Searchlight, %s/%s\n\n' "$(go env GOOS)" "$(go env GOARCH)"
  printf 'Searchlight itself is under the MIT License (LICENSE). It is built with Go and\n'
  printf 'links in the software below, each under its own licence.\n\n'
  printf '================================================================================\n'
  printf 'Go %s, its runtime and standard library\n\n' "$(go env GOVERSION)"
  cat "$(go env GOROOT)/LICENSE"
  "$licenses" report ./cmd/searchlight --ignore "$module" --template tools/notices.tpl
  echo
} > "$out"
