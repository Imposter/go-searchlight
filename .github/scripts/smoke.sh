#!/usr/bin/env bash
# Smoke-tests a Searchlight node, or a cluster behind a load balancer, as it starts: waits
# for /readyz on an admin listener, then creates an index, indexes a document and finds it
# with a search, through the API. release.yml runs it against every release binary and
# image before anything is published; ci.yml against the compose samples.
#
#   smoke.sh <API base URL> <admin base URL>
#
# SMOKE_TOKEN    the bearer token to send; unset for a node run with insecure_no_auth.
# SMOKE_PID      the node's process, when the caller started it: the wait for readiness
#                stops as soon as it exits.
# SMOKE_TIMEOUT  seconds to wait for readiness (default 120).
set -euo pipefail

api=${1:?usage: smoke.sh <API base URL> <admin base URL>}
admin=${2:?usage: smoke.sh <API base URL> <admin base URL>}
deadline=$((SECONDS + ${SMOKE_TIMEOUT:-120}))

until curl -fs -o /dev/null "$admin/readyz"; do
  if [ -n "${SMOKE_PID:-}" ] && ! kill -0 "$SMOKE_PID" 2>/dev/null; then
    echo "smoke: the node exited before it was ready" >&2
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "smoke: $admin/readyz is not 200 after ${SMOKE_TIMEOUT:-120}s" >&2
    curl -sS "$admin/readyz" >&2 || true
    exit 1
  fi
  sleep 1
done
echo "smoke: ready"

auth=()
if [ -n "${SMOKE_TOKEN:-}" ]; then
  auth=(-H "Authorization: Bearer $SMOKE_TOKEN")
fi
call() {
  curl -fsS -X "$1" ${auth[@]+"${auth[@]}"} -H 'Content-Type: application/json' "$api$2" -d "$3"
}

# jq on Windows ends its lines with CRLF.
call PUT /indexes/smoke \
  '{"mapping": {"dynamic": "strict", "fields": {"title": "text", "brand": "keyword", "price": "number"}}}' > /dev/null
seq=$(call PUT /indexes/smoke/docs/sku-1 '{"title": "Oak chair", "brand": "Acme", "price": 30}' | jq -er .seq | tr -d '\r')
found=$(call POST "/indexes/smoke/_search?wait_for_seq=$seq" \
  '{"query": {"all": [{"field": "brand", "op": "eq", "value": "ACME"}, {"field": "price", "op": "lt", "value": 50}]}}')
if ! jq -e '.total.value == 1 and .hits[0].id == "sku-1"' > /dev/null <<< "$found"; then
  echo "smoke: the search did not find sku-1: $found" >&2
  exit 1
fi
echo "smoke: created an index, indexed sku-1 at seq $seq and found it"
