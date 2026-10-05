# Searchlight developer targets. CI runs `make build`, then the short tier as
# `make test TESTFLAGS=-race` and both tiers as `make test-race`, in two jobs.
#
#   make build       compile every package and the binary into bin/
#   make test        the short tier: go test -short ./..., under two minutes
#   make test-heavy  the short and heavy tiers: go test ./..., real-time cluster and
#                    latency tests and full matrices included (half an hour at most)
#   make test-all    test-heavy with the property tests' long modes (SEARCHLIGHT_LONG=1)
#   make test-race   test-heavy under -race, as CI runs it (needs cgo and gcc)
#   make lint        go vet and golangci-lint
#   make bench       every benchmark, with allocations
#   make docker      the container image (deploy/Dockerfile) as $(IMAGE), searchlight:dev by default
#   make parity      regenerate testdata/parity/*.json and internal/analysis/tables.go from
#                    scrape-bot at SCRAPE_BOT_REF (needs uv and a scrape-bot clone; its venv is
#                    used as it is, never re-synced)
#
# TESTFLAGS adds go test flags to every test target, such as -race or -count=2.

GO            ?= go
GOLANGCI_LINT ?= golangci-lint
TESTFLAGS     ?=
BENCH         ?= .
SCRAPE_BOT    ?= E:/code/scrape_bot
SCRAPE_BOT_REF ?= 325c3345ec8f8f892e895ea9edc0e0f81d0a7fcb
VERSION       ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS       := -s -w -X main.version=$(VERSION)
IMAGE         ?= searchlight:dev
DOCKER        ?= docker

.PHONY: all build test test-heavy test-all test-race lint bench docker parity tidy clean

all: build test lint

build:
	$(GO) build ./...
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/ ./cmd/searchlight

test:
	$(GO) test $(TESTFLAGS) -short -timeout 10m ./...

test-heavy:
	$(GO) test $(TESTFLAGS) -timeout 30m ./...

test-all: export SEARCHLIGHT_LONG = 1
test-all:
	$(GO) test $(TESTFLAGS) -timeout 60m ./...

test-race:
	$(MAKE) test-heavy TESTFLAGS="$(TESTFLAGS) -race"

lint:
	$(GO) vet ./...
	$(GOLANGCI_LINT) run

bench:
	$(GO) test -run '^$$' -bench '$(BENCH)' -benchmem ./...

docker:
	$(DOCKER) build -f deploy/Dockerfile -t $(IMAGE) \
		--build-arg VERSION=$(VERSION) \
		--build-arg REVISION=$(shell git rev-parse HEAD 2>/dev/null || echo unknown) \
		--build-arg CREATED=$(shell date -u +%Y-%m-%dT%H:%M:%SZ) \
		.

parity:
	uv run --project $(SCRAPE_BOT) --no-sync python tools/parity/gen.py --repo $(SCRAPE_BOT) --ref $(SCRAPE_BOT_REF)

tidy:
	$(GO) mod tidy

clean:
	rm -rf bin
