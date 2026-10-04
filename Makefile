# Searchlight developer targets. CI runs `make build test lint`.
#
#   make build    compile every package and the binary into bin/
#   make test     go test -race ./... (RACE= to drop -race where cgo/gcc is missing); the
#                 cluster suite runs three-node clusters, hence a 30 minute timeout
#   make lint     go vet and golangci-lint
#   make bench    every benchmark, with allocations
#   make parity   regenerate testdata/parity/*.json and internal/analysis/tables.go from
#                 scrape-bot at SCRAPE_BOT_REF (needs uv and a scrape-bot clone; its venv is
#                 used as it is, never re-synced)

GO            ?= go
GOLANGCI_LINT ?= golangci-lint
RACE          ?= -race
BENCH         ?= .
SCRAPE_BOT    ?= E:/code/scrape_bot
SCRAPE_BOT_REF ?= 325c3345ec8f8f892e895ea9edc0e0f81d0a7fcb
VERSION       ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS       := -s -w -X main.version=$(VERSION)

.PHONY: all build test lint bench parity tidy clean

all: build test lint

build:
	$(GO) build ./...
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/ ./cmd/searchlight

test:
	$(GO) test $(RACE) -timeout 30m ./...

lint:
	$(GO) vet ./...
	$(GOLANGCI_LINT) run

bench:
	$(GO) test -run '^$$' -bench '$(BENCH)' -benchmem ./...

parity:
	uv run --project $(SCRAPE_BOT) --no-sync python tools/parity/gen.py --repo $(SCRAPE_BOT) --ref $(SCRAPE_BOT_REF)

tidy:
	$(GO) mod tidy

clean:
	rm -rf bin
