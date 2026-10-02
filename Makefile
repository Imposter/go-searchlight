# Searchlight developer targets. CI runs `make build test lint`.
#
#   make build    compile every package and the binary into bin/
#   make test     go test -race ./... (RACE= to drop -race where cgo/gcc is missing)
#   make lint     go vet and golangci-lint
#   make bench    every benchmark, with allocations
#   make parity   regenerate testdata/parity/*.json from scrape-bot (needs uv and a scrape-bot checkout)

GO            ?= go
GOLANGCI_LINT ?= golangci-lint
RACE          ?= -race
BENCH         ?= .
SCRAPE_BOT    ?= E:/code/scrape_bot
VERSION       ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS       := -s -w -X main.version=$(VERSION)

.PHONY: all build test lint bench parity tidy clean

all: build test lint

build:
	$(GO) build ./...
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/ ./cmd/searchlight

test:
	$(GO) test $(RACE) ./...

lint:
	$(GO) vet ./...
	$(GOLANGCI_LINT) run

bench:
	$(GO) test -run '^$$' -bench '$(BENCH)' -benchmem ./...

parity:
	uv run --project $(SCRAPE_BOT) python tools/parity/gen.py

tidy:
	$(GO) mod tidy

clean:
	rm -rf bin
