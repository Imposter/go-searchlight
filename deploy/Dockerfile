# syntax=docker/dockerfile:1
#
# Searchlight's container image: a static, pure-Go binary on distroless, run as the
# distroless nonroot user (65532). Build it from the repository root:
#
#   make docker
#   docker build -f deploy/Dockerfile --build-arg VERSION=$(git describe --tags --always) -t searchlight:dev .
#
# Only store_url is required (SEARCHLIGHT_STORE_URL, or SEARCHLIGHT_STORE_URL_FILE for a
# mounted secret), with tokens_file; data_dir is /var/lib/searchlight, a volume. Every
# other setting is a SEARCHLIGHT_* variable or a --flag: see docs/operations.md.
#
# The admin listener is :8781 here (loopback outside a container), so probes and
# Prometheus reach it; keep that port private. The HEALTHCHECK runs the binary's own
# probe, "searchlight healthcheck", against /readyz; Kubernetes probes the admin port
# with httpGet instead.
#
# The base images are pinned by digest; Dependabot (.github/dependabot.yml) bumps them.

FROM --platform=$BUILDPLATFORM golang:1.25@sha256:699337d620559a59b4a2bb298ad59611e535d2ee755a34cf2d2a98f37578dc80 AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/searchlight ./cmd/searchlight \
 && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG VERSION=dev
ARG REVISION=unknown
ARG CREATED=unknown
LABEL org.opencontainers.image.title="searchlight" \
      org.opencontainers.image.description="A search engine that behaves like Elasticsearch and keeps its durable state in SQL." \
      org.opencontainers.image.source="https://github.com/Imposter/go-searchlight" \
      org.opencontainers.image.url="https://github.com/Imposter/go-searchlight" \
      org.opencontainers.image.documentation="https://github.com/Imposter/go-searchlight/blob/main/docs/operations.md" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.licenses="Proprietary"
COPY --from=build /out/searchlight /usr/local/bin/searchlight
COPY --from=build --chown=65532:65532 /out/data /var/lib/searchlight
ENV SEARCHLIGHT_LISTEN=:8780 \
    SEARCHLIGHT_ADMIN_LISTEN=:8781 \
    SEARCHLIGHT_DATA_DIR=/var/lib/searchlight
USER 65532:65532
EXPOSE 8780 8781
VOLUME ["/var/lib/searchlight"]
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=5s --timeout=3s --start-period=2m CMD ["/usr/local/bin/searchlight", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/searchlight"]
