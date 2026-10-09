# syntax=docker/dockerfile:1.7
FROM golang:1.26.8-bookworm AS build

WORKDIR /src
ENV CGO_ENABLED=1 GOTOOLCHAIN=local GOMAXPROCS=2
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
COPY adapter ./adapter
COPY delegation ./delegation
COPY federation ./federation
COPY filesnapshot ./filesnapshot
COPY ingestion ./ingestion
COPY migration ./migration
COPY operations ./operations
COPY provider ./provider
COPY query ./query
COPY resolver ./resolver
COPY sourceproof ./sourceproof
COPY transportissuer ./transportissuer
COPY watch ./watch
COPY sandbox ./sandbox
COPY scripts/provision_duckbridge.py scripts/duckbridge-driver.patch ./scripts/
ARG DUCKBRIDGE=0
ARG VERSION=dev
# The module/cache mounts avoid downloading irrelevant platform archives on
# every build. CGO uses the target platform's compiler; build native images.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    if [ "$DUCKBRIDGE" = "1" ]; then \
      apt-get update && apt-get install --no-install-recommends -y python3 && \
      python3 scripts/provision_duckbridge.py /src/artifacts/duckbridge && \
      CGO_CXXFLAGS=-I/src/artifacts/duckbridge/headers \
        go build -trimpath -modfile /src/artifacts/duckbridge/duckbridge.mod \
        -tags duckdb_arrow,duckbridge -ldflags "-s -w -X main.version=${VERSION}" -o /out/kelvo ./cmd/kelvo; \
    elif [ "$DUCKBRIDGE" = "0" ]; then \
      go build -trimpath -tags duckdb_arrow -ldflags "-s -w -X main.version=${VERSION}" -o /out/kelvo ./cmd/kelvo; \
    else exit 2; fi \
    && cc -O2 -Wall -Wextra -Werror -std=c11 -o /out/kelvo-landlock sandbox/launcher.c

FROM build AS adapter-build
COPY adapters/go ./adapters/go
# Build the optional adapter against this exact SDK checkout. Its published
# module pin remains unchanged for consumers outside this image.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go work init . ./adapters/go \
    && sdk_version=$(awk '$1 == "github.com/SYNEHQ/kelvo-go" { print $2 }' adapters/go/go.mod) \
    && test -n "$sdk_version" \
    && go work edit "-replace=github.com/SYNEHQ/kelvo-go@${sdk_version}=." \
    && cd adapters/go \
    && go build -mod=readonly -p 2 -tags duckdb_arrow -trimpath \
         -ldflags "-s -w" -o /out/kelvo-adapter-go ./cmd/kelvo-adapter-go \
    && cd /out && sha256sum kelvo-adapter-go > kelvo-adapter-go.sha256

FROM debian:bookworm-slim AS runtime-base
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates libstdc++6 libgomp1 tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 kelvo \
    && useradd --no-log-init --uid 65532 --gid 65532 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin kelvo \
    && mkdir -p /etc/kelvo /opt/kelvo/extensions /data \
    && install -d -m 0755 /usr/share/kelvo

LABEL org.opencontainers.image.title="Kelvo Go" \
      org.opencontainers.image.vendor="SYNEHQ" \
      org.opencontainers.image.source="https://github.com/SYNEHQ/kelvo-go" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build --chmod=0555 /out/kelvo /out/kelvo-landlock /usr/local/bin/
COPY LICENSE NOTICE /usr/share/doc/kelvo/
COPY licenses/ /usr/share/doc/kelvo/licenses/
USER 65532:65532
WORKDIR /tmp
ENV HOME=/nonexistent TMPDIR=/tmp
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/kelvo"]
CMD ["help"]

# The worker target adds the optional database-operation adapter. Containment
# remains mandatory. This image does not grant host or cgroup privileges.
FROM runtime-base AS worker
COPY --from=adapter-build --chmod=0555 /out/kelvo-adapter-go /usr/local/bin/
COPY --from=adapter-build --chmod=0444 /out/kelvo-adapter-go.sha256 /usr/share/kelvo/

# Keep the default target small for gateways and read-only deployments.
FROM runtime-base AS runtime
