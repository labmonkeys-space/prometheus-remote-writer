# syntax=docker/dockerfile:1

# ---- Builder ----------------------------------------------------------------
# protobuf-compiler: prost-build codegen. cmake + clang: vendored librdkafka.
FROM rust:1-bookworm AS builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev cmake clang \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cargo build --release --locked --bin onms-remote-write-gateway

# ---- Runtime ----------------------------------------------------------------
# rustls is used for TLS (no OpenSSL); only CA certificates are needed.
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/onms-remote-write-gateway /usr/local/bin/onms-remote-write-gateway

# /metrics, /healthz, /readyz
EXPOSE 9100

USER nobody
ENTRYPOINT ["/usr/local/bin/onms-remote-write-gateway"]
