FROM rust:1.85-bookworm AS builder

ARG TARGETPLATFORM=linux/amd64
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    clang \
    cmake \
    protobuf-compiler \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY rust/ ./rust/
COPY proto/ ./rust/proto/
COPY proto/ ./rust/gateway/proto/

WORKDIR /build/rust
RUN rustup target add x86_64-unknown-linux-gnu
RUN cargo build --release -p gateway --bin gateway

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 10001 gateway
WORKDIR /app

COPY --from=builder /build/rust/target/release/gateway /app/gateway

USER gateway
EXPOSE 50051

ENTRYPOINT ["/app/gateway"]
