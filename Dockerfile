# syntax=docker/dockerfile:1

FROM rust:1.88-slim AS builder

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && printf 'fn main() {}\n' > src/main.rs
RUN cargo build --release
RUN rm -rf src

COPY src ./src
COPY config ./config
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/binance-momentum /usr/local/bin/binance-momentum
COPY --from=builder /app/config /config

EXPOSE 8080

USER 65532:65532
ENTRYPOINT ["/usr/local/bin/binance-momentum"]
