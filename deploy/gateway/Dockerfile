FROM rust:1.97-bookworm AS builder
WORKDIR /build
COPY deploy/gateway/Cargo.toml deploy/gateway/Cargo.lock ./
COPY deploy/gateway/vendor ./vendor
COPY deploy/gateway/src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10003 gateway \
    && install -d -o gateway -g gateway /var/lib/phenogram-gateway
COPY --from=builder /build/target/release/phenogram-data-plane-gateway /usr/local/bin/phenogram-data-plane-gateway
USER gateway
EXPOSE 8080 8082 9090
ENTRYPOINT ["/usr/local/bin/phenogram-data-plane-gateway"]
