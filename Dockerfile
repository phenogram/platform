FROM rust:1.97-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY assets ./assets
COPY migrations ./migrations
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 10001 phenogram
COPY --from=builder /build/target/release/phenogram-platform /usr/local/bin/phenogram-platform
USER phenogram
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/phenogram-platform"]
