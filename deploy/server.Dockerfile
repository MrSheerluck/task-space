FROM rust:1.96-bookworm AS builder

WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build -p task-server --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/task-server /usr/local/bin/task-space-server

EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/task-space-server"]
