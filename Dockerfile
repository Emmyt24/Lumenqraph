# Multi-stage build producing one image with all four service binaries.
# Uses rustls throughout (no OpenSSL), so the runtime only needs CA certs.
#
# Dependency caching: a cargo-chef planner emits recipe.json (the dependency
# graph only), and the builder cooks it before any source is copied. A
# source-only change therefore reuses the cached dependency layer and only
# recompiles the workspace crates. BuildKit cache mounts keep the cargo
# registry/git caches warm across builds (see cache-from/cache-to in CI).

FROM rust:1-slim@sha256:17d1ba895198f9934c6314ec5346a0d5115372f3243390c3d731e242f35c2f27 AS chef
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
# Copy the pinned toolchain first so rustup installs and selects the exact
# Rust version declared in rust-toolchain.toml (matching CI) instead of the
# floating toolchain baked into the base image.
COPY rust-toolchain.toml ./
RUN rustup show active-toolchain \
    && rustc --version > /rustc-version.txt
# Cook the dependency graph from the recipe before copying any source, so a
# source-only change does not invalidate the compiled dependencies.
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY migrations ./migrations
# Build-time metadata baked into the binary via option_env!() macros.
# Pass these with --build-arg GIT_COMMIT=$(git rev-parse --short HEAD)
# and --build-arg BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ) at image
# build time so the /health endpoint reports real values instead of "unknown".
ARG GIT_COMMIT=""
ARG BUILD_TIME=""
ENV LUMENQRAPH_GIT_SHA=${GIT_COMMIT}
ENV LUMENQRAPH_BUILD_TIME=${BUILD_TIME}
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --workspace

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171 AS runtime
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/lumenqraph-indexer /usr/local/bin/
COPY --from=builder /app/target/release/lumenqraph-api /usr/local/bin/
COPY --from=builder /app/target/release/lumenqraph-webhooks /usr/local/bin/
COPY --from=builder /app/target/release/lumenqraph-mcp /usr/local/bin/
# Record the rustc version used to build the binaries so it is observable
# from the image (e.g. `docker run --rm <image> cat /rustc-version.txt`).
COPY --from=builder /rustc-version.txt /rustc-version.txt
# Static explorer UI, served same-origin by the API (EXPLORER_DIR=/app/explorer).
COPY explorer /app/explorer
# Entrypoint for single-slot hosts that run the indexer + API as one process
# (Render's free tier has no worker type). Unused by compose/Fly.
COPY scripts/run-all-in-one.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/run-all-in-one.sh
# Create a non-root user and group for running services
RUN groupadd -r lumenqraph && useradd -r -g lumenqraph lumenqraph
# Set permissions on the app directory
RUN chown -R lumenqraph:lumenqraph /app
# Switch to non-root user
USER lumenqraph
# Default to the API; override `command:` per service in compose.
CMD ["lumenqraph-api"]
