# syntax=docker/dockerfile:1
#
# The wasmkeep server in a distroless image, running as a non-root user.
#
#   docker build -f deploy/Dockerfile -t wasmkeep .          (from the repo root)
#   docker run --rm wasmkeep --help
#
# Stages: `builder` compiles the binary with the toolchain pinned in
# rust-toolchain.toml; the default (last) stage is the release image. The
# `demo` stage adds the counter example, for deploy/compose/ only.
#
# The image carries LICENSE and NOTICE under /usr/share/doc/wasmkeep/, and
# the third-party notices when the build is given them as the `notices`
# context (the release workflow does):
#
#   scripts/notices.sh
#   docker buildx build -f deploy/Dockerfile --build-context notices=target/notices .
#
# Base images are pinned by the digest of their multi-architecture index.
# Both are Debian 13, so the binary's glibc matches the runtime's: move them
# together. To refresh, resolve the new index digest of each tag (for example
# `docker buildx imagetools inspect TAG`) and keep the Rust tag equal to the
# channel in rust-toolchain.toml (the build fails otherwise).

ARG RUST_IMAGE=docker.io/library/rust:1.99.1-slim-trixie@sha256:7eab7ce19273ce785e0ca7578bdf7bc1fcbae621775a8197379a04c9f6e03cd5
ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian13:nonroot@sha256:08476c244888bfb425bae1ae4dd7c17f093e972aab8426883d97f3c6f280f7c1

# Replaced by a `notices` build context when given one (see above); by
# default it holds NOTICE alone, so the copy below never reads an empty stage.
FROM scratch AS notices
COPY NOTICE /

FROM ${RUST_IMAGE} AS builder
WORKDIR /src
COPY . .
# The image's compiler must be the pinned one: no second toolchain download.
RUN pinned=$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml) \
 && rustc --version | grep -q "^rustc ${pinned} " \
 || { echo "rust-toolchain.toml wants ${pinned}; image has $(rustc --version)" >&2; exit 1; }
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p wasmkeep-cli \
 && install -D -m 0755 target/release/wasmkeep /out/usr/local/bin/wasmkeep \
 && install -D -m 0644 -t /out/usr/share/doc/wasmkeep LICENSE NOTICE \
 && install -d -o 65532 -g 65532 /out/var/lib/wasmkeep /out/var/lib/wasmkeep/data /out/var/lib/wasmkeep/cache

# The counter example, built for wasm32-wasip2 (used by the demo stage only;
# rustup installed the target from rust-toolchain.toml above).
FROM builder AS counter
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/examples/counter/target \
    cd examples/counter \
 && cargo build --release --locked --target wasm32-wasip2 \
 && install -D -m 0644 target/wasm32-wasip2/release/counter.wasm /out/components/counter.wasm

FROM ${RUNTIME_IMAGE} AS runtime
COPY --from=builder /out/ /
COPY --from=notices / /usr/share/doc/wasmkeep/
USER 65532:65532
WORKDIR /var/lib/wasmkeep
EXPOSE 8080 7443
LABEL org.opencontainers.image.title="wasmkeep" \
      org.opencontainers.image.description="Self-hosted actor platform: Wasm components, SQLite per actor, S3 replication" \
      org.opencontainers.image.source="https://github.com/wasmkeep/wasmkeep" \
      org.opencontainers.image.licenses="Apache-2.0"
ENTRYPOINT ["/usr/local/bin/wasmkeep"]
CMD ["--help"]

# deploy/compose/ deploys this component; not part of the release image.
FROM runtime AS demo
COPY --from=counter /out/ /

# The release image.
FROM runtime
