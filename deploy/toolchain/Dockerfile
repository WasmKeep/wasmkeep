# syntax=docker/dockerfile:1.29.0@sha256:572cebbc2b15e6f84c077742952674889cb63cd5e9b80abdffcbe587b6f19183
#
# The toolchain image: `wasmkeep` and the build tools of every Recommended
# and Supported SDK, at the versions CI builds the examples with, so a
# project builds with nothing installed but Docker or Podman
# (docs/deploy/toolchain.md):
#
#   docker build -f deploy/toolchain/Dockerfile -t wasmkeep-toolchain .   (from the repo root)
#   podman run --rm -v "$PWD:/src" wasmkeep-toolchain wasmkeep deploy --bucket ...
#
# | Language   | Tools                                                         |
# | Rust       | Rust (rust-toolchain.toml) with wasm32-wasip2, rustfmt, clippy |
# | TypeScript | Node.js 22 and npm (jco, componentize-qjs and esbuild come with @wasmkeep/sdk) |
# | Python     | Python 3.13, componentize-py                                  |
# | Go         | Go 1.24 and TinyGo, wit-bindgen-go                            |
# | C, Zig     | clang, lld and wasi-libc (Debian's), Zig (the ziglang package) |
# | all        | wasm-tools, wit-bindgen                                       |
#
# Not included: the Experimental languages (standard Go, D, MoonBit, C#),
# whose tools are large or amd64-only. Every version below is the one
# .github/workflows/ci.yml installs, checked by scripts/check-toolchain.py;
# downloads are checked by SHA-256 for both architectures. Exceptions:
# Node.js (CI takes the newest 22.x; this pins one), and clang 19 from
# Debian 13 where CI's Ubuntu 24.04 has clang 18.
#
# Like deploy/Dockerfile: the base image is pinned by digest (the same
# one), the `wasmkeep` binary is built here with the pinned toolchain, and
# LICENSE, NOTICE and the third-party notices (the `notices` context, as
# the release workflow gives it) are in /usr/share/doc/wasmkeep/. The tools'
# own licences are where each installs them (/usr/share/doc for Debian's
# packages, /opt/<tool>/ for the others).

ARG RUST_IMAGE=docker.io/library/rust:1.99.1-slim-trixie@sha256:7eab7ce19273ce785e0ca7578bdf7bc1fcbae621775a8197379a04c9f6e03cd5

FROM scratch AS notices
COPY NOTICE /

FROM ${RUST_IMAGE} AS wasmkeep
WORKDIR /src
COPY . .
RUN pinned=$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml) \
 && rustc --version | grep -q "^rustc ${pinned} " \
 || { echo "rust-toolchain.toml wants ${pinned}; image has $(rustc --version)" >&2; exit 1; }
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p wasmkeep-cli \
 && install -D -m 0755 target/release/wasmkeep /out/usr/local/bin/wasmkeep \
 && install -D -m 0644 -t /out/usr/share/doc/wasmkeep LICENSE NOTICE

FROM ${RUST_IMAGE} AS toolchain
ARG TARGETARCH

# Versions: as in .github/workflows/ci.yml (scripts/check-toolchain.py).
ARG NODE_VERSION=22.23.5
ARG NODE_SHA256_AMD64=cdeb8670a0c81f51b4dc4ac61919f9b1ad917b304bf8221eb939570663d240f8
ARG NODE_SHA256_ARM64=2babb9eb5264860d540849f4b064a6fbdebcdc323657d9d382ce23d313fd5e09
ARG GO_VERSION=1.24.7
ARG GO_SHA256_AMD64=da18191ddb7db8a9339816f3e2b54bdded8047cdc2a5d67059478f8d1595c43f
ARG GO_SHA256_ARM64=fd2bccce882e29369f56c86487663bb78ba7ea9e02188a5b0269303a0c3d33ab
ARG TINYGO_VERSION=0.39.0
ARG TINYGO_SHA256_AMD64=a119801579ef0a7a4f3ee285f0caf55241eaa9e115154c2028b696770b47a846
ARG TINYGO_SHA256_ARM64=b2ba04d35e1020f9674ed1133de02721952ba45af2b61fa666fb8144570b02b0
ARG WASM_TOOLS_VERSION=1.263.0
ARG WASM_TOOLS_SHA256_AMD64=21c036c1740a0552124c79c6e2170cb61c5b016005671a6d5bc06cf642165431
ARG WASM_TOOLS_SHA256_ARM64=773b5e6bf957f0d93a9a337c9c5fc4c35509706301a80799f57a76fa367fd1ca
ARG WIT_BINDGEN_VERSION=0.63.0
ARG WIT_BINDGEN_SHA256_AMD64=bf4efe883ab70d34a4740253911dda20caf895d58a003af58369506b04684cbe
ARG WIT_BINDGEN_SHA256_ARM64=1f9b4fb9f3af15374e2a2d026f6a1fcd8b48f37655f59f48a32cb421e169cbe4
ARG WIT_BINDGEN_GO_VERSION=0.7.0
ARG COMPONENTIZE_PY_VERSION=0.25.1
ARG ZIGLANG_VERSION=0.17.1

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl git xz-utils \
      clang lld wasi-libc libclang-rt-19-dev-wasm32 \
      python3 python3-venv \
 && rm -rf /var/lib/apt/lists/*

# What rust-toolchain.toml names, on the base image's toolchain (the
# `wasmkeep` stage checked it is the pinned one).
RUN rustup component add rustfmt clippy && rustup target add wasm32-wasip2

# TinyGo's microcontroller and Windows/macOS files (about 800 MB of its
# 1.1 GB) are left out: the image builds for wasip2 only. `device/arm`
# stays, by which TinyGo recognizes its root.
# fetch URL SHA256 DIR: downloads, checks and unpacks a tarball into DIR.
SHELL ["/bin/bash", "-eo", "pipefail", "-c"]
RUN cat > /usr/local/bin/fetch-checked <<'SH' && chmod 0755 /usr/local/bin/fetch-checked
#!/bin/bash
set -euo pipefail
url=$1 sum=$2 dir=$3 file=$(mktemp)
curl -fsSL -o "$file" "$url"
echo "$sum  $file" | sha256sum -c --quiet
mkdir -p "$dir"
tar xf "$file" -C "$dir" --strip-components=1 --no-same-owner
rm -f "$file"
SH
RUN case "$TARGETARCH" in \
      amd64) node=x64;   rust=x86_64;  a=AMD64 ;; \
      arm64) node=arm64; rust=aarch64; a=ARM64 ;; \
      *) echo "unsupported architecture $TARGETARCH" >&2; exit 1 ;; \
    esac \
 && sum() { local v="${1}_SHA256_$a"; echo "${!v}"; } \
 && fetch-checked "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${node}.tar.xz" "$(sum NODE)" /opt/node \
 && fetch-checked "https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz" "$(sum GO)" /opt/go \
 && fetch-checked "https://github.com/tinygo-org/tinygo/releases/download/v${TINYGO_VERSION}/tinygo${TINYGO_VERSION}.linux-${TARGETARCH}.tar.gz" "$(sum TINYGO)" /opt/tinygo \
 && fetch-checked "https://github.com/bytecodealliance/wasm-tools/releases/download/v${WASM_TOOLS_VERSION}/wasm-tools-${WASM_TOOLS_VERSION}-${rust}-linux.tar.gz" "$(sum WASM_TOOLS)" /opt/wasm-tools \
 && fetch-checked "https://github.com/bytecodealliance/wit-bindgen/releases/download/v${WIT_BINDGEN_VERSION}/wit-bindgen-${WIT_BINDGEN_VERSION}-${rust}-linux.tar.gz" "$(sum WIT_BINDGEN)" /opt/wit-bindgen \
 && ln -s /opt/wasm-tools/wasm-tools /opt/wit-bindgen/wit-bindgen /usr/local/bin/ \
 && find /opt/tinygo/src/device -mindepth 1 -maxdepth 1 ! -name arm -exec rm -rf {} + \
 && rm -rf /opt/tinygo/lib/nrfx /opt/tinygo/lib/mingw-w64 /opt/tinygo/lib/macos-minimal-sdk

ENV PATH=/opt/node/bin:/opt/go/bin:/opt/tinygo/bin:/opt/python/bin:$PATH
# Module downloads verified against the Go checksum database (go.sum).
RUN GOBIN=/usr/local/bin GOPATH=/tmp/go GOCACHE=/tmp/go-cache \
      go install "go.bytecodealliance.org/cmd/wit-bindgen-go@v${WIT_BINDGEN_GO_VERSION}" \
 && rm -rf /tmp/go /tmp/go-cache

# componentize-py and Zig (the ziglang package, as in CI), in a venv of
# Debian's Python 3.13; `zig` runs it.
RUN python3 -m venv /opt/python \
 && /opt/python/bin/pip install --no-cache-dir \
      "componentize-py==${COMPONENTIZE_PY_VERSION}" "ziglang==${ZIGLANG_VERSION}" \
 && printf '#!/bin/sh\nexec /opt/python/bin/python3 -m ziglang "$@"\n' > /usr/local/bin/zig \
 && chmod 0755 /usr/local/bin/zig

COPY --from=wasmkeep /out/ /
COPY --from=notices / /usr/share/doc/wasmkeep/

# Each tool says its version: a broken download fails the build here.
RUN wasmkeep --version && rustc --version && cargo clippy --version \
 && node --version && npm --version && go version && tinygo version \
 && componentize-py --version && zig version \
 && wasm-tools --version && wit-bindgen --version && wit-bindgen-go --version \
 && clang --version | head -1

WORKDIR /src
LABEL org.opencontainers.image.title="wasmkeep-toolchain" \
      org.opencontainers.image.description="wasmkeep and the build tools of its SDKs (Rust, TypeScript, Python, Go, C, Zig)" \
      org.opencontainers.image.source="https://github.com/wasmkeep/wasmkeep" \
      org.opencontainers.image.licenses="Apache-2.0"
CMD ["bash"]
