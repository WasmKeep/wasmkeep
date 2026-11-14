# The toolchain image

`ghcr.io/wasmkeep/toolchain:<version>` holds `wasmkeep` and the build
tools of every Recommended and Supported language
([Languages](../languages.md)), at the versions CI builds the examples
with. With it, a project builds with nothing installed but Podman or
Docker: no rustup, Node.js, Python or TinyGo on your machine.

It is published from the release after `0.4.1` on, for linux/amd64 and
linux/arm64, signed like the server image, and tagged with the release's
version: use the image of the version your nodes run. The examples below
set it once:

```bash
v=X.Y.Z   # a release from the one after 0.4.1 on
```

## What is in it

| Language | Tools |
| --- | --- |
| Rust | Rust 1.99.1 (the repository's `rust-toolchain.toml`), the `wasm32-wasip2` target, rustfmt, clippy |
| TypeScript | Node.js 22 and npm; `@wasmkeep/sdk` brings jco, componentize-qjs and esbuild at its own pins with `npm install` |
| Python | Python 3.13, componentize-py 0.25.1 |
| Go | Go 1.24.7, TinyGo 0.39.0 (for `wasip2`: its microcontroller files are left out), wit-bindgen-go 0.7.0 |
| C, Zig | clang, lld and wasi-libc from Debian 13 (clang 19; CI's Ubuntu has 18), Zig 0.17.1 (the `ziglang` package) |
| Every one | `wasmkeep`, wasm-tools 1.263.0, wit-bindgen 0.63.0, git, curl |

Not in it: the Experimental languages (standard Go, D, MoonBit, C#),
whose tools are large or published for amd64 only. Build those with their
own tools ([Languages](../languages.md)).

**Size**: about 880 MB to download and 2.8 GB unpacked (linux/amd64).
Rust, TinyGo and Zig make most of it. The image is a build tool, never
something to run a node on: nodes run `ghcr.io/wasmkeep/wasmkeep`
([the image on its own](../../deploy/compose/README.md#the-image-on-its-own)).

`deploy/toolchain/Dockerfile` builds it, from the server image's pinned
base (Debian 13 with Rust); every download is checked by its SHA-256, and
`scripts/check-toolchain.py` (run by `./scripts/check.sh` and CI) fails
when a version there differs from what CI installs.

## Check it

Signed with keyless cosign by the release workflow, as the server image
(cosign 3):

```bash
cosign verify ghcr.io/wasmkeep/toolchain:$v \
  --certificate-identity https://github.com/WasmKeep/wasmkeep/.github/workflows/release.yml@refs/tags/v$v \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

It prints the verified digest: pin that (`ghcr.io/wasmkeep/toolchain@sha256:...`)
in a build you repeat. The `dev` tag is a test of the release process,
overwritten without notice.

## Build and deploy a project with it

Mount the project's directory, run the command there. With rootless
Podman, the files the build writes are yours:

```bash
podman run --rm -v "$PWD:/src" -w /src \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION -e AWS_ENDPOINT \
  ghcr.io/wasmkeep/toolchain:$v wasmkeep deploy --bucket s3://my-bucket/prod
```

`wasmkeep deploy` runs every `build` of `wasmkeep.toml` in the container,
then deploys (`-e NAME` passes your value of `NAME` on; leave out what you
do not set). For a TypeScript project, run `npm install` the same way
first. `wasmkeep dev` works too; it must listen on every address of the
container to be reached from your machine:

```bash
podman run --rm -it -v "$PWD:/src" -w /src -p 8080:8080 \
  ghcr.io/wasmkeep/toolchain:$v wasmkeep dev --listen 0.0.0.0:8080
```

**With Docker**, the container runs as root and its files would be
root's: run it as yourself, with a home and a Cargo directory it can
write:

```bash
docker run --rm -v "$PWD:/src" -w /src --user "$(id -u):$(id -g)" \
  -e HOME=/tmp -e CARGO_HOME=/tmp/cargo \
  ghcr.io/wasmkeep/toolchain:$v wasmkeep dev --listen 0.0.0.0:8080
```

**Downloads are kept between runs** with a volume for each cache, added to
the commands above: `-v wasmkeep-cargo:/usr/local/cargo/registry` (Podman)
or `-v wasmkeep-cargo:/tmp/cargo` (Docker, with the `CARGO_HOME` above),
and `-v wasmkeep-npm:/root/.npm` (Podman) or `-v wasmkeep-npm:/tmp/.npm`
(Docker).

## Or as the project's build commands

To keep `wasmkeep` on your machine and build in the image, put the image
in each `build` of `wasmkeep.toml`. A build runs in the file's directory,
in the system shell, so `$PWD` is the project (a TOML literal string,
in single quotes, keeps the double quotes as they are):

```toml
[classes.counter]
component = "counter/target/wasm32-wasip2/release/counter.wasm"
build = 'podman run --rm -v "$PWD:/src" -w /src/counter ghcr.io/wasmkeep/toolchain:X.Y.Z cargo build --release --target wasm32-wasip2'
watch = ["counter/src", "counter/Cargo.toml"]
```

`wasmkeep dev` and `wasmkeep deploy` then run on your machine, and only
the builds in the container. Name the version in the file (the shell
does not see `$v`), and the same one in every `build`.

## Build the image yourself

From a clone, at the release's tag:

```bash
docker build -f deploy/toolchain/Dockerfile -t wasmkeep-toolchain .
podman run --rm -v "$PWD:/src:ro" wasmkeep-toolchain deploy/toolchain/test.sh
```

`deploy/toolchain/test.sh` is what the release workflow runs on the image
it pushes, on both platforms, before it tags it. It builds
`examples/todo-front` through its `wasmkeep.toml` with `wasmkeep dev`, and
a signed-in request goes through the front to the internal actor. It also
builds a counter in each language. The nightly workflow runs it on
linux/amd64 too, which catches what moves under the pins: Debian's
packages, and the registries the examples download from.
