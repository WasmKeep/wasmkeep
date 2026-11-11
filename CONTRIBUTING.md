# Contributing to WasmKeep

Thanks for your interest. WasmKeep is pre-alpha; the design is described in
[docs/overview.md](docs/overview.md), [docs/concepts.md](docs/concepts.md),
[docs/cluster.md](docs/cluster.md) and the ADRs ([docs/adr](docs/adr)), and
the roadmap is in [ROADMAP.md](ROADMAP.md). Read the ADRs that touch your
change before proposing anything non-trivial.

## Building and testing

Use the Rust toolchain in `rust-toolchain.toml`. Run `./scripts/doctor.sh`
to check local prerequisites; it reports missing tools without installing them.
Tests use nextest's process isolation; do not fall back to `cargo test`.
The separate madsim suite deliberately uses `cargo test` with its own configuration.

Run `./scripts/check.sh` before committing:

- Documentation only: publication paths and light local link checks, without mdBook.
- Rust: formatting, Clippy and changed crates' tests plus their dependents.
- Lease, replication or simulation changes: the deterministic simulation suite too.
- Scripts: script tests and shell syntax checks; benchmark shell changes run preflight tests.
- A guest SDK (`sdks/typescript`, `sdks/python`, `sdks/go`, `sdks/c`): its own
  unit tests (`npm test`, `sdks/python/test/run.sh`, `go vet` and `go test`,
  `sdks/c/test/run.sh` with the host's C compiler), plus the Rust
  suite, since the examples build on it. A missing toolchain (no `go`, or no
  `node_modules` until `npm ci`) is skipped with a note. The Zig SDK
  (`sdks/zig`) wraps the C SDK and has no unit tests of its own; nor has
  the MoonBit SDK (`sdks/moonbit`), which its examples cover.

The base is the merge base with `origin/main`; fetch first. Set `CHECK_BASE` to
an explicit PR base when needed. Checks include staged, unstaged and untracked
changes, and scan the commits introduced since that base for personal
filesystem paths.

Before merging any code change, run `./scripts/check.sh --full`. It adds all
workspace tests, scanner/runner tests, every guest SDK's unit tests,
200-seed simulations, the exact CI MSRV
check, dependency checks and the full docs site. It does not replace CI's OS and
architecture matrix, S3/failover tests, LTX Go conformance, every language/example
build, or real-bucket durability validation. Run the relevant additional checks
for changes in those areas. The light documentation check validates relative
links and Markdown anchors, not remote links.

Merges rely on `./scripts/check.sh --full` run locally; hosted CI (GitHub
Actions) adds the OS and architecture matrix, most of the checks listed
above, and the publication backstop described below.

## Code rules

- Edition 2024. `unwrap`, `expect` and `panic!` are denied outside tests. Unit
  tests get this for free; integration tests (`tests/*.rs`) and benches start
  with `#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]`.
- `#![forbid(unsafe_code)]` in every crate except `wasmkeep-runtime`, where each
  `unsafe` block needs a `// SAFETY:` comment, and `wasmkeep-sdk`, which denies
  unsafe everywhere except its wit-bindgen generated `bindings` module.
- Library crates use `thiserror`; `anyhow` only in `wasmkeep-cli`.
- `wasmkeep-runtime` and `wasmkeep-storage` never depend on hyper, `tokio::net` or
  `wasmkeep-cluster`.
- Every host function a guest can call is declared in `wasmkeep-wit` first.
- A new dependency needs a one-line justification in the pull request.

## Pull requests

- Branch from `main`, keep the change small, and expect a squash merge.
- Use [Conventional Commits](https://www.conventionalcommits.org) for the title,
  e.g. `feat(runtime): pre-compile components to the cache`.
- Checklist: tests added or updated, docs updated, WIT change noted, benchmark
  impact noted.
- Changes to the WIT world, the bucket layout or the config schema are decided
  in an ADR ([docs/adr](docs/adr)). Open an issue to discuss such a change
  before writing it.
- Files under `bench/` and `docs/` must omit personal filesystem paths. Use
  relative paths or `[repo]`/`[scratch]` placeholders, stage the intended files,
  and run `python3 -I scripts/check-benchmark-paths.py` before committing.
  The check reads the index, so an unstaged redaction does not hide private
  staged content. Reviewed system paths, documented product routes and
  documentation-site links are allowed; test files are scanned too.

To run this check automatically before local commits, opt into the repository
hook with `git config --local core.hooksPath .githooks`. If you already use a
hook manager, invoke `python3 -I scripts/check-benchmark-paths.py` from your
existing pre-commit hook instead of replacing it. Use isolated Python mode
(`-I`) so local modules and `PYTHONPATH` cannot shadow the checker's imports.

`./scripts/check.sh` also checks the files introduced or modified by every
commit since its base with
`python3 -I scripts/check-benchmark-paths.py --range BASE..HEAD`. A path
removed in a later commit still fails. A CI job runs the same range check on
the commits each push or pull request introduces, as a backstop; it runs
after the push, so it cannot prevent one. Review
artifacts for other private information too, since this guard checks
filesystem paths only.

## Contributor agreement

Contributions need a signed **Contributor License Agreement** ([CLA.md](CLA.md)).
On your first pull request, the CLA check asks you to sign by posting one
comment; that covers all your later contributions. You keep the copyright in your work; the
CLA lets the project ship it under Apache-2.0 and also under commercial
licenses ([ADR 0004](docs/adr/0004-cla-for-dual-licensing.md) explains why).

## License

The project is licensed under the [Apache License 2.0](LICENSE). The name is
not: see [TRADEMARK.md](TRADEMARK.md).
