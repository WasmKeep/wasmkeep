# WasmKeep

Self-hosted actor platform: Wasm components (Wasmtime) + SQLite per actor + S3 replication.
Read README.md, docs/overview.md, CONTRIBUTING.md and the relevant ADRs in docs/adr before non-trivial changes. Run `./scripts/check.sh --full` before merging. Changes to leases, replication or deploys need a case in sim/ or crates/wasmkeep-server/tests/cluster/ and an adversarial review.

## Conventions
- Rust stable, edition 2024. Before committing, run `./scripts/check.sh`: docs-only changes need publication-path and light link checks; Rust changes need fmt, clippy and affected tests; lease/replication changes also need simulations. Run `./scripts/check.sh --full` before merging any code change. Never substitute `cargo test` for nextest (except the separate madsim suite).
- Workspace crates: wasmkeep-wit, wasmkeep-runtime, wasmkeep-storage, wasmkeep-cluster, wasmkeep-server, wasmkeep-cli, wasmkeep-sdk.
- Every host function callable by a guest is declared in crates/wasmkeep-wit first, then implemented.
- wasmkeep-runtime and wasmkeep-storage never import hyper, tokio::net, or the cluster crate.
- Pin wasmtime to one version per milestone; do not bump it in a feature PR.
- Prefer small PR-sized changes, one focused change each.
- Examples in examples/ are built in CI; if you change the WIT world, update every example and the SDK in the same change.
- Write a failing test before fixing a bug. For anything involving ordering, leases or failover, add a case to sim/.
- No new dependency without a one-line justification in the PR description.

## Commands
- Build: `cargo build --workspace`
- Test: `cargo nextest run --workspace`
- Prerequisites: `./scripts/doctor.sh` (reports missing tools; installs nothing).
- Checks: `./scripts/check.sh` before commits; `./scripts/check.sh --full` before merging code; `./scripts/check.sh --s3` (alone or with `--full`) runs the suites gated on an S3 bucket, against moto or the bucket the AWS_* variables name; `./scripts/check.sh --deep` runs every simulation for 20,000 seeds in release mode, one at a time (about half an hour on a fast workstation). Every run ends with what it did not run. Full covers local Rust checks, script tests, the guest SDKs' unit tests, every example and language whose tools are installed, publication paths in the commit history, simulations, MSRV, dependencies and the docs build. CI platform matrices, S3/failover (`--s3`), Go LTX conformance and real-bucket soaks remain separate.
- Build an example component: `cd examples/hello && cargo build --release --target wasm32-wasip2` (plain cargo, see docs/adr/0001)
- Run: `cargo run -p wasmkeep-cli -- serve examples/hello/target/wasm32-wasip2/release/hello.wasm`
- Bench: `./bench/run.sh` (writes bench/results/<date>-<commit>.csv; numbers in docs must come from these files); `./bench/bucket.sh` (the one-node bucket comparison alone), `./bench/cluster.sh` (three nodes against celld) and `./bench/soak.sh` (a real bucket) write their own files there

## Scope
ROADMAP.md says what is built, what comes next and what is not planned. Do not start work it lists under "Not planned" (a hosted cloud, a global anycast network, Cloudflare Workers API compatibility, our own JavaScript engine), and discuss items marked "on demand" in an issue first.
