# Releasing

For maintainers. A release is a signed tag on a commit of `main` that CI
passed on; two workflows do the rest, and a person approves each step that
cannot be undone.

## What a release is

| Part | Where | Made by |
| --- | --- | --- |
| Linux binaries (x86_64, aarch64), SBOMs, checksums, Sigstore bundles | the GitHub release | `.github/workflows/release.yml`, on the tag |
| The image, linux/amd64 and linux/arm64, signed | `ghcr.io/wasmkeep/wasmkeep:<version>` | `release.yml` |
| The Rust SDK | crates.io, `wasmkeep-sdk` | `.github/workflows/publish.yml`, when the release is published |
| The TypeScript SDK | npm, `@wasmkeep/sdk` | `publish.yml` |
| The Python SDK | PyPI, `wasmkeep` | `publish.yml` |
| The Go SDK | this repository, tag `sdks/go/v<version>` | a maintainer, with the release's tag |
| The docs site | https://docs.wasmkeep.dev (GitHub Pages) | `.github/workflows/docs.yml`, when the release is published |

One version for all of it: the workspace version in `Cargo.toml`, which
every SDK manifest carries (`scripts/check-versions.py`).

## Steps

The SDKs are published with every release from 0.2.0 on; 0.1.0 is on no
registry.

1. **Prepare.** In one pull request: the version in `Cargo.toml` and every
   manifest, `publish = true` in `crates/wasmkeep-sdk/Cargo.toml` (it stays
   so from then on), the release's section in `CHANGELOG.md` (it becomes the
   release text), and install commands in the docs at the new version.
   Merge it, then start the full suite on that commit of `main` by hand
   (`gh workflow run ci.yml --ref main`: tests on Linux x86_64, arm64 and
   macOS, the S3 suites, every language) and wait for it to pass. A push
   runs the fast jobs only. On a workstation, run the
   [mixed-version check](#the-mixed-version-check) on that commit too: for
   a patch release it is a gate, and the tag waits for it to exit 0.
2. **Tag.** Two signed tags on that commit: `v<version>`, and
   `sdks/go/v<version>` for the Go module (`git tag -s`, with a signing key
   added to your GitHub account). `release.yml` first checks that
   `v<version>` is an annotated tag on the commit whose signature GitHub
   verifies, and stops otherwise. It then waits for a full CI
   run on the commit (by hand or nightly), builds and signs everything,
   pushes the image and opens a **draft** release. A tag on a commit the
   full suite did not pass on fails here: run it on the commit, then the
   workflow again. Never move or delete a
   tag: the module proxy and the registries keep what they saw.
3. **Publish the draft.** Read it, then publish it. That starts
   `publish.yml`.
4. **Approve the registries.** `publish.yml` first checks the release (the
   tag is annotated and signed, with a signature GitHub verifies, and is the
   version of every manifest, CI passed on its commit, the
   `registries` environment is protected), builds every package from that
   commit and asks each registry what it already has. Only then does each
   registry that lacks the version wait for a reviewer. Before approving,
   look at the run: it is `publish.yml`, started by the release, on the
   tag. Approving publishes: a version on a registry stays there for good.
5. **Check from outside.** https://docs.wasmkeep.dev shows the new
   version's docs once `docs.yml` has run (it builds only from a verified
   signed release tag). In an empty directory: `cargo add wasmkeep-sdk`,
   `npm install @wasmkeep/sdk`, `pip install wasmkeep` and
   `go get github.com/wasmkeep/wasmkeep/sdks/go@v<version>` each give the
   new version, and each registry's page names this repository as the
   source.

## The mixed-version check

A gate before tagging any release. It runs on a workstation, alone
(nothing heavy beside it: it times node starts and deliveries), and never
in pull request CI or in `check.sh`:

```bash
./scripts/mixed-version.sh            # from the previous release, v0.8.1 by default
./scripts/mixed-version.sh v0.8.1     # or OLD_VERSION=v0.8.1
```

The old version must be 0.8.1 or later. It runs in one of two modes, by
the minor versions of the old release and of the checkout's build, and
says which on its first line and in the report:

- **The next minor version** (0.9.0 after v0.8.1): every phase below. It
  is the proof of
  [ADR 0043, section 8](adr/0043-rolling-upgrades.md#8-how-the-mixed-version-check-becomes-the-proof),
  and the old release must be the last of its minor version.
- **The same minor version** (a patch: 0.9.1 after v0.9.0): the upgrade
  under load, one node back and up again, the four paths at the end of
  this section and both CLIs on the mixed cluster. Nothing is gated
  between two patches, so the gate, the returning old node, the tracing
  phase and the old CLI on a finalized cluster are left out.

It starts three nodes of the old release (downloaded with `gh` and checked
against its `SHA256SUMS` and a SHA-256 pinned in the script) on a local
moto bucket, deploys a test app with rooms and their pushers, and moves
one node at a time to the binary built from the checkout (drain, stop,
swap, start, undrain). A version without a pinned SHA-256 is refused
unless `OLD_SHA256` gives it.

After each step it checks durable writes, `actors.call` between every pair
of nodes, alarms, secrets, env files, WebSocket room delivery, and
`cluster status` from both versions' CLIs. While the cluster is mixed, both
CLIs run `deploy`, `secrets set`, `cluster drain` and `backup create`. Then,
for the next minor version, every phase counting in the exit status:

- **Back and forward.** One node returns to the old release and goes up
  again, with the same checks, before anything is finalized.
- **The gate.** A deploy of a `wasmkeep.toml` with `apart-from` and
  `spread` (deploy manifest 8) is refused while a node of the old release
  is live, and so is `cluster upgrade finalize`, which names that node.
  With every node new the deploy is still refused, naming the floor. After
  `cluster upgrade finalize` it is accepted, and a class kept apart from
  another wakes on another node.
- **The returning old node.** After the finalize, a node of the old
  release is started: it stops itself on the app's upgrade floor, which it
  names. With the gated format written it is started again, and stops on
  the manifest it cannot read (a node of 0.8.1 reads the current manifest
  before the floor); so does a node of v0.8.0, which reads no floor
  (`MIXED_VERSION_OLDER_VERSION`). Each must stop itself with that reason
  in its log, answer no request and write no lease, and the counter goes
  on through the others.
- **Tracing.** Two new nodes export to a collector the script runs
  (`scripts/mixed-version/otlp-sink.mjs`), with `--trace-sample 0`. A
  client's sampled `traceparent` forwarded by the node of the old release
  is not followed and nothing is exported for it; through a new node with
  `--trace-trust-incoming` it is followed on both nodes; and the old
  node's guest receives a tracing node's header as any other
  ([ADR 0044, section 4](adr/0044-traces.md#4-sampling)).
- **The old CLI on the finalized cluster.** `cluster status`, `secrets
  set`, `cluster drain`, `backup create` and `deploy`: each works, and what
  it wrote is read by the new nodes, or it is refused with the message it
  is known by and changes nothing. `backup create` into an archive that
  holds a run with the app's floor (backup format 2) must be refused and
  add no run to it: nothing there is removed or rewritten, and at most
  the `started` record of the run it gave up is left, which the new CLI
  reads past.

**The new side's version.** Only a `wasmkeep` of the next minor version
writes a gated format or finalizes, and a build of the checkout reports
the workspace version, which is bumped in the release's pull request. The
run a tag waits for is the plain one, on the commit to tag: the script
uses the release build and overrides nothing.

Before the bump, the next-minor phases can still be run:

```bash
MIXED_VERSION_NEW_VERSION=0.9.0 ./scripts/mixed-version.sh
```

That builds the new side with the cargo feature `test-version` of
`wasmkeep-cluster` and `WASMKEEP_TEST_VERSION` in the build's environment,
read by its `build.rs` alone. The binary reports `0.9.0+test.<commit>`,
warns at every `serve`, and is built into `target/mixed-version/build`,
never `target/release`. No release build sets the feature, without it the
variable is ignored, and nothing changes the version of a built binary;
`release.yml` stops if the binary or the image it built reports `+test`,
or on a tag anything but the tag's version. Without the variable, a
checkout whose version still has the old release's minor version runs the
same-minor mode.

Four paths of the upgrade have checks of their own, each failing when it
could not be brought about:

- **The start floor.** No client sees a failure from a node's start. The
  calls the started node refuses as signed before its start are counted. On
  one machine's clock, peers rarely call a node within its first second, so
  the report notes when there were none.
- **The stored idle position.** Once a room and its pusher have both left
  memory, the first publish reaches every client with no history fetch.
  This is checked with the checkout's pusher, on a room built with the old
  SDK and then on one built with the new SDK.
- **Kept apart.** A pusher woken on its room's node is sent on to a peer.
  A placed wake-up refused as apart is read and run by a node of the other
  version, in both directions.
- **A drain with the room.** A node of the checkout holding the room is
  drained with clients connected, and every client still holds every
  acknowledged number.

It writes `target/mixed-version/report.txt`, ending with a table of
passes, failures and seconds for each step. It exits with 1 if any check
failed, in any phase. The same run as an ignored test:
`WASMKEEP_MIXED_VERSION=1 cargo nextest run -p wasmkeep-cli --test
mixed_version --run-ignored only --no-capture`.

A passing run does not by itself make a rolling upgrade supported. A
release's upgrading page says whether it can be installed one node at a
time, from this check's result. Without that statement the rule stays the
one in [Upgrading WasmKeep](cluster-upgrades.md#upgrading-wasmkeep): stop every
node, upgrade them all, start them all. After a release, add its SHA-256
to `PINNED_SHA256` at the top of the script and make it the default
`OLD_VERSION`.

## If a run stops half-way

- **A registry refused, or an approval was rejected or ran out**: use
  "Re-run failed jobs" on the run, or run the workflow by hand on the tag
  (Run workflow, with the tag as the ref). It publishes the same files.
- **Running `release.yml` by hand** (a dry run, or again on a tag) works
  from a `v<version>` tag or from `main` only: the workflow signs and pushes
  images with its own identity, so it refuses any other ref. The signed
  files also get a GitHub build provenance attestation:
  `gh attestation verify <file> --repo WasmKeep/wasmkeep`.
- **`publish.yml` itself is wrong at the tag**: it cannot be fixed for
  that tag, since a run always uses the tag's own copy. Publish what is
  missing by hand from a checkout of the tag (`cargo publish`,
  `npm publish`, `twine upload`), and fix the workflow for the next
  release.

Either way a registry that has the version starts no job and asks for no
approval, and PyPI is asked file by file, so a release that got its wheel
but not its source archive gets the missing one. Nothing is retried in a
loop: a registry that refused keeps its error in the job's log.

## Set up once

The registries trust this repository's `publish.yml` by its file name and
its environment, with no stored token ("trusted publishing"). They do not
check the branch or the tag: a copy of the file on any branch has the same
identity. The `registries` environment is therefore the boundary, and its
settings matter.

- **GitHub, the `registries` environment**: required reviewers;
  deployment limited to the tags `v*`; administrators not allowed to
  bypass it; "prevent self-review"
  once there is more than one maintainer. `publish.yml` refuses to run if
  the environment is missing, has no reviewers or accepts any ref.
- **GitHub, the tags**: the ruleset that keeps `v*` and `sdks/go/v*` from
  being moved or deleted. With several maintainers, also restrict who may
  create them.
- **The names.** A registry can trust a workflow only for a package that
  exists, except PyPI. Reserve the crate and the npm package once, by
  hand, with a placeholder version `0.0.0` that says so in its
  description: `cargo publish` of a crate named `wasmkeep-sdk`, and
  `npm publish` of `@wasmkeep/sdk`. Reserve the unscoped npm name
  `wasmkeep-ts` the same way, as a placeholder that points to
  `@wasmkeep/sdk`: it is the name of the SDK's build command, and `npx`
  looks a command up as a package when it is not installed. Every real version then goes out
  through `publish.yml`, with a provenance statement.
- **crates.io**: in the crate's settings, a trusted publisher: owner
  `WasmKeep`, repository `wasmkeep`, workflow `publish.yml`, environment
  `registries`.
- **npm**: in the package's settings, a trusted publisher with the same
  values, **with `npm publish` allowed** (a new one allows only staged
  publishes otherwise). npm removes a trusted publisher that has not
  published within two days of being created: create it during step 1 of
  the first release, not earlier. The fields are case-sensitive, and
  `repository.url` in `package.json` must be this repository.
- **PyPI**: a *pending publisher* for the project `wasmkeep` with the
  same four values. The first run creates the project. A pending publisher
  does not reserve the name: make the first release soon after.

`publish.yml` keeps its file name and its jobs stay in it: a renamed or
reusable workflow would be refused by the registries.

## Pinned tools

Nothing a release builds with is fetched unchecked: `rustup-init`
(`scripts/build-portable.sh`), TinyGo, LDC and the WASI adapter
(`scripts/componentize-core.sh`) are pinned by version and SHA-256, and the
Python tools CI and the workflows install come from
`scripts/requirements/*.txt`, installed with `--require-hashes`. To move a
Python pin, edit the `.in` beside it and run
`uv pip compile --universal --generate-hashes --python-version 3.12 --no-header X.in -o X.txt`.
