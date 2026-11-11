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
| The Go SDK | this repository, tag `sdks/go/v<version>` | a maintainer, by hand |

One version for all of it: the workspace version in `Cargo.toml`, which
every SDK manifest carries (`scripts/check-versions.py`).

## Steps

1. **Prepare.** In one pull request: the version in `Cargo.toml` and every
   manifest, the release's section in `CHANGELOG.md` (it becomes the
   release text), and install commands in the docs at the new version.
   Merge it, and wait for CI to pass on that commit of `main`.
2. **Tag.** A signed tag `v<version>` on that commit. `release.yml` waits
   for CI on the commit, builds and signs everything, pushes the image and
   opens a **draft** release. A tag on a commit CI did not pass on fails
   here: run CI again on the commit, then the workflow again.
3. **Publish the draft.** Read it, then publish it. That starts
   `publish.yml`.
4. **Approve the registries.** `publish.yml` checks that the tag is the
   version of every manifest and that CI passed on its commit, then waits
   for a reviewer of the `registries` environment before each registry.
   Approving publishes: a version on a registry stays there for good.
5. **Tag the Go module.** A signed tag `sdks/go/v<version>` on the same
   commit. Run `publish.yml` again by hand for the tag (`workflow_dispatch`):
   its last job asks the module proxy for the version, so pkg.go.dev shows
   it. Never move or delete a tag of the module: the proxy keeps what it
   saw.
6. **Check from outside.** In an empty directory: `cargo add wasmkeep-sdk`,
   `npm install @wasmkeep/sdk`, `pip install wasmkeep` and
   `go get github.com/wasmkeep/wasmkeep/sdks/go@v<version>` each give the
   new version.

## If a run stops half-way

Run `publish.yml` again for the tag. Each registry is asked first whether
it has the version, and one that does is skipped, so nothing is published
twice. A registry that refused keeps its error in the job's log; nothing
is retried in a loop.

## Set up once

The registries trust this repository's `publish.yml` by name, with no
stored token ("trusted publishing"). Each is set up once, by an owner of
the package on that registry.

- **GitHub.** An environment named `registries` in this repository, with
  required reviewers. Without them a published release would publish to
  the registries with nobody in between.
- **PyPI.** A *pending publisher* for the project `wasmkeep`: owner
  `WasmKeep`, repository `wasmkeep`, workflow `publish.yml`, environment
  `registries`. The first run creates the project. A pending publisher
  does not reserve the name: make the first release soon after.
- **crates.io.** A crate must exist before it can trust a workflow: the
  first `cargo publish -p wasmkeep-sdk` is by hand, with a token, from the
  release's tag. Then, in the crate's settings, a trusted publisher with
  the same four values. Later releases need no token.
- **npm.** The same: the first `npm publish` of `@wasmkeep/sdk` is by hand
  from `sdks/typescript` at the release's tag. Then a trusted publisher in
  the package's settings with the same values, **with `npm publish`
  allowed** (a new one allows only staged publishes otherwise). It must
  publish once within two days of being created, or npm removes it. The
  fields are case-sensitive, and `repository.url` in `package.json` must
  be this repository.

For the first release, then: PyPI goes out through `publish.yml`;
crates.io and npm are published by hand from the tag, and `publish.yml`
skips them, since they have the version.

`publish.yml` keeps its file name and its jobs stay in it: the registries
match the name of the workflow that asks, and a renamed or reusable
workflow would be refused.
