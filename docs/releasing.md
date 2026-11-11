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

One version for all of it: the workspace version in `Cargo.toml`, which
every SDK manifest carries (`scripts/check-versions.py`).

## Steps

The SDKs are published with every release from 0.2.0 on; 0.1.0 is on no
registry.

1. **Prepare.** In one pull request: the version in `Cargo.toml` and every
   manifest, `publish = true` in `crates/wasmkeep-sdk/Cargo.toml` (it stays
   so from then on), the release's section in `CHANGELOG.md` (it becomes the
   release text), and install commands in the docs at the new version.
   Merge it, and wait for CI to pass on that commit of `main`.
2. **Tag.** Two signed tags on that commit: `v<version>`, and
   `sdks/go/v<version>` for the Go module. `release.yml` waits for CI on the
   commit, builds and signs everything, pushes the image and opens a
   **draft** release. A tag on a commit CI did not pass on fails here: run
   CI again on the commit, then the workflow again. Never move or delete a
   tag: the module proxy and the registries keep what they saw.
3. **Publish the draft.** Read it, then publish it. That starts
   `publish.yml`.
4. **Approve the registries.** `publish.yml` first checks the release (the
   tag is the version of every manifest, CI passed on its commit, the
   `registries` environment is protected), builds every package from that
   commit and asks each registry what it already has. Only then does each
   registry that lacks the version wait for a reviewer. Before approving,
   look at the run: it is `publish.yml`, started by the release, on the
   tag. Approving publishes: a version on a registry stays there for good.
5. **Check from outside.** In an empty directory: `cargo add wasmkeep-sdk`,
   `npm install @wasmkeep/sdk`, `pip install wasmkeep` and
   `go get github.com/wasmkeep/wasmkeep/sdks/go@v<version>` each give the
   new version, and each registry's page names this repository as the
   source.

## If a run stops half-way

- **A registry refused, or an approval was rejected or ran out**: use
  "Re-run failed jobs" on the run, or run the workflow by hand on the tag
  (Run workflow, with the tag as the ref). It publishes the same files.
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
  `npm publish` of `@wasmkeep/sdk`. Every real version then goes out
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
