#!/usr/bin/env bash
# Quick checks follow changed files; --full is the local pre-merge code gate.
# --s3 runs the suites gated on an S3 bucket (moto unless WASMKEEP_TEST_S3_BUCKET
# and the AWS_* variables already name one). --deep runs every simulation
# for CHECK_DEEP_SEEDS seeds (20,000 by default), one at a time, in release
# mode at low priority (about half an hour at 20,000 on an 8-core desktop:
# about 11 s of tests at 200 seeds), logging to target/deep/.
# Whatever a run does not cover is listed at the end under "not run", so a
# green run says what it skipped.
# The guest SDKs' own unit tests (TypeScript, Python, Go, C) run when their files change (or with --full),
# each skipped with a note when its toolchain is missing.
set -euo pipefail
cd "$(dirname "$0")/.."
full=false
s3=false
deep=false
for arg in "$@"; do
  case "$arg" in
    --full) full=true ;;
    --s3) s3=true ;;
    --deep) deep=true ;;
    *) echo "usage: $0 [--full] [--s3] [--deep]" >&2; exit 2 ;;
  esac
done
not_run=()
skip() { echo "skipped: $*"; not_run+=("$*"); }
base=$(git merge-base HEAD "${CHECK_BASE:-origin/main}") || {
  echo 'Cannot determine check base; fetch origin/main or set CHECK_BASE.' >&2; exit 1;
}
mapfile -d '' -t changed < <(git diff --name-only -z "$base"; git ls-files --others --exclude-standard -z)
step() { printf '\n== %s\n' "$*"; }
# Check staged blobs as well as every introduced commit, including leaks removed
# in a later commit. The light check also scans unstaged/untracked changed files.
step 'publication paths'
python3 -I scripts/check-benchmark-paths.py
python3 -I scripts/check-benchmark-paths.py --range "$base..HEAD"
python3 -I scripts/check-docs.py "${changed[@]}"
# The TypeScript SDK ships a copy of the WIT; a world changed in one place
# only would build guests against another world than the host's.
if [ -d crates/wasmkeep-wit/wit ] && [ -d sdks/typescript/wit ]; then
  step 'WIT copies'
  diff -r crates/wasmkeep-wit/wit sdks/typescript/wit >/dev/null || {
    echo 'sdks/typescript/wit differs from crates/wasmkeep-wit/wit: run npm run sync in sdks/typescript' >&2; exit 1;
  }
fi
# The Python package carries a copy too, so an installed wheel has the world.
if [ -d crates/wasmkeep-wit/wit ] && [ -d sdks/python/wasmkeep/wit ]; then
  step 'WIT copy (Python)'
  diff -r crates/wasmkeep-wit/wit sdks/python/wasmkeep/wit >/dev/null || {
    echo 'sdks/python/wasmkeep/wit differs from crates/wasmkeep-wit/wit: run sdks/python/scripts/sync-wit.sh' >&2; exit 1;
  }
fi
# A tag's release text is its version's section of CHANGELOG.md
# (release.yml): a version bump adds the section in the same change.
step 'release notes'
version=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
[ -n "$(./scripts/release-notes.sh "$version")" ] || {
  echo "CHANGELOG.md has no section for $version, the workspace version" >&2; exit 1;
}

rust=false; everything=false; simulation=false; scripts=false; preflight=false
packages=(); sdk_ts=false; sdk_py=false; sdk_go=false; sdk_c=false
for path in "${changed[@]}"; do
  case "$path" in
    Cargo.toml|Cargo.lock|rust-toolchain.toml|.cargo/*|examples/*|sdks/*|wit/*|crates/wasmkeep-wit/*)
      rust=true; everything=true ;;
    crates/*/*|bench/compare/*)
      rust=true
      dir=$(cut -d/ -f1-2 <<<"$path")
      if [ ! -f "$dir/Cargo.toml" ]; then everything=true; continue; fi
      name=$(sed -n 's/^name *= *"\([^"]*\)"/\1/p' "$dir/Cargo.toml" | head -n 1)
      if [ -n "$name" ]; then packages+=("$name"); else everything=true; fi ;;
  esac
  case "$path" in
    # The simulations build wasmkeep-sim and wasmkeep-cluster only.
    sim/*|crates/wasmkeep-cluster/*|Cargo.toml|Cargo.lock)
      simulation=true ;;
  esac
  case "$path" in scripts/*) scripts=true ;; esac
  case "$path" in
    sdks/typescript/*) sdk_ts=true ;;
    sdks/python/*) sdk_py=true ;;
    sdks/go/*) sdk_go=true ;;
    sdks/c/*) sdk_c=true ;;
  esac
  case "$path" in bench/*.sh) preflight=true ;; esac
  if [[ "$path" == *.sh && -f "$path" ]]; then bash -n "$path"; fi
done

if $full; then
  ./scripts/doctor.sh
elif $rust || $simulation || $preflight; then
  ./scripts/doctor.sh --rust
fi
if $full || $scripts; then
  step 'script tests'
  for test in scripts/test_*.py; do python3 -I "$test"; done
fi
if $full || $sdk_ts || $sdk_py || $sdk_go || $sdk_c; then
  step 'SDK unit tests'
  if $full || $sdk_ts; then
    if [ -d sdks/typescript/node_modules ]; then (cd sdks/typescript && npm test --silent)
    else skip 'TypeScript (run npm ci in sdks/typescript)'; fi
  fi
  if $full || $sdk_py; then sdks/python/test/run.sh; fi
  # The C SDK's pure parts and clients, natively (the Zig SDK wraps them).
  if $full || $sdk_c; then sdks/c/test/run.sh; fi
  if $full || $sdk_go; then
    if command -v go >/dev/null 2>&1; then (cd sdks/go && go vet ./... && go test ./...)
    else skip 'Go (go not installed)'; fi
  fi
fi
if $full || $rust || $simulation; then
  step fmt; cargo fmt --all --check
  step clippy; cargo clippy --workspace --all-targets --locked -- -D warnings
fi
if $full || $rust; then
  step tests
  if $full; then
    cargo nextest run --workspace --locked
  else
    # The cluster suite over TLS repeats the cluster suite on another
    # transport: --full only.
    if $everything; then
      filter='all()'
    else
      mapfile -t packages < <(printf '%s\n' "${packages[@]}" | sort -u)
      filter=$(printf 'rdeps(%s) | ' "${packages[@]}")
      filter="(${filter% | })"
    fi
    cargo nextest run --workspace --locked -E "$filter - binary(cluster_over_tls)"
    skip 'the cluster suite over TLS (cluster_over_tls): --full'
  fi
elif $preflight; then
  cargo nextest run -p wasmkeep-compare --test preflight --locked
fi
# Every simulation but madsim_lease, the retired per-actor lease's: the
# server no longer runs that protocol, so it runs under --deep only.
gate_sims=()
for test in sim/tests/madsim_*.rs; do
  [ -e "$test" ] || continue
  name=$(basename "$test" .rs)
  [ "$name" = madsim_lease ] || gate_sims+=(--test "$name")
done
if $full || $simulation; then
  step 'simulations (200 seeds)'
  RUSTFLAGS='--cfg madsim' WASMKEEP_SIM_SEEDS=200 cargo test -p wasmkeep-sim --locked --target-dir target/madsim --lib "${gate_sims[@]}"
  $deep || skip 'madsim_lease (the retired per-actor lease): --deep'
fi
if $deep; then
  seeds=${CHECK_DEEP_SEEDS:-20000}
  step "simulations ($seeds seeds, release, one at a time)"
  mkdir -p target/deep
  log="target/deep/$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse --short HEAD)-$seeds.log"
  for test in sim/tests/madsim_*.rs; do
    [ -e "$test" ] || continue
    name=$(basename "$test" .rs)
    started=$(date +%s)
    RUSTFLAGS='--cfg madsim' WASMKEEP_SIM_SEEDS=$seeds nice -n 19 \
      cargo test -p wasmkeep-sim --locked --release --target-dir target/madsim-release \
      --test "$name" -- --nocapture --test-threads 1 2>&1 | tee -a "$log"
    echo "deep: $name $seeds seeds in $(( $(date +%s) - started )) s" | tee -a "$log"
  done
  echo "deep: log in $log"
fi
if $s3; then
  step 'S3 suites'
  ./scripts/s3-suites.sh
elif $full || $rust; then
  skip 'the S3 suites (s3, failover, cluster_tls, cluster_status, deploy_config): --s3'
fi
if $full; then
  # Every example and every language against the same tests, as the CI's
  # "examples" and "languages" jobs run them; skipped only on request
  # (CHECK_SKIP_LANGS=1), and then listed as not run.
  if [ "${CHECK_SKIP_LANGS:-}" = 1 ]; then
    skip 'the examples and every language (CHECK_SKIP_LANGS=1)'
  else
    step 'examples and languages'
    langs_skipped=$(mktemp)
    LANGUAGES_SKIPPED="$langs_skipped" ./scripts/languages.sh
    while IFS= read -r line; do not_run+=("$line"); done < "$langs_skipped"
    rm -f "$langs_skipped"
  fi
  msrv=$(sed -n 's/^rust-version = "\([^"]*\)"/\1/p' Cargo.toml)
  step MSRV; cargo +"$msrv" check --workspace --all-targets --locked
  step dependencies; cargo deny check; ./scripts/licenses.sh >/dev/null
  step 'docs site'; ./scripts/docs-site.sh >/dev/null
fi
# Never covered here: other operating systems and architectures, the LTX
# conformance against the Go implementation, and a soak against a real
# bucket.
if $full; then
  not_run+=('other platforms, LTX Go conformance, real-bucket soak')
fi
if [ "${#not_run[@]}" -gt 0 ]; then
  step 'not run'
  printf -- '- %s\n' "${not_run[@]}"
fi
step 'selected checks passed'
