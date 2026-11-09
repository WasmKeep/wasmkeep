## What

<!-- What changes, and why. Link the issue if there is one. -->

## Tests

<!-- What proves it works; for a bug fix, a test that fails without it. -->

## Checklist

- [ ] `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo nextest run --workspace`
- [ ] Docs (`docs/` pages and the README) updated if behaviour changed
- [ ] Changes to leases, replication or deploys: a case in `sim/` or `tests/cluster/`
- [ ] New dependency: one line on why, and its license (`scripts/licenses.sh` passes)
- [ ] I have signed the CLA (see CLA.md)
