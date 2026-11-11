# 0005. Replicate as LTX v3 files, with our own codec

- Status: accepted (2026-11-02: the codec round-trips against `superfly/ltx` v0.5.3 in CI)
- Date: 2026-11-02
- Milestone: [M2](../../ROADMAP.md#m2-cluster-and-durability)

## Context

M2 ships each actor's committed changes to the bucket and restores them on
another node. The open question was whether to reuse the LTX format
(Litestream and LiteFS) for tooling compatibility, or a simpler format of our
own.

Findings (2026-11-02):

- The reference implementation (`github.com/superfly/ltx`, Go) is at **format
  version 3**: `LTX1` magic, 100-byte header, **6-byte page header** with
  optional per-page LZ4, a 16-byte trailer with a CRC-ISO-64 file checksum, and
  rolling pre-apply / post-apply database checksums.
- The only Rust crate, `litetx` 0.1.0 (Fly.io), has one release from 2023-09
  and implements the older 4-byte page header: it cannot read current files.
- Whatever we ship needs the same ingredients anyway: page-level transaction
  files, a monotonic transaction id, integrity checksums (on every segment,
  and `wasmkeep bucket verify`), and snapshots.

## Decision

- Segments are **LTX v3 files**, written and read by our own codec in
  `wasmkeep-storage` (no dependency on `litetx`).
- **Conformance is tested, not assumed**: CI generates files with the Go `ltx`
  tool and checks we decode them, and checks the Go tool verifies files we
  write. Until that passes we do not claim compatibility.
- Fencing does not live in the file format: the lease **generation** is part of
  every object key (`wal/<generation>-<txid>.ltx` as first planned; the final
  key format is in [ADR 0007](0007-replication-protocol.md)), and conditional
  writes reject stale generations.
- Page payloads use LZ4 (`lz4_flex`, pure Rust) as LTX v3 does; snapshots are
  LTX snapshot files (all pages), not zstd copies of the database.

## Alternatives

- **Own format**: about the same amount of code once checksums and snapshots
  are included, and no tooling. Rejected.
- **`litetx` crate**: unmaintained and incompatible with current LTX.
- **Ship raw SQLite WAL files**: WAL frames carry salts and checksums tied to
  one WAL file; restoring across checkpoints is fragile. LTX exists to fix this.

## Consequences

- Operators can inspect and verify our bucket with Litestream's `ltx` tool.
- We own a codec of roughly 600 lines plus tests; the spec is short and stable.
- New dependency when implemented: `lz4_flex`. CRC-ISO-64 is small enough to
  implement directly with a test vector.

## Conformance notes (2026-11-02)

- `conformance/ltx` (Go, pinned to `superfly/ltx` v0.5.3) writes the fixtures
  in `crates/wasmkeep-storage/tests/ltx` and verifies the files our encoder
  writes. CI runs both directions and fails if the reference starts writing
  different fixtures.
- Files are not byte-identical across implementations: LZ4 output differs,
  and the file checksum covers compressed sizes and index offsets. Content,
  headers and both database checksums are identical.
- We do not read LZ4 frame-format pages (older writers); they are rejected
  as unsupported rather than misread.
