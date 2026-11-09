//! LTX conformance with the reference implementation (`github.com/superfly/ltx`).
//!
//! `tests/ltx/*.ltx` were written by the reference encoder
//! (`conformance/ltx`, `go run . gen`); we must decode them exactly. We also
//! write the same files ourselves into `target/ltx-conformance/`, which CI
//! hands to the reference decoder (`go run . verify`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::path::PathBuf;

use wasmkeep_storage::ltx::{self, DbChecksum, Encoder, HEADER_FLAG_NO_CHECKSUM, Header};

/// Same as `page` in conformance/ltx/main.go.
fn page(pgno: u32, size: u32, seed: u8) -> Vec<u8> {
    (0..size)
        .map(|i| (i.wrapping_mul(31).wrapping_add(pgno.wrapping_mul(7)) as u8).wrapping_add(seed))
        .collect()
}

struct Spec {
    name: &'static str,
    page_size: u32,
    commit: u32,
    min_txid: u64,
    max_txid: u64,
    /// Empty: a snapshot of every page.
    pgnos: &'static [u32],
    flags: u32,
    node_id: u64,
}

/// Same as `specs` in conformance/ltx/main.go.
const SPECS: &[Spec] = &[
    Spec {
        name: "snapshot-512",
        page_size: 512,
        commit: 4,
        min_txid: 1,
        max_txid: 1,
        pgnos: &[],
        flags: 0,
        node_id: 0,
    },
    Spec {
        name: "snapshot-4096",
        page_size: 4096,
        commit: 3,
        min_txid: 1,
        max_txid: 5,
        pgnos: &[],
        flags: 0,
        node_id: 7,
    },
    Spec {
        name: "incremental-4096",
        page_size: 4096,
        commit: 6,
        min_txid: 6,
        max_txid: 9,
        pgnos: &[2, 5, 6],
        flags: 0,
        node_id: 7,
    },
    Spec {
        name: "no-checksum",
        page_size: 1024,
        commit: 2,
        min_txid: 3,
        max_txid: 3,
        pgnos: &[1],
        flags: HEADER_FLAG_NO_CHECKSUM,
        node_id: 0,
    },
];

impl Spec {
    fn pgnos(&self) -> Vec<u32> {
        if self.pgnos.is_empty() {
            (1..=self.commit).collect()
        } else {
            self.pgnos.to_vec()
        }
    }

    /// Database checksums before and after the file (seed 0 before; the
    /// file's pages rewritten with seed 1).
    fn checksums(&self) -> (DbChecksum, DbChecksum) {
        let (mut before, mut after) = (DbChecksum::default(), DbChecksum::default());
        let changed = self.pgnos();
        for p in 1..=self.commit {
            before.toggle(p, &page(p, self.page_size, 0), self.page_size);
            let seed = u8::from(changed.contains(&p));
            after.toggle(p, &page(p, self.page_size, seed), self.page_size);
        }
        (before, after)
    }

    fn header(&self) -> Header {
        let checked = self.flags & HEADER_FLAG_NO_CHECKSUM == 0;
        Header {
            flags: self.flags,
            page_size: self.page_size,
            commit: self.commit,
            min_txid: self.min_txid,
            max_txid: self.max_txid,
            timestamp_ms: 1_700_000_000_000,
            pre_apply_checksum: if checked && self.min_txid != 1 {
                self.checksums().0.value()
            } else {
                0
            },
            node_id: self.node_id,
            ..Header::default()
        }
    }
}

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/ltx")
}

#[test]
fn files_written_by_the_reference_decode() {
    for spec in SPECS {
        let bytes = std::fs::read(fixtures().join(format!("{}.ltx", spec.name))).unwrap();
        let file = ltx::decode(&bytes).unwrap_or_else(|e| panic!("{}: {e}", spec.name));
        assert_eq!(file.header, spec.header(), "{}", spec.name);
        let expected: Vec<_> = spec
            .pgnos()
            .into_iter()
            .map(|p| (p, page(p, spec.page_size, 1)))
            .collect();
        assert!(file.pages == expected, "{}: pages differ", spec.name);
        let post = if spec.flags & HEADER_FLAG_NO_CHECKSUM == 0 {
            spec.checksums().1.value()
        } else {
            0
        };
        assert_eq!(file.trailer.post_apply_checksum, post, "{}", spec.name);
    }
}

#[test]
fn we_write_what_the_reference_writes() {
    let out = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/ltx-conformance");
    std::fs::create_dir_all(&out).unwrap();
    for spec in SPECS {
        let mut enc = Encoder::new(spec.header()).unwrap();
        for p in spec.pgnos() {
            enc.add_page(p, &page(p, spec.page_size, 1)).unwrap();
        }
        let bytes = enc.finish(spec.checksums().1).unwrap();
        let ours = ltx::decode(&bytes).unwrap();
        let theirs =
            ltx::decode(&std::fs::read(fixtures().join(format!("{}.ltx", spec.name))).unwrap())
                .unwrap();
        // LZ4 output may differ between implementations, and with it the
        // page sizes, the index and so the file checksum. The content and
        // both database checksums must not.
        assert_eq!(ours.header, theirs.header, "{}", spec.name);
        assert!(ours.pages == theirs.pages, "{}: pages differ", spec.name);
        assert_eq!(
            ours.trailer.post_apply_checksum, theirs.trailer.post_apply_checksum,
            "{}",
            spec.name
        );
        std::fs::write(out.join(format!("{}.ltx", spec.name)), bytes).unwrap();
    }
}
