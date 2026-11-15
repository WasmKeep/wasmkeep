//! Local replica reuse without opening SQLite (which could checkpoint a WAL).
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::ltx::DbChecksum;
use crate::wal::WalCapture;

/// Reuse only a sidecar at the authoritative head, an empty/absent WAL,
/// and a main file whose checksum matches that sidecar. The sidecar format
/// is `generation txid checksum`. No SQLite connection is opened here.
pub fn local_is_at(path: &Path, generation: u64, txid: u64) -> Option<DbChecksum> {
    let mut sidecar = path.as_os_str().to_owned();
    sidecar.push(".replica");
    let text = std::fs::read_to_string(PathBuf::from(sidecar)).ok()?;
    let mut parts = text.split_whitespace().map(|s| s.parse::<u64>().ok());
    let (Some(Some(at_generation)), Some(Some(at_txid)), Some(Some(sum))) =
        (parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    let wal_empty = std::fs::metadata(PathBuf::from(wal)).map_or(true, |m| m.len() == 0);
    if (at_generation, at_txid) != (generation, txid) || !wal_empty {
        return None;
    }
    let mut h = [0u8; 18];
    let page_size = std::fs::File::open(path)
        .and_then(|mut f| f.read_exact(&mut h))
        .map(|()| match u16::from_be_bytes([h[16], h[17]]) {
            1 => 65_536,
            raw => u32::from(raw),
        })
        .unwrap_or(4096);
    let found = WalCapture::start(path, page_size).ok()?.checksum();
    (found.value() == sum).then_some(found)
}
