//! Capturing committed changes from SQLite's write-ahead log, for
//! replication (ADR 0007).
//!
//! With automatic checkpoints off, every committed transaction appends its
//! pages to `<db>-wal` and nothing moves them into the main file until we
//! checkpoint. After each event the capture reads the frames appended since
//! its last position, checks them the way SQLite does (salts and the running
//! checksum), and keeps the newest version of each page up to the last commit
//! frame. Frames after the last commit belong to a transaction still open or
//! rolled back, and are left for later.
//!
//! WAL format (sqlite.org/fileformat2.html#walformat): a 32-byte header
//! (magic, version, page size, checkpoint sequence, salt-1, salt-2, checksum),
//! then frames of a 24-byte header (page number, database size in pages for a
//! commit frame else 0, salt-1, salt-2, checksum) followed by the page.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::ltx::{DbChecksum, lock_pgno, page_checksum};

const WAL_HEADER: usize = 32;
const FRAME_HEADER: usize = 24;
const MAGIC_LE: u32 = 0x377f_0682;
const MAGIC_BE: u32 = 0x377f_0683;

/// Why changes could not be captured.
#[derive(Debug, thiserror::Error)]
pub enum CaptureError {
    /// Reading the database or its WAL failed.
    #[error("reading {path}: {source}")]
    Io {
        /// The file.
        path: PathBuf,
        /// The cause.
        #[source]
        source: std::io::Error,
    },
    /// The WAL changed in a way the capture did not cause (another writer, or
    /// a checkpoint behind its back). Replication must restart from a
    /// snapshot.
    #[error("the WAL was reset or rewritten underneath the capture: {0}")]
    Lost(&'static str),
    /// Changes were committed since the last capture; they are returned here
    /// and must be shipped before a snapshot is taken.
    #[error("changes are pending")]
    Pending(Box<Changes>),
    /// SQLite refused a statement the capture needs (a checkpoint).
    #[error("sqlite: {0}")]
    Sql(String),
    /// The database file is not a whole number of pages.
    #[error("database file is {len} bytes, not a multiple of the {page_size}-byte page size")]
    Torn {
        /// File length.
        len: u64,
        /// Page size.
        page_size: u32,
    },
}

/// The pages committed since the previous capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Changes {
    /// Database size after the changes, in pages.
    pub commit: u32,
    /// Page size in bytes.
    pub page_size: u32,
    /// Changed pages, newest version of each (pages beyond `commit` are left
    /// out: the database shrank).
    pub pages: BTreeMap<u32, Vec<u8>>,
    /// Database checksum before the changes.
    pub pre_apply: DbChecksum,
    /// Database checksum after the changes.
    pub post_apply: DbChecksum,
}

impl Changes {
    /// Encodes the changes as one LTX file covering transactions
    /// `min_txid..=max_txid`. With `min_txid == 1` it is a snapshot and must
    /// hold every page (see [`crate::ActorDb::snapshot`]).
    pub fn to_ltx(
        &self,
        min_txid: u64,
        max_txid: u64,
        timestamp_ms: i64,
    ) -> Result<Vec<u8>, crate::ltx::LtxError> {
        let header = crate::ltx::Header {
            page_size: self.page_size,
            commit: self.commit,
            min_txid,
            max_txid,
            timestamp_ms,
            pre_apply_checksum: if min_txid == 1 {
                0
            } else {
                self.pre_apply.value()
            },
            ..crate::ltx::Header::default()
        };
        let mut enc = crate::ltx::Encoder::new(header)?;
        for (pgno, data) in &self.pages {
            enc.add_page(*pgno, data)?;
        }
        enc.finish(self.post_apply)
    }
}

/// Where the capture is in an actor's WAL, and the per-page checksums of the
/// database as of that position.
#[derive(Debug)]
pub struct WalCapture {
    db_path: PathBuf,
    wal_path: std::sync::Arc<Path>,
    page_size: u32,
    /// Checksum of each page, index `pgno - 1`; its length is the database
    /// size in pages.
    page_sums: Vec<u64>,
    db: DbChecksum,
    /// Next byte of the WAL to read; 0 before the header is read.
    offset: u64,
    /// The header we read: salts and checksum byte order.
    header: Option<WalHeader>,
    /// Running checksum after the last frame consumed.
    running: (u32, u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalHeader {
    big_endian: bool,
    salt: (u32, u32),
}

impl WalCapture {
    /// Starts from a database whose WAL is empty (just checkpointed with
    /// `TRUNCATE`), checksumming its pages from the main file.
    pub fn start(db_path: &Path, page_size: u32) -> Result<Self, CaptureError> {
        let io = |source| CaptureError::Io {
            path: db_path.to_owned(),
            source,
        };
        // Page by page: a large database is never held in memory.
        let file = File::open(db_path).map_err(io)?;
        let len = file.metadata().map_err(io)?.len();
        let size = page_size as usize;
        if size == 0 || len % size as u64 != 0 {
            return Err(CaptureError::Torn { len, page_size });
        }
        let pages = usize::try_from(len / size as u64).unwrap_or(usize::MAX);
        let mut reader = std::io::BufReader::with_capacity(size * 16, file);
        let mut page = vec![0u8; size];
        let lock = lock_pgno(page_size);
        let mut db = DbChecksum::default();
        let mut page_sums = Vec::with_capacity(pages);
        for i in 0..pages {
            reader.read_exact(&mut page).map_err(io)?;
            let page = page.as_slice();
            let pgno = u32::try_from(i + 1).unwrap_or(u32::MAX);
            if pgno == lock {
                page_sums.push(0);
            } else {
                db.toggle(pgno, page, page_size);
                page_sums.push(page_checksum(pgno, page));
            }
        }
        let mut wal_path = db_path.as_os_str().to_owned();
        wal_path.push("-wal");
        Ok(Self {
            db_path: db_path.to_owned(),
            wal_path: PathBuf::from(wal_path).into(),
            page_size,
            page_sums,
            db,
            offset: 0,
            header: None,
            running: (0, 0),
        })
    }

    /// The database checksum as of the last capture.
    pub fn checksum(&self) -> DbChecksum {
        self.db
    }

    /// The database size in pages as of the last capture.
    pub fn pages(&self) -> u32 {
        u32::try_from(self.page_sums.len()).unwrap_or(u32::MAX)
    }

    /// Bytes of WAL consumed so far (to decide when to checkpoint).
    pub fn wal_bytes(&self) -> u64 {
        self.offset
    }

    /// The WAL file.
    pub fn wal_path(&self) -> std::sync::Arc<Path> {
        std::sync::Arc::clone(&self.wal_path)
    }

    /// Current size of the WAL file (0 if there is none), captured or not.
    pub fn wal_len(&self) -> u64 {
        std::fs::metadata(&self.wal_path).map_or(0, |m| m.len())
    }

    /// Call after `PRAGMA wal_checkpoint(TRUNCATE)` succeeded: the WAL is
    /// empty and the next write starts a new one.
    pub fn wal_truncated(&mut self) {
        self.offset = 0;
        self.header = None;
        self.running = (0, 0);
    }

    /// Reads what was committed since the last call. `None` if nothing was.
    pub fn capture(&mut self) -> Result<Option<Changes>, CaptureError> {
        let io = |source| CaptureError::Io {
            path: self.wal_path.to_path_buf(),
            source,
        };
        let mut file = match File::open(&self.wal_path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && self.offset == 0 => {
                return Ok(None);
            }
            Err(e) => return Err(io(e)),
        };
        let len = file.metadata().map_err(io)?.len();
        if len < self.offset {
            return Err(CaptureError::Lost("the WAL shrank"));
        }
        if self.offset == 0 {
            if len < WAL_HEADER as u64 {
                return Ok(None);
            }
            let mut h = [0u8; WAL_HEADER];
            file.read_exact(&mut h).map_err(io)?;
            let header = parse_header(&h, self.page_size)?;
            self.running = checksum(header.big_endian, (0, 0), &h[..24]);
            if self.running != (be32(&h[24..]), be32(&h[28..])) {
                // A header still being written; try again after the next commit.
                return Ok(None);
            }
            self.header = Some(header);
            self.offset = WAL_HEADER as u64;
        }
        let Some(header) = self.header else {
            return Err(CaptureError::Lost("no WAL header"));
        };
        // A WAL restarted by someone else has a new header with new salts.
        let mut h = [0u8; WAL_HEADER];
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        file.read_exact(&mut h).map_err(io)?;
        if parse_header(&h, self.page_size)? != header {
            return Err(CaptureError::Lost("the WAL header changed"));
        }

        // Frame by frame: memory holds one frame, plus where the newest
        // version of each page is, never the whole WAL.
        let size = self.page_size as usize;
        let mut reader = std::io::BufReader::with_capacity((FRAME_HEADER + size) * 16, &mut file);
        reader.seek(SeekFrom::Start(self.offset)).map_err(io)?;
        let mut f = vec![0u8; FRAME_HEADER + size];
        let mut running = self.running;
        // Page number -> offset of its page data in the WAL.
        let mut pending: BTreeMap<u32, u64> = BTreeMap::new();
        let mut committed: Option<Committed> = None;
        let mut at = self.offset;
        while at + f.len() as u64 <= len {
            if let Err(e) = reader.read_exact(&mut f) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(io(e));
            }
            if (be32(&f[8..]), be32(&f[12..])) != header.salt {
                break; // a stale frame from before the last restart
            }
            let sum = checksum(header.big_endian, running, &f[..8]);
            let sum = checksum(header.big_endian, sum, &f[FRAME_HEADER..]);
            if sum != (be32(&f[16..]), be32(&f[20..])) {
                break; // stale, or still being written
            }
            running = sum;
            pending.insert(be32(&f), at + FRAME_HEADER as u64);
            at += f.len() as u64;
            let commit = be32(&f[4..]);
            if commit != 0 {
                let mut pages = committed.take().map(|c| c.0).unwrap_or_default();
                pages.append(&mut pending);
                committed = Some((pages, commit, at, running));
            }
        }
        drop(reader);
        let Some((pages, commit, consumed_to, running)) = committed else {
            return Ok(None);
        };

        // Read the newest version of each committed page.
        let mut data = BTreeMap::new();
        let mut page = vec![0u8; size];
        for (pgno, offset) in pages {
            file.seek(SeekFrom::Start(offset)).map_err(io)?;
            file.read_exact(&mut page).map_err(io)?;
            data.insert(pgno, page.clone());
        }

        let pre_apply = self.db;
        let lock = lock_pgno(self.page_size);
        // The database shrank: pages past the end leave the checksum.
        while self.page_sums.len() > commit as usize {
            let old = self.page_sums.pop().unwrap_or(0);
            self.db = DbChecksum::from_value(self.db.value() ^ old);
        }
        self.page_sums.resize(commit as usize, 0);
        let mut out = BTreeMap::new();
        for (pgno, page) in data {
            if pgno == 0 || pgno > commit || pgno == lock {
                continue;
            }
            let i = pgno as usize - 1;
            let new = page_checksum(pgno, &page);
            self.db = DbChecksum::from_value(self.db.value() ^ self.page_sums[i] ^ new);
            self.page_sums[i] = new;
            out.insert(pgno, page);
        }
        self.offset = consumed_to;
        self.running = running;
        Ok(Some(Changes {
            commit,
            page_size: self.page_size,
            pages: out,
            pre_apply,
            post_apply: self.db,
        }))
    }

    /// Every page of the main file, which must be up to date (just
    /// checkpointed with nothing uncaptured). Fails if its checksum differs
    /// from the one the capture tracked: the capture missed something.
    pub fn snapshot(&self) -> Result<Changes, CaptureError> {
        let fresh = Self::start(&self.db_path, self.page_size)?;
        if fresh.db != self.db || fresh.page_sums.len() != self.page_sums.len() {
            return Err(CaptureError::Lost(
                "the database differs from what was captured",
            ));
        }
        let mut bytes = Vec::new();
        File::open(&self.db_path)
            .and_then(|mut f| f.read_to_end(&mut bytes))
            .map_err(|source| CaptureError::Io {
                path: self.db_path.clone(),
                source,
            })?;
        let lock = lock_pgno(self.page_size);
        let pages = bytes
            .chunks_exact(self.page_size as usize)
            .enumerate()
            .map(|(i, p)| (u32::try_from(i + 1).unwrap_or(u32::MAX), p.to_vec()))
            .filter(|(pgno, _)| *pgno != lock)
            .collect();
        Ok(Changes {
            commit: self.pages(),
            page_size: self.page_size,
            pages,
            pre_apply: DbChecksum::default(),
            post_apply: self.db,
        })
    }

    /// The database file this capture follows.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }
}

/// Frames up to the last commit seen so far: the pages, the database size,
/// bytes consumed and the running checksum after them.
/// Pages of the transactions committed so far (page number -> offset of its
/// newest version), the database size, where they end, and the running
/// checksum there.
type Committed = (BTreeMap<u32, u64>, u32, u64, (u32, u32));

fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

fn parse_header(h: &[u8], page_size: u32) -> Result<WalHeader, CaptureError> {
    let big_endian = match be32(h) {
        MAGIC_LE => false,
        MAGIC_BE => true,
        _ => return Err(CaptureError::Lost("not a WAL file")),
    };
    // A page size of 65536 is stored as 1.
    let stored = be32(&h[8..]);
    let size = if stored == 1 { 65_536 } else { stored };
    if size != page_size {
        return Err(CaptureError::Lost("WAL page size differs"));
    }
    Ok(WalHeader {
        big_endian,
        salt: (be32(&h[16..]), be32(&h[20..])),
    })
}

/// SQLite's WAL checksum over `data` (a multiple of 8 bytes), continuing
/// from `(s0, s1)`.
fn checksum(big_endian: bool, (mut s0, mut s1): (u32, u32), data: &[u8]) -> (u32, u32) {
    for pair in data.as_chunks::<8>().0 {
        let word = |b: &[u8]| {
            let a = [b[0], b[1], b[2], b[3]];
            if big_endian {
                u32::from_be_bytes(a)
            } else {
                u32::from_le_bytes(a)
            }
        };
        s0 = s0.wrapping_add(word(&pair[..4])).wrapping_add(s1);
        s1 = s1.wrapping_add(word(&pair[4..])).wrapping_add(s0);
    }
    (s0, s1)
}

/// Rebuilds a database at `path` (which must not exist) from a snapshot
/// file and the segments after it, verifying every checksum on the way.
/// Returns the database checksum at the end. See [`Restorer`] for the
/// streaming form, one file and one page in memory at a time.
///
/// On error `path` is left partly written: remove it before retrying. Files
/// without checksums are refused, and so is an empty chain.
pub fn restore(
    path: &Path,
    snapshot: Option<&crate::ltx::File>,
    segments: &[crate::ltx::File],
) -> Result<DbChecksum, RestoreError> {
    let mut restorer = Restorer::create(path, u64::MAX)?;
    if let Some(s) = snapshot {
        restorer.apply_file(s)?;
    }
    for seg in segments {
        restorer.apply_file(seg)?;
    }
    restorer.finish()
}

/// Builds a database file from a chain of LTX files, applying one file at a
/// time straight into the file: memory stays at one page (plus the file
/// being applied, compressed), whatever the database size, and the checksum
/// is updated page by page rather than recomputed.
///
/// Every file must carry checksums (a file without them could not be
/// checked against the chain), and no file may grow the database past
/// `max_bytes`.
pub struct Restorer {
    path: PathBuf,
    file: std::fs::File,
    page_size: u32,
    commit: u32,
    max_bytes: u64,
    db: DbChecksum,
    applied: usize,
    page: Vec<u8>,
    /// Set by a failed apply: the file and checksum are then half-updated,
    /// so nothing more may be applied.
    failed: bool,
}

impl std::fmt::Debug for Restorer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Restorer")
            .field("path", &self.path)
            .field("commit", &self.commit)
            .field("applied", &self.applied)
            .finish_non_exhaustive()
    }
}

impl Restorer {
    /// Creates `path` (which must not exist).
    ///
    /// # Errors
    /// The file exists or cannot be created.
    pub fn create(path: &Path, max_bytes: u64) -> Result<Self, RestoreError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|source| RestoreError::Io {
                path: path.to_owned(),
                source,
            })?;
        Ok(Self {
            path: path.to_owned(),
            file,
            page_size: 0,
            commit: 0,
            max_bytes,
            db: DbChecksum::default(),
            applied: 0,
            page: Vec::new(),
            failed: false,
        })
    }

    /// Applies an encoded LTX file, decoding it page by page.
    ///
    /// # Errors
    /// The file is invalid, does not follow the chain, or is too large.
    pub fn apply(&mut self, bytes: &[u8]) -> Result<(), RestoreError> {
        self.guarded(|r| r.apply_inner(bytes))
    }

    /// Runs one apply; a failure poisons the restorer.
    fn guarded(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<(), RestoreError>,
    ) -> Result<(), RestoreError> {
        if self.failed {
            return Err(RestoreError::Chain("an earlier file failed to apply"));
        }
        let result = f(self);
        self.failed = result.is_err();
        result
    }

    fn apply_inner(&mut self, bytes: &[u8]) -> Result<(), RestoreError> {
        // The header first: refuse a file before decompressing any of it.
        let header = bytes
            .get(..crate::ltx::HEADER_SIZE)
            .ok_or_else(|| RestoreError::Invalid("shorter than a header".into()))
            .and_then(|h| {
                crate::ltx::Header::decode(h).map_err(|e| RestoreError::Invalid(e.to_string()))
            })?;
        self.begin(&header)?;
        let trailer =
            match crate::ltx::visit(bytes, |_| Ok(()), |pgno, data| self.write_page(pgno, data)) {
                Ok((_, trailer)) => trailer,
                Err(crate::ltx::Visit::Ltx(e)) => return Err(RestoreError::Invalid(e.to_string())),
                Err(crate::ltx::Visit::Stopped(e)) => return Err(e),
            };
        self.end(trailer.post_apply_checksum)
    }

    /// Applies an already decoded file.
    ///
    /// # Errors
    /// As [`Self::apply`].
    pub fn apply_file(&mut self, file: &crate::ltx::File) -> Result<(), RestoreError> {
        self.guarded(|r| {
            r.begin(&file.header)?;
            for (pgno, data) in &file.pages {
                // A decoded file is checked by `ltx::decode`, but this one
                // may have been built by hand.
                if *pgno == 0 || *pgno > file.header.commit || data.len() != r.page.len() {
                    return Err(RestoreError::Invalid(format!("page {pgno} out of range")));
                }
                r.write_page(*pgno, data)?;
            }
            r.end(file.trailer.post_apply_checksum)
        })
    }

    /// Checks a file's header against the chain, and resizes the database
    /// to its page count.
    fn begin(&mut self, h: &crate::ltx::Header) -> Result<(), RestoreError> {
        if h.no_checksum() {
            // Our encoder never writes these; one in the bucket would skip
            // every chain check.
            return Err(RestoreError::Chain("file without checksums"));
        }
        if self.page_size == 0 {
            self.page_size = h.page_size;
            self.page = vec![0; h.page_size as usize];
        } else if h.page_size != self.page_size {
            return Err(RestoreError::Chain("page size changed"));
        }
        if u64::from(h.commit).saturating_mul(u64::from(h.page_size)) > self.max_bytes {
            return Err(RestoreError::TooLarge(self.max_bytes));
        }
        if self.applied > 0 && h.pre_apply_checksum != self.db.value() {
            return Err(RestoreError::Chain(
                "pre-apply checksum does not match the database",
            ));
        }
        // Pages the database grows into start out as zeros; pages it
        // shrinks away leave the checksum.
        if h.commit > self.commit {
            let zeros = vec![0u8; self.page_size as usize];
            for p in self.commit.saturating_add(1)..=h.commit {
                self.db.toggle(p, &zeros, self.page_size);
            }
        } else {
            for p in h.commit.saturating_add(1)..=self.commit {
                self.read_page(p)?;
                let page = std::mem::take(&mut self.page);
                self.db.toggle(p, &page, self.page_size);
                self.page = page;
            }
        }
        self.file
            .set_len(u64::from(h.commit) * u64::from(self.page_size))
            .map_err(|source| self.io(source))?;
        self.commit = h.commit;
        Ok(())
    }

    fn write_page(&mut self, pgno: u32, data: &[u8]) -> Result<(), RestoreError> {
        use std::io::{Seek, SeekFrom, Write};
        self.read_page(pgno)?;
        let old = std::mem::take(&mut self.page);
        self.db.toggle(pgno, &old, self.page_size);
        self.page = old;
        self.db.toggle(pgno, data, self.page_size);
        let at = u64::from(pgno - 1) * u64::from(self.page_size);
        self.file
            .seek(SeekFrom::Start(at))
            .and_then(|_| self.file.write_all(data))
            .map_err(|source| self.io(source))
    }

    /// Reads page `pgno` (within the current size) into `self.page`.
    fn read_page(&mut self, pgno: u32) -> Result<(), RestoreError> {
        use std::io::{Read, Seek, SeekFrom};
        let at = u64::from(pgno - 1) * u64::from(self.page_size);
        let mut page = std::mem::take(&mut self.page);
        let read = self
            .file
            .seek(SeekFrom::Start(at))
            .and_then(|_| self.file.read_exact(&mut page));
        self.page = page;
        read.map_err(|source| self.io(source))
    }

    /// Every file carries checksums (`begin` refuses others).
    fn end(&mut self, post_apply: u64) -> Result<(), RestoreError> {
        if post_apply != self.db.value() {
            return Err(RestoreError::Chain(
                "post-apply checksum does not match the pages",
            ));
        }
        self.applied += 1;
        Ok(())
    }

    /// Syncs the file; returns the database checksum.
    ///
    /// # Errors
    /// Nothing was applied, or the sync failed.
    pub fn finish(self) -> Result<DbChecksum, RestoreError> {
        if self.applied == 0 {
            return Err(RestoreError::Chain("nothing to restore"));
        }
        self.file.sync_all().map_err(|source| RestoreError::Io {
            path: self.path.clone(),
            source,
        })?;
        Ok(self.db)
    }

    fn io(&self, source: std::io::Error) -> RestoreError {
        RestoreError::Io {
            path: self.path.clone(),
            source,
        }
    }
}

/// Why a restore failed. The actor must not run on a partial history.
#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    /// The files do not form one history.
    #[error("restore chain broken: {0}")]
    Chain(&'static str),
    /// A file is not valid LTX.
    #[error("invalid file: {0}")]
    Invalid(String),
    /// A file would grow the database past the size cap.
    #[error("the database would exceed {0} bytes")]
    TooLarge(u64),
    /// Writing the database failed.
    #[error("writing {path}: {source}")]
    Io {
        /// The file.
        path: PathBuf,
        /// The cause.
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_matches_sqlite_reference_shape() {
        // Two words: s0 = 0 + 1 + 0 = 1; s1 = 0 + 2 + 1 = 3 (little-endian).
        let data = [1, 0, 0, 0, 2, 0, 0, 0];
        assert_eq!(checksum(false, (0, 0), &data), (1, 3));
        let data = [0, 0, 0, 1, 0, 0, 0, 2];
        assert_eq!(checksum(true, (0, 0), &data), (1, 3));
    }
}
