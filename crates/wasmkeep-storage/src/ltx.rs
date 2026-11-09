//! LTX v3 transaction files: the replication format (ADR 0005).
//!
//! One file holds the pages one or more transactions changed, LZ4-compressed,
//! with a rolling checksum of the whole database before and after, and a
//! CRC-64 of the file itself. A file whose first transaction is 1 is a
//! snapshot: every page of the database, in order.
//!
//! Layout (all integers big-endian):
//!
//! ```text
//! header   100 bytes  "LTX1", flags, page size, commit (db size in pages),
//!                     min/max txid, timestamp, pre-apply checksum, WAL
//!                     offset/size/salts, node id, 20 reserved bytes
//! pages    per page:  pgno u32, flags u16 (bit 0: size follows),
//!                     size u32, LZ4 block
//! end      6 zero bytes
//! index    per page:  uvarint pgno, offset, size; uvarint 0; u64 index size
//! trailer  post-apply checksum u64, file checksum u64
//! ```
//!
//! The file checksum is CRC-64/ISO over every byte before it, except that
//! page data counts uncompressed. Written to match `github.com/superfly/ltx`
//! v0.5.3; files it writes decode here and vice versa (see the conformance
//! fixtures in `tests/ltx`).

use std::fmt;

/// `"LTX1"`, the first four bytes of every file.
pub const MAGIC: &[u8; 4] = b"LTX1";
/// Header length in bytes.
pub const HEADER_SIZE: usize = 100;
/// Page header length in bytes.
pub const PAGE_HEADER_SIZE: usize = 6;
/// Trailer length in bytes.
pub const TRAILER_SIZE: usize = 16;
/// Set on every checksum, so that a valid checksum is never zero.
pub const CHECKSUM_FLAG: u64 = 1 << 63;
/// Header flag: database checksums are not tracked.
pub const HEADER_FLAG_NO_CHECKSUM: u32 = 1 << 1;
/// Page header flag: a 4-byte size and an LZ4 block follow.
const PAGE_FLAG_SIZE: u16 = 1;
/// The byte SQLite reserves for locking; the page holding it is never stored.
const PENDING_BYTE: u64 = 0x4000_0000;

/// Why a file could not be written or read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LtxError {
    /// The file is not LTX, is truncated, or breaks a format rule.
    #[error("invalid LTX file: {0}")]
    Invalid(String),
    /// The file checksum does not match its contents.
    #[error("LTX file checksum mismatch")]
    ChecksumMismatch,
    /// A valid file this codec does not read (LZ4 frame-format pages, from
    /// old writers).
    #[error("unsupported LTX file: {0}")]
    Unsupported(String),
}

fn invalid(msg: impl Into<String>) -> LtxError {
    LtxError::Invalid(msg.into())
}

/// The page holding SQLite's lock byte, for a page size.
pub fn lock_pgno(page_size: u32) -> u32 {
    u32::try_from(PENDING_BYTE / u64::from(page_size.max(1))).unwrap_or(u32::MAX) + 1
}

fn is_valid_page_size(size: u32) -> bool {
    (512..=65_536).contains(&size) && size.is_power_of_two()
}

/// File header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Header {
    /// `HEADER_FLAG_NO_CHECKSUM` or 0.
    pub flags: u32,
    /// Database page size in bytes.
    pub page_size: u32,
    /// Database size after the transactions, in pages.
    pub commit: u32,
    /// First transaction in the file.
    pub min_txid: u64,
    /// Last transaction in the file.
    pub max_txid: u64,
    /// Milliseconds since the Unix epoch.
    pub timestamp_ms: i64,
    /// Database checksum before applying the file (0 on snapshots).
    pub pre_apply_checksum: u64,
    /// Offset in the WAL the file was made from; 0 if not from a WAL.
    pub wal_offset: i64,
    /// Size of that WAL segment; 0 if not from a WAL.
    pub wal_size: i64,
    /// WAL header salt 1; 0 if not from a WAL.
    pub wal_salt1: u32,
    /// WAL header salt 2; 0 if not from a WAL.
    pub wal_salt2: u32,
    /// Id of the node that wrote the file; 0 if unset.
    pub node_id: u64,
}

impl Header {
    /// A file starting at transaction 1 holds the whole database.
    pub fn is_snapshot(&self) -> bool {
        self.min_txid == 1
    }

    /// Whether database checksums are left out.
    pub fn no_checksum(&self) -> bool {
        self.flags & HEADER_FLAG_NO_CHECKSUM != 0
    }

    /// Checks the format rules the reference implementation enforces.
    pub fn validate(&self) -> Result<(), LtxError> {
        if self.flags & !HEADER_FLAG_NO_CHECKSUM != 0 {
            return Err(invalid(format!("invalid flags: {:#010x}", self.flags)));
        }
        if !is_valid_page_size(self.page_size) {
            return Err(invalid(format!("invalid page size: {}", self.page_size)));
        }
        if self.min_txid == 0 || self.max_txid == 0 {
            return Err(invalid("transaction ids required"));
        }
        if self.min_txid > self.max_txid {
            return Err(invalid("transaction ids out of order"));
        }
        if self.wal_offset < 0 || self.wal_size < 0 {
            return Err(invalid("negative WAL offset or size"));
        }
        if (self.wal_salt1 != 0 || self.wal_salt2 != 0) && self.wal_offset == 0 {
            return Err(invalid("WAL offset required if a salt is set"));
        }
        if self.wal_offset == 0 && self.wal_size != 0 {
            return Err(invalid("WAL offset required if a WAL size is set"));
        }
        if self.is_snapshot() || self.no_checksum() {
            if self.pre_apply_checksum != 0 {
                return Err(invalid("pre-apply checksum not allowed"));
            }
        } else if self.pre_apply_checksum & CHECKSUM_FLAG == 0 {
            return Err(invalid("pre-apply checksum required"));
        }
        Ok(())
    }

    /// The 100-byte encoding.
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut b = [0u8; HEADER_SIZE];
        b[0..4].copy_from_slice(MAGIC);
        b[4..8].copy_from_slice(&self.flags.to_be_bytes());
        b[8..12].copy_from_slice(&self.page_size.to_be_bytes());
        b[12..16].copy_from_slice(&self.commit.to_be_bytes());
        b[16..24].copy_from_slice(&self.min_txid.to_be_bytes());
        b[24..32].copy_from_slice(&self.max_txid.to_be_bytes());
        b[32..40].copy_from_slice(&self.timestamp_ms.to_be_bytes());
        b[40..48].copy_from_slice(&self.pre_apply_checksum.to_be_bytes());
        b[48..56].copy_from_slice(&self.wal_offset.to_be_bytes());
        b[56..64].copy_from_slice(&self.wal_size.to_be_bytes());
        b[64..68].copy_from_slice(&self.wal_salt1.to_be_bytes());
        b[68..72].copy_from_slice(&self.wal_salt2.to_be_bytes());
        b[72..80].copy_from_slice(&self.node_id.to_be_bytes());
        b
    }

    /// Decodes and validates a header.
    pub fn decode(b: &[u8]) -> Result<Self, LtxError> {
        let mut r = Reader::new(b);
        if r.take(4)? != MAGIC {
            return Err(invalid("not an LTX file"));
        }
        let header = Self {
            flags: r.u32()?,
            page_size: r.u32()?,
            commit: r.u32()?,
            min_txid: r.u64()?,
            max_txid: r.u64()?,
            timestamp_ms: r.i64()?,
            pre_apply_checksum: r.u64()?,
            wal_offset: r.i64()?,
            wal_size: r.i64()?,
            wal_salt1: r.u32()?,
            wal_salt2: r.u32()?,
            node_id: r.u64()?,
        };
        header.validate()?;
        Ok(header)
    }
}

/// File trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trailer {
    /// Database checksum after applying the file.
    pub post_apply_checksum: u64,
    /// CRC-64/ISO of the file (with `CHECKSUM_FLAG`).
    pub file_checksum: u64,
}

/// Checksum of one page, as summed into the database checksum.
pub fn page_checksum(pgno: u32, data: &[u8]) -> u64 {
    let mut h = Crc64::new();
    h.write(&pgno.to_be_bytes());
    h.write(data);
    CHECKSUM_FLAG | h.sum()
}

/// Rolling checksum of a database: XOR of its page checksums, lock page
/// excluded. `ChecksumFlag` alone for an empty database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DbChecksum(u64);

impl Default for DbChecksum {
    fn default() -> Self {
        Self(CHECKSUM_FLAG)
    }
}

impl DbChecksum {
    /// Continues from a stored checksum.
    pub fn from_value(value: u64) -> Self {
        Self(value | CHECKSUM_FLAG)
    }

    /// Adds a page (or removes it: XOR is its own inverse).
    pub fn toggle(&mut self, pgno: u32, data: &[u8], page_size: u32) {
        if pgno != lock_pgno(page_size) {
            self.0 = CHECKSUM_FLAG | (self.0 ^ page_checksum(pgno, data));
        }
    }

    /// The checksum value.
    pub fn value(self) -> u64 {
        self.0
    }
}

/// Writes one file into memory.
#[derive(Debug)]
pub struct Encoder {
    out: Vec<u8>,
    hash: Crc64,
    header: Header,
    index: Vec<(u32, u64, u64)>,
    prev_pgno: u32,
}

impl Encoder {
    /// Starts a file. Pages follow with [`add_page`](Self::add_page).
    pub fn new(header: Header) -> Result<Self, LtxError> {
        header.validate()?;
        let mut enc = Self {
            out: Vec::new(),
            hash: Crc64::new(),
            header,
            index: Vec::new(),
            prev_pgno: 0,
        };
        enc.write(&header.encode());
        Ok(enc)
    }

    /// Adds a page. Pages go in ascending order; a snapshot has every page
    /// from 1 to `commit` except the lock page.
    pub fn add_page(&mut self, pgno: u32, data: &[u8]) -> Result<(), LtxError> {
        let h = &self.header;
        if pgno == 0 || pgno > h.commit {
            return Err(invalid(format!(
                "page {pgno} out of range 1..={}",
                h.commit
            )));
        }
        if data.len() != h.page_size as usize {
            return Err(invalid(format!("page {pgno} is {} bytes", data.len())));
        }
        let lock = lock_pgno(h.page_size);
        if pgno == lock {
            return Err(invalid("the lock page is never stored"));
        }
        check_order(h.is_snapshot(), lock, self.prev_pgno, pgno)?;

        let offset = self.out.len() as u64;
        let compressed = lz4_flex::block::compress(data);
        let size = u32::try_from(compressed.len()).map_err(|_| invalid("page too large"))?;
        self.write(&pgno.to_be_bytes());
        self.write(&PAGE_FLAG_SIZE.to_be_bytes());
        self.write(&size.to_be_bytes());
        self.out.extend_from_slice(&compressed);
        self.hash.write(data); // uncompressed, as the reference does
        self.index
            .push((pgno, offset, self.out.len() as u64 - offset));
        self.prev_pgno = pgno;
        Ok(())
    }

    /// Ends the file with the database checksum after applying it (ignored
    /// when the header says no checksums).
    pub fn finish(mut self, post_apply: DbChecksum) -> Result<Vec<u8>, LtxError> {
        if self.index.is_empty() {
            return Err(invalid("a file needs at least one page"));
        }
        if self.header.is_snapshot()
            && self.prev_pgno != last_pgno(self.header.commit, self.header.page_size)
        {
            return Err(invalid("snapshot is missing pages"));
        }
        self.write(&[0u8; PAGE_HEADER_SIZE]);

        let start = self.out.len();
        let mut index = Vec::new();
        for &(pgno, offset, size) in &self.index {
            uvarint(&mut index, u64::from(pgno));
            uvarint(&mut index, offset);
            uvarint(&mut index, size);
        }
        uvarint(&mut index, 0);
        self.write(&index);
        let len = (self.out.len() - start) as u64;
        self.write(&len.to_be_bytes());

        let post = if self.header.no_checksum() {
            0
        } else {
            post_apply.value()
        };
        self.write(&post.to_be_bytes());
        let file = CHECKSUM_FLAG | self.hash.sum();
        self.out.extend_from_slice(&file.to_be_bytes());
        Ok(self.out)
    }

    fn write(&mut self, b: &[u8]) {
        self.out.extend_from_slice(b);
        self.hash.write(b);
    }
}

/// The highest page number a snapshot of `commit` pages ends with.
fn last_pgno(commit: u32, page_size: u32) -> u32 {
    if commit == lock_pgno(page_size) {
        commit - 1
    } else {
        commit
    }
}

fn check_order(snapshot: bool, lock: u32, prev: u32, pgno: u32) -> Result<(), LtxError> {
    let ok = if snapshot {
        let next = if prev + 1 == lock { prev + 2 } else { prev + 1 };
        pgno == next
    } else {
        pgno > prev
    };
    if ok {
        Ok(())
    } else {
        Err(invalid(format!("page {pgno} out of order after {prev}")))
    }
}

/// A decoded, verified file.
#[derive(Clone, PartialEq, Eq)]
pub struct File {
    /// The header.
    pub header: Header,
    /// `(pgno, data)` in file order.
    pub pages: Vec<(u32, Vec<u8>)>,
    /// The trailer.
    pub trailer: Trailer,
}

impl fmt::Debug for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("File")
            .field("header", &self.header)
            .field("pages", &self.pages.iter().map(|p| p.0).collect::<Vec<_>>())
            .field("trailer", &self.trailer)
            .finish()
    }
}

/// Decodes a whole file and verifies everything the reference verifies:
/// structure, page order, the page index, the file checksum, and (for
/// snapshots) the post-apply database checksum.
pub fn decode(bytes: &[u8]) -> Result<File, LtxError> {
    let mut pages = Vec::new();
    let (header, trailer) = visit(
        bytes,
        |_| Ok::<(), std::convert::Infallible>(()),
        |pgno, data| {
            pages.push((pgno, data.to_vec()));
            Ok(())
        },
    )
    .map_err(|e| match e {
        Visit::Ltx(e) => e,
        Visit::Stopped(never) => match never {},
    })?;
    Ok(File {
        header,
        pages,
        trailer,
    })
}

/// Why [`visit`] stopped.
#[derive(Debug)]
pub enum Visit<E> {
    /// The file is invalid.
    Ltx(LtxError),
    /// A callback refused.
    Stopped(E),
}

impl<E> From<LtxError> for Visit<E> {
    fn from(e: LtxError) -> Self {
        Self::Ltx(e)
    }
}

/// Decodes a file page by page, one page in memory at a time: `on_header`
/// sees the header before any page is decompressed, `on_page` each page in
/// file order. Checks everything [`decode`] checks; the file checksum and
/// trailer only at the end, so a caller writing pages somewhere must treat
/// them as unverified until this returns `Ok`.
///
/// # Errors
/// [`Visit::Ltx`] for an invalid file, [`Visit::Stopped`] for a callback's
/// error.
pub fn visit<E>(
    bytes: &[u8],
    on_header: impl FnOnce(&Header) -> Result<(), E>,
    mut on_page: impl FnMut(u32, &[u8]) -> Result<(), E>,
) -> Result<(Header, Trailer), Visit<E>> {
    let mut r = Reader::new(bytes);
    let mut hash = Crc64::new();
    let head = r.take(HEADER_SIZE)?;
    hash.write(head);
    let header = Header::decode(head)?;
    on_header(&header).map_err(Visit::Stopped)?;
    let lock = lock_pgno(header.page_size);

    let mut offsets = Vec::new();
    let mut db = DbChecksum::default();
    let mut prev = 0;
    let mut data = vec![0u8; header.page_size as usize];
    loop {
        let offset = r.pos as u64;
        let ph = r.take(PAGE_HEADER_SIZE)?;
        hash.write(ph);
        let pgno = u32::from_be_bytes([ph[0], ph[1], ph[2], ph[3]]);
        let flags = u16::from_be_bytes([ph[4], ph[5]]);
        if pgno == 0 && flags == 0 {
            break;
        }
        if pgno == 0 || flags & !PAGE_FLAG_SIZE != 0 {
            return Err(invalid(format!("bad page header ({pgno}, {flags:#x})")).into());
        }
        if flags & PAGE_FLAG_SIZE == 0 {
            return Err(LtxError::Unsupported("LZ4 frame-format page".into()).into());
        }
        if pgno > header.commit || pgno == lock {
            return Err(invalid(format!("page {pgno} not allowed")).into());
        }
        check_order(header.is_snapshot(), lock, prev, pgno)?;
        let size_bytes = r.take(4)?;
        hash.write(size_bytes);
        let size = u32::from_be_bytes([size_bytes[0], size_bytes[1], size_bytes[2], size_bytes[3]]);
        let compressed = r.take(size as usize)?;
        let n = lz4_flex::block::decompress_into(compressed, &mut data)
            .map_err(|e| invalid(format!("page {pgno}: {e}")))?;
        if n != data.len() {
            return Err(invalid(format!("page {pgno} decompressed to {n} bytes")).into());
        }
        hash.write(&data);
        db.toggle(pgno, &data, header.page_size);
        offsets.push((pgno, offset, r.pos as u64 - offset));
        on_page(pgno, &data).map_err(Visit::Stopped)?;
        prev = pgno;
    }
    if offsets.is_empty() {
        return Err(invalid("no pages").into());
    }
    if header.is_snapshot() && prev != last_pgno(header.commit, header.page_size) {
        return Err(invalid("snapshot is missing pages").into());
    }

    let index_start = r.pos;
    for &(pgno, offset, size) in &offsets {
        let entry = (r.uvarint()?, r.uvarint()?, r.uvarint()?);
        if entry != (u64::from(pgno), offset, size) {
            return Err(invalid("page index does not match the pages").into());
        }
    }
    if r.uvarint()? != 0 {
        return Err(invalid("page index has extra entries").into());
    }
    let index_len = (r.pos - index_start) as u64;
    if r.u64()? != index_len {
        return Err(invalid("wrong page index size").into());
    }
    hash.write(&bytes[index_start..r.pos]);

    let post_bytes = r.take(8)?;
    hash.write(post_bytes);
    let trailer = Trailer {
        post_apply_checksum: u64::from_be_bytes(post_bytes.try_into().unwrap_or_default()),
        file_checksum: r.u64()?,
    };
    if r.pos != bytes.len() {
        return Err(invalid("data after the trailer").into());
    }
    if trailer.file_checksum & CHECKSUM_FLAG == 0 {
        return Err(invalid("bad file checksum format").into());
    }
    if CHECKSUM_FLAG | hash.sum() != trailer.file_checksum {
        return Err(LtxError::ChecksumMismatch.into());
    }
    if header.no_checksum() {
        if trailer.post_apply_checksum != 0 {
            return Err(invalid("post-apply checksum not allowed").into());
        }
    } else if trailer.post_apply_checksum & CHECKSUM_FLAG == 0 {
        return Err(invalid("post-apply checksum required").into());
    } else if header.is_snapshot() && trailer.post_apply_checksum != db.value() {
        return Err(invalid("snapshot post-apply checksum does not match its pages").into());
    }
    Ok((header, trailer))
}

/// `<min>-<max>.ltx` with 16-digit lowercase hex transaction ids.
pub fn file_name(min_txid: u64, max_txid: u64) -> String {
    format!("{min_txid:016x}-{max_txid:016x}.ltx")
}

/// Parses [`file_name`]'s format.
pub fn parse_file_name(name: &str) -> Option<(u64, u64)> {
    let (min, rest) = name.strip_suffix(".ltx")?.split_once('-')?;
    let hex16 = |s: &str| {
        (s.len() == 16 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
            .then(|| u64::from_str_radix(s, 16).ok())
            .flatten()
    };
    Some((hex16(min)?, hex16(rest)?))
}

fn uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], LtxError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.b.len())
            .ok_or_else(|| invalid("truncated"))?;
        let s = &self.b[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u32(&mut self) -> Result<u32, LtxError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> Result<u64, LtxError> {
        let mut a = [0u8; 8];
        a.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(a))
    }

    fn i64(&mut self) -> Result<i64, LtxError> {
        self.u64().map(|v| v as i64)
    }

    fn uvarint(&mut self) -> Result<u64, LtxError> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.take(1)?[0];
            v |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(invalid("varint too long"))
    }
}

/// CRC-64 with the ISO 3309 polynomial, reflected, as Go's `hash/crc64`.
#[derive(Debug, Clone)]
struct Crc64(u64);

const fn crc64_table() -> [u64; 256] {
    const POLY: u64 = 0xD800_0000_0000_0000;
    let mut table = [0u64; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC64_TABLE: [u64; 256] = crc64_table();

impl Crc64 {
    fn new() -> Self {
        Self(!0)
    }

    fn write(&mut self, data: &[u8]) {
        let mut crc = self.0;
        for &b in data {
            crc = CRC64_TABLE[((crc as u8) ^ b) as usize] ^ (crc >> 8);
        }
        self.0 = crc;
    }

    fn sum(&self) -> u64 {
        !self.0
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // test code

    use super::*;

    fn crc(data: &[u8]) -> u64 {
        let mut h = Crc64::new();
        h.write(data);
        h.sum()
    }

    #[test]
    fn crc64_matches_go() {
        // Go: crc64.Checksum([]byte("123456789"), crc64.MakeTable(crc64.ISO))
        assert_eq!(crc(b"123456789"), 0xb909_56c7_75a4_1001);
        assert_eq!(crc(b""), 0);
        let mut h = Crc64::new();
        h.write(b"1234");
        h.write(b"56789");
        assert_eq!(h.sum(), 0xb909_56c7_75a4_1001, "streaming");
    }

    #[test]
    fn varints_round_trip() {
        for v in [0, 1, 127, 128, 300, u64::from(u32::MAX), u64::MAX] {
            let mut b = Vec::new();
            uvarint(&mut b, v);
            assert_eq!(Reader::new(&b).uvarint().unwrap(), v);
        }
    }

    fn page(pgno: u32, size: u32) -> Vec<u8> {
        (0..size).map(|i| (i as u8) ^ (pgno as u8)).collect()
    }

    fn snapshot(commit: u32) -> (Vec<u8>, DbChecksum) {
        let header = Header {
            page_size: 512,
            commit,
            min_txid: 1,
            max_txid: 1,
            timestamp_ms: 1_700_000_000_000,
            ..Header::default()
        };
        let mut enc = Encoder::new(header).unwrap();
        let mut db = DbChecksum::default();
        for pgno in 1..=commit {
            let p = page(pgno, 512);
            enc.add_page(pgno, &p).unwrap();
            db.toggle(pgno, &p, 512);
        }
        (enc.finish(db).unwrap(), db)
    }

    #[test]
    fn snapshots_round_trip() {
        let (bytes, db) = snapshot(5);
        let file = decode(&bytes).unwrap();
        assert_eq!(file.header.commit, 5);
        assert_eq!(file.pages.len(), 5);
        assert_eq!(file.pages[2], (3, page(3, 512)));
        assert_eq!(file.trailer.post_apply_checksum, db.value());
    }

    #[test]
    fn incremental_files_carry_both_checksums() {
        let (_, before) = snapshot(5);
        let mut after = before;
        let new3 = vec![7u8; 512];
        after.toggle(3, &page(3, 512), 512);
        after.toggle(3, &new3, 512);
        let header = Header {
            page_size: 512,
            commit: 5,
            min_txid: 2,
            max_txid: 2,
            pre_apply_checksum: before.value(),
            ..Header::default()
        };
        let mut enc = Encoder::new(header).unwrap();
        enc.add_page(3, &new3).unwrap();
        let file = decode(&enc.finish(after).unwrap()).unwrap();
        assert_eq!(file.header.pre_apply_checksum, before.value());
        assert_eq!(file.trailer.post_apply_checksum, after.value());
        assert_eq!(file.pages, vec![(3, new3)]);
    }

    #[test]
    fn corruption_is_detected() {
        let (bytes, _) = snapshot(3);
        let good = decode(&bytes).unwrap();
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x01;
            // The checksum covers page data uncompressed (as the reference
            // does), so a flip inside an LZ4 block that decodes to the same
            // page is harmless; anything else must be rejected.
            if let Ok(file) = decode(&bad) {
                assert_eq!(
                    file, good,
                    "flipping byte {i} changed the content unnoticed"
                );
            }
        }
        for n in 0..bytes.len() {
            assert!(decode(&bytes[..n]).is_err(), "truncated to {n} bytes");
        }
    }

    #[test]
    fn format_rules_are_enforced() {
        let header = Header {
            page_size: 512,
            commit: 3,
            min_txid: 1,
            max_txid: 1,
            ..Header::default()
        };
        let mut enc = Encoder::new(header).unwrap();
        assert!(
            enc.add_page(2, &page(2, 512)).is_err(),
            "snapshots start at 1"
        );
        enc.add_page(1, &page(1, 512)).unwrap();
        assert!(
            enc.add_page(3, &page(3, 512)).is_err(),
            "no gaps in snapshots"
        );
        assert!(enc.add_page(2, &[0; 100]).is_err(), "page size");
        assert!(enc.add_page(4, &page(4, 512)).is_err(), "beyond commit");
        enc.add_page(2, &page(2, 512)).unwrap();
        assert!(enc.finish(DbChecksum::default()).is_err(), "page 3 missing");

        let bad = |h: Header| h.validate().is_err();
        let ok = Header {
            page_size: 4096,
            commit: 1,
            min_txid: 2,
            max_txid: 3,
            pre_apply_checksum: CHECKSUM_FLAG | 5,
            ..Header::default()
        };
        assert!(ok.validate().is_ok());
        assert!(bad(Header {
            page_size: 1000,
            ..ok
        }));
        assert!(bad(Header { min_txid: 4, ..ok }));
        assert!(bad(Header {
            pre_apply_checksum: 0,
            ..ok
        }));
        assert!(bad(Header { flags: 1, ..ok }));
        assert!(
            bad(Header { min_txid: 1, ..ok }),
            "snapshots have no pre-apply checksum"
        );
    }

    #[test]
    fn the_lock_page_is_skipped_in_snapshots() {
        let lock = lock_pgno(65_536);
        assert_eq!(lock, 16_385);
        assert!(check_order(true, lock, lock - 1, lock + 1).is_ok());
        assert!(check_order(true, lock, lock - 1, lock).is_err());
        assert_eq!(last_pgno(lock, 65_536), lock - 1);
    }

    #[test]
    fn file_names() {
        assert_eq!(file_name(1, 0x2a), "0000000000000001-000000000000002a.ltx");
        assert_eq!(
            parse_file_name("0000000000000001-000000000000002a.ltx"),
            Some((1, 0x2a))
        );
        assert_eq!(
            parse_file_name("0000000000000001-000000000000002A.ltx"),
            None
        );
        assert_eq!(parse_file_name("1-2.ltx"), None);
    }
}
