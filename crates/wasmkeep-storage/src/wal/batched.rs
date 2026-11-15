//! Boundary capture is deliberately separate from strict's capture loop.
use super::*;
use std::collections::VecDeque;

/// Default upper bound for an encoded batched segment (1 MiB).
pub const DEFAULT_SEGMENT_BYTES: u64 = 1 << 20;

/// A committed mailbox/flush boundary. Counts are relative to the captured
/// position when returned; call `boundaries` again after a partial capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalBoundary {
    /// Process-local capture identity, stable across checkpoints and unique across restarts.
    pub session: u64,
    /// Identity within this capture session; never resets at a checkpoint.
    pub sequence: u64,
    /// WAL byte offset immediately after a commit frame.
    pub offset: u64,
    /// Database size at this boundary, in pages.
    pub commit: u32,
    /// Distinct pages the segment would carry, excluding truncated/lock pages.
    pub distinct_pages: usize,
    /// Conservative encoded LTX size, including worst-case LZ4 and index bytes.
    pub estimated_bytes: u64,
}

/// The next segment's extent and reservation estimate, without consumption.
/// Reserve the node budget against `boundary.estimated_bytes.min(cap)` before
/// capture. Appending marks does not invalidate it; consuming its prefix does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundaryPlan {
    first_sequence: u64,
    /// Last included boundary and cumulative segment estimate.
    pub boundary: WalBoundary,
    /// Number of whole marked events included.
    pub boundaries: usize,
    /// One event exceeds the estimate cap and must be uploaded alone.
    pub oversize: bool,
}

/// One capture, ending at a recorded boundary. The caller must encode and
/// hand off `changes` without cancellation; discarding it breaks the chain.
#[derive(Debug)]
pub struct BoundaryChanges {
    /// Pages and checksums to encode.
    pub changes: Changes,
    /// Last boundary included, with counts relative to this segment's start.
    pub boundary: WalBoundary,
    /// Number of recorded boundaries included (an oversize event takes one).
    pub boundaries: usize,
    /// The first event alone exceeds the configured estimate cap.
    pub oversize: bool,
}

/// Upper bound for `Changes::to_ltx`, not raw WAL size. Every page is LZ4
/// compressed, even if it expands. The index uses at most three ten-byte
/// varints per page; its terminator and length also count.
pub fn estimated_segment_bytes(page_size: u32, distinct_pages: usize) -> u64 {
    let fixed =
        crate::ltx::HEADER_SIZE + crate::ltx::PAGE_HEADER_SIZE + 1 + 8 + crate::ltx::TRAILER_SIZE;
    let per_page = lz4_flex::block::get_maximum_output_size(page_size as usize)
        + crate::ltx::PAGE_HEADER_SIZE
        + 4
        + 30;
    (fixed as u64).saturating_add((distinct_pages as u64).saturating_mul(per_page as u64))
}

#[derive(Debug, Clone, Copy, Default)]
struct Cursor {
    offset: u64,
    header: Option<WalHeader>,
    running: (u32, u32),
}

#[derive(Debug)]
struct Event {
    cursor: Cursor,
    commit: u32,
    pages: BTreeMap<u32, u64>,
    boundary: WalBoundary,
}

#[derive(Debug, Default)]
pub(super) struct BoundaryScan {
    pub(super) sequence: u64,
    cursor: Cursor,
    events: VecDeque<Event>,
    // Only page locations, never payloads. Updated incrementally at a mark;
    // rebuilt from retained event deltas after a partial capture.
    pages: BTreeMap<u32, u64>,
}

impl WalCapture {
    /// Scan newly committed frames without advancing the captured checksum or
    /// position. Call only between mailbox jobs or at a flush point. Several
    /// transactions since the previous mark form one indivisible event.
    /// A read-only mark returns the last pending boundary without adding one.
    pub fn mark_boundary(&mut self) -> Result<Option<WalBoundary>, CaptureError> {
        if self.scan.events.is_empty() {
            self.scan.cursor = Cursor {
                offset: self.offset,
                header: self.header,
                running: self.running,
            };
        }
        let Some((pages, commit, cursor)) = self.scan_since(self.scan.cursor)? else {
            return Ok(self.scan.events.back().map(|e| e.boundary));
        };
        self.scan
            .pages
            .extend(pages.iter().map(|(&p, &at)| (p, at)));
        self.scan.sequence = self
            .scan
            .sequence
            .checked_add(1)
            .ok_or(CaptureError::Lost("boundary sequence exhausted"))?;
        let boundary = self.boundary(self.scan.sequence, cursor.offset, commit, &self.scan.pages);
        self.scan.cursor = cursor;
        self.scan.events.push_back(Event {
            cursor,
            commit,
            pages,
            boundary,
        });
        Ok(Some(boundary))
    }

    /// Pending boundaries and their current cumulative distinct-page counts.
    pub fn boundaries(&self) -> impl Iterator<Item = WalBoundary> + '_ {
        self.scan.events.iter().map(|e| e.boundary)
    }

    /// Whether any marked commits have yet to be captured.
    pub fn has_pending_boundaries(&self) -> bool {
        !self.scan.events.is_empty()
    }

    /// Checks for committed frames beyond the captured position, including
    /// writes not yet marked by the mailbox. Does not create a boundary.
    pub fn has_uncaptured_commits(&self) -> Result<bool, CaptureError> {
        if self.has_pending_boundaries() {
            return Ok(true);
        }
        Ok(self
            .scan_since(Cursor {
                offset: self.offset,
                header: self.header,
                running: self.running,
            })?
            .is_some())
    }

    /// Plan the next capped capture without consuming WAL or reading pages.
    pub fn next_capture(&self, byte_cap: u64) -> Option<BoundaryPlan> {
        let mut count: usize = 0;
        for (i, event) in self.scan.events.iter().enumerate() {
            let alone = self.boundary(
                event.boundary.sequence,
                event.cursor.offset,
                event.commit,
                &event.pages,
            );
            if alone.estimated_bytes > byte_cap {
                if i == 0 {
                    count = 1;
                }
                break;
            }
            if event.boundary.estimated_bytes <= byte_cap {
                count = i + 1;
            }
        }
        let last = count.checked_sub(1).and_then(|i| self.scan.events.get(i))?;
        Some(BoundaryPlan {
            first_sequence: self.scan.events.front()?.boundary.sequence,
            boundary: last.boundary,
            boundaries: count,
            oversize: last.boundary.estimated_bytes > byte_cap,
        })
    }

    /// Capture the longest prefix of whole events fitting `byte_cap`. If the
    /// first event alone exceeds it, capture that event alone. Stop before an
    /// oversize event even if a later shrink could make the union smaller.
    /// Use `DEFAULT_SEGMENT_BYTES` for the ADR's default.
    /// Unmarked commits are never included. No checkpoint is performed here.
    pub fn capture_bounded(
        &mut self,
        byte_cap: u64,
    ) -> Result<Option<BoundaryChanges>, CaptureError> {
        let Some(plan) = self.next_capture(byte_cap) else {
            return Ok(None);
        };
        self.capture_plan(plan).map(Some)
    }

    /// Consume exactly the reserved prefix, even if more events were marked.
    /// A consumed or changed prefix is refused before capture state advances.
    pub fn capture_plan(&mut self, plan: BoundaryPlan) -> Result<BoundaryChanges, CaptureError> {
        let BoundaryPlan {
            first_sequence,
            boundary,
            boundaries: count,
            oversize,
        } = plan;
        let Some(last) = count.checked_sub(1).and_then(|i| self.scan.events.get(i)) else {
            return Err(CaptureError::Lost("boundary plan disappeared"));
        };
        if self.scan.events.front().map(|e| e.boundary.sequence) != Some(first_sequence)
            || last.boundary != boundary
        {
            return Err(CaptureError::Lost("boundary plan is no longer the prefix"));
        }
        let cursor = last.cursor;
        let mut pages = BTreeMap::new();
        for event in self.scan.events.iter().take(count) {
            pages.extend(event.pages.iter().map(|(&p, &at)| (p, at)));
        }
        let io = |source: std::io::Error| {
            if matches!(
                source.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::NotFound
            ) {
                CaptureError::Lost("boundary WAL was truncated or removed")
            } else {
                CaptureError::Io {
                    path: self.wal_path.to_path_buf(),
                    source,
                }
            }
        };
        let mut file = File::open(&self.wal_path).map_err(io)?;
        let mut h = [0; WAL_HEADER];
        file.read_exact(&mut h).map_err(io)?;
        if Some(parse_header(&h, self.page_size)?) != cursor.header
            || file.metadata().map_err(io)?.len() < cursor.offset
        {
            return Err(CaptureError::Lost("boundary WAL was reset"));
        }
        let mut out = BTreeMap::new();
        for (pgno, offset) in pages {
            if pgno == 0 || pgno > boundary.commit || pgno == lock_pgno(self.page_size) {
                continue;
            }
            let mut page = vec![0; self.page_size as usize];
            file.seek(SeekFrom::Start(offset)).map_err(io)?;
            file.read_exact(&mut page).map_err(io)?;
            out.insert(pgno, page);
        }
        // All fallible reads precede advancing any capture state.
        let pre_apply = self.db;
        while self.page_sums.len() > boundary.commit as usize {
            let old = self.page_sums.pop().unwrap_or(0);
            self.db = DbChecksum::from_value(self.db.value() ^ old);
        }
        self.page_sums.resize(boundary.commit as usize, 0);
        for (&pgno, page) in &out {
            let i = pgno as usize - 1;
            let new = page_checksum(pgno, page);
            self.db = DbChecksum::from_value(self.db.value() ^ self.page_sums[i] ^ new);
            self.page_sums[i] = new;
        }
        self.offset = cursor.offset;
        self.header = cursor.header;
        self.running = cursor.running;
        self.scan.events.drain(..count);
        // Counts must now be relative to the new captured position, including
        // pages rewritten both before and after it.
        let mut pages = BTreeMap::new();
        for event in &mut self.scan.events {
            pages.extend(event.pages.iter().map(|(&p, &at)| (p, at)));
            event.boundary = boundary_for(
                self.session,
                self.page_size,
                event.boundary.sequence,
                event.cursor.offset,
                event.commit,
                &pages,
            );
        }
        self.scan.pages = pages;
        Ok(BoundaryChanges {
            changes: Changes {
                commit: boundary.commit,
                page_size: self.page_size,
                pages: out,
                pre_apply,
                post_apply: self.db,
            },
            boundary,
            boundaries: count,
            oversize,
        })
    }

    fn boundary(
        &self,
        sequence: u64,
        offset: u64,
        commit: u32,
        pages: &BTreeMap<u32, u64>,
    ) -> WalBoundary {
        boundary_for(
            self.session,
            self.page_size,
            sequence,
            offset,
            commit,
            pages,
        )
    }

    // Read only appended frames. Keep speculative frames local: a rollback
    // may overwrite them with another transaction using the same WAL salts.
    fn scan_since(&self, mut cursor: Cursor) -> Result<Option<Scanned>, CaptureError> {
        let io = |source: std::io::Error| {
            if matches!(
                source.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::NotFound
            ) {
                CaptureError::Lost("boundary WAL was truncated or removed")
            } else {
                CaptureError::Io {
                    path: self.wal_path.to_path_buf(),
                    source,
                }
            }
        };
        let mut file = match File::open(&self.wal_path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && cursor.offset == 0 => {
                return Ok(None);
            }
            Err(e) => return Err(io(e)),
        };
        let len = file.metadata().map_err(io)?.len();
        if len < cursor.offset {
            return Err(CaptureError::Lost("the WAL shrank"));
        }
        if len < WAL_HEADER as u64 {
            return Ok(None);
        }
        let mut h = [0; WAL_HEADER];
        file.read_exact(&mut h).map_err(io)?;
        let header = parse_header(&h, self.page_size)?;
        if cursor.offset == 0 {
            cursor.running = checksum(header.big_endian, (0, 0), &h[..24]);
            if cursor.running != (be32(&h[24..]), be32(&h[28..])) {
                return Ok(None);
            }
            cursor.offset = WAL_HEADER as u64;
            cursor.header = Some(header);
        } else if cursor.header != Some(header) {
            return Err(CaptureError::Lost("the WAL header changed"));
        }
        let mut reader =
            std::io::BufReader::with_capacity((FRAME_HEADER + self.page_size as usize) * 16, file);
        reader.seek(SeekFrom::Start(cursor.offset)).map_err(io)?;
        let mut frame = vec![0; FRAME_HEADER + self.page_size as usize];
        let mut pending = BTreeMap::new();
        let mut pages = BTreeMap::new();
        let mut committed = None;
        while cursor.offset + frame.len() as u64 <= len {
            reader.read_exact(&mut frame).map_err(io)?;
            if (be32(&frame[8..]), be32(&frame[12..])) != header.salt {
                break;
            }
            let sum = checksum(header.big_endian, cursor.running, &frame[..8]);
            let sum = checksum(header.big_endian, sum, &frame[FRAME_HEADER..]);
            if sum != (be32(&frame[16..]), be32(&frame[20..])) {
                break;
            }
            pending.insert(be32(&frame), cursor.offset + FRAME_HEADER as u64);
            cursor.offset += frame.len() as u64;
            cursor.running = sum;
            let commit = be32(&frame[4..]);
            if commit != 0 {
                pages.append(&mut pending);
                committed = Some((commit, cursor));
            }
        }
        Ok(committed.map(|(commit, cursor)| (pages, commit, cursor)))
    }
}

type Scanned = (BTreeMap<u32, u64>, u32, Cursor);

fn boundary_for(
    session: u64,
    page_size: u32,
    sequence: u64,
    offset: u64,
    commit: u32,
    pages: &BTreeMap<u32, u64>,
) -> WalBoundary {
    let distinct_pages = pages
        .range(1..=commit)
        .filter(|(p, _)| **p != lock_pgno(page_size))
        .count();
    WalBoundary {
        session,
        sequence,
        offset,
        commit,
        distinct_pages,
        estimated_bytes: estimated_segment_bytes(page_size, distinct_pages),
    }
}
