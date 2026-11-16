//! Wire format for staged single-actor restores (ADR 0033).
use crate::replication::SegmentId;

/// Validated description of a staged LTX snapshot. No credentials belong here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// Source position or backup run, for operator diagnostics.
    pub source: String,
    /// Encoded LTX byte count.
    pub size: u64,
    /// SQLite page size.
    pub page_size: u32,
    /// Verified post-apply checksum.
    pub checksum: u64,
    /// Optional compare-and-replace head.
    pub if_head: Option<SegmentId>,
}
impl Request {
    /// Stable, bounded text format.
    pub fn encode(&self) -> Vec<u8> {
        format!(
            "wasmkeep-restore 1\n{}\n{} {} {}\n{}\n",
            self.source,
            self.size,
            self.page_size,
            self.checksum,
            self.if_head
                .map_or_else(|| "-".into(), |p| format!("{}:{}", p.generation, p.txid))
        )
        .into_bytes()
    }
    /// Reject malformed or oversized requests before reading their staged data.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > 4096 {
            return None;
        }
        let mut lines = std::str::from_utf8(bytes).ok()?.lines();
        if lines.next()? != "wasmkeep-restore 1" {
            return None;
        }
        let source = lines.next()?.to_owned();
        if source.is_empty() {
            return None;
        }
        let mut fields = lines.next()?.split_whitespace();
        let size: u64 = fields.next()?.parse().ok()?;
        if size == 0 {
            return None;
        }
        let page_size: u32 = fields.next()?.parse().ok()?;
        let checksum = fields.next()?.parse().ok()?;
        if fields.next().is_some()
            || !(512..=65536).contains(&page_size)
            || !page_size.is_power_of_two()
        {
            return None;
        }
        let head = lines.next()?;
        let if_head = if head == "-" {
            None
        } else {
            let (generation, txid) = head.split_once(':')?;
            Some(SegmentId {
                generation: generation.parse().ok()?,
                txid: txid.parse().ok()?,
            })
        };
        if lines.next().is_some() {
            return None;
        }
        Some(Self {
            source,
            size,
            page_size,
            checksum,
            if_head,
        })
    }
}

/// Replay ordering: a repeated restore is fenced, a later restore or delete
/// supersedes it. Equal bucket timestamps only identify the same request.
pub fn replay(
    marker: Option<&str>,
    replaced: Option<&str>,
    at: u64,
    id: &str,
) -> Option<&'static str> {
    if let Some((time, done)) = marker.and_then(|s| s.split_once(' '))
        && let Ok(time) = time.parse::<u64>()
    {
        if time == at && done == id {
            return Some("done");
        }
        if time > at {
            return Some("superseded");
        }
    }
    if let Some(rest) = replaced.and_then(|s| s.strip_prefix("delete "))
        && let Some((_, time)) = rest.split_once(' ')
        && time.parse::<u64>().is_ok_and(|time| time >= at)
    {
        return Some("superseded");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actor_restore_request_roundtrip_and_validation() {
        let request = Request {
            source: "backup run-1 4:8".into(),
            size: 1234,
            page_size: 4096,
            checksum: 123,
            if_head: Some(SegmentId {
                generation: 5,
                txid: 10,
            }),
        };
        assert_eq!(Request::decode(&request.encode()), Some(request.clone()));
        let mut bad = request.clone();
        bad.page_size = 1000;
        assert!(Request::decode(&bad.encode()).is_none());
        let mut bad = request;
        bad.source = "two\nlines".into();
        assert!(Request::decode(&bad.encode()).is_none());
        assert!(Request::decode(&vec![b'x'; 4097]).is_none());
    }
}
