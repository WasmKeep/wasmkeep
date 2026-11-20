//! One range map per actor (ADR 0040). Deploy [`Directory`] as an internal
//! class, or delegate your class's init/handle to [`handler`]. The directory
//! never calls another actor; [`Client`] is for its callers.
//!
//! Bounds are unsigned 64-bit points; `hi = None` means 2^64. Versions fit
//! SQLite's positive signed integer range. Only the creator may mutate a map.
//! The latest split/merge op id is retained: owners keep one pending operation
//! and retry it before submitting the next. Reusing an older id is not a replay.
use crate::wire::{self, Reader, Writer};
#[cfg(feature = "actor")]
use crate::{Value, params, storage};

/// Maximum number of ranges in a map.
pub const MAX_ENTRIES: usize = 4096;
/// Maximum owner metadata per entry, in bytes.
pub const MAX_META: usize = 1024;
const PREFIX: &str = "/wasmkeep-directory/v1/";
#[cfg(feature = "actor")]
const MAX_BODY: usize = 16 * 1024 * 1024;

/// A half-open range. `None` is the exclusive upper bound 2^64.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub lo: u64,
    pub hi: Option<u64>,
    pub target: String,
    pub version: u64,
    pub meta: Vec<u8>,
}
/// A consistent map snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Map {
    pub version: u64,
    pub entries: Vec<Entry>,
}
impl Map {
    /// Finds a point in this snapshot, without contacting the directory.
    pub fn lookup(&self, point: u64) -> Option<&Entry> {
        self.entries
            .iter()
            .find(|e| e.lo <= point && e.hi.is_none_or(|hi| point < hi))
    }
}
/// Which adjacent entry supplies the merged target and metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keep {
    Left,
    Right,
}
/// A conditional, replayable map change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Split {
        lo: u64,
        at: u64,
        target: String,
        meta: Vec<u8>,
        expect: u64,
        op: String,
    },
    Merge {
        lo: u64,
        keep: Keep,
        expect: u64,
        op: String,
    },
}
impl Change {
    #[cfg(any(feature = "actor", test))]
    fn condition(&self) -> (u64, &str) {
        match self {
            Self::Split { expect, op, .. } | Self::Merge { expect, op, .. } => (*expect, op),
        }
    }
}
/// A directory refusal. A conflict includes the current map version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    NotFound,
    Forbidden,
    Conflict(u64),
    Invalid(String),
    Storage(String),
    Call(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[cfg(feature = "actor")]
impl From<crate::SqlError> for Error {
    fn from(e: crate::SqlError) -> Self {
        Self::Storage(e.to_string())
    }
}
fn invalid() -> Error {
    Error::Invalid("invalid directory request or reply".into())
}
fn target_meta(target: &str, meta: &[u8]) -> Result<(), Error> {
    let Some((class, name)) = target.split_once('/') else {
        return Err(invalid());
    };
    if class.is_empty()
        || name.is_empty()
        || target.len() > 1024
        || target.chars().any(char::is_control)
        || meta.len() > MAX_META
    {
        return Err(invalid());
    }
    Ok(())
}
/// FNV-1a over bytes, followed by fmix64. Shared with the TypeScript SDK.
pub fn point(key: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in key {
        h = (h ^ u64::from(*b)).wrapping_mul(0x100_0000_01b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}
#[cfg(any(feature = "actor", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct State {
    owner: String,
    map: Map,
    last_op: Option<String>,
    last_result: Option<Vec<u8>>,
}
#[cfg(any(feature = "actor", test))]
impl State {
    fn create(owner: &str, target: &str, meta: &[u8]) -> Result<Self, Error> {
        target_meta(target, meta)?;
        Ok(Self {
            owner: owner.into(),
            map: Map {
                version: 1,
                entries: vec![Entry {
                    lo: 0,
                    hi: None,
                    target: target.into(),
                    version: 1,
                    meta: meta.into(),
                }],
            },
            last_op: None,
            last_result: None,
        })
    }
    fn change(&self, caller: &str, change: &Change) -> Result<Self, Error> {
        if caller != self.owner {
            return Err(Error::Forbidden);
        }
        let (expect, op) = change.condition();
        if op.is_empty() || op.len() > 1024 {
            return Err(invalid());
        }
        if self.last_op.as_deref() == Some(op) {
            return Ok(self.clone());
        }
        if expect != self.map.version {
            return Err(Error::Conflict(self.map.version));
        }
        let version = self
            .map
            .version
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(invalid)?;
        let mut next = self.clone();
        let entries = &mut next.map.entries;
        match change {
            Change::Split {
                lo,
                at,
                target,
                meta,
                ..
            } => {
                target_meta(target, meta)?;
                if entries.len() >= MAX_ENTRIES {
                    return Err(invalid());
                }
                let i = entries
                    .iter()
                    .position(|e| e.lo == *lo)
                    .ok_or_else(invalid)?;
                if at <= lo || entries[i].hi.is_some_and(|hi| *at >= hi) {
                    return Err(invalid());
                }
                let hi = entries[i].hi;
                entries[i].hi = Some(*at);
                entries[i].version = version;
                entries.insert(
                    i + 1,
                    Entry {
                        lo: *at,
                        hi,
                        target: target.clone(),
                        meta: meta.clone(),
                        version,
                    },
                );
            }
            Change::Merge { lo, keep, .. } => {
                let i = entries
                    .iter()
                    .position(|e| e.lo == *lo)
                    .filter(|i| *i > 0)
                    .ok_or_else(invalid)?;
                let right = entries.remove(i);
                entries[i - 1].hi = right.hi;
                entries[i - 1].version = version;
                if *keep == Keep::Right {
                    entries[i - 1].target = right.target;
                    entries[i - 1].meta = right.meta;
                }
            }
        }
        next.map.version = version;
        next.last_op = Some(op.into());
        next.last_result = Some(encode_map(&next.map));
        Ok(next)
    }
}
#[cfg(any(feature = "actor", test))]
fn encode_entry(w: &mut Writer, e: &Entry) {
    w.u64(e.lo)
        .u64(u64::from(e.hi.is_some()))
        .u64(e.hi.unwrap_or(0))
        .bytes(e.target.as_bytes())
        .u64(e.version)
        .bytes(&e.meta);
}
pub(crate) fn decode_entry(r: &mut Reader<'_>) -> Option<Entry> {
    let lo = r.u64()?;
    let has_hi = r.u64()?;
    let hi = r.u64()?;
    let hi = match (has_hi, hi) {
        (0, 0) => None,
        (1, v) if v > lo => Some(v),
        _ => return None,
    };
    let target = String::from_utf8(r.bytes()?.to_vec()).ok()?;
    let version = r.u64()?;
    let meta = r.bytes()?.to_vec();
    target_meta(&target, &meta).ok()?;
    Some(Entry {
        lo,
        hi,
        target,
        version,
        meta,
    })
}
#[cfg(any(feature = "actor", test))]
fn encode_map(map: &Map) -> Vec<u8> {
    let mut w = Writer::default();
    w.u64(map.version).u64(map.entries.len() as u64);
    for e in &map.entries {
        encode_entry(&mut w, e);
    }
    w.finish()
}
fn decode_map(body: &[u8]) -> Option<Map> {
    let mut r = Reader::new(body);
    let version = r.u64()?;
    let count = r.u64()?;
    if count == 0 || count > MAX_ENTRIES as u64 || version == 0 || version > i64::MAX as u64 {
        return None;
    }
    let mut entries = Vec::new();
    for _ in 0..count {
        entries.push(decode_entry(&mut r)?);
    }
    if !r.is_empty()
        || entries[0].lo != 0
        || entries.last()?.hi.is_some()
        || entries
            .iter()
            .any(|e| e.version == 0 || e.version > version)
        || entries.windows(2).any(|p| p[0].hi != Some(p[1].lo))
    {
        return None;
    }
    Some(Map { version, entries })
}
fn encode_change(c: &Change) -> (&'static str, Vec<u8>) {
    let mut w = Writer::default();
    let operation = match c {
        Change::Split {
            lo,
            at,
            target,
            meta,
            expect,
            op,
        } => {
            w.u64(*lo)
                .u64(*at)
                .bytes(target.as_bytes())
                .bytes(meta)
                .u64(*expect)
                .bytes(op.as_bytes());
            "split"
        }
        Change::Merge {
            lo,
            keep,
            expect,
            op,
        } => {
            w.u64(*lo)
                .u64(u64::from(*keep == Keep::Right))
                .u64(*expect)
                .bytes(op.as_bytes());
            "merge"
        }
    };
    (operation, w.finish())
}
#[cfg(any(feature = "actor", test))]
fn text(r: &mut Reader<'_>) -> Result<String, Error> {
    String::from_utf8(r.bytes().ok_or_else(invalid)?.to_vec()).map_err(|_| invalid())
}
fn number(r: &mut Reader<'_>) -> Result<u64, Error> {
    r.u64().ok_or_else(invalid)
}
#[cfg(any(feature = "actor", test))]
fn decode_change(operation: &str, r: &mut Reader<'_>) -> Result<Change, Error> {
    let lo = number(r)?;
    Ok(if operation == "split" {
        Change::Split {
            lo,
            at: number(r)?,
            target: text(r)?,
            meta: r.bytes().ok_or_else(invalid)?.to_vec(),
            expect: number(r)?,
            op: text(r)?,
        }
    } else {
        let keep = match number(r)? {
            0 => Keep::Left,
            1 => Keep::Right,
            _ => return Err(invalid()),
        };
        Change::Merge {
            lo,
            keep,
            expect: number(r)?,
            op: text(r)?,
        }
    })
}
/// Reaches a directory actor by `class/name`; no lookup caching.
#[derive(Debug, Clone)]
pub struct Client {
    address: String,
}
impl Client {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
        }
    }
    fn call(&self, op: &str, body: Vec<u8>) -> Result<Vec<u8>, Error> {
        let r = wire::call(&self.address, "POST", format!("{PREFIX}{op}"), vec![], body)
            .map_err(Error::Call)?;
        match r.status {
            200 => Ok(r.body),
            403 => Err(Error::Forbidden),
            404 => Err(Error::NotFound),
            409 => Err(Error::Conflict(
                Reader::new(&r.body).u64().ok_or_else(invalid)?,
            )),
            _ => Err(Error::Call(format!(
                "directory answered {}: {}",
                r.status,
                String::from_utf8_lossy(&r.body)
            ))),
        }
    }
    /// Creates the map durably, or returns its current map if already owned.
    pub fn create(&self, target: &str, meta: &[u8]) -> Result<Map, Error> {
        target_meta(target, meta)?;
        let mut w = Writer::default();
        w.bytes(target.as_bytes()).bytes(meta);
        decode_map(&self.call("create", w.finish())?).ok_or_else(invalid)
    }
    pub fn entries(&self) -> Result<Map, Error> {
        decode_map(&self.call("entries", vec![])?).ok_or_else(invalid)
    }
    /// Returns the range and the map version from one event.
    pub fn lookup(&self, point: u64) -> Result<(Entry, u64), Error> {
        let mut w = Writer::default();
        w.u64(point);
        let body = self.call("lookup", w.finish())?;
        let mut r = Reader::new(&body);
        let version = number(&mut r)?;
        let entry = decode_entry(&mut r).ok_or_else(invalid)?;
        if !r.is_empty() {
            return Err(invalid());
        }
        Ok((entry, version))
    }
    /// Splits or merges in one durable transaction. Retry unknown outcomes
    /// with the same op id before issuing any subsequent change.
    pub fn change(&self, change: &Change) -> Result<Map, Error> {
        let (op, body) = encode_change(change);
        decode_map(&self.call(op, body)?).ok_or_else(invalid)
    }
    /// The owner hands the map to `owner`; repeated, it changes nothing.
    pub fn transfer(&self, owner: &str) -> Result<(), Error> {
        let mut w = Writer::default();
        w.bytes(owner.as_bytes());
        self.call("transfer", w.finish())?;
        Ok(())
    }
    pub fn delete(&self, expect: u64) -> Result<(), Error> {
        let mut w = Writer::default();
        w.u64(expect);
        self.call("delete", w.finish())?;
        Ok(())
    }
}
/// Ready-made directory class: `wasmkeep_sdk::export!(Directory)`.
#[cfg(feature = "actor")]
#[derive(Debug)]
pub struct Directory;
#[cfg(feature = "actor")]
impl crate::Actor for Directory {
    fn on_init() -> Result<(), String> {
        handler::init().map_err(|e| e.to_string())
    }
}
#[cfg(feature = "actor")]
impl crate::http::Guest for Directory {
    fn handle(request: crate::http::IncomingRequest, out: crate::http::ResponseOutparam) {
        handler::handle(request, out);
    }
}
/// Handler functions for an application's own directory class.
#[cfg(feature = "actor")]
pub mod handler {
    use super::*;
    /// Initializes the two tables. There is no map until an inside caller creates it.
    pub fn init() -> Result<(), Error> {
        storage::exec(
            "CREATE TABLE IF NOT EXISTS wasmkeep_directory_map (id INTEGER PRIMARY KEY CHECK(id=1), owner TEXT NOT NULL, version INTEGER NOT NULL, last_op TEXT, last_result BLOB)",
            &[],
        )?;
        storage::exec(
            "CREATE TABLE IF NOT EXISTS wasmkeep_directory_entries (lo BLOB PRIMARY KEY CHECK(length(lo)=8), target TEXT NOT NULL, version INTEGER NOT NULL, meta BLOB NOT NULL)",
            &[],
        )?;
        Ok(())
    }
    fn load() -> Result<State, Error> {
        let rows = storage::query(
            "SELECT owner, version, last_op, last_result FROM wasmkeep_directory_map WHERE id=1",
            &[],
        )?;
        if rows.is_empty() {
            return Err(Error::NotFound);
        }
        let corrupt = || Error::Storage("invalid directory state".into());
        let owner = rows
            .get(0, "owner")
            .and_then(Value::as_str)
            .ok_or_else(corrupt)?
            .to_owned();
        let version = rows
            .get(0, "version")
            .and_then(Value::as_i64)
            .and_then(|v| u64::try_from(v).ok())
            .ok_or_else(corrupt)?;
        let last_op = rows
            .get(0, "last_op")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let last_result = rows
            .get(0, "last_result")
            .and_then(Value::as_bytes)
            .map(<[u8]>::to_vec);
        let rows = storage::query(
            "SELECT lo,target,version,meta FROM wasmkeep_directory_entries ORDER BY lo",
            &[],
        )?;
        let mut entries = Vec::new();
        for i in 0..rows.len() {
            let lo = u64::from_be_bytes(
                rows.get(i, "lo")
                    .and_then(Value::as_bytes)
                    .ok_or_else(corrupt)?
                    .try_into()
                    .map_err(|_| corrupt())?,
            );
            entries.push(Entry {
                lo,
                hi: None,
                target: rows
                    .get(i, "target")
                    .and_then(Value::as_str)
                    .ok_or_else(corrupt)?
                    .into(),
                version: rows
                    .get(i, "version")
                    .and_then(Value::as_i64)
                    .and_then(|v| u64::try_from(v).ok())
                    .ok_or_else(corrupt)?,
                meta: rows
                    .get(i, "meta")
                    .and_then(Value::as_bytes)
                    .ok_or_else(corrupt)?
                    .into(),
            });
        }
        for i in 1..entries.len() {
            entries[i - 1].hi = Some(entries[i].lo);
        }
        Ok(State {
            owner,
            map: Map { version, entries },
            last_op,
            last_result,
        })
    }
    // Persist only changed boundaries, not the whole map. All checks and
    // writes are inside the same transaction and actor event.
    fn save(before: Option<&State>, after: &State) -> Result<(), Error> {
        if let Some(before) = before {
            for e in &before.map.entries {
                if after
                    .map
                    .entries
                    .binary_search_by_key(&e.lo, |n| n.lo)
                    .is_err()
                {
                    storage::exec(
                        "DELETE FROM wasmkeep_directory_entries WHERE lo=?1",
                        &params![e.lo.to_be_bytes().to_vec()],
                    )?;
                }
            }
        }
        for e in &after.map.entries {
            if before.is_some_and(|b| {
                b.map
                    .entries
                    .binary_search_by_key(&e.lo, |old| old.lo)
                    .is_ok_and(|i| b.map.entries[i].version == e.version)
            }) {
                continue;
            }
            storage::exec(
                "INSERT INTO wasmkeep_directory_entries (lo,target,version,meta) VALUES (?1,?2,?3,?4) ON CONFLICT(lo) DO UPDATE SET target=excluded.target,version=excluded.version,meta=excluded.meta",
                &params![
                    e.lo.to_be_bytes().to_vec(),
                    e.target.clone(),
                    i64::try_from(e.version).map_err(|_| invalid())?,
                    e.meta.clone()
                ],
            )?;
        }
        storage::exec(
            "INSERT INTO wasmkeep_directory_map (id,owner,version,last_op,last_result) VALUES (1,?1,?2,?3,?4) ON CONFLICT(id) DO UPDATE SET version=excluded.version,last_op=excluded.last_op,last_result=excluded.last_result",
            &params![
                after.owner.clone(),
                i64::try_from(after.map.version).map_err(|_| invalid())?,
                after.last_op.clone(),
                after.last_result.clone()
            ],
        )?;
        Ok(())
    }
    fn transaction(f: impl FnOnce() -> Result<Vec<u8>, Error>) -> Result<Vec<u8>, Error> {
        storage::exec("BEGIN", &[])?;
        let result = f().and_then(|body| {
            storage::exec_durable("COMMIT", &[])?;
            Ok(body)
        });
        if result.is_err() {
            let _ = storage::exec("ROLLBACK", &[]);
        }
        result
    }
    fn serve(caller: &str, operation: &str, body: &[u8]) -> Result<Vec<u8>, Error> {
        let mut r = Reader::new(body);
        match operation {
            "create" => {
                let target = text(&mut r)?;
                let meta = r.bytes().ok_or_else(invalid)?;
                if !r.is_empty() {
                    return Err(invalid());
                }
                transaction(|| match load() {
                    Ok(current) if current.owner == caller => Ok(encode_map(&current.map)),
                    Ok(current) => Err(Error::Conflict(current.map.version)),
                    Err(Error::NotFound) => {
                        let next = State::create(caller, &target, meta)?;
                        save(None, &next)?;
                        Ok(encode_map(&next.map))
                    }
                    Err(e) => Err(e),
                })
            }
            "split" | "merge" => {
                let change = decode_change(operation, &mut r)?;
                if !r.is_empty() {
                    return Err(invalid());
                }
                transaction(|| {
                    let before = load()?;
                    let after = before.change(caller, &change)?;
                    if after != before {
                        save(Some(&before), &after)?;
                    }
                    after.last_result.ok_or_else(invalid)
                })
            }
            "transfer" => {
                // The owner names its successor (ADR 0040, section 13); the
                // map's version is unchanged. Repeated: already done.
                let owner = text(&mut r)?;
                if !r.is_empty() {
                    return Err(invalid());
                }
                transaction(|| {
                    let state = load()?;
                    if state.owner != owner {
                        if state.owner != caller {
                            return Err(Error::Forbidden);
                        }
                        storage::exec(
                            "UPDATE wasmkeep_directory_map SET owner=?1 WHERE id=1",
                            &params![owner.clone()],
                        )?;
                    }
                    Ok(vec![])
                })
            }
            "entries" => {
                if !r.is_empty() {
                    return Err(invalid());
                }
                Ok(encode_map(&load()?.map))
            }
            "lookup" => {
                let point = number(&mut r)?;
                if !r.is_empty() {
                    return Err(invalid());
                }
                let state = load()?;
                let e = state.map.lookup(point).ok_or_else(invalid)?;
                let mut w = Writer::default();
                w.u64(state.map.version);
                encode_entry(&mut w, e);
                Ok(w.finish())
            }
            "delete" => {
                let expect = number(&mut r)?;
                if !r.is_empty() {
                    return Err(invalid());
                }
                let state = load()?;
                if state.owner != caller {
                    return Err(Error::Forbidden);
                }
                if expect != state.map.version {
                    return Err(Error::Conflict(state.map.version));
                }
                storage::delete_self_durable()?;
                Ok(vec![])
            }
            _ => Err(Error::NotFound),
        }
    }
    /// Serves the v1 binary protocol. The trusted host caller header is
    /// required even for reads; never add outward calls to this handler.
    pub fn handle(request: crate::http::IncomingRequest, out: crate::http::ResponseOutparam) {
        let result = (|| {
            let caller = wire::caller(&request)
                .filter(|c| !c.is_empty())
                .ok_or(Error::Forbidden)?;
            if !matches!(request.method(), crate::http::Method::Post) {
                return Err(invalid());
            }
            let path = request.path_with_query().unwrap_or_default();
            let operation = path.strip_prefix(PREFIX).ok_or(Error::NotFound)?;
            let body = crate::http::read_body(&request, MAX_BODY).ok_or_else(invalid)?;
            serve(&caller, operation, &body)
        })();
        let (status, body) = match result {
            Ok(b) => (200, b),
            Err(Error::Conflict(v)) => {
                let mut w = Writer::default();
                w.u64(v);
                (409, w.finish())
            }
            Err(e) => {
                let status = match e {
                    Error::Forbidden => 403,
                    Error::NotFound => 404,
                    Error::Invalid(_) => 400,
                    _ => 500,
                };
                (status, e.to_string().into_bytes())
            }
        };
        crate::http::respond(out, status, "application/octet-stream", &body);
    }
}
#[cfg(test)]
#[path = "directory/tests.rs"]
mod tests;
