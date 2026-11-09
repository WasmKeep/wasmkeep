//! Key-value storage built from actors ([ADR 0015]).
//!
//! Keys are spread over a fixed number of **shard actors** of one class
//! (`<class>/shard-0` .. `shard-<n-1>`), chosen by a hash of the key. Each
//! shard keeps its keys in its own SQLite database, so every operation on a
//! key is strongly consistent: the shard runs one event at a time.
//!
//! - The shard class is a component that exports [`Shard`] (or calls
//!   [`shard`]'s functions from its own code).
//! - Callers use a [`Client`], which reaches the shard with `actors.call`.
//!
//! ```ignore
//! // The shard class (its own component, deployed as class `kv`):
//! use wasmkeep_sdk::kv::Shard;
//! wasmkeep_sdk::export!(Shard);
//!
//! // Any other actor:
//! let kv = wasmkeep_sdk::kv::Client::new("kv", 8);
//! let version = kv.put("user:42", b"Ada")?;
//! let entry = kv.get("user:42")?;                      // Some(Entry { .. })
//! kv.compare_and_set("user:42", Some(version), b"Ada L.")?; // Err(Conflict) if it changed
//! ```
//!
//! Limits, stated plainly: one shard handles one operation at a time, so
//! throughput per shard is about one durable write per bucket round trip
//! (a [`PutOptions::local`] write skips that wait); there are no
//! transactions across keys; and the shard count is part of the data layout
//! (changing it moves keys, so it needs a migration). The [`Client`] API is
//! the stable part: a platform primitive could serve the same calls later.
//!
//! [ADR 0015]: https://github.com/wasmkeep/wasmkeep/blob/main/docs/adr/0015-kv-and-queues-as-sdk-patterns.md

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;

use crate::http::{self, IncomingRequest, Method, ResponseOutparam};
use crate::wire::{self, Reader, Writer};
use crate::{SqlError, Value, alarms, now_ms, params, storage};

/// Keys are at most this long (bytes).
pub const MAX_KEY_LEN: usize = 1024;

const PREFIX: &str = "/wasmkeep-kv/v1";
const EXPECT: &str = "x-wasmkeep-kv-expect";
const TTL: &str = "x-wasmkeep-kv-ttl-ms";
const DURABLE: &str = "x-wasmkeep-kv-durable";
/// Request bodies a shard accepts: a value plus framing, under the 16 MB
/// `actors.call` cap.
const MAX_BODY: usize = 16 * 1024 * 1024;

/// A stored value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The bytes stored.
    pub value: Vec<u8>,
    /// Changes on every write of the key, and is never reused within a
    /// shard (not even after a delete), so a compare-and-set on an old
    /// version always loses.
    pub version: u64,
    /// When the key expires (milliseconds since the Unix epoch), if it does.
    pub expires_at: Option<u64>,
}

/// What a conditional write expects to find.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Expect {
    /// Write whatever is there.
    #[default]
    Any,
    /// Write only if the key is absent (or expired).
    Absent,
    /// Write only if the key holds this version.
    Version(u64),
}

impl Expect {
    /// Whether a key currently at `current` (`None`: absent) meets this.
    pub fn allows(self, current: Option<u64>) -> bool {
        match self {
            Expect::Any => true,
            Expect::Absent => current.is_none(),
            Expect::Version(v) => current == Some(v),
        }
    }

    fn encode(self) -> String {
        match self {
            Expect::Any => "any".into(),
            Expect::Absent => "absent".into(),
            Expect::Version(v) => v.to_string(),
        }
    }

    fn decode(s: &str) -> Option<Self> {
        match s {
            "any" => Some(Expect::Any),
            "absent" => Some(Expect::Absent),
            n => n.parse().ok().map(Expect::Version),
        }
    }
}

/// Options for [`Client::put_with`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutOptions {
    /// The condition the write needs (default: none).
    pub expect: Expect,
    /// Expire the key this many milliseconds after the write.
    pub ttl_ms: Option<u64>,
    /// Return only once the write is durable (`exec-durable`: in the
    /// bucket in a cluster, fsynced on one node). Default `true`.
    pub durable: bool,
}

impl Default for PutOptions {
    fn default() -> Self {
        Self {
            expect: Expect::Any,
            ttl_ms: None,
            durable: true,
        }
    }
}

impl PutOptions {
    /// A write acknowledged once committed on the shard's node, before it
    /// reaches the bucket: faster, but a node lost in that window loses it.
    pub fn local() -> Self {
        Self {
            durable: false,
            ..Self::default()
        }
    }
}

/// Why a KV operation failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// A conditional write found another version (or a key where it
    /// expected none).
    Conflict,
    /// The key is longer than [`MAX_KEY_LEN`].
    KeyTooLong,
    /// The shard's database refused the operation.
    Storage(String),
    /// The call to the shard failed: the outcome of a write is then not
    /// known (it may have applied). Re-read before retrying a conditional
    /// write.
    Call(String),
    /// The shard answered with an unexpected status.
    Refused(u16, String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Conflict => f.write_str("conflict: the key changed"),
            Error::KeyTooLong => write!(f, "key longer than {MAX_KEY_LEN} bytes"),
            Error::Storage(m) => write!(f, "storage: {m}"),
            Error::Call(m) => f.write_str(m),
            Error::Refused(s, m) => write!(f, "shard answered {s}: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<SqlError> for Error {
    fn from(e: SqlError) -> Self {
        Error::Storage(e.to_string())
    }
}

/// The shard (0-based) that holds `key` when there are `shards` of them:
/// FNV-1a (64 bits) of the key, modulo the count. Part of the data layout:
/// it never changes.
pub fn shard_for(key: &[u8], shards: u32) -> u32 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // The remainder is below `shards`, so it fits.
    u32::try_from(hash % u64::from(shards.max(1))).unwrap_or(0)
}

/// The actor name of shard `index`: `shard-<index>`.
pub fn shard_name(index: u32) -> String {
    format!("shard-{index}")
}

/// Reaches the shards of one class. Cheap to create; keep one per class.
#[derive(Debug, Clone)]
pub struct Client {
    class: String,
    shards: u32,
    cache_ms: u64,
}

impl Client {
    /// Keys spread over `shards` actors of `class` (at least one). Every
    /// caller must use the same count.
    pub fn new(class: &str, shards: u32) -> Self {
        Self {
            class: class.to_owned(),
            shards: shards.max(1),
            cache_ms: 0,
        }
    }

    /// Serve [`get`](Self::get) from an in-memory cache in this actor's
    /// instance when the entry was read less than `max_age_ms` ago. This
    /// actor's own writes through the client update the cache, so it reads
    /// its own writes; other actors' writes show up within `max_age_ms`.
    /// The cache is lost with the instance (hibernation, a failed event,
    /// a deploy).
    pub fn with_read_cache(mut self, max_age_ms: u64) -> Self {
        self.cache_ms = max_age_ms;
        self
    }

    fn address(&self, key: &[u8]) -> String {
        format!("{}/{}", self.class, shard_name(shard_for(key, self.shards)))
    }

    fn cache_key(&self, key: &[u8]) -> Vec<u8> {
        let mut k = self.class.as_bytes().to_vec();
        k.push(0);
        k.extend_from_slice(key);
        k
    }

    fn remember(&self, key: &[u8], entry: Option<Entry>) {
        if self.cache_ms > 0 {
            CACHE.with(|c| c.borrow_mut().insert(self.cache_key(key), now_ms(), entry));
        }
    }

    fn forget(&self, key: &[u8]) {
        if self.cache_ms > 0 {
            CACHE.with(|c| c.borrow_mut().remove(&self.cache_key(key)));
        }
    }

    /// The entry at `key`, from the read cache if one is set and fresh.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Entry>, Error> {
        let key = key.as_ref();
        if self.cache_ms > 0 {
            let now = now_ms();
            let hit = CACHE.with(|c| c.borrow().get(&self.cache_key(key), now, self.cache_ms));
            if let Some(entry) = hit {
                return Ok(entry);
            }
        }
        self.get_fresh(key)
    }

    /// The entry at `key`, always asking the shard.
    pub fn get_fresh(&self, key: impl AsRef<[u8]>) -> Result<Option<Entry>, Error> {
        let key = key.as_ref();
        check_key(key)?;
        let path = format!("{PREFIX}/k/{}", wire::hex(key));
        let response = wire::call(&self.address(key), "GET", path, Vec::new(), Vec::new())
            .map_err(Error::Call)?;
        let entry = match response.status {
            200 => Some(decode_entry(&response.body).ok_or_else(bad_reply)?),
            404 => None,
            s => return Err(refused(s, &response.body)),
        };
        self.remember(key, entry.clone());
        Ok(entry)
    }

    /// Stores `value` at `key` (durably); returns the new version.
    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<u64, Error> {
        self.put_with(key, value, PutOptions::default())
    }

    /// Stores `value` only if `key` is at `expected` (`None`: absent);
    /// [`Error::Conflict`] otherwise.
    pub fn compare_and_set(
        &self,
        key: impl AsRef<[u8]>,
        expected: Option<u64>,
        value: impl AsRef<[u8]>,
    ) -> Result<u64, Error> {
        let expect = expected.map_or(Expect::Absent, Expect::Version);
        self.put_with(
            key,
            value,
            PutOptions {
                expect,
                ..PutOptions::default()
            },
        )
    }

    /// Stores `value` at `key` with `options`; returns the new version.
    pub fn put_with(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        options: PutOptions,
    ) -> Result<u64, Error> {
        let (key, value) = (key.as_ref(), value.as_ref());
        check_key(key)?;
        let mut headers = vec![
            (EXPECT.to_owned(), options.expect.encode()),
            (
                DURABLE.to_owned(),
                if options.durable { "1" } else { "0" }.into(),
            ),
        ];
        if let Some(ttl) = options.ttl_ms {
            headers.push((TTL.to_owned(), ttl.to_string()));
        }
        let path = format!("{PREFIX}/k/{}", wire::hex(key));
        let sent = now_ms();
        let response = wire::call(&self.address(key), "PUT", path, headers, value.to_vec());
        let response = response.map_err(|e| {
            self.forget(key);
            Error::Call(e)
        })?;
        match response.status {
            200 => {
                let version = Reader::new(&response.body).u64().ok_or_else(bad_reply)?;
                let expires_at = options.ttl_ms.map(|t| sent.saturating_add(t));
                self.remember(
                    key,
                    Some(Entry {
                        value: value.to_vec(),
                        version,
                        expires_at,
                    }),
                );
                Ok(version)
            }
            409 => {
                self.forget(key);
                Err(Error::Conflict)
            }
            s => {
                self.forget(key);
                Err(refused(s, &response.body))
            }
        }
    }

    /// Removes `key`; `true` if it was there.
    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<bool, Error> {
        self.delete_with(key, Expect::Any)
    }

    /// Removes `key` only if it holds `version`; [`Error::Conflict`]
    /// otherwise.
    pub fn delete_if(&self, key: impl AsRef<[u8]>, version: u64) -> Result<bool, Error> {
        self.delete_with(key, Expect::Version(version))
    }

    fn delete_with(&self, key: impl AsRef<[u8]>, expect: Expect) -> Result<bool, Error> {
        let key = key.as_ref();
        check_key(key)?;
        let path = format!("{PREFIX}/k/{}", wire::hex(key));
        let headers = vec![(EXPECT.to_owned(), expect.encode())];
        let response = wire::call(&self.address(key), "DELETE", path, headers, Vec::new());
        let response = response.map_err(|e| {
            self.forget(key);
            Error::Call(e)
        })?;
        match response.status {
            200 => {
                let removed = Reader::new(&response.body).u64().ok_or_else(bad_reply)? == 1;
                self.remember(key, None);
                Ok(removed)
            }
            409 => {
                self.forget(key);
                Err(Error::Conflict)
            }
            s => {
                self.forget(key);
                Err(refused(s, &response.body))
            }
        }
    }

    /// Up to `limit` keys starting with `prefix`, in byte order, after
    /// `after` (exclusive) if given: pass the last key of a page to get the
    /// next one. Asks every shard (one call each), so it costs more than a
    /// read; keep it for listings, not hot paths.
    pub fn list(
        &self,
        prefix: impl AsRef<[u8]>,
        after: Option<&[u8]>,
        limit: u32,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let prefix = prefix.as_ref();
        let mut path = format!("{PREFIX}/list?prefix={}&limit={limit}", wire::hex(prefix));
        if let Some(after) = after {
            path.push_str("&after=");
            path.push_str(&wire::hex(after));
        }
        let mut pages = Vec::new();
        for index in 0..self.shards {
            let address = format!("{}/{}", self.class, shard_name(index));
            let response = wire::call(&address, "GET", path.clone(), Vec::new(), Vec::new())
                .map_err(Error::Call)?;
            if response.status != 200 {
                return Err(refused(response.status, &response.body));
            }
            pages.push(decode_keys(&response.body).ok_or_else(bad_reply)?);
        }
        Ok(merge_pages(pages, limit))
    }
}

fn check_key(key: &[u8]) -> Result<(), Error> {
    if key.len() > MAX_KEY_LEN {
        return Err(Error::KeyTooLong);
    }
    Ok(())
}

fn bad_reply() -> Error {
    Error::Refused(200, "malformed reply from shard".into())
}

fn refused(status: u16, body: &[u8]) -> Error {
    Error::Refused(status, String::from_utf8_lossy(body).trim().to_owned())
}

/// Merges sorted pages from every shard into one sorted page of `limit`.
fn merge_pages(pages: Vec<Vec<Vec<u8>>>, limit: u32) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = pages.into_iter().flatten().collect();
    keys.sort();
    keys.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    keys
}

fn encode_entry(entry: &Entry) -> Vec<u8> {
    let mut w = Writer::default();
    w.u64(entry.version)
        .u64(entry.expires_at.unwrap_or(0))
        .bytes(&entry.value);
    w.finish()
}

fn decode_entry(buf: &[u8]) -> Option<Entry> {
    let mut r = Reader::new(buf);
    let version = r.u64()?;
    let expires_at = Some(r.u64()?).filter(|t| *t != 0);
    let value = r.bytes()?.to_vec();
    r.is_empty().then_some(Entry {
        value,
        version,
        expires_at,
    })
}

fn encode_keys(keys: &[Vec<u8>]) -> Vec<u8> {
    let mut w = Writer::default();
    w.u64(keys.len() as u64);
    for k in keys {
        w.bytes(k);
    }
    w.finish()
}

fn decode_keys(buf: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut r = Reader::new(buf);
    let n = r.u64()?;
    let mut keys = Vec::new();
    for _ in 0..n {
        keys.push(r.bytes()?.to_vec());
    }
    r.is_empty().then_some(keys)
}

thread_local! {
    static CACHE: RefCell<ReadCache> = RefCell::new(ReadCache::new(4096));
}

/// A bounded map of recent reads. When full, expired entries go first,
/// then everything: simple, and a full cache is refilled by the next reads.
#[derive(Debug)]
struct ReadCache {
    entries: HashMap<Vec<u8>, (u64, Option<Entry>)>,
    capacity: usize,
}

impl ReadCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            capacity,
        }
    }

    /// `Some(entry)` if read less than `max_age` ago (and not expired since).
    #[allow(clippy::option_option, reason = "a miss differs from a cached absence")]
    fn get(&self, key: &[u8], now: u64, max_age: u64) -> Option<Option<Entry>> {
        let (at, entry) = self.entries.get(key)?;
        if now.saturating_sub(*at) >= max_age {
            return None;
        }
        match entry {
            Some(e) if e.expires_at.is_some_and(|t| t <= now) => Some(None),
            other => Some(other.clone()),
        }
    }

    fn insert(&mut self, key: Vec<u8>, now: u64, entry: Option<Entry>) {
        if self.entries.len() >= self.capacity && !self.entries.contains_key(&key) {
            self.entries.retain(|_, (_, e)| {
                e.as_ref()
                    .is_none_or(|e| e.expires_at.is_none_or(|t| t > now))
            });
            if self.entries.len() >= self.capacity {
                self.entries.clear();
            }
        }
        self.entries.insert(key, (now, entry));
    }

    fn remove(&mut self, key: &[u8]) {
        self.entries.remove(key);
    }
}

/// A ready-made shard class: `wasmkeep_sdk::export!(Shard)`. It answers
/// only other actors (an outside client gets `403`), and uses the actor's
/// alarm to delete expired keys.
#[derive(Debug)]
pub struct Shard;

impl crate::Actor for Shard {
    fn on_init() -> Result<(), String> {
        shard::init().map_err(|e| e.to_string())
    }

    fn on_alarm() -> Result<(), String> {
        shard::on_alarm().map_err(|e| e.to_string())
    }
}

impl http::Guest for Shard {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        shard::handle(request, out);
    }
}

/// The shard side: the table, the operations on it and the protocol
/// [`Client`] speaks. For a shard class with code of its own; otherwise
/// export [`Shard`].
pub mod shard {
    use super::*;

    /// How many expired keys one alarm deletes; more are left to the next.
    const PURGE_BATCH: u32 = 500;

    /// Creates the tables (idempotent). Call from `on_init`.
    pub fn init() -> Result<(), SqlError> {
        storage::exec(
            "CREATE TABLE IF NOT EXISTS wasmkeep_kv (
                key BLOB PRIMARY KEY,
                value BLOB NOT NULL,
                version INTEGER NOT NULL,
                expires_at INTEGER
            ) WITHOUT ROWID",
            &[],
        )?;
        storage::exec(
            "CREATE INDEX IF NOT EXISTS wasmkeep_kv_expiry ON wasmkeep_kv (expires_at)
             WHERE expires_at IS NOT NULL",
            &[],
        )?;
        storage::exec(
            "CREATE TABLE IF NOT EXISTS wasmkeep_kv_meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                seq INTEGER NOT NULL
            )",
            &[],
        )?;
        storage::exec(
            "INSERT OR IGNORE INTO wasmkeep_kv_meta (id, seq) VALUES (1, 0)",
            &[],
        )?;
        Ok(())
    }

    fn now() -> i64 {
        i64::try_from(now_ms()).unwrap_or(i64::MAX)
    }

    fn as_u64(v: Option<&Value>) -> Option<u64> {
        v.and_then(Value::as_i64)
            .and_then(|v| u64::try_from(v).ok())
    }

    /// The live entry at `key` (an expired one reads as absent).
    pub fn get(key: &[u8]) -> Result<Option<Entry>, Error> {
        let rows = storage::query(
            "SELECT value, version, expires_at FROM wasmkeep_kv
             WHERE key = ?1 AND (expires_at IS NULL OR expires_at > ?2)",
            &params![key, now()],
        )?;
        if rows.is_empty() {
            return Ok(None);
        }
        let value = rows
            .get(0, "value")
            .and_then(Value::as_bytes)
            .unwrap_or_default()
            .to_vec();
        let version = as_u64(rows.get(0, "version")).unwrap_or(0);
        let expires_at = as_u64(rows.get(0, "expires_at"));
        Ok(Some(Entry {
            value,
            version,
            expires_at,
        }))
    }

    /// Runs one write, durably if asked. Each operation ends in exactly one
    /// such write, so it needs no explicit transaction: the shard runs one
    /// event at a time, so nothing interleaves, and `exec-durable` also
    /// covers every write committed before it.
    fn write(durable: bool, sql: &str, params: &[Value]) -> Result<u64, Error> {
        Ok(if durable {
            storage::exec_durable(sql, params)?
        } else {
            storage::exec(sql, params)?
        })
    }

    /// A new version: one more than any this shard has handed out. Committed
    /// before the write that uses it, so a write that then fails only skips
    /// a number.
    fn next_version() -> Result<u64, Error> {
        storage::exec(
            "UPDATE wasmkeep_kv_meta SET seq = seq + 1 WHERE id = 1",
            &[],
        )?;
        let rows = storage::query("SELECT seq FROM wasmkeep_kv_meta WHERE id = 1", &[])?;
        as_u64(rows.get(0, "seq")).ok_or_else(|| Error::Storage("no version counter".into()))
    }

    /// Writes `value` at `key` if `options.expect` holds; returns the new
    /// version.
    pub fn put(key: &[u8], value: &[u8], options: PutOptions) -> Result<u64, Error> {
        check_key(key)?;
        let now = now_ms();
        let expires_at = options.ttl_ms.map(|t| now.saturating_add(t));
        let current = get(key)?.map(|e| e.version);
        if !options.expect.allows(current) {
            return Err(Error::Conflict);
        }
        let version = next_version()?;
        let expires = expires_at.and_then(|t| i64::try_from(t).ok());
        let version_i = i64::try_from(version).unwrap_or(i64::MAX);
        write(
            options.durable,
            "INSERT INTO wasmkeep_kv (key, value, version, expires_at) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (key) DO UPDATE SET
                value = excluded.value, version = excluded.version,
                expires_at = excluded.expires_at",
            &params![key, value, version_i, expires],
        )?;
        if let Some(at) = expires_at {
            // The alarm deletes it; an earlier pending alarm already covers it.
            if alarms::get_alarm().is_none_or(|pending| pending > at) {
                alarms::set_alarm(at);
            }
        }
        Ok(version)
    }

    /// Removes `key` if `expect` holds; `true` if a live entry was removed.
    pub fn delete(key: &[u8], expect: Expect, durable: bool) -> Result<bool, Error> {
        check_key(key)?;
        let current = get(key)?.map(|e| e.version);
        if !expect.allows(current) {
            return Err(Error::Conflict);
        }
        write(
            durable,
            "DELETE FROM wasmkeep_kv WHERE key = ?1",
            &params![key],
        )?;
        Ok(current.is_some())
    }

    /// Up to `limit` live keys starting with `prefix`, after `after`.
    pub fn list(prefix: &[u8], after: Option<&[u8]>, limit: u32) -> Result<Vec<Vec<u8>>, Error> {
        let len = i64::try_from(prefix.len()).unwrap_or(i64::MAX);
        let rows = storage::query(
            "SELECT key FROM wasmkeep_kv
             WHERE key >= ?1 AND substr(key, 1, ?2) = ?1
               AND (?3 IS NULL OR key > ?3)
               AND (expires_at IS NULL OR expires_at > ?4)
             ORDER BY key LIMIT ?5",
            &params![
                prefix,
                len,
                after.map(<[u8]>::to_vec),
                now(),
                i64::from(limit)
            ],
        )?;
        Ok((0..rows.len())
            .filter_map(|i| {
                rows.get(i, "key")
                    .and_then(Value::as_bytes)
                    .map(<[u8]>::to_vec)
            })
            .collect())
    }

    /// Deletes expired keys (up to a batch) and sets the alarm for the next
    /// expiry. The [`Shard`] class runs it as its alarm.
    pub fn on_alarm() -> Result<(), Error> {
        storage::exec(
            "DELETE FROM wasmkeep_kv WHERE key IN (
                SELECT key FROM wasmkeep_kv WHERE expires_at IS NOT NULL AND expires_at <= ?1
                ORDER BY expires_at LIMIT ?2)",
            &params![now(), i64::from(PURGE_BATCH)],
        )?;
        let rows = storage::query(
            "SELECT MIN(expires_at) AS next FROM wasmkeep_kv WHERE expires_at IS NOT NULL",
            &[],
        )?;
        match as_u64(rows.get(0, "next")) {
            Some(next) => alarms::set_alarm(next),
            None => alarms::clear_alarm(),
        }
        Ok(())
    }

    /// Serves [`Client`]'s protocol. Requests from outside clients (without
    /// `x-wasmkeep-caller`) get `403`: only other actors may use the shard.
    pub fn handle(request: IncomingRequest, out: ResponseOutparam) {
        handle_allowing(request, out, |_| true);
    }

    /// Like [`handle`], also refusing (`403`) actors for which `allow`
    /// returns `false`; it gets the caller's `tenant/class/name`.
    pub fn handle_allowing(
        request: IncomingRequest,
        out: ResponseOutparam,
        allow: impl Fn(&str) -> bool,
    ) {
        let (status, body) = match wire::caller(&request) {
            Some(caller) if allow(&caller) => serve(&request),
            _ => (403, b"only other actors may use this shard\n".to_vec()),
        };
        let content_type = if status == 200 {
            "application/octet-stream"
        } else {
            "text/plain"
        };
        http::respond(out, status, content_type, &body);
    }

    fn serve(request: &IncomingRequest) -> (u16, Vec<u8>) {
        let path = request.path_with_query().unwrap_or_default();
        let Some(rest) = path.strip_prefix(PREFIX) else {
            return (404, b"not a kv path\n".to_vec());
        };
        let headers = wire::request_headers(request);
        let method = request.method();
        let result = if let Some(hex) = rest.strip_prefix("/k/") {
            let Some(key) = wire::unhex(hex.split('?').next().unwrap_or_default()) else {
                return (400, b"key is not hex\n".to_vec());
            };
            let expect = match wire::header(&headers, EXPECT).map(Expect::decode) {
                None => Expect::Any,
                Some(Some(e)) => e,
                Some(None) => return (400, b"bad expect header\n".to_vec()),
            };
            let durable = wire::header(&headers, DURABLE) != Some("0");
            match method {
                Method::Get => get(&key).map(|e| match e {
                    Some(e) => (200, encode_entry(&e)),
                    None => (404, b"no such key\n".to_vec()),
                }),
                Method::Put => {
                    let Some(value) = http::read_body(request, MAX_BODY) else {
                        return (413, b"value too large\n".to_vec());
                    };
                    let ttl_ms = wire::header(&headers, TTL).and_then(|t| t.parse().ok());
                    let options = PutOptions {
                        expect,
                        ttl_ms,
                        durable,
                    };
                    put(&key, &value, options).map(|v| {
                        let mut w = Writer::default();
                        w.u64(v);
                        (200, w.finish())
                    })
                }
                Method::Delete => delete(&key, expect, durable).map(|removed| {
                    let mut w = Writer::default();
                    w.u64(u64::from(removed));
                    (200, w.finish())
                }),
                _ => Ok((405, b"use GET, PUT or DELETE\n".to_vec())),
            }
        } else if rest.starts_with("/list") && matches!(method, Method::Get) {
            let prefix = wire::query_param(rest, "prefix").map_or(Some(Vec::new()), wire::unhex);
            let after = wire::query_param(rest, "after").map(wire::unhex);
            let limit = wire::query_param(rest, "limit")
                .and_then(|l| l.parse().ok())
                .unwrap_or(1000_u32);
            match (prefix, after) {
                (Some(prefix), None) => list(&prefix, None, limit),
                (Some(prefix), Some(Some(after))) => list(&prefix, Some(&after), limit),
                _ => return (400, b"prefix or after is not hex\n".to_vec()),
            }
            .map(|keys| (200, encode_keys(&keys)))
        } else {
            Ok((404, b"not a kv path\n".to_vec()))
        };
        match result {
            Ok(r) => r,
            Err(Error::Conflict) => (409, b"conflict\n".to_vec()),
            Err(Error::KeyTooLong) => (400, b"key too long\n".to_vec()),
            Err(e) => (500, format!("{e}\n").into_bytes()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_hash_is_fnv1a_and_stable() {
        // Known FNV-1a 64 values: "" and "a".
        assert_eq!(
            shard_for(b"", u32::MAX),
            (0xcbf2_9ce4_8422_2325_u64 % u64::from(u32::MAX)) as u32
        );
        assert_eq!(
            shard_for(b"a", 1 << 16),
            (0xaf63_dc4c_8601_ec8c_u64 % (1 << 16)) as u32
        );
        assert_eq!(shard_for(b"anything", 1), 0);
        assert_eq!(shard_for(b"anything", 0), 0, "zero shards means one");
        // Spread: 1,000 keys over 8 shards touch every shard.
        let mut seen = [0_u32; 8];
        for i in 0..1000 {
            seen[shard_for(format!("user:{i}").as_bytes(), 8) as usize] += 1;
        }
        assert!(seen.iter().all(|n| *n > 60), "{seen:?}");
    }

    #[test]
    fn expectations() {
        assert!(Expect::Any.allows(None));
        assert!(Expect::Any.allows(Some(3)));
        assert!(Expect::Absent.allows(None));
        assert!(!Expect::Absent.allows(Some(3)));
        assert!(Expect::Version(3).allows(Some(3)));
        assert!(!Expect::Version(3).allows(Some(4)));
        assert!(!Expect::Version(3).allows(None));
        for e in [
            Expect::Any,
            Expect::Absent,
            Expect::Version(0),
            Expect::Version(u64::MAX),
        ] {
            assert_eq!(Expect::decode(&e.encode()), Some(e));
        }
        assert_eq!(Expect::decode("nope"), None);
    }

    #[test]
    fn entries_and_key_lists_round_trip() {
        let e = Entry {
            value: b"v".to_vec(),
            version: 9,
            expires_at: Some(1234),
        };
        assert_eq!(decode_entry(&encode_entry(&e)), Some(e.clone()));
        let forever = Entry {
            expires_at: None,
            ..e
        };
        assert_eq!(decode_entry(&encode_entry(&forever)), Some(forever));
        let keys = vec![b"a".to_vec(), Vec::new(), vec![0, 255]];
        assert_eq!(decode_keys(&encode_keys(&keys)), Some(keys));
        assert_eq!(decode_keys(&[0, 0, 0, 0, 0, 0, 0, 1]), None);
    }

    #[test]
    fn pages_from_shards_merge_in_order() {
        let pages = vec![
            vec![b"a".to_vec(), b"d".to_vec()],
            vec![b"b".to_vec(), b"c".to_vec(), b"e".to_vec()],
            vec![],
        ];
        assert_eq!(
            merge_pages(pages, 4),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );
    }

    #[test]
    fn read_cache_ages_expires_and_stays_bounded() {
        let mut c = ReadCache::new(2);
        let e = |v: u64, exp: Option<u64>| Entry {
            value: vec![],
            version: v,
            expires_at: exp,
        };
        c.insert(b"k".to_vec(), 100, Some(e(1, None)));
        assert_eq!(c.get(b"k", 150, 100), Some(Some(e(1, None))));
        assert_eq!(c.get(b"k", 200, 100), None, "too old");
        c.insert(b"gone".to_vec(), 100, None);
        assert_eq!(c.get(b"gone", 120, 100), Some(None), "absence is cached");
        assert_eq!(c.get(b"other", 120, 100), None, "a miss is not an absence");

        // An entry that expired while cached reads as absent.
        c.insert(b"k".to_vec(), 100, Some(e(2, Some(130))));
        assert_eq!(c.get(b"k", 129, 100), Some(Some(e(2, Some(130)))));
        assert_eq!(c.get(b"k", 130, 100), Some(None));

        // Full: expired entries are dropped first, then everything.
        c.insert(b"third".to_vec(), 140, Some(e(3, None)));
        assert!(c.entries.len() <= 2);
        assert_eq!(c.get(b"third", 141, 100), Some(Some(e(3, None))));
        c.remove(b"third");
        assert_eq!(c.get(b"third", 141, 100), None);
    }
}
