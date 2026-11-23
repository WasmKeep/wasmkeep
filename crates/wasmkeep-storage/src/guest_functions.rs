//! The host's `instr`, `replace` and two-argument `trim`, `ltrim` and
//! `rtrim` on guest connections, in place of SQLite's own.
//!
//! SQLite's versions run inside one virtual machine step, where the progress
//! handler that enforces the statement's budgets cannot stop them, and take
//! time quadratic in their arguments: minutes, or far longer, on values of a
//! few MiB. These follow SQLite's algorithms step for step, so they answer
//! exactly as SQLite does (blobs, numbers, NULLs and invalid UTF-8
//! included), and check the statement's budgets as they go.

use std::borrow::Cow;
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::{ToSql, ToSqlOutput, ValueRef};
use rusqlite::{Connection, Error, Result};

/// Bytes compared (or positions stepped over) between two budget checks:
/// a few milliseconds of work.
const WORK_PER_CHECK: u64 = 1 << 22;

/// When the running statement must stop, as the progress handler reads it.
#[derive(Debug, Clone)]
pub(crate) struct Budget {
    /// Wall-clock deadline (`clock_ms`); 0: none.
    pub(crate) deadline: Arc<AtomicU64>,
    /// Thread CPU deadline (`thread_cpu_ns`); 0: none.
    pub(crate) cpu_deadline: Arc<AtomicU64>,
}

impl Budget {
    /// Whether the running statement is past one of its deadlines.
    pub(crate) fn spent(&self) -> bool {
        let at = self.deadline.load(Ordering::Relaxed);
        let cpu_at = self.cpu_deadline.load(Ordering::Relaxed);
        (at != 0 && crate::db::clock_ms() > at)
            || (cpu_at != 0 && crate::db::thread_cpu_ns() > cpu_at)
    }
}

/// Counts work and checks the budget every [`WORK_PER_CHECK`] units.
struct Meter<'a> {
    budget: &'a Budget,
    work: u64,
}

impl Meter<'_> {
    fn charge(&mut self, units: usize) -> Result<()> {
        self.work += u64::try_from(units).unwrap_or(u64::MAX).max(1);
        if self.work >= WORK_PER_CHECK {
            self.work = 0;
            if self.budget.spent() {
                // The message is what the caller recognises as a stop.
                return Err(Error::UserFunctionError(
                    "interrupted: the statement reached its time or CPU budget".into(),
                ));
            }
        }
        Ok(())
    }
}

/// A text result made of raw bytes, as SQLite's own functions return them
/// (not necessarily valid UTF-8).
struct RawText(Vec<u8>);

impl ToSql for RawText {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(&self.0)))
    }
}

/// An argument as `sqlite3_value_text` gives it, and whether it is a blob;
/// `None` for NULL.
fn text_arg<'a>(ctx: &'a Context<'_>, i: usize) -> Result<Option<(Cow<'a, [u8]>, bool)>> {
    Ok(match ctx.get_raw(i) {
        ValueRef::Null => None,
        ValueRef::Integer(n) => Some((Cow::Owned(n.to_string().into_bytes()), false)),
        ValueRef::Real(f) => Some((Cow::Owned(real_text(f)?), false)),
        ValueRef::Text(t) => Some((Cow::Borrowed(t), false)),
        ValueRef::Blob(b) => Some((Cow::Borrowed(b), true)),
    })
}

/// A real as SQLite renders it as text, asked of SQLite itself (its format
/// is not Rust's), on a plain connection of this thread.
fn real_text(f: f64) -> Result<Vec<u8>> {
    thread_local! {
        static PLAIN: RefCell<Option<Connection>> = const { RefCell::new(None) };
    }
    PLAIN.with(|plain| {
        let mut plain = plain.borrow_mut();
        if plain.is_none() {
            *plain = Some(Connection::open_in_memory()?);
        }
        let conn = plain
            .as_ref()
            .ok_or_else(|| Error::UserFunctionError("no connection".into()))?;
        conn.prepare_cached("SELECT CAST(?1 AS TEXT)")?
            .query_row([f], |r| Ok(r.get_ref(0)?.as_bytes()?.to_vec()))
    })
}

/// `instr(haystack, needle)`: SQLite's `instrFunc`.
fn instr(ctx: &Context<'_>, budget: &Budget) -> Result<Option<i64>> {
    let (Some((hay, hay_blob)), Some((needle, needle_blob))) =
        (text_arg(ctx, 0)?, text_arg(ctx, 1)?)
    else {
        return Ok(None);
    };
    let Some(&first) = needle.first() else {
        return Ok(Some(1));
    };
    // Characters are counted unless both are blobs.
    let is_text = !(hay_blob && needle_blob);
    let mut meter = Meter { budget, work: 0 };
    let mut pos = 0;
    let mut n = 1;
    while needle.len() <= hay.len() - pos {
        meter.charge(1)?;
        if hay[pos] == first {
            meter.charge(needle.len())?;
            if hay[pos..pos + needle.len()] == needle[..] {
                return Ok(Some(n));
            }
        }
        n += 1;
        pos += 1;
        while is_text && pos < hay.len() && hay[pos] & 0xc0 == 0x80 {
            pos += 1;
        }
    }
    Ok(Some(0))
}

/// `replace(x, pattern, replacement)`: SQLite's `replaceFunc`. `max_len` is
/// the connection's `SQLITE_LIMIT_LENGTH`.
fn replace(ctx: &Context<'_>, budget: &Budget, max_len: usize) -> Result<Option<RawText>> {
    let Some((s, _)) = text_arg(ctx, 0)? else {
        return Ok(None);
    };
    let Some((pattern, _)) = text_arg(ctx, 1)? else {
        return Ok(None);
    };
    // SQLite reads the pattern as a C string: empty when it starts with NUL.
    let Some(&first) = pattern.first().filter(|&&b| b != 0) else {
        return Ok(Some(RawText(s.into_owned())));
    };
    let Some((rep, _)) = text_arg(ctx, 2)? else {
        return Ok(None);
    };
    let mut meter = Meter { budget, work: 0 };
    let mut out = Vec::with_capacity(s.len());
    let mut out_len = s.len();
    let mut i = 0;
    while i + pattern.len() <= s.len() {
        meter.charge(1)?;
        let found = s[i] == first && {
            meter.charge(pattern.len())?;
            s[i..i + pattern.len()] == pattern[..]
        };
        if found {
            if rep.len() > pattern.len() {
                out_len += rep.len() - pattern.len();
                if out_len > max_len {
                    // No message: rusqlite would set it after the code,
                    // which turns the code back into a plain error.
                    return Err(Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_TOOBIG),
                        None,
                    ));
                }
            }
            out.extend_from_slice(&rep);
            i += pattern.len();
        } else {
            out.push(s[i]);
            i += 1;
        }
    }
    out.extend_from_slice(&s[i..]);
    Ok(Some(RawText(out)))
}

/// Trim from the left (1), the right (2) or both (3).
#[derive(Debug, Clone, Copy)]
struct Sides(u8);

/// `trim(x, set)`, `ltrim` and `rtrim`: SQLite's `trimFunc`.
fn trim(ctx: &Context<'_>, budget: &Budget, sides: Sides) -> Result<Option<RawText>> {
    let Some((input, _)) = text_arg(ctx, 0)? else {
        return Ok(None);
    };
    let Some((set, _)) = text_arg(ctx, 1)? else {
        return Ok(None);
    };
    // The set is read as a C string, split into characters as SQLite's
    // SQLITE_SKIP_UTF8 does: a byte, and if it is 0xc0 or above, every
    // continuation byte after it.
    let set = &set[..set.iter().position(|&b| b == 0).unwrap_or(set.len())];
    let (mut lo, mut hi) = (0, input.len());
    // A character is longer than a byte only where a byte of 0xc0 or above
    // is followed by a continuation byte.
    let multibyte = set.windows(2).any(|w| w[0] >= 0xc0 && w[1] & 0xc0 == 0x80);
    if !multibyte {
        // Each character one byte: a match consumes that byte, whichever
        // character of the set it is. Nothing allocated: the set can be
        // as large as a value.
        let mut member = [false; 256];
        for &b in set {
            member[usize::from(b)] = true;
        }
        if sides.0 & 1 != 0 {
            while lo < hi && member[usize::from(input[lo])] {
                lo += 1;
            }
        }
        if sides.0 & 2 != 0 {
            while hi > lo && member[usize::from(input[hi - 1])] {
                hi -= 1;
            }
        }
        return Ok(Some(RawText(input[lo..hi].to_vec())));
    }
    // Each character as (offset, length) in the set: 8 bytes each, outside
    // SQLite's heap cap.
    let mut chars: Vec<(u32, u32)> = Vec::new();
    let mut z = 0;
    while z < set.len() {
        let start = z;
        z += 1;
        if set[start] >= 0xc0 {
            while z < set.len() && set[z] & 0xc0 == 0x80 {
                z += 1;
            }
        }
        let span = |n: usize| u32::try_from(n).unwrap_or(u32::MAX);
        chars.push((span(start), span(z - start)));
    }
    let mut meter = Meter { budget, work: 0 };
    if sides.0 & 1 != 0 {
        while lo < hi {
            match first_match(set, &chars, &mut meter, &input[lo..hi], false)? {
                Some(len) => lo += len,
                None => break,
            }
        }
    }
    if sides.0 & 2 != 0 {
        while lo < hi {
            match first_match(set, &chars, &mut meter, &input[lo..hi], true)? {
                Some(len) => hi -= len,
                None => break,
            }
        }
    }
    Ok(Some(RawText(input[lo..hi].to_vec())))
}

/// The length of the first character of `chars`, in order, that `rest`
/// starts with (or ends with, from the right).
fn first_match(
    set: &[u8],
    chars: &[(u32, u32)],
    meter: &mut Meter<'_>,
    rest: &[u8],
    right: bool,
) -> Result<Option<usize>> {
    for &(at, len) in chars {
        let (at, len) = (at as usize, len as usize);
        let c = &set[at..at + len];
        meter.charge(c.len())?;
        let here = if right {
            rest.len().checked_sub(c.len()).map(|at| &rest[at..])
        } else {
            rest.get(..c.len())
        };
        if here == Some(c) {
            return Ok(Some(c.len()));
        }
    }
    Ok(None)
}

/// Installs the functions on a guest connection.
pub(crate) fn register(conn: &Connection, budget: &Budget, max_len: usize) -> Result<()> {
    // Innocuous and deterministic, as SQLite's own: usable in views,
    // triggers, CHECK constraints and indexes under `trusted_schema=OFF`.
    let flags = FunctionFlags::SQLITE_UTF8
        | FunctionFlags::SQLITE_DETERMINISTIC
        | FunctionFlags::SQLITE_INNOCUOUS;
    let b = budget.clone();
    conn.create_scalar_function("instr", 2, flags, move |ctx| instr(ctx, &b))?;
    let b = budget.clone();
    conn.create_scalar_function("replace", 3, flags, move |ctx| replace(ctx, &b, max_len))?;
    for (name, sides) in [("trim", 3), ("ltrim", 1), ("rtrim", 2)] {
        let b = budget.clone();
        conn.create_scalar_function(name, 2, flags, move |ctx| trim(ctx, &b, Sides(sides)))?;
    }
    Ok(())
}
