//! The host's `instr`, `replace`, two-argument `trim`, `ltrim` and
//! `rtrim`, and `like` and `glob` (the `LIKE` and `GLOB` operators) on
//! guest connections, in place of SQLite's own.
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

/// What `LIKE` or `GLOB` matches with: SQLite's `compareInfo`.
#[derive(Debug, Clone, Copy)]
struct Wildcards {
    /// `%` or `*`; 0: none (the escape is `%`).
    all: u32,
    /// `_` or `?`; 0: none (the escape is `_`).
    one: u32,
    /// `[` for `GLOB`; 0 for `LIKE`.
    set: u32,
    /// ASCII letters match in either case (`LIKE`).
    no_case: bool,
}

const GLOB: Wildcards = Wildcards {
    all: b'*' as u32,
    one: b'?' as u32,
    set: b'[' as u32,
    no_case: false,
};

const LIKE: Wildcards = Wildcards {
    all: b'%' as u32,
    one: b'_' as u32,
    set: 0,
    no_case: true,
};

/// `sqlite3Utf8Trans1`: the payload bits of a lead byte of 0xc0 or above.
const UTF8_TRANS1: [u8; 64] = [
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x00, 0x00,
];

/// `sqlite3Utf8Read` on a C string ending at `z`'s end: the character at
/// `*i` (0 at the end, where `*i` stays), and `*i` moved past it. Any
/// number of continuation bytes are taken, as SQLite takes them.
fn utf8_read(z: &[u8], i: &mut usize, meter: &mut Meter<'_>) -> Result<u32> {
    let Some(&lead) = z.get(*i) else {
        return Ok(0);
    };
    let start = *i;
    *i += 1;
    let mut c = u32::from(lead);
    if c >= 0xc0 {
        c = u32::from(UTF8_TRANS1[usize::from(lead - 0xc0)]);
        while let Some(&b) = z.get(*i)
            && b & 0xc0 == 0x80
        {
            c = (c << 6).wrapping_add(u32::from(b & 0x3f));
            *i += 1;
        }
        if c < 0x80 || c & 0xFFFF_F800 == 0xD800 {
            c = 0xFFFD;
        }
    }
    meter.charge(*i - start)?;
    Ok(c)
}

/// `SQLITE_SKIP_UTF8`: past the character at `i`.
fn utf8_skip(z: &[u8], mut i: usize) -> usize {
    if z.get(i).is_some_and(|&b| b >= 0xc0) {
        i += 1;
        while z.get(i).is_some_and(|&b| b & 0xc0 == 0x80) {
            i += 1;
        }
    } else {
        i += 1;
    }
    i
}

/// A C string: the bytes before the first NUL.
fn c_str(z: &[u8]) -> &[u8] {
    &z[..z.iter().position(|&b| b == 0).unwrap_or(z.len())]
}

/// Where the match goes on after its latest `*` (or `%`) failed further
/// on. SQLite's `patternCompare` recurses at every wildcard, but a call
/// that reaches the next wildcard never answers "no match, try further":
/// it matches, or fails the whole comparison. So only the latest
/// wildcard's search is ever resumed, and one of these replaces the
/// recursion (and its stack, one frame per wildcard).
#[derive(Debug, Clone, Copy)]
enum Resume {
    /// Find the next of the two ASCII bytes (the character in both cases)
    /// in the value from `next`, and go on past it at `pattern`.
    Byte {
        stop: [u8; 2],
        pattern: usize,
        next: usize,
    },
    /// Find the next character `c` (not ASCII) from `next`.
    Char { c: u32, pattern: usize, next: usize },
    /// `*[`: try the set at `pattern` at each character from `next`.
    Set { pattern: usize, next: usize },
}

/// How a comparison from one position ended: SQLite's `SQLITE_MATCH`,
/// `SQLITE_NOMATCH` and `SQLITE_NOWILDCARDMATCH`.
enum Outcome {
    Match,
    NoMatch,
    Never,
}

/// `patternCompare`: whether `string` matches `pattern`, with `escape`
/// the escape character (`LIKE`) or `[` (`GLOB`).
fn pattern_compare(
    pattern: &[u8],
    string: &[u8],
    w: Wildcards,
    escape: u32,
    meter: &mut Meter<'_>,
) -> Result<bool> {
    let (p, s) = (pattern, string);
    let (mut pi, mut si) = (0, 0);
    let mut escaped: Option<usize> = None;
    let mut resume: Option<Resume> = None;
    loop {
        let outcome = 'compare: loop {
            let mut c = utf8_read(p, &mut pi, meter)?;
            if c == 0 {
                break if si == s.len() {
                    Outcome::Match
                } else {
                    Outcome::NoMatch
                };
            }
            if c == w.all {
                // Further wildcards, and each `?` takes one character.
                loop {
                    c = utf8_read(p, &mut pi, meter)?;
                    if c == w.all || (c == w.one && w.one != 0) {
                        if c == w.one && utf8_read(s, &mut si, meter)? == 0 {
                            break 'compare Outcome::Never;
                        }
                    } else {
                        break;
                    }
                }
                if c == 0 {
                    break Outcome::Match;
                }
                if c == escape {
                    if w.set == 0 {
                        c = utf8_read(p, &mut pi, meter)?;
                        if c == 0 {
                            break Outcome::Never;
                        }
                    } else {
                        // `[` is one byte.
                        resume = Some(Resume::Set {
                            pattern: pi - 1,
                            next: si,
                        });
                        break Outcome::NoMatch;
                    }
                }
                resume = Some(if c < 0x80 {
                    // `c` is ASCII: one byte.
                    let b = c as u8;
                    let stop = if w.no_case {
                        [b.to_ascii_uppercase(), b.to_ascii_lowercase()]
                    } else {
                        [b, b]
                    };
                    Resume::Byte {
                        stop,
                        pattern: pi,
                        next: si,
                    }
                } else {
                    Resume::Char {
                        c,
                        pattern: pi,
                        next: si,
                    }
                });
                break Outcome::NoMatch;
            }
            if c == escape {
                if w.set == 0 {
                    c = utf8_read(p, &mut pi, meter)?;
                    if c == 0 {
                        break Outcome::NoMatch;
                    }
                    escaped = Some(pi);
                } else {
                    let mut prior = 0;
                    let mut seen = false;
                    let mut invert = false;
                    c = utf8_read(s, &mut si, meter)?;
                    if c == 0 {
                        break Outcome::NoMatch;
                    }
                    let mut c2 = utf8_read(p, &mut pi, meter)?;
                    if c2 == u32::from(b'^') {
                        invert = true;
                        c2 = utf8_read(p, &mut pi, meter)?;
                    }
                    if c2 == u32::from(b']') {
                        if c == u32::from(b']') {
                            seen = true;
                        }
                        c2 = utf8_read(p, &mut pi, meter)?;
                    }
                    while c2 != 0 && c2 != u32::from(b']') {
                        let peek = p.get(pi).copied().unwrap_or(0);
                        if c2 == u32::from(b'-') && peek != b']' && peek != 0 && prior > 0 {
                            c2 = utf8_read(p, &mut pi, meter)?;
                            if c >= prior && c <= c2 {
                                seen = true;
                            }
                            prior = 0;
                        } else {
                            if c == c2 {
                                seen = true;
                            }
                            prior = c2;
                        }
                        c2 = utf8_read(p, &mut pi, meter)?;
                    }
                    if c2 == 0 || seen == invert {
                        break Outcome::NoMatch;
                    }
                    continue;
                }
            }
            let c2 = utf8_read(s, &mut si, meter)?;
            if c == c2 {
                continue;
            }
            if w.no_case
                && c < 0x80
                && c2 < 0x80
                && (c as u8).to_ascii_lowercase() == (c2 as u8).to_ascii_lowercase()
            {
                continue;
            }
            if c == w.one && escaped != Some(pi) && c2 != 0 {
                continue;
            }
            break Outcome::NoMatch;
        };
        match outcome {
            Outcome::Match => return Ok(true),
            Outcome::Never => return Ok(false),
            Outcome::NoMatch => {}
        }
        // Go on from the latest wildcard's next candidate, as SQLite's
        // search loop after a recursive call that did not match.
        let Some(at) = resume.as_mut() else {
            return Ok(false);
        };
        escaped = None;
        match at {
            Resume::Byte {
                stop,
                pattern,
                next,
            } => {
                let rest = &s[*next..];
                let found = rest.iter().position(|b| stop.contains(b));
                meter.charge(found.map_or(rest.len(), |n| n + 1))?;
                let Some(n) = found else {
                    return Ok(false);
                };
                *next += n + 1;
                (pi, si) = (*pattern, *next);
            }
            Resume::Char { c, pattern, next } => loop {
                let c2 = utf8_read(s, next, meter)?;
                if c2 == 0 {
                    return Ok(false);
                }
                if c2 == *c {
                    (pi, si) = (*pattern, *next);
                    break;
                }
            },
            Resume::Set { pattern, next } => {
                if *next == s.len() {
                    return Ok(false);
                }
                (pi, si) = (*pattern, *next);
                let after = utf8_skip(s, *next);
                meter.charge(after - *next)?;
                *next = after;
            }
        }
    }
}

/// `like(pattern, value[, escape])` and `glob(pattern, value)`: SQLite's
/// `likeFunc`. `max_pattern` is the connection's
/// `SQLITE_LIMIT_LIKE_PATTERN_LENGTH`.
fn like(
    ctx: &Context<'_>,
    budget: &Budget,
    w: Wildcards,
    max_pattern: usize,
) -> Result<Option<i64>> {
    let pattern = text_arg(ctx, 0)?;
    if pattern.as_ref().map_or(0, |(p, _)| p.len()) > max_pattern {
        return Err(Error::UserFunctionError(
            "LIKE or GLOB pattern too complex".into(),
        ));
    }
    let mut w = w;
    let mut meter = Meter { budget, work: 0 };
    let escape = if ctx.len() == 3 {
        let Some((e, _)) = text_arg(ctx, 2)? else {
            return Ok(None);
        };
        let e = c_str(&e);
        // One character, as `sqlite3Utf8CharLen` counts them.
        if e.is_empty() || utf8_skip(e, 0) < e.len() {
            return Err(Error::UserFunctionError(
                "ESCAPE expression must be a single character".into(),
            ));
        }
        let escape = utf8_read(e, &mut 0, &mut meter)?;
        if escape == w.all {
            w.all = 0;
        }
        if escape == w.one {
            w.one = 0;
        }
        escape
    } else {
        w.set
    };
    let (Some((p, _)), Some((s, _))) = (pattern, text_arg(ctx, 1)?) else {
        return Ok(None);
    };
    let matched = pattern_compare(c_str(&p), c_str(&s), w, escape, &mut meter)?;
    Ok(Some(i64::from(matched)))
}
/// Installs the functions on a guest connection. `max_len` and
/// `max_pattern` are its `SQLITE_LIMIT_LENGTH` and
/// `SQLITE_LIMIT_LIKE_PATTERN_LENGTH`.
pub(crate) fn register(
    conn: &Connection,
    budget: &Budget,
    max_len: usize,
    max_pattern: usize,
) -> Result<()> {
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
    // The `LIKE` and `GLOB` operators call these. `PRAGMA
    // case_sensitive_like`, which would put SQLite's own back, is refused.
    for (name, args, w) in [("like", 2, LIKE), ("like", 3, LIKE), ("glob", 2, GLOB)] {
        let b = budget.clone();
        conn.create_scalar_function(name, args, flags, move |ctx| like(ctx, &b, w, max_pattern))?;
    }
    Ok(())
}
