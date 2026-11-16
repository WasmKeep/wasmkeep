//! The small JSON reader and writer the [`db`](crate::db) pattern speaks
//! between a caller and the database actor (ADR 0029). Internal: it reads
//! any JSON text, and writes exactly what JavaScript's `JSON.stringify`
//! writes for the same values, so the Rust and TypeScript SDKs send the
//! same bytes. No dependency: the SDK stays two crates.
//!
//! Reading follows `JSON.parse` where the two could differ: a repeated
//! key keeps its last value, and numbers are read as `f64`. Unlike it, a
//! `\u` escape of a lone surrogate is refused (a Rust string cannot hold
//! one), nesting is capped at [`MAX_DEPTH`] and an object's members at
//! [`MAX_MEMBERS`].

// Without the `actor` feature only the error envelope (`problem`) uses this
// module, and not all of it.
#![cfg_attr(not(feature = "actor"), allow(dead_code))]

use std::fmt::Write as _;

/// How deep arrays and objects may nest. The db protocol needs four.
pub(crate) const MAX_DEPTH: usize = 16;

/// Members one object may have. The db protocol's have at most four.
pub(crate) const MAX_MEMBERS: usize = 64;

/// A parsed JSON value. Object members keep their order; a repeated key
/// keeps its last value (as `JSON.parse`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Json {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    /// The member `key` of an object.
    pub(crate) fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Object(members) => members.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// Whether this is an object whose keys are all in `allowed`.
    pub(crate) fn has_only(&self, allowed: &[&str]) -> bool {
        match self {
            Json::Object(members) => members.iter().all(|(k, _)| allowed.contains(&k.as_str())),
            _ => false,
        }
    }

    /// Appends this value as JSON text, as `JSON.stringify` writes it.
    pub(crate) fn write(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Json::Number(n) => out.push_str(&number_to_string(*n)),
            Json::String(s) => write_str(out, s),
            Json::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write(out);
                }
                out.push(']');
            }
            Json::Object(members) => {
                out.push('{');
                for (i, (k, v)) in members.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_str(out, k);
                    out.push(':');
                    v.write(out);
                }
                out.push('}');
            }
        }
    }

    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Json::String(s) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn as_array(&self) -> Option<&[Json]> {
        match self {
            Json::Array(items) => Some(items),
            _ => None,
        }
    }

    /// A number that is a whole number from 0 to 2^53 - 1 (what a
    /// JavaScript number holds exactly).
    pub(crate) fn as_count(&self) -> Option<u64> {
        const MAX_SAFE: f64 = 9_007_199_254_740_991.0;
        match self {
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "checked: a whole number in 0..=2^53-1"
            )]
            Json::Number(n) if n.fract() == 0.0 && (0.0..=MAX_SAFE).contains(n) => Some(*n as u64),
            _ => None,
        }
    }
}

/// Parses a whole JSON text (whitespace around it allowed).
pub(crate) fn parse(text: &str) -> Option<Json> {
    parse_with(text, false)
}

/// Like [`parse`], but a repeated key in any object is an error rather than
/// "the last wins", so a value cannot be hidden by overwriting it. What
/// the error envelope's readers use, in both SDKs.
pub(crate) fn parse_unique(text: &str) -> Option<Json> {
    parse_with(text, true)
}

fn parse_with(text: &str, unique: bool) -> Option<Json> {
    let mut p = Parser {
        s: text.as_bytes(),
        at: 0,
        unique,
    };
    let v = p.value(0)?;
    p.ws();
    (p.at == p.s.len()).then_some(v)
}

struct Parser<'a> {
    s: &'a [u8],
    at: usize,
    unique: bool,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.s.get(self.at).copied()
    }

    fn ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at += 1;
        }
    }

    fn eat(&mut self, lit: &[u8]) -> Option<()> {
        if self.s.get(self.at..self.at + lit.len())? == lit {
            self.at += lit.len();
            Some(())
        } else {
            None
        }
    }

    fn value(&mut self, depth: usize) -> Option<Json> {
        self.ws();
        match self.peek()? {
            b'n' => self.eat(b"null").map(|()| Json::Null),
            b't' => self.eat(b"true").map(|()| Json::Bool(true)),
            b'f' => self.eat(b"false").map(|()| Json::Bool(false)),
            b'"' => self.string().map(Json::String),
            b'[' => {
                if depth >= MAX_DEPTH {
                    return None;
                }
                self.at += 1;
                let mut items = Vec::new();
                self.ws();
                if self.peek()? == b']' {
                    self.at += 1;
                    return Some(Json::Array(items));
                }
                loop {
                    items.push(self.value(depth + 1)?);
                    self.ws();
                    match self.peek()? {
                        b',' => self.at += 1,
                        b']' => {
                            self.at += 1;
                            return Some(Json::Array(items));
                        }
                        _ => return None,
                    }
                }
            }
            b'{' => {
                if depth >= MAX_DEPTH {
                    return None;
                }
                self.at += 1;
                let mut members: Vec<(String, Json)> = Vec::new();
                self.ws();
                if self.peek()? == b'}' {
                    self.at += 1;
                    return Some(Json::Object(members));
                }
                loop {
                    self.ws();
                    if self.peek()? != b'"' {
                        return None;
                    }
                    let key = self.string()?;
                    self.ws();
                    self.eat(b":")?;
                    let value = self.value(depth + 1)?;
                    // As `JSON.parse`: the last of a repeated key wins.
                    // The protocol's objects are small: a cap keeps this
                    // linear search from going quadratic.
                    if self.unique && members.iter().any(|(k, _)| *k == key) {
                        return None;
                    }
                    members.retain(|(k, _)| *k != key);
                    if members.len() >= MAX_MEMBERS {
                        return None;
                    }
                    members.push((key, value));
                    self.ws();
                    match self.peek()? {
                        b',' => self.at += 1,
                        b'}' => {
                            self.at += 1;
                            return Some(Json::Object(members));
                        }
                        _ => return None,
                    }
                }
            }
            b'-' | b'0'..=b'9' => self.number().map(Json::Number),
            _ => None,
        }
    }

    fn digits(&mut self) -> usize {
        let start = self.at;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.at += 1;
        }
        self.at - start
    }

    /// The JSON number grammar, read as the nearest `f64` (as JavaScript).
    fn number(&mut self) -> Option<f64> {
        let start = self.at;
        if self.peek() == Some(b'-') {
            self.at += 1;
        }
        match self.peek()? {
            b'0' => self.at += 1,
            b'1'..=b'9' => {
                self.digits();
            }
            _ => return None,
        }
        if self.peek() == Some(b'.') {
            self.at += 1;
            if self.digits() == 0 {
                return None;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.at += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.at += 1;
            }
            if self.digits() == 0 {
                return None;
            }
        }
        std::str::from_utf8(&self.s[start..self.at])
            .ok()?
            .parse()
            .ok()
    }

    fn hex4(&mut self) -> Option<u32> {
        let h = std::str::from_utf8(self.s.get(self.at..self.at + 4)?).ok()?;
        if !h.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        self.at += 4;
        u32::from_str_radix(h, 16).ok()
    }

    fn string(&mut self) -> Option<String> {
        self.eat(b"\"")?;
        let mut out = String::new();
        loop {
            // Copy the run up to the next quote, backslash or control.
            let start = self.at;
            while let Some(b) = self.peek() {
                if b == b'"' || b == b'\\' || b < 0x20 {
                    break;
                }
                self.at += 1;
            }
            out.push_str(std::str::from_utf8(&self.s[start..self.at]).ok()?);
            match self.peek()? {
                b'"' => {
                    self.at += 1;
                    return Some(out);
                }
                b'\\' => {
                    self.at += 1;
                    let c = self.peek()?;
                    self.at += 1;
                    match c {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let hi = self.hex4()?;
                            let code = if (0xd800..0xdc00).contains(&hi) {
                                self.eat(b"\\u")?;
                                let lo = self.hex4()?;
                                if !(0xdc00..0xe000).contains(&lo) {
                                    return None;
                                }
                                0x10000 + ((hi - 0xd800) << 10) + (lo - 0xdc00)
                            } else {
                                hi
                            };
                            // A lone low surrogate is not a char either.
                            out.push(char::from_u32(code)?);
                        }
                        _ => return None,
                    }
                }
                // A raw control character inside a string.
                _ => return None,
            }
        }
    }
}

/// Appends `s` as a JSON string, escaped as `JSON.stringify` does: `"`,
/// `\`, the five short escapes, and other controls as `\u00xx`.
pub(crate) fn write_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// `v` as JavaScript's `String(v)` writes a number (shortest digits that
/// read back as `v`; exponent form below 1e-6 and from 1e21), except that
/// negative zero is `-0` so it survives the trip. `NaN`, `Infinity` and
/// `-Infinity` as JavaScript spells them.
pub(crate) fn number_to_string(v: f64) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0" } else { "0" }.into();
    }
    // Rust's `{:e}` gives the fewest digits that read back as `v`. When
    // several strings of that length do, ECMAScript takes the one closest
    // to `v`: the value correctly rounded to that many digits, which
    // `{:.N$e}` computes exactly (and which reads back too, being closer).
    let shortest = format!("{:e}", v.abs());
    let len = shortest
        .split_once('e')
        .map_or(1, |(m, _)| m.bytes().filter(u8::is_ascii_digit).count());
    let sci = format!("{:.*e}", len.saturating_sub(1), v.abs());
    let Some((mantissa, exp)) = sci.split_once('e') else {
        return sci;
    };
    let mut digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
    }
    let Ok(exp) = exp.parse::<i32>() else {
        return sci;
    };
    let k = i32::try_from(digits.len()).unwrap_or(i32::MAX);
    // ECMAScript's Number::toString: the value is 0.digits × 10^n.
    let n = exp + 1;
    let mut out = String::new();
    if v < 0.0 {
        out.push('-');
    }
    if k <= n && n <= 21 {
        out.push_str(&digits);
        out.extend(std::iter::repeat_n(
            '0',
            usize::try_from(n - k).unwrap_or(0),
        ));
    } else if 0 < n && n <= 21 {
        let (int, frac) = digits.split_at(usize::try_from(n).unwrap_or(0));
        out.push_str(int);
        out.push('.');
        out.push_str(frac);
    } else if -6 < n && n <= 0 {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', usize::try_from(-n).unwrap_or(0)));
        out.push_str(&digits);
    } else {
        let (first, rest) = digits.split_at(1);
        out.push_str(first);
        if !rest.is_empty() {
            out.push('.');
            out.push_str(rest);
        }
        out.push('e');
        out.push(if n - 1 < 0 { '-' } else { '+' });
        let _ = write!(out, "{}", (n - 1).abs());
    }
    out
}

/// Reads what [`number_to_string`] writes (and any other decimal form
/// JavaScript's `Number(s)` would read the same way for those values).
pub(crate) fn number_from_string(s: &str) -> Option<f64> {
    match s {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        // Rust would read "inf", "nan" and "+1"; JavaScript's spellings only.
        s if s
            .bytes()
            .all(|b| b.is_ascii_digit() || b"-.eE+".contains(&b))
            && s.bytes().any(|b| b.is_ascii_digit())
            && !s.starts_with('+') =>
        {
            s.parse().ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json() {
        assert_eq!(parse(" null "), Some(Json::Null));
        assert_eq!(
            parse("[true,false]"),
            Some(Json::Array(vec![Json::Bool(true), Json::Bool(false)]))
        );
        assert_eq!(parse("-1.5e2"), Some(Json::Number(-150.0)));
        assert_eq!(
            parse(r#""a\"\\\/\b\f\n\r\t\u00e9\ud83d\ude00""#),
            Some(Json::String("a\"\\/\u{8}\u{c}\n\r\t\u{e9}\u{1f600}".into()))
        );
        let o = parse(r#"{"a":1,"b":[],"a":2}"#).unwrap();
        assert_eq!(o.get("a"), Some(&Json::Number(2.0)), "the last repeat wins");
        assert!(o.has_only(&["a", "b"]));
        assert!(!o.has_only(&["a"]));
        for bad in [
            "",
            "nul",
            "[1,]",
            "{\"a\"}",
            "01",
            "1.",
            ".5",
            "-",
            "1e",
            "+1",
            "\"\\ud800\"",
            "\"\\udc00\"",
            "\"\\ud800\\u0041\"",
            "\"a\nb\"",
            "\"\\x\"",
            "[1] 2",
            "{'a':1}",
            "\"abc",
            "NaN",
            "Infinity",
        ] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
        let deep = "[".repeat(MAX_DEPTH) + &"]".repeat(MAX_DEPTH);
        assert!(parse(&deep).is_some());
        let deeper = "[".repeat(MAX_DEPTH + 1) + &"]".repeat(MAX_DEPTH + 1);
        assert_eq!(parse(&deeper), None);
        let object = |n: usize| {
            let members: Vec<String> = (0..n).map(|i| format!("\"k{i}\":0")).collect();
            format!("{{{}}}", members.join(","))
        };
        assert!(parse(&object(MAX_MEMBERS)).is_some());
        assert_eq!(parse(&object(MAX_MEMBERS + 1)), None, "too many members");
        assert!(
            parse(r#"{"a":1,"a":2,"a":3}"#).is_some(),
            "repeats do not count"
        );
    }

    #[test]
    fn counts_are_safe_whole_numbers() {
        assert_eq!(Json::Number(3.0).as_count(), Some(3));
        assert_eq!(
            Json::Number(9_007_199_254_740_991.0).as_count(),
            Some(9_007_199_254_740_991)
        );
        assert_eq!(Json::Number(9_007_199_254_740_992.0).as_count(), None);
        assert_eq!(Json::Number(-1.0).as_count(), None);
        assert_eq!(Json::Number(1.5).as_count(), None);
    }

    #[test]
    fn strings_are_escaped_as_json_stringify() {
        let mut s = String::new();
        write_str(
            &mut s,
            "a\"\\\u{0}\u{1f}\u{7f}\n\t\u{8}\u{c}\r/é\u{2028}\u{1f600}",
        );
        assert_eq!(
            s,
            "\"a\\\"\\\\\\u0000\\u001f\u{7f}\\n\\t\\b\\f\\r/é\u{2028}\u{1f600}\""
        );
        assert_eq!(
            parse(&s),
            Some(Json::String(
                "a\"\\\u{0}\u{1f}\u{7f}\n\t\u{8}\u{c}\r/é\u{2028}\u{1f600}".into()
            ))
        );
    }

    #[test]
    fn numbers_are_written_as_javascript_writes_them() {
        // What `String(v)` gives in JavaScript, value by value.
        let cases: &[(f64, &str)] = &[
            (1.5, "1.5"),
            (2.0, "2"),
            (-2.0, "-2"),
            (0.1, "0.1"),
            (100.0, "100"),
            (123_456_789.0, "123456789"),
            (1e21, "1e+21"),
            (1e20, "100000000000000000000"),
            (1.5e21, "1.5e+21"),
            (1e-6, "0.000001"),
            (1e-7, "1e-7"),
            (1.25e-7, "1.25e-7"),
            (0.000_123, "0.000123"),
            (f64::MAX, "1.7976931348623157e+308"),
            (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
            (5e-324, "5e-324"),
            (9_007_199_254_740_993.0, "9007199254740992"),
            (0.0, "0"),
            (-0.0, "-0"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
            // Two shortest strings read back; JavaScript takes the closer.
            (f64::from_bits(0x42d8_ffff_f2f9_52c8), "109951159362891.12"),
        ];
        for (v, s) in cases {
            assert_eq!(number_to_string(*v), *s, "{v:e}");
            let back = number_from_string(s).unwrap();
            assert!(
                back == *v && back.is_sign_negative() == v.is_sign_negative(),
                "{s}"
            );
        }
        assert!(number_to_string(f64::NAN) == "NaN" && number_from_string("NaN").unwrap().is_nan());
        for bad in ["", "inf", "nan", "+1", "1x", "infinity", "-"] {
            assert_eq!(number_from_string(bad), None, "{bad:?}");
        }
    }
}
