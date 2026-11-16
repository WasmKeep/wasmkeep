//! The structured error envelope: an opt-in way for an actor to answer an
//! HTTP failure the caller can act on. The TypeScript SDK
//! (`@wasmkeep/sdk/contracts`) writes and reads the same bytes.
//!
//! It is only an HTTP response convention, used when a handler chooses to.
//! It is not a transport failure ([`CallError`](crate::actors::CallError):
//! no answer came) and not a storage failure
//! ([`SqlError`](crate::SqlError), or a write not known to be durable). An
//! answer is an envelope only when its status is 400 or more and its content
//! type is [`CONTENT_TYPE`]; any other failure is [`Failure::Plain`], a
//! legacy or foreign answer, and is left as it is.
//!
//! The body is UTF-8 JSON of at most [`MAX_BYTES`]:
//!
//! ```text
//! {"v":1,"code":"cart.out_of_stock","message":"Out of stock","details":{"left":0}}
//! ```
//!
//! `code` is stable and meant for programs, `message` is public text meant
//! for people, `details` is optional JSON. Fields a reader does not know
//! are ignored; a version it does not know is reported, not guessed at.
//! Nothing here says whether to retry: a timeout or a 503 does not prove a
//! write was not committed. A message and details are public: leave out stack
//! traces, queries, storage locations and credentials.
//!
//! ```ignore
//! use wasmkeep_sdk::problem::{self, Problem};
//!
//! let p = Problem::new("cart.out_of_stock", "Out of stock")?.with_details(r#"{"left":0}"#)?;
//! http::respond_with(out, &problem::response(409, &p)?);
//! ```

use crate::front::actors::Response;
use crate::json::{self, Json};

/// The content type of an envelope.
pub const CONTENT_TYPE: &str = "application/vnd.wasmkeep.error+json";
/// The envelope version this module reads and writes.
pub const VERSION: u32 = 1;
/// The longest body, in bytes.
pub const MAX_BYTES: usize = 16 * 1024;
/// The longest `code`, in characters.
pub const MAX_CODE_LENGTH: usize = 64;
/// The longest `message`, in UTF-16 code units (as JavaScript counts).
pub const MAX_MESSAGE_LENGTH: usize = 512;
/// The deepest nesting of `details` (arrays and objects).
pub const MAX_DETAILS_DEPTH: usize = 8;

/// Why an envelope cannot be written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProblemError {
    /// The status is not 400 to 599.
    Status,
    /// The code is empty, too long, or not a letter followed by letters,
    /// digits and `_ . : -`.
    Code,
    /// The message is longer than [`MAX_MESSAGE_LENGTH`].
    Message,
    /// The details are not JSON, are nested too deeply, or have a number
    /// that is not finite.
    Details,
    /// The whole body would pass [`MAX_BYTES`].
    TooLarge,
}

impl std::fmt::Display for ProblemError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ProblemError::Status => "status must be 400 to 599",
            ProblemError::Code => "invalid error code",
            ProblemError::Message => "message too long",
            ProblemError::Details => "invalid details",
            ProblemError::TooLarge => "the error is too large",
        })
    }
}

impl std::error::Error for ProblemError {}

/// A public, structured error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    code: String,
    message: String,
    details: Option<String>,
}

fn valid_code(code: &str) -> bool {
    let mut chars = code.chars();
    code.chars().count() <= MAX_CODE_LENGTH
        && chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

fn valid_message(message: &str) -> bool {
    message.encode_utf16().count() <= MAX_MESSAGE_LENGTH
}

/// Whether `value` (rooted at `depth`, 1 for the details themselves) fits
/// the limits; the parser has already capped the members of an object.
fn details_ok(value: &Json, depth: usize) -> bool {
    match value {
        Json::Number(n) => n.is_finite(),
        Json::Array(items) => {
            depth <= MAX_DETAILS_DEPTH && items.iter().all(|v| details_ok(v, depth + 1))
        }
        Json::Object(members) => {
            depth <= MAX_DETAILS_DEPTH && members.iter().all(|(_, v)| details_ok(v, depth + 1))
        }
        _ => true,
    }
}

impl Problem {
    /// An error with a `code` and a public `message`.
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Result<Problem, ProblemError> {
        let (code, message) = (code.into(), message.into());
        if !valid_code(&code) {
            return Err(ProblemError::Code);
        }
        if !valid_message(&message) {
            return Err(ProblemError::Message);
        }
        Ok(Problem {
            code,
            message,
            details: None,
        })
    }

    /// With `details`: JSON text, checked and written back in canonical
    /// form (no whitespace).
    pub fn with_details(mut self, json_text: &str) -> Result<Problem, ProblemError> {
        let parsed = json::parse(json_text)
            .filter(|v| details_ok(v, 1))
            .ok_or(ProblemError::Details)?;
        let mut text = String::new();
        parsed.write(&mut text);
        self.details = Some(text);
        Ok(self)
    }

    /// The stable code.
    pub fn code(&self) -> &str {
        &self.code
    }

    /// The public message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The details, as JSON text, if any.
    pub fn details(&self) -> Option<&str> {
        self.details.as_deref()
    }

    /// The envelope's body.
    pub fn to_body(&self) -> Result<String, ProblemError> {
        let mut out = format!("{{\"v\":{VERSION},\"code\":");
        json::write_str(&mut out, &self.code);
        out.push_str(",\"message\":");
        json::write_str(&mut out, &self.message);
        if let Some(d) = &self.details {
            out.push_str(",\"details\":");
            out.push_str(d);
        }
        out.push('}');
        if out.len() > MAX_BYTES {
            return Err(ProblemError::TooLarge);
        }
        Ok(out)
    }
}

/// The response for `problem` with `status` (400 to 599).
pub fn response(status: u16, problem: &Problem) -> Result<Response, ProblemError> {
    if !(400..=599).contains(&status) {
        return Err(ProblemError::Status);
    }
    Ok(Response {
        status,
        headers: vec![("content-type".into(), CONTENT_TYPE.into())],
        body: problem.to_body()?.into_bytes(),
    })
}

/// What an answer whose status is not a success turns out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// A valid envelope.
    Problem {
        /// The HTTP status.
        status: u16,
        /// The error.
        problem: Problem,
    },
    /// An envelope of a version this module does not read.
    UnsupportedVersion {
        /// The HTTP status.
        status: u16,
        /// The `v` it carries.
        version: i64,
    },
    /// Claims to be an envelope but is not a valid one.
    Malformed {
        /// The HTTP status.
        status: u16,
        /// What is wrong.
        reason: &'static str,
    },
    /// No envelope: a legacy, foreign or plain-text failure. `text` is the
    /// start of the body (at most 512 characters) when it is UTF-8.
    Plain {
        /// The HTTP status.
        status: u16,
        /// The start of the body.
        text: Option<String>,
    },
}

fn media_type_is_envelope(headers: &[(String, String)]) -> bool {
    headers
        .iter()
        .filter(|(n, _)| n.eq_ignore_ascii_case("content-type"))
        .any(|(_, v)| {
            v.split(';')
                .next()
                .is_some_and(|m| m.trim().eq_ignore_ascii_case(CONTENT_TYPE))
        })
}

/// Classifies an answer with `status`, `headers` and `body`.
pub fn classify(status: u16, headers: &[(String, String)], body: &[u8]) -> Failure {
    if status < 400 || !media_type_is_envelope(headers) {
        return Failure::Plain {
            status,
            text: preview(body),
        };
    }
    let malformed = |reason| Failure::Malformed { status, reason };
    if body.len() > MAX_BYTES {
        return malformed("the body is too large");
    }
    let Ok(text) = std::str::from_utf8(body) else {
        return malformed("the body is not UTF-8");
    };
    let Some(parsed) = json::parse(text) else {
        return malformed("the body is not JSON");
    };
    if !matches!(parsed, Json::Object(_)) {
        return malformed("the body is not an object");
    }
    let version = match parsed.get("v") {
        Some(Json::Number(n)) if n.fract() == 0.0 && n.abs() <= 9_007_199_254_740_991.0 => {
            #[allow(
                clippy::cast_possible_truncation,
                reason = "checked: a whole safe integer"
            )]
            let v = *n as i64;
            v
        }
        _ => return malformed("v is missing or not an integer"),
    };
    if version != i64::from(VERSION) {
        return Failure::UnsupportedVersion { status, version };
    }
    let Some(code) = parsed
        .get("code")
        .and_then(Json::as_str)
        .filter(|c| valid_code(c))
    else {
        return malformed("code is missing or invalid");
    };
    let Some(message) = parsed
        .get("message")
        .and_then(Json::as_str)
        .filter(|m| valid_message(m))
    else {
        return malformed("message is missing or too long");
    };
    let details = match parsed.get("details") {
        None => None,
        Some(d) if details_ok(d, 1) => {
            let mut t = String::new();
            d.write(&mut t);
            Some(t)
        }
        Some(_) => return malformed("details are invalid"),
    };
    Failure::Problem {
        status,
        problem: Problem {
            code: code.into(),
            message: message.into(),
            details,
        },
    }
}

/// [`classify`] for a [`Response`] from `actors::call`.
pub fn classify_response(response: &Response) -> Failure {
    classify(response.status, &response.headers, &response.body)
}

fn preview(body: &[u8]) -> Option<String> {
    if body.len() > 64 * 1024 {
        return None;
    }
    let text = std::str::from_utf8(body).ok()?;
    Some(text.chars().take(512).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VECTORS: &str =
        include_str!("../../../sdks/typescript/test/fixtures/problem-vectors.json");

    fn vectors() -> Json {
        json::parse(VECTORS).expect("the shared vectors are JSON")
    }

    fn headers(ct: &str) -> Vec<(String, String)> {
        vec![("Content-Type".into(), ct.into())]
    }

    fn items<'a>(v: &'a Json, key: &str) -> &'a [Json] {
        v.get(key).and_then(Json::as_array).expect("list")
    }

    fn text<'a>(v: &'a Json, key: &str) -> &'a str {
        v.get(key).and_then(Json::as_str).expect("string")
    }

    #[test]
    fn classifies_the_shared_vectors() {
        let all = vectors();
        for v in items(&all, "decode") {
            let name = text(v, "name");
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "test data"
            )]
            let status = v.get("status").and_then(Json::as_count).expect("status") as u16;
            let got = classify(
                status,
                &headers(text(v, "content_type")),
                text(v, "body").as_bytes(),
            );
            let want = v.get("expect").expect("expect");
            match (text(want, "kind"), got) {
                ("problem", Failure::Problem { problem, .. }) => {
                    assert_eq!(problem.code(), text(want, "code"), "{name}");
                    assert_eq!(problem.message(), text(want, "message"), "{name}");
                    let got_details = problem.details().map(|d| json::parse(d).expect("details"));
                    assert_eq!(got_details.as_ref(), want.get("details"), "{name}");
                }
                ("unsupported-version", Failure::UnsupportedVersion { version, .. }) => {
                    assert_eq!(
                        Some(version),
                        want.get("version")
                            .and_then(Json::as_count)
                            .map(|n| i64::try_from(n).expect("small")),
                        "{name}"
                    );
                }
                ("malformed", Failure::Malformed { .. }) | ("plain", Failure::Plain { .. }) => {}
                (kind, got) => panic!("{name}: expected {kind}, got {got:?}"),
            }
        }
    }

    #[test]
    fn writes_the_shared_vectors_byte_for_byte() {
        let all = vectors();
        for v in items(&all, "produce") {
            let mut p = Problem::new(text(v, "code"), text(v, "message")).expect("valid");
            if let Some(d) = v.get("details") {
                let mut t = String::new();
                d.write(&mut t);
                p = p.with_details(&t).expect("details");
            }
            let status = u16::try_from(v.get("status").and_then(Json::as_count).expect("status"))
                .expect("u16");
            let r = response(status, &p).expect("response");
            assert_eq!(
                String::from_utf8(r.body).expect("utf8"),
                text(v, "body"),
                "{}",
                text(v, "name")
            );
            assert_eq!(
                r.headers,
                vec![("content-type".to_string(), CONTENT_TYPE.to_string())]
            );
            assert_eq!(text(&all, "content_type"), CONTENT_TYPE);
        }
    }

    #[test]
    fn a_written_envelope_classifies_back() {
        let p = Problem::new("a.b", "Say \"hi\"\n\u{e9}\u{1f600}")
            .and_then(|p| p.with_details(r#" { "a" : [ 1 , 2.5e0 , "x" ] } "#))
            .expect("valid");
        let r = response(422, &p).expect("response");
        assert_eq!(p.details(), Some(r#"{"a":[1,2.5,"x"]}"#));
        assert_eq!(
            classify_response(&r),
            Failure::Problem {
                status: 422,
                problem: p
            }
        );
    }

    #[test]
    fn refuses_what_a_reader_would_refuse() {
        assert_eq!(Problem::new("1a", "m"), Err(ProblemError::Code));
        assert_eq!(Problem::new("a b", "m"), Err(ProblemError::Code));
        assert_eq!(Problem::new("", "m"), Err(ProblemError::Code));
        assert_eq!(Problem::new("a".repeat(65), "m"), Err(ProblemError::Code));
        assert_eq!(
            Problem::new("a", "m".repeat(513)),
            Err(ProblemError::Message)
        );
        // 512 astral characters are 1024 UTF-16 units: too long, as in TypeScript.
        assert_eq!(
            Problem::new("a", "\u{1f600}".repeat(257)),
            Err(ProblemError::Message)
        );
        let ok = Problem::new("a", "m").expect("valid");
        assert_eq!(ok.clone().with_details("{"), Err(ProblemError::Details));
        assert_eq!(ok.clone().with_details("1e999"), Err(ProblemError::Details));
        assert_eq!(
            ok.clone().with_details(&"[".repeat(9)),
            Err(ProblemError::Details)
        );
        let nested = format!("{}{}", "[".repeat(8), "]".repeat(8));
        assert!(ok.clone().with_details(&nested).is_ok());
        let big = format!("\"{}\"", "d".repeat(MAX_BYTES));
        let p = ok.clone().with_details(&big).expect("details");
        assert_eq!(p.to_body(), Err(ProblemError::TooLarge));
        assert_eq!(response(200, &ok).unwrap_err(), ProblemError::Status);
        assert_eq!(response(600, &ok).unwrap_err(), ProblemError::Status);
    }

    #[test]
    fn a_hostile_body_is_malformed_not_trusted() {
        let h = headers(CONTENT_TYPE);
        let big = format!(
            r#"{{"v":1,"code":"x","message":"{}"}}"#,
            "m".repeat(MAX_BYTES)
        );
        for body in [
            big.as_bytes(),
            &[0x7b, 0xff, 0xfe],
            br#"{"v":1,"code":"x","mess"#,
            b"",
            b"null",
            br#"{"v":1.5,"code":"x","message":"m"}"#,
            br#"{"v":1,"code":"x","message":"m","details":1e999}"#,
        ] {
            assert!(
                matches!(classify(400, &h, body), Failure::Malformed { .. }),
                "{body:?}"
            );
        }
        assert_eq!(
            classify(400, &h, br#"{"v":-3,"code":"x","message":"m"}"#),
            Failure::UnsupportedVersion {
                status: 400,
                version: -3
            }
        );
    }

    #[test]
    fn plain_failures_keep_a_bounded_text() {
        let long = "x".repeat(2000);
        match classify(502, &[], long.as_bytes()) {
            Failure::Plain {
                status: 502,
                text: Some(t),
            } => assert_eq!(t.chars().count(), 512),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            classify(500, &[], &[0xff]),
            Failure::Plain {
                status: 500,
                text: None
            }
        );
    }
}
