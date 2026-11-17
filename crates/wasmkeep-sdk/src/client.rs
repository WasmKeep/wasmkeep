//! Typed clients for calls between actors: the Rust side of the TypeScript
//! SDK's typed clients (`@wasmkeep/sdk/contracts`, `docs/typed-clients.md`).
//!
//! An operation (a name, a method, a path, an input, an answer and the errors
//! it may return) is declared once with [`contract!`](crate::contract), and
//! the macro writes a client whose methods take and return values, not
//! bytes. Every input is checked before anything is sent and every answer
//! before it is returned, so an HTTP 200 with the wrong shape is a
//! [`Failure::InvalidResponse`], not a value you believe.
//!
//! ```ignore
//! use wasmkeep_sdk::client::{Failure, Nullable};
//! use wasmkeep_sdk::url_path;
//!
//! wasmkeep_sdk::record! {
//!     #[derive(Debug, Clone, PartialEq)]
//!     pub struct AddItem { pub sku: String, pub qty: i64 }
//! }
//! wasmkeep_sdk::record! {
//!     #[derive(Debug, Clone, PartialEq)]
//!     pub struct Count { pub count: i64, pub note: Option<Nullable<String>> }
//! }
//! wasmkeep_sdk::record! {
//!     #[derive(Debug, Clone, PartialEq)]
//!     pub struct OutOfStock { pub left: i64 }
//! }
//!
//! wasmkeep_sdk::contract! {
//!     pub mod cart {
//!         fn add_item(AddItem) -> Count
//!             = POST |i: &AddItem| url_path!("/items/{}", i.sku),
//!             errors AddItemError {
//!                 OutOfStock("cart.out_of_stock"): OutOfStock,
//!                 Closed("cart.closed"),
//!             };
//!         fn get() -> Count = GET "/";
//!     }
//! }
//!
//! let alice = cart::Client::new("cart/alice");
//! match alice.add_item(&AddItem { sku: "A1".into(), qty: 2 }) {
//!     Ok(reply) => println!("{}", reply.value.count),
//!     Err(Failure::Declared { error: cart::AddItemError::OutOfStock(o), .. }) => {
//!         println!("{} left", o.left);
//!     }
//!     Err(other) => println!("{}", other.kind()),
//! }
//! ```
//!
//! This is a client for calls from one actor (or a front) to another, over
//! `actors::call` and `calls::call_many`, as in TypeScript. Nothing here
//! retries, and nothing says a call is safe to retry: after a
//! [`Failure::Transport`] or a 5xx a write may have been committed. See
//! `docs/typed-clients.md` for the contract rules both SDKs share.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;

use crate::front::{actors, calls};
use crate::json;
pub use crate::json::Json as Value;
use crate::problem::{self, Problem};

/// The most calls one batch holds (the host's limit: a longer batch starts
/// none).
pub const MAX_BATCH: usize = 16;

/// The longest success body an operation decodes by default, in bytes.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

// ---------------------------------------------------------------------
// Codecs

/// What a codec refuses: where (`items[2].sku`, empty at the top) and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError {
    /// Where in the value.
    pub path: String,
    /// Why it was refused.
    pub reason: String,
}

impl CodecError {
    /// An error at the top of a value.
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            path: String::new(),
            reason: reason.into(),
        }
    }

    /// The same error, one level down under `segment` (a key or `[index]`).
    #[must_use]
    pub fn within(self, segment: &str) -> Self {
        let sep = if self.path.is_empty() || self.path.starts_with('[') {
            ""
        } else {
            "."
        };
        Self {
            path: format!("{segment}{sep}{}", self.path),
            reason: self.reason,
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.path.is_empty() {
            f.write_str(&self.reason)
        } else {
            write!(f, "{}: {}", self.path, self.reason)
        }
    }
}

impl std::error::Error for CodecError {}

fn describe(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
        Value::String(_) => "a string",
        Value::Number(_) => "a number",
        Value::Bool(_) => "a boolean",
    }
}

/// A type that is written as JSON (an operation's input, as the TypeScript
/// codecs' `parse` checks it before it is sent).
pub trait Encode {
    /// The JSON value, or why this value cannot be sent.
    ///
    /// # Errors
    /// A [`CodecError`] for a number JavaScript cannot hold exactly.
    fn encode(&self) -> Result<Value, CodecError>;

    /// The request body: `None` for no body (the unit type).
    #[doc(hidden)]
    fn encode_body(&self) -> Result<Option<Value>, CodecError> {
        self.encode().map(Some)
    }

    /// Whether this is an absent key (`None` of an optional field).
    #[doc(hidden)]
    fn is_absent(&self) -> bool {
        false
    }
}

/// A type that is read from JSON and checked, never asserted.
pub trait Decode: Sized {
    /// Reads `v`.
    ///
    /// # Errors
    /// A [`CodecError`] saying where and why it is not a `Self`.
    fn decode(v: &Value) -> Result<Self, CodecError>;

    /// Reads a value that may be absent (an empty body, no `details`).
    #[doc(hidden)]
    fn decode_opt(v: Option<&Value>) -> Result<Self, CodecError> {
        match v {
            Some(v) => Self::decode(v),
            None => Err(CodecError::new("expected a value, got undefined")),
        }
    }

    /// Reads an object member that may be missing.
    #[doc(hidden)]
    fn decode_field(v: Option<&Value>) -> Result<Self, CodecError> {
        match v {
            Some(v) => Self::decode(v),
            None => Err(CodecError::new("missing")),
        }
    }
}

impl<T: Encode + ?Sized> Encode for &T {
    fn encode(&self) -> Result<Value, CodecError> {
        (**self).encode()
    }
    fn encode_body(&self) -> Result<Option<Value>, CodecError> {
        (**self).encode_body()
    }
    fn is_absent(&self) -> bool {
        (**self).is_absent()
    }
}

/// The unit type is no value: an empty body, or an error without details
/// (`codec.none()` in TypeScript).
impl Encode for () {
    fn encode(&self) -> Result<Value, CodecError> {
        Ok(Value::Null)
    }
    fn encode_body(&self) -> Result<Option<Value>, CodecError> {
        Ok(None)
    }
}

impl Decode for () {
    fn decode(v: &Value) -> Result<Self, CodecError> {
        Err(CodecError::new(format!(
            "expected no value, got {}",
            describe(v)
        )))
    }
    fn decode_opt(v: Option<&Value>) -> Result<Self, CodecError> {
        match v {
            None => Ok(()),
            Some(v) => Self::decode(v),
        }
    }
}

/// Any JSON, not checked (`codec.unknown()`).
impl Encode for Value {
    fn encode(&self) -> Result<Value, CodecError> {
        Ok(self.clone())
    }
}

impl Decode for Value {
    fn decode(v: &Value) -> Result<Self, CodecError> {
        Ok(v.clone())
    }
}

impl Encode for str {
    fn encode(&self) -> Result<Value, CodecError> {
        Ok(Value::String(self.to_owned()))
    }
}

impl Encode for String {
    fn encode(&self) -> Result<Value, CodecError> {
        Ok(Value::String(self.clone()))
    }
}

impl Decode for String {
    fn decode(v: &Value) -> Result<Self, CodecError> {
        match v {
            Value::String(s) => Ok(s.clone()),
            other => Err(CodecError::new(format!(
                "expected a string, got {}",
                describe(other)
            ))),
        }
    }
}

impl Encode for bool {
    fn encode(&self) -> Result<Value, CodecError> {
        Ok(Value::Bool(*self))
    }
}

impl Decode for bool {
    fn decode(v: &Value) -> Result<Self, CodecError> {
        match v {
            Value::Bool(b) => Ok(*b),
            other => Err(CodecError::new(format!(
                "expected a boolean, got {}",
                describe(other)
            ))),
        }
    }
}

/// A finite number (`codec.number()`).
impl Encode for f64 {
    fn encode(&self) -> Result<Value, CodecError> {
        if self.is_finite() {
            Ok(Value::Number(*self))
        } else {
            Err(CodecError::new("expected a finite number"))
        }
    }
}

impl Decode for f64 {
    fn decode(v: &Value) -> Result<Self, CodecError> {
        match v {
            Value::Number(n) if n.is_finite() => Ok(*n),
            other => Err(CodecError::new(format!(
                "expected a finite number, got {}",
                describe(other)
            ))),
        }
    }
}

// A whole number JavaScript holds exactly (`codec.integer()`): at most
// 2^53 - 1 in magnitude, whatever the Rust type could hold.
macro_rules! integers {
    ($($t:ty),*) => {$(
        impl Encode for $t {
            fn encode(&self) -> Result<Value, CodecError> {
                #[allow(clippy::cast_precision_loss, clippy::cast_lossless, reason = "checked against 2^53 - 1 below")]
                let n = *self as f64;
                if n.abs() <= MAX_SAFE_INTEGER {
                    Ok(Value::Number(n))
                } else {
                    Err(CodecError::new("expected a safe integer (at most 2^53 - 1)"))
                }
            }
        }
        impl Decode for $t {
            fn decode(v: &Value) -> Result<Self, CodecError> {
                match v {
                    Value::Number(n) if n.fract() == 0.0 && n.abs() <= MAX_SAFE_INTEGER => {
                        #[allow(clippy::cast_possible_truncation, reason = "a whole safe integer")]
                        let whole = *n as i64;
                        <$t>::try_from(whole)
                            .map_err(|_| CodecError::new(format!("{whole} does not fit {}", stringify!($t))))
                    }
                    other => Err(CodecError::new(format!(
                        "expected a safe integer, got {}",
                        describe(other)
                    ))),
                }
            }
        }
    )*};
}
integers!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);

/// An object key that may be absent (`codec.optional`). `None` is left out
/// when written; `null` is not absent and is refused when read: use
/// [`Nullable`] for a value that may be `null`.
impl<T: Encode> Encode for Option<T> {
    fn encode(&self) -> Result<Value, CodecError> {
        match self {
            Some(v) => v.encode(),
            None => Ok(Value::Null),
        }
    }
    fn is_absent(&self) -> bool {
        self.is_none()
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode(v: &Value) -> Result<Self, CodecError> {
        T::decode(v).map(Some)
    }
    fn decode_opt(v: Option<&Value>) -> Result<Self, CodecError> {
        v.map(T::decode).transpose()
    }
    fn decode_field(v: Option<&Value>) -> Result<Self, CodecError> {
        Self::decode_opt(v)
    }
}

/// A value that may be `null` but must be present (`codec.nullable`).
/// `Option<Nullable<T>>` is an optional key that may also be `null`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nullable<T>(pub Option<T>);

impl<T: Encode> Encode for Nullable<T> {
    fn encode(&self) -> Result<Value, CodecError> {
        match &self.0 {
            Some(v) => v.encode(),
            None => Ok(Value::Null),
        }
    }
}

impl<T: Decode> Decode for Nullable<T> {
    fn decode(v: &Value) -> Result<Self, CodecError> {
        match v {
            Value::Null => Ok(Self(None)),
            other => T::decode(other).map(|t| Self(Some(t))),
        }
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn encode(&self) -> Result<Value, CodecError> {
        self.iter()
            .enumerate()
            .map(|(i, v)| v.encode().map_err(|e| e.within(&format!("[{i}]"))))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array)
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode(v: &Value) -> Result<Self, CodecError> {
        match v {
            Value::Array(items) => items
                .iter()
                .enumerate()
                .map(|(i, x)| T::decode(x).map_err(|e| e.within(&format!("[{i}]"))))
                .collect(),
            other => Err(CodecError::new(format!(
                "expected an array, got {}",
                describe(other)
            ))),
        }
    }
}

/// An object with any keys (`codec.record`).
impl<T: Encode> Encode for BTreeMap<String, T> {
    fn encode(&self) -> Result<Value, CodecError> {
        self.iter()
            .map(|(k, v)| Ok((k.clone(), v.encode().map_err(|e| e.within(k))?)))
            .collect::<Result<Vec<_>, CodecError>>()
            .map(Value::Object)
    }
}

impl<T: Decode> Decode for BTreeMap<String, T> {
    fn decode(v: &Value) -> Result<Self, CodecError> {
        match v {
            Value::Object(members) => members
                .iter()
                .map(|(k, x)| Ok((k.clone(), T::decode(x).map_err(|e| e.within(k))?)))
                .collect(),
            other => Err(CodecError::new(format!(
                "expected an object, got {}",
                describe(other)
            ))),
        }
    }
}

#[doc(hidden)]
pub fn __expected_object(v: &Value) -> CodecError {
    CodecError::new(format!("expected an object, got {}", describe(v)))
}

#[doc(hidden)]
#[must_use]
pub fn __field_name(ident: &str) -> &str {
    ident.strip_prefix("r#").unwrap_or(ident)
}

/// Declares a struct that is read from and written as a JSON object
/// (`codec.object`): the declared keys, in declaration order, a missing key
/// refused unless its type is `Option`, and keys it does not declare
/// dropped, so a peer that adds a field does not break an older reader.
///
/// ```ignore
/// wasmkeep_sdk::record! {
///     #[derive(Debug, Clone, PartialEq)]
///     pub struct AddItem { pub sku: String, pub qty: i64 }
/// }
/// ```
#[macro_export]
macro_rules! record {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $( $(#[$fmeta:meta])* $fvis:vis $field:ident : $ty:ty ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis struct $name {
            $( $(#[$fmeta])* $fvis $field: $ty ),*
        }

        impl $crate::client::Encode for $name {
            fn encode(&self) -> ::core::result::Result<$crate::client::Value, $crate::client::CodecError> {
                #[allow(unused_mut)]
                let mut members = ::std::vec::Vec::new();
                $(
                    if !$crate::client::Encode::is_absent(&self.$field) {
                        let name = $crate::client::__field_name(stringify!($field));
                        members.push((
                            name.to_owned(),
                            $crate::client::Encode::encode(&self.$field).map_err(|e| e.within(name))?,
                        ));
                    }
                )*
                Ok($crate::client::Value::Object(members))
            }
        }

        impl $crate::client::Decode for $name {
            fn decode(v: &$crate::client::Value) -> ::core::result::Result<Self, $crate::client::CodecError> {
                if !matches!(v, $crate::client::Value::Object(_)) {
                    return Err($crate::client::__expected_object(v));
                }
                Ok(Self {
                    $(
                        $field: {
                            let name = $crate::client::__field_name(stringify!($field));
                            $crate::client::Decode::decode_field(v.get(name)).map_err(|e| e.within(name))?
                        },
                    )*
                })
            }
        }
    };
}

// ---------------------------------------------------------------------
// Paths

fn is_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~')
}

fn scalar(text: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(text.len());
    for &b in text.as_bytes() {
        if is_unreserved(b) {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(char::from(HEX[usize::from(b >> 4)]));
            out.push(char::from(HEX[usize::from(b & 0x0f)]));
        }
    }
    out
}

/// One value as one path segment, percent-encoded (`/` becomes `%2F`). A
/// value of `.` or `..` is refused.
///
/// # Errors
/// A [`CodecError`] for `.` or `..`.
pub fn segment(value: &dyn fmt::Display) -> Result<String, CodecError> {
    let text = value.to_string();
    if text == "." || text == ".." {
        return Err(CodecError::new("a path value cannot be . or .."));
    }
    Ok(scalar(&text))
}

/// Builds a path from a template whose `{}` are filled with `values`, each
/// percent-encoded as one segment. See [`url_path!`](crate::url_path).
///
/// # Errors
/// A [`CodecError`] when a value is `.` or `..`, or the number of `{}` is
/// not the number of values.
pub fn url_path(template: &str, values: &[&dyn fmt::Display]) -> Result<String, CodecError> {
    let pieces: Vec<&str> = template.split("{}").collect();
    if pieces.len() != values.len() + 1 {
        return Err(CodecError::new(
            "the path template has a different number of {} than values",
        ));
    }
    let mut out = String::from(pieces[0]);
    for (value, piece) in values.iter().zip(&pieces[1..]) {
        out.push_str(&segment(*value)?);
        out.push_str(piece);
    }
    Ok(out)
}

/// Appends a query string to `path`: one `key=value` per pair, in order,
/// percent-encoded. A key given twice is repeated (an array in
/// TypeScript); leave a pair out to leave its key out.
///
/// # Errors
/// Never fails today; the `Result` keeps the signature of [`url_path`].
pub fn with_query(path: &str, query: &[(&str, &dyn fmt::Display)]) -> Result<String, CodecError> {
    if query.is_empty() {
        return Ok(path.to_owned());
    }
    let parts: Vec<String> = query
        .iter()
        .map(|(k, v)| format!("{}={}", scalar(k), scalar(&v.to_string())))
        .collect();
    let joiner = if path.contains('?') { '&' } else { '?' };
    Ok(format!("{path}{joiner}{}", parts.join("&")))
}

/// [`url_path`] with the values written after the template:
/// `url_path!("/carts/{}/items/{}", cart, sku)`.
#[macro_export]
macro_rules! url_path {
    ($template:expr $(, $value:expr)* $(,)?) => {
        $crate::client::url_path($template, &[$(&$value as &dyn ::core::fmt::Display),*])
    };
}

fn check_path(path: &str) -> Result<(), CodecError> {
    if !path.starts_with('/') {
        return Err(CodecError::new("path must start with /"));
    }
    if path
        .chars()
        .any(|c| c <= ' ' || c == '\u{7f}' || c == '#' || c == '\\')
    {
        return Err(CodecError::new(
            "path has a space, control character, # or backslash",
        ));
    }
    // A dot segment, written or percent-encoded, is resolved away by URL
    // normalization: it would name another path than the one built.
    let before_query = path.split('?').next().unwrap_or(path);
    for seg in before_query.split('/') {
        let seg = seg.to_ascii_lowercase();
        if matches!(
            seg.as_str(),
            "." | ".." | "%2e" | "%2e." | ".%2e" | "%2e%2e"
        ) {
            return Err(CodecError::new("path has a . or .. segment"));
        }
    }
    Ok(())
}

/// How an operation's path is built from its input: a fixed `&str`, or a
/// closure taking `&Input` (annotate its parameter) and returning
/// `Result<String, CodecError>` (use [`url_path!`](crate::url_path)).
pub trait PathSpec<I> {
    /// The path for `input`.
    ///
    /// # Errors
    /// A [`CodecError`] when the path cannot be built safely.
    fn path(&self, input: &I) -> Result<String, CodecError>;
}

impl<I> PathSpec<I> for &'static str {
    fn path(&self, _: &I) -> Result<String, CodecError> {
        Ok((*self).to_owned())
    }
}

impl<I, F> PathSpec<I> for F
where
    F: Fn(&I) -> Result<String, CodecError>,
{
    fn path(&self, input: &I) -> Result<String, CodecError> {
        self(input)
    }
}

// ---------------------------------------------------------------------
// The transport

/// A call that did not get an answer: a transport failure, never the
/// callee's own HTTP answer. `kind` is text so that a kind from a newer
/// host passes through; [`known`](Self::known) narrows it. A timeout or
/// `failed` does not say the callee did nothing: a write may have been
/// committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallError {
    /// `invalid-address`, `reentrant`, `too-large`, `timeout`, `failed`, or
    /// a kind a newer host reports.
    pub kind: String,
    /// What the host said.
    pub message: String,
}

/// The kinds of [`CallError`] this SDK knows (the WIT `call-error` cases).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallErrorKind {
    /// The address is not `class/name`, or names no class.
    InvalidAddress,
    /// The call would re-enter an actor already in this call chain.
    Reentrant,
    /// A body over the host's limit.
    TooLarge,
    /// The call ran out of time.
    Timeout,
    /// The callee or its node failed or could not be reached.
    Failed,
}

/// The kinds the host reports for a call that got no answer.
pub const CALL_ERROR_KINDS: [&str; 5] = [
    "invalid-address",
    "reentrant",
    "too-large",
    "timeout",
    "failed",
];

impl CallError {
    /// An error of `kind`.
    pub fn new(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
        }
    }

    /// `kind` when it is one this SDK knows, else `None`.
    #[must_use]
    pub fn known(&self) -> Option<CallErrorKind> {
        Some(match self.kind.as_str() {
            "invalid-address" => CallErrorKind::InvalidAddress,
            "reentrant" => CallErrorKind::Reentrant,
            "too-large" => CallErrorKind::TooLarge,
            "timeout" => CallErrorKind::Timeout,
            "failed" => CallErrorKind::Failed,
            _ => return None,
        })
    }
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl std::error::Error for CallError {}

impl From<actors::CallError> for CallError {
    fn from(e: actors::CallError) -> Self {
        match e {
            actors::CallError::InvalidAddress(m) => Self::new("invalid-address", m),
            actors::CallError::Reentrant => Self::new(
                "reentrant",
                "the call would re-enter an actor already in this call chain",
            ),
            actors::CallError::TooLarge => Self::new("too-large", "body too large"),
            actors::CallError::Timeout => Self::new("timeout", "the call timed out"),
            actors::CallError::Failed(m) => Self::new("failed", m),
        }
    }
}

/// A request to send, as the transport takes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireRequest {
    /// The HTTP method, upper case.
    pub method: String,
    /// The path, with its query.
    pub path: String,
    /// The headers.
    pub headers: Vec<(String, String)>,
    /// The body; `None` sends none.
    pub body: Option<Vec<u8>>,
}

/// An answer, as the transport returns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireAnswer {
    /// The HTTP status.
    pub status: u16,
    /// The headers.
    pub headers: Vec<(String, String)>,
    /// The body.
    pub body: Vec<u8>,
}

/// How requests reach actors. [`ActorTransport`] is `actors::call` and
/// `calls::call_many`; a test passes its own.
pub trait Transport {
    /// One call.
    ///
    /// # Errors
    /// A [`CallError`] when no answer came.
    fn call(&self, address: &str, request: &WireRequest) -> Result<WireAnswer, CallError>;

    /// Several calls at once: one entry per call, in input order.
    fn call_many(&self, calls: &[(String, WireRequest)]) -> Vec<Result<WireAnswer, CallError>>;
}

/// `actors::call` and `calls::call_many` as a [`Transport`]: address
/// handling, the call chain and the body limits are the host's.
#[derive(Debug, Clone, Copy, Default)]
pub struct ActorTransport;

fn to_actor_request(r: &WireRequest) -> actors::Request {
    actors::Request {
        method: r.method.clone(),
        path: r.path.clone(),
        headers: r.headers.clone(),
        body: r.body.clone().unwrap_or_default(),
    }
}

fn from_actor_response(r: actors::Response) -> WireAnswer {
    WireAnswer {
        status: r.status,
        headers: r.headers,
        body: r.body,
    }
}

impl Transport for ActorTransport {
    fn call(&self, address: &str, request: &WireRequest) -> Result<WireAnswer, CallError> {
        actors::call(address, &to_actor_request(request))
            .map(from_actor_response)
            .map_err(CallError::from)
    }

    fn call_many(&self, many: &[(String, WireRequest)]) -> Vec<Result<WireAnswer, CallError>> {
        let sent: Vec<(String, actors::Request)> = many
            .iter()
            .map(|(a, r)| (a.clone(), to_actor_request(r)))
            .collect();
        calls::call_many(&sent)
            .into_iter()
            .map(|r| r.map(from_actor_response).map_err(CallError::from))
            .collect()
    }
}

// ---------------------------------------------------------------------
// Operations

/// An HTTP method, for [`contract!`](crate::contract).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Method(&'static str);

impl Method {
    /// The method's name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }

    /// Whether the input is sent as a JSON body: `POST`, `PUT` and `PATCH`
    /// do; `GET`, `HEAD` and `DELETE` send none (the input goes in the path).
    #[must_use]
    pub fn has_body(self) -> bool {
        !matches!(self.0, "GET" | "HEAD" | "DELETE")
    }
}

/// The methods a contract can name.
#[allow(non_upper_case_globals, reason = "spelled as the HTTP methods")]
pub mod method {
    use super::Method;
    /// `GET`.
    pub const GET: Method = Method("GET");
    /// `HEAD`.
    pub const HEAD: Method = Method("HEAD");
    /// `POST`.
    pub const POST: Method = Method("POST");
    /// `PUT`.
    pub const PUT: Method = Method("PUT");
    /// `PATCH`.
    pub const PATCH: Method = Method("PATCH");
    /// `DELETE`.
    pub const DELETE: Method = Method("DELETE");
}

/// The error codes an operation declares, with their details decoded.
/// [`contract!`](crate::contract) writes one enum per operation.
pub trait Declared: Sized {
    /// `None` when `code` is not declared; otherwise the details decoded
    /// (`details` is `None` when the envelope carries none).
    fn decode(code: &str, details: Option<&Value>) -> Option<Result<Self, CodecError>>;

    /// The code this error is declared under.
    fn code(&self) -> &'static str;
}

/// The errors of an operation that declares none: every error envelope is
/// then a [`Failure::Problem`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoErrors {}

impl Declared for NoErrors {
    fn decode(_: &str, _: Option<&Value>) -> Option<Result<Self, CodecError>> {
        None
    }
    fn code(&self) -> &'static str {
        match *self {}
    }
}

/// Whether `code` is a valid error code (the envelope's rule).
#[must_use]
pub const fn valid_code(code: &str) -> bool {
    let b = code.as_bytes();
    if b.is_empty() || b.len() > problem::MAX_CODE_LENGTH || !b[0].is_ascii_alphabetic() {
        return false;
    }
    let mut i = 1;
    while i < b.len() {
        let c = b[i];
        if !(c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b':' | b'-')) {
            return false;
        }
        i += 1;
    }
    true
}

/// One call to an actor: its name, method, path, input, answer and declared
/// errors, as a value both sides can build from the same declaration
/// ([`contract!`](crate::contract)).
pub struct Operation<I, O, E = NoErrors> {
    name: &'static str,
    method: Method,
    path: fn(&I) -> Result<String, CodecError>,
    max_response_bytes: usize,
    types: PhantomData<fn() -> (O, E)>,
}

impl<I, O, E> Clone for Operation<I, O, E> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<I, O, E> Copy for Operation<I, O, E> {}

impl<I, O, E> fmt::Debug for Operation<I, O, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Operation")
            .field("name", &self.name)
            .field("method", &self.method.0)
            .finish_non_exhaustive()
    }
}

impl<I, O, E> Operation<I, O, E> {
    /// An operation. `name` is for logs and errors; it is not sent.
    #[must_use]
    pub fn new(
        name: &'static str,
        method: Method,
        path: fn(&I) -> Result<String, CodecError>,
    ) -> Self {
        Self {
            name,
            method,
            path,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            types: PhantomData,
        }
    }

    /// The longest success body decoded, in bytes (default 1 MiB; the
    /// host's own limits on bodies still apply).
    #[must_use]
    pub fn max_response_bytes(mut self, bytes: usize) -> Self {
        self.max_response_bytes = bytes;
        self
    }

    /// The name given to [`new`](Self::new).
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The HTTP method.
    #[must_use]
    pub fn method(&self) -> Method {
        self.method
    }
}

impl<I: Encode, O, E> Operation<I, O, E> {
    /// The request for `input`: the path built, the input checked, a JSON
    /// body when the method takes one.
    ///
    /// # Errors
    /// A [`CodecError`] when the input is not valid or the path cannot be
    /// built safely; nothing is sent.
    pub fn request(&self, input: &I) -> Result<WireRequest, CodecError> {
        let encoded = input.encode_body()?;
        let path = (self.path)(input)?;
        check_path(&path)?;
        let mut headers = vec![("accept".to_owned(), "application/json".to_owned())];
        let body = match encoded {
            Some(value) if self.method.has_body() => {
                let mut text = String::new();
                value.write(&mut text);
                headers.push(("content-type".to_owned(), "application/json".to_owned()));
                Some(text.into_bytes())
            }
            _ => None,
        };
        Ok(WireRequest {
            method: self.method.0.to_owned(),
            path,
            headers,
            body,
        })
    }
}

// ---------------------------------------------------------------------
// Results

/// A call that succeeded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Success<O> {
    /// The 2xx status.
    pub status: u16,
    /// The answer, decoded.
    pub value: O,
}

/// What went wrong, one of nine kinds ([`kind`](Failure::kind) names them as
/// TypeScript does). Nothing the callee or the network does is a panic.
#[derive(Debug, Clone, PartialEq)]
pub enum Failure<E = NoErrors> {
    /// No answer came: the callee may or may not have run.
    Transport(CallError),
    /// An error the operation declares, its details decoded.
    Declared {
        /// The HTTP status.
        status: u16,
        /// The envelope's message (public text).
        message: String,
        /// The declared error.
        error: E,
    },
    /// A valid envelope whose code the operation does not declare.
    Problem {
        /// The HTTP status.
        status: u16,
        /// The envelope.
        problem: Problem,
    },
    /// A declared code whose details do not match their type.
    InvalidDetails {
        /// The HTTP status.
        status: u16,
        /// The envelope.
        problem: Problem,
        /// What is wrong with the details.
        reason: String,
    },
    /// An envelope of a version this SDK does not read.
    UnsupportedEnvelope {
        /// The HTTP status.
        status: u16,
        /// The `v` it carries.
        version: i64,
    },
    /// Claims to be an envelope but is not valid.
    MalformedEnvelope {
        /// The HTTP status.
        status: u16,
        /// What is wrong.
        reason: String,
    },
    /// A failure without an envelope: a legacy actor, plain text, a
    /// redirect. `text` is the start of the body when it is UTF-8.
    Http {
        /// The HTTP status.
        status: u16,
        /// The start of the body.
        text: Option<String>,
    },
    /// A success status whose body is too large, not JSON, or the wrong
    /// shape.
    InvalidResponse {
        /// The HTTP status.
        status: u16,
        /// What is wrong.
        reason: String,
    },
    /// The input is not valid: nothing was sent.
    InvalidRequest {
        /// What is wrong.
        reason: String,
    },
}

impl<E> Failure<E> {
    /// The kind's name, as TypeScript's `failure.kind`.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Failure::Transport(_) => "transport",
            Failure::Declared { .. } => "declared",
            Failure::Problem { .. } => "problem",
            Failure::InvalidDetails { .. } => "invalid-details",
            Failure::UnsupportedEnvelope { .. } => "unsupported-envelope",
            Failure::MalformedEnvelope { .. } => "malformed-envelope",
            Failure::Http { .. } => "http",
            Failure::InvalidResponse { .. } => "invalid-response",
            Failure::InvalidRequest { .. } => "invalid-request",
        }
    }
}

impl<E> fmt::Display for Failure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Transport(e) => write!(f, "transport: {e}"),
            Failure::Declared {
                status, message, ..
            } => write!(f, "declared error ({status}): {message}"),
            Failure::Problem { status, problem } => {
                write!(
                    f,
                    "problem {} ({status}): {}",
                    problem.code(),
                    problem.message()
                )
            }
            Failure::InvalidDetails {
                problem, reason, ..
            } => {
                write!(f, "invalid details for {}: {reason}", problem.code())
            }
            Failure::UnsupportedEnvelope { version, .. } => {
                write!(f, "unsupported error envelope version {version}")
            }
            Failure::MalformedEnvelope { reason, .. } => {
                write!(f, "malformed error envelope: {reason}")
            }
            Failure::Http { status, text } => match text {
                Some(t) => write!(f, "http {status}: {t}"),
                None => write!(f, "http {status}"),
            },
            Failure::InvalidResponse { status, reason } => {
                write!(f, "invalid response ({status}): {reason}")
            }
            Failure::InvalidRequest { reason } => write!(f, "invalid request: {reason}"),
        }
    }
}

impl<E: fmt::Debug> std::error::Error for Failure<E> {}

/// A call's result.
pub type Outcome<O, E = NoErrors> = Result<Success<O>, Failure<E>>;

fn interpret<I, O: Decode, E: Declared>(
    operation: &Operation<I, O, E>,
    answer: &WireAnswer,
) -> Outcome<O, E> {
    let status = answer.status;
    if (200..300).contains(&status) {
        return interpret_success(operation, answer);
    }
    match problem::classify(status, &answer.headers, &answer.body) {
        problem::Failure::Plain { status, text } => Err(Failure::Http { status, text }),
        problem::Failure::Malformed { status, reason } => Err(Failure::MalformedEnvelope {
            status,
            reason: reason.to_owned(),
        }),
        problem::Failure::UnsupportedVersion { status, version } => {
            Err(Failure::UnsupportedEnvelope { status, version })
        }
        problem::Failure::Problem { status, problem } => {
            let details = problem.details().and_then(json::parse);
            match E::decode(problem.code(), details.as_ref()) {
                None => Err(Failure::Problem { status, problem }),
                Some(Ok(error)) => Err(Failure::Declared {
                    status,
                    message: problem.message().to_owned(),
                    error,
                }),
                Some(Err(e)) => Err(Failure::InvalidDetails {
                    status,
                    problem,
                    reason: e.to_string(),
                }),
            }
        }
    }
}

fn interpret_success<I, O: Decode, E>(
    operation: &Operation<I, O, E>,
    answer: &WireAnswer,
) -> Outcome<O, E> {
    let status = answer.status;
    let invalid = |reason: String| Failure::InvalidResponse { status, reason };
    if answer.body.len() > operation.max_response_bytes {
        return Err(invalid(format!(
            "the body is longer than {} bytes",
            operation.max_response_bytes
        )));
    }
    let value = if answer.body.is_empty() {
        None
    } else {
        let text = std::str::from_utf8(&answer.body)
            .map_err(|_| invalid("the body is not UTF-8".into()))?;
        Some(json::parse(text).ok_or_else(|| invalid("the body is not JSON".into()))?)
    };
    O::decode_opt(value.as_ref())
        .map(|value| Success { status, value })
        .map_err(|e| invalid(e.to_string()))
}

// ---------------------------------------------------------------------
// The client

/// A typed client over a [`Transport`].
#[derive(Debug, Clone)]
pub struct Client<T> {
    transport: T,
}

/// A typed client for the app's actors, from an actor or a front.
#[must_use]
pub fn actor_client() -> Client<ActorTransport> {
    Client::new(ActorTransport)
}

impl<T: Transport> Client<T> {
    /// A client over `transport`.
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    /// Calls `operation` on the actor at `address` (`class/name`) and waits.
    ///
    /// # Errors
    /// A [`Failure`] for anything but a success.
    pub fn call<I: Encode, O: Decode, E: Declared>(
        &self,
        address: &str,
        operation: &Operation<I, O, E>,
        input: &I,
    ) -> Outcome<O, E> {
        let request = operation.request(input).map_err(invalid_request)?;
        match self.transport.call(address, &request) {
            Ok(answer) => interpret(operation, &answer),
            Err(e) => Err(Failure::Transport(e)),
        }
    }

    /// Starts a batch: several operations sent at once over
    /// `calls::call_many`, each with its own result.
    pub fn batch(&self) -> Batch<'_, T> {
        Batch {
            transport: &self.transport,
            entries: Vec::new(),
        }
    }
}

fn invalid_request<E>(e: CodecError) -> Failure<E> {
    Failure::InvalidRequest {
        reason: e.to_string(),
    }
}

type Interpreter = Box<dyn FnOnce(Result<WireAnswer, CallError>) -> Box<dyn Any>>;

struct Entry {
    send: Option<(String, WireRequest)>,
    /// Turns what came back (or the failure to send) into the typed result.
    finish: Interpreter,
    /// The result already, for an entry that was never sent.
    done: Option<Box<dyn Any>>,
}

/// Several calls at once. [`add`](Self::add) each, then [`send`](Self::send):
///
/// ```ignore
/// let mut batch = client.batch();
/// let cart = batch.add("cart/alice", &cart::get(), &());
/// let profile = batch.add("profile/alice", &profile::get(), &());
/// let mut results = batch.send();
/// let cart = results.take(cart);
/// ```
///
/// Every entry is its own result, so one failure does not hide the others,
/// whether it is a transport error or a hostile answer. At most
/// [`MAX_BATCH`] entries: a longer batch sends nothing and every entry is a
/// `transport` failure, as the host does. An entry whose input is not valid
/// is an `invalid-request` and is left out of what is sent.
#[must_use]
pub struct Batch<'a, T> {
    transport: &'a T,
    entries: Vec<Entry>,
}

impl<T> fmt::Debug for Batch<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Batch")
            .field("entries", &self.entries.len())
            .finish_non_exhaustive()
    }
}

/// Which entry of a batch a result belongs to, typed as the operation.
#[derive(Debug)]
pub struct Handle<O, E> {
    index: usize,
    types: PhantomData<fn() -> (O, E)>,
}

impl<O, E> Clone for Handle<O, E> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<O, E> Copy for Handle<O, E> {}

impl<T: Transport> Batch<'_, T> {
    /// Adds a call and returns the handle of its result.
    pub fn add<I, O, E>(
        &mut self,
        address: &str,
        operation: &Operation<I, O, E>,
        input: &I,
    ) -> Handle<O, E>
    where
        I: Encode + 'static,
        O: Decode + 'static,
        E: Declared + 'static,
    {
        let operation = *operation;
        let handle = Handle {
            index: self.entries.len(),
            types: PhantomData,
        };
        let finish: Interpreter = Box::new(move |got| {
            let outcome: Outcome<O, E> = match got {
                Ok(answer) => interpret(&operation, &answer),
                Err(e) => Err(Failure::Transport(e)),
            };
            Box::new(outcome)
        });
        match operation.request(input) {
            Ok(request) => self.entries.push(Entry {
                send: Some((address.to_owned(), request)),
                finish,
                done: None,
            }),
            Err(e) => {
                let outcome: Outcome<O, E> = Err(invalid_request(e));
                self.entries.push(Entry {
                    send: None,
                    finish,
                    done: Some(Box::new(outcome)),
                });
            }
        }
        handle
    }

    /// Sends every valid entry at once and waits once.
    pub fn send(self) -> BatchResults {
        let Batch { transport, entries } = self;
        let too_many = entries.len() > MAX_BATCH;
        let mut sent: Vec<(String, WireRequest)> = Vec::new();
        let mut at: Vec<usize> = Vec::new();
        let mut finishers: Vec<Option<Interpreter>> = Vec::new();
        let mut results: Vec<Option<Box<dyn Any>>> = Vec::new();
        for (i, entry) in entries.into_iter().enumerate() {
            let Entry { send, finish, done } = entry;
            match (too_many, send, done) {
                (false, Some(call), _) => {
                    sent.push(call);
                    at.push(i);
                    results.push(None);
                    finishers.push(Some(finish));
                }
                (false, None, done) => {
                    results.push(done);
                    finishers.push(None);
                }
                (true, _, _) => {
                    let error =
                        CallError::new("failed", format!("too many calls: at most {MAX_BATCH}"));
                    results.push(Some(finish(Err(error))));
                    finishers.push(None);
                }
            }
        }
        if !sent.is_empty() {
            let mut answers = transport.call_many(&sent).into_iter();
            for i in at {
                let got = answers.next().unwrap_or_else(|| {
                    Err(CallError::new(
                        "failed",
                        "call_many returned fewer results than calls",
                    ))
                });
                if let Some(finish) = finishers[i].take() {
                    results[i] = Some(finish(got));
                }
            }
        }
        BatchResults { results }
    }
}

/// The results of a sent [`Batch`], one per entry.
#[derive(Debug)]
pub struct BatchResults {
    results: Vec<Option<Box<dyn Any>>>,
}

impl BatchResults {
    /// The result of the entry `handle` names. Each can be taken once.
    pub fn take<O: 'static, E: 'static>(&mut self, handle: Handle<O, E>) -> Outcome<O, E> {
        let taken = self
            .results
            .get_mut(handle.index)
            .and_then(Option::take)
            .and_then(|b| b.downcast::<Outcome<O, E>>().ok());
        match taken {
            Some(outcome) => *outcome,
            None => Err(Failure::InvalidRequest {
                reason: "this handle's result was already taken, or is from another batch".into(),
            }),
        }
    }

    /// How many entries there were.
    #[must_use]
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Whether the batch was empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }
}

// ---------------------------------------------------------------------
// The macro

#[doc(hidden)]
#[macro_export]
macro_rules! __or_ty {
    ($t:ty ; $default:ty) => {
        $t
    };
    (; $default:ty) => {
        $default
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __arg {
    ($arg:ident ; $t:ty) => {
        $arg
    };
    ($arg:ident ;) => {
        &()
    };
}

/// Declares an actor class's operations with typed inputs, answers and
/// errors, and writes a client for them. The macro is plain `macro_rules!`.
///
/// ```ignore
/// wasmkeep_sdk::contract! {
///     /// What the cart class answers.
///     pub mod cart {
///         /// Adds an item.
///         fn add_item(AddItem) -> Count
///             = POST |i: &AddItem| url_path!("/items/{}", i.sku),
///             errors AddItemError {
///                 OutOfStock("cart.out_of_stock"): OutOfStock,
///                 Closed("cart.closed"),
///             };
///         fn get() -> Count = GET "/";
///     }
/// }
/// ```
///
/// Each operation is `fn name(Input) -> Output = METHOD path`. Leave the
/// input out for none (`()`); the output may be `()` too. `METHOD` is one of
/// `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`; a `POST`, `PUT` or
/// `PATCH` sends the input as a JSON body, the others send none and put the
/// input in the path. The path is a string, or a closure over `&Input`
/// (annotate its parameter) returning `Result<String, CodecError>`; build it
/// with [`url_path!`](crate::url_path). `errors Name { Variant("code"):
/// Details, .. }` declares the application errors the operation may return
/// (leave `: Details` out for an error without details) as the enum `Name`;
/// any other code is a [`Failure::Problem`].
///
/// Types are anything that is [`Encode`] and [`Decode`]: the primitives,
/// `Vec`, `Option`, [`Nullable`], and structs declared with
/// [`record!`](crate::record).
///
/// The macro writes a module holding, for each operation, a function
/// returning its [`Operation`], and a `Client` with one method per
/// operation:
///
/// ```ignore
/// let alice = cart::Client::new("cart/alice");
/// let reply = alice.add_item(&AddItem { sku: "A1".into(), qty: 2 })?;
/// let again = actor_client().call("cart/bob", &cart::get(), &());
/// ```
#[macro_export]
macro_rules! contract {
    (
        $(#[$cmeta:meta])*
        $vis:vis mod $module:ident {
            $(
                $(#[$ometa:meta])*
                fn $op:ident ( $($input:ty)? ) -> $output:ty
                    = $method:ident $path:expr
                    $(, errors $errs:ident {
                        $( $variant:ident ( $code:literal ) $(: $details:ty)? ),+ $(,)?
                    } )?
                ;
            )*
        }
    ) => {
        $(#[$cmeta])*
        #[allow(missing_docs, unreachable_pub, dead_code)]
        $vis mod $module {
            #[allow(unused_imports)]
            use super::*;

            $(
                $(#[$ometa])*
                pub fn $op() -> $crate::client::Operation<
                    $crate::__or_ty!($($input)? ; ()),
                    $output,
                    $crate::__or_ty!($($errs)? ; $crate::client::NoErrors),
                > {
                    $crate::client::Operation::new(
                        concat!(stringify!($module), ".", stringify!($op)),
                        $crate::client::method::$method,
                        |input: &$crate::__or_ty!($($input)? ; ())| {
                            $crate::client::PathSpec::path(&($path), input)
                        },
                    )
                }

                $(
                    #[derive(Debug, Clone, PartialEq)]
                    pub enum $errs {
                        $( $variant($crate::__or_ty!($($details)? ; ())) ),+
                    }

                    const _: () = {
                        $( assert!($crate::client::valid_code($code), "invalid error code"); )+
                    };

                    impl $crate::client::Declared for $errs {
                        fn decode(
                            code: &str,
                            details: ::core::option::Option<&$crate::client::Value>,
                        ) -> ::core::option::Option<
                            ::core::result::Result<Self, $crate::client::CodecError>,
                        > {
                            $(
                                if code == $code {
                                    return ::core::option::Option::Some(
                                        <$crate::__or_ty!($($details)? ; ()) as $crate::client::Decode>::decode_opt(details)
                                            .map($errs::$variant),
                                    );
                                }
                            )+
                            ::core::option::Option::None
                        }

                        fn code(&self) -> &'static str {
                            match self {
                                $( Self::$variant(_) => $code ),+
                            }
                        }
                    }
                )?
            )*

            /// A client bound to one actor.
            #[derive(Debug, Clone)]
            pub struct Client<T: $crate::client::Transport = $crate::client::ActorTransport> {
                inner: $crate::client::Client<T>,
                address: ::std::string::String,
            }

            impl Client<$crate::client::ActorTransport> {
                /// A client for the actor at `address` (`class/name`), over
                /// `actors::call`.
                pub fn new(address: impl ::core::convert::Into<::std::string::String>) -> Self {
                    Self::with_transport($crate::client::ActorTransport, address)
                }
            }

            impl<T: $crate::client::Transport> Client<T> {
                /// A client over another transport (a test's).
                pub fn with_transport(
                    transport: T,
                    address: impl ::core::convert::Into<::std::string::String>,
                ) -> Self {
                    Self {
                        inner: $crate::client::Client::new(transport),
                        address: address.into(),
                    }
                }

                /// The actor this client calls.
                pub fn address(&self) -> &str {
                    &self.address
                }

                /// The untyped client, for batches.
                pub fn client(&self) -> &$crate::client::Client<T> {
                    &self.inner
                }

                $(
                    $(#[$ometa])*
                    pub fn $op(&self $(, input: &$input)?) -> $crate::client::Outcome<
                        $output,
                        $crate::__or_ty!($($errs)? ; $crate::client::NoErrors),
                    > {
                        self.inner.call(&self.address, &$op(), $crate::__arg!(input ; $($input)?))
                    }
                )*
            }
        }
    };
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    //! Ported case by case from sdks/typescript/test/typed-client.test.ts,
    //! and the shared problem vectors (sdks/typescript/test/fixtures/
    //! problem-vectors.json) driven through the client's failure mapping.
    use super::*;
    use std::cell::{Cell, RefCell};

    crate::record! {
        #[derive(Debug, Clone, PartialEq)]
        struct AddItem { sku: String, qty: i64 }
    }
    crate::record! {
        #[derive(Debug, Clone, PartialEq)]
        struct Count { count: i64, note: Option<Nullable<String>> }
    }
    crate::record! {
        #[derive(Debug, Clone, PartialEq)]
        struct OutOfStock { left: i64 }
    }

    crate::contract! {
        mod cart {
            fn add_item(AddItem) -> Count
                = POST |i: &AddItem| crate::url_path!("/items/{}", i.sku),
                errors AddItemError {
                    OutOfStock("cart.out_of_stock"): OutOfStock,
                    Closed("cart.closed"),
                };
            fn ping() -> () = GET "/ping";
            fn small() -> Value = GET "/";
        }
    }

    type Handler = Box<dyn Fn(&str, &WireRequest) -> Result<WireAnswer, CallError>>;

    struct Fake {
        handle: Handler,
        calls: RefCell<Vec<(String, WireRequest)>>,
        many: Cell<usize>,
    }

    impl Transport for &Fake {
        fn call(&self, address: &str, request: &WireRequest) -> Result<WireAnswer, CallError> {
            self.calls
                .borrow_mut()
                .push((address.to_owned(), request.clone()));
            (self.handle)(address, request)
        }
        fn call_many(&self, calls: &[(String, WireRequest)]) -> Vec<Result<WireAnswer, CallError>> {
            self.many.set(self.many.get() + 1);
            calls.iter().map(|(a, r)| (self.handle)(a, r)).collect()
        }
    }

    fn fake(f: impl Fn(&str, &WireRequest) -> Result<WireAnswer, CallError> + 'static) -> Fake {
        Fake {
            handle: Box::new(f),
            calls: RefCell::new(Vec::new()),
            many: Cell::new(0),
        }
    }

    fn answer(status: u16, ct: &str, body: &str) -> Result<WireAnswer, CallError> {
        Ok(WireAnswer {
            status,
            headers: vec![("content-type".into(), ct.into())],
            body: body.as_bytes().to_vec(),
        })
    }
    fn json_answer(status: u16, body: &str) -> Result<WireAnswer, CallError> {
        answer(status, "application/json", body)
    }
    fn raw(status: u16, body: &[u8]) -> Result<WireAnswer, CallError> {
        Ok(WireAnswer {
            status,
            headers: vec![],
            body: body.to_vec(),
        })
    }
    fn problem_answer(
        status: u16,
        code: &str,
        details: Option<&str>,
    ) -> Result<WireAnswer, CallError> {
        let mut p = Problem::new(code, "m").unwrap();
        if let Some(d) = details {
            p = p.with_details(d).unwrap();
        }
        let r = problem::response(status, &p).unwrap();
        Ok(WireAnswer {
            status: r.status,
            headers: r.headers,
            body: r.body,
        })
    }
    fn item(sku: &str, qty: i64) -> AddItem {
        AddItem {
            sku: sku.into(),
            qty,
        }
    }

    #[test]
    fn a_success_is_decoded_and_the_request_carries_the_encoded_path_and_body() {
        let t = fake(|_, _| json_answer(200, r#"{"count":3,"extra":1}"#));
        let c = cart::Client::with_transport(&t, "cart/alice");
        let r = c.add_item(&item("a/b", 2)).unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(
            r.value,
            Count {
                count: 3,
                note: None
            }
        );
        let calls = t.calls.borrow();
        assert_eq!(calls[0].0, "cart/alice");
        assert_eq!(calls[0].1.method, "POST");
        assert_eq!(calls[0].1.path, "/items/a%2Fb");
        assert_eq!(
            calls[0].1.body.as_deref(),
            Some(&br#"{"sku":"a/b","qty":2}"#[..])
        );
        assert!(
            calls[0]
                .1
                .headers
                .contains(&("content-type".into(), "application/json".into()))
        );
        assert!(
            calls[0]
                .1
                .headers
                .contains(&("accept".into(), "application/json".into()))
        );
    }

    #[test]
    fn http_200_with_the_wrong_shape_is_invalid_response_not_a_value() {
        for body in [
            r#"{"count":"3"}"#,
            r#"{"count":1.5}"#,
            "[]",
            "null",
            r#"{"note":null}"#,
            "not json",
            "",
        ] {
            let t = fake(move |_, _| json_answer(200, body));
            let r = cart::Client::with_transport(&t, "a/b").add_item(&item("s", 1));
            assert!(
                matches!(r, Err(Failure::InvalidResponse { .. })),
                "{body}: {r:?}"
            );
        }
        let t = fake(|_, _| raw(200, &[0xff]));
        let r = cart::Client::with_transport(&t, "a/b").add_item(&item("s", 1));
        assert!(matches!(r, Err(Failure::InvalidResponse { .. })));
        // null in an optional-and-nullable key is a value; null in a plain key is not.
        let t = fake(|_, _| json_answer(200, r#"{"count":1,"note":null}"#));
        let r = cart::Client::with_transport(&t, "a/b")
            .add_item(&item("s", 1))
            .unwrap();
        assert_eq!(r.value.note, Some(Nullable(None)));
    }

    #[test]
    fn an_empty_success_is_no_value_and_an_oversized_one_is_refused() {
        let t = fake(|_, _| raw(204, b""));
        let r = cart::Client::with_transport(&t, "a/b").ping().unwrap();
        assert_eq!((r.status, r.value), (204, ()));
        let t = fake(|_, _| json_answer(200, r#""123456789""#));
        let c = Client::new(&t);
        let op = cart::small().max_response_bytes(8);
        assert!(matches!(
            c.call("a/b", &op, &()),
            Err(Failure::InvalidResponse { .. })
        ));
        assert!(c.call("a/b", &cart::small(), &()).is_ok());
    }

    #[test]
    fn input_that_is_not_valid_sends_nothing() {
        let t = fake(|_, _| json_answer(200, "{}"));
        // 2^53 is not a safe integer.
        let r = cart::Client::with_transport(&t, "a/b").add_item(&item("s", 1 << 53));
        assert!(matches!(r, Err(Failure::InvalidRequest { .. })));
        // A path value of `..` is refused.
        let r = cart::Client::with_transport(&t, "a/b").add_item(&item("..", 1));
        assert!(matches!(r, Err(Failure::InvalidRequest { .. })));
        assert!(t.calls.borrow().is_empty());
    }

    #[test]
    fn declared_errors_come_back_typed_and_undeclared_and_odd_ones_stay_explicit() {
        let run = |a: Result<WireAnswer, CallError>| {
            let t = fake(move |_, _| a.clone());
            cart::Client::with_transport(&t, "a/b").add_item(&item("s", 1))
        };
        match run(problem_answer(
            409,
            "cart.out_of_stock",
            Some(r#"{"left":2}"#),
        )) {
            Err(Failure::Declared {
                status: 409, error, ..
            }) => {
                assert_eq!(error.code(), "cart.out_of_stock");
                assert_eq!(
                    error,
                    cart::AddItemError::OutOfStock(OutOfStock { left: 2 })
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            run(problem_answer(409, "cart.closed", None)),
            Err(Failure::Declared {
                error: cart::AddItemError::Closed(()),
                ..
            })
        ));
        assert!(matches!(
            run(problem_answer(409, "cart.new_thing", Some(r#"{"a":1}"#))),
            Err(Failure::Problem { .. })
        ));
        assert!(matches!(
            run(problem_answer(
                409,
                "cart.out_of_stock",
                Some(r#"{"left":"many"}"#)
            )),
            Err(Failure::InvalidDetails { .. })
        ));
        assert!(matches!(
            run(problem_answer(
                409,
                "cart.closed",
                Some(r#"{"surprise":1}"#)
            )),
            Err(Failure::InvalidDetails { .. })
        ));
        let ct = problem::CONTENT_TYPE;
        assert!(matches!(
            run(answer(400, ct, r#"{"v":7,"code":"x","message":"m"}"#)),
            Err(Failure::UnsupportedEnvelope { version: 7, .. })
        ));
        assert!(matches!(
            run(answer(400, ct, "garbage")),
            Err(Failure::MalformedEnvelope { .. })
        ));
        // A prototype-named code is just an undeclared code.
        assert!(matches!(
            run(problem_answer(400, "constructor", None)),
            Err(Failure::Problem { .. })
        ));
    }

    #[test]
    fn legacy_answers_plain_text_and_bare_json_failures_are_http_failures() {
        let t = fake(|_, _| answer(500, "text/plain", "internal error\n"));
        match cart::Client::with_transport(&t, "a/b").ping() {
            Err(Failure::Http { status: 500, text }) => {
                assert_eq!(text.as_deref(), Some("internal error\n"))
            }
            other => panic!("{other:?}"),
        }
        let t = fake(|_, _| json_answer(404, r#"{"error":"nope"}"#));
        assert!(matches!(
            cart::Client::with_transport(&t, "a/b").ping(),
            Err(Failure::Http { status: 404, .. })
        ));
        let t = fake(|_, _| raw(302, b""));
        assert!(matches!(
            cart::Client::with_transport(&t, "a/b").ping(),
            Err(Failure::Http { .. })
        ));
    }

    #[test]
    fn transport_failures_are_their_own_kind_whatever_the_status_would_suggest() {
        for kind in [
            "timeout",
            "failed",
            "reentrant",
            "too-large",
            "invalid-address",
            "future-kind",
        ] {
            let t = fake(move |_, _| Err(CallError::new(kind, kind)));
            match cart::Client::with_transport(&t, "a/b").ping() {
                Err(Failure::Transport(e)) => {
                    assert_eq!(e.kind, kind);
                    assert_eq!(e.known().is_some(), kind != "future-kind");
                }
                other => panic!("{other:?}"),
            }
        }
        // A 503 answer is the callee's answer, not a transport failure.
        let t = fake(|_, _| raw(503, b"busy"));
        assert!(matches!(
            cart::Client::with_transport(&t, "a/b").ping(),
            Err(Failure::Http { status: 503, .. })
        ));
    }

    #[test]
    fn the_bound_client_calls_its_actor() {
        let t = fake(|_, _| json_answer(200, r#"{"count":1}"#));
        let c = cart::Client::with_transport(&t, "cart/alice");
        assert!(c.add_item(&item("s", 1)).is_ok());
        assert_eq!(t.calls.borrow()[0].0, "cart/alice");
        assert_eq!(c.address(), "cart/alice");
    }

    #[test]
    fn batch_keeps_input_order_and_each_entry_fails_or_succeeds_on_its_own() {
        let seen = std::rc::Rc::new(RefCell::new(Vec::<String>::new()));
        let s2 = seen.clone();
        let t = fake(move |address, r| {
            s2.borrow_mut().push(format!("{address}{}", r.path));
            match address {
                "gone/x" => Err(CallError::new("invalid-address", "no such class")),
                "late/x" => Err(CallError::new("timeout", "timeout")),
                "full/x" => problem_answer(409, "cart.out_of_stock", Some(r#"{"left":0}"#)),
                _ => json_answer(200, &format!(r#"{{"count":{}}}"#, s2.borrow().len())),
            }
        });
        let c = Client::new(&t);
        let mut b = c.batch();
        let h0 = b.add("a/1", &cart::add_item(), &item("s", 1));
        let h1 = b.add("gone/x", &cart::ping(), &());
        let h2 = b.add("full/x", &cart::add_item(), &item("s", 1));
        let h3 = b.add("a/2", &cart::add_item(), &item("bad", 1 << 60));
        let h4 = b.add("late/x", &cart::ping(), &());
        let h5 = b.add("a/3", &cart::add_item(), &item("s", 1));
        let mut r = b.send();
        assert_eq!(r.len(), 6);
        assert!(r.take(h0).is_ok());
        assert!(matches!(r.take(h1), Err(Failure::Transport(e)) if e.kind == "invalid-address"));
        assert!(matches!(r.take(h2), Err(Failure::Declared { .. })));
        assert!(matches!(r.take(h3), Err(Failure::InvalidRequest { .. })));
        assert!(matches!(r.take(h4), Err(Failure::Transport(e)) if e.kind == "timeout"));
        assert!(r.take(h5).is_ok());
        // Taken once only.
        assert!(matches!(r.take(h5), Err(Failure::InvalidRequest { .. })));
        // The invalid entry was never sent.
        assert_eq!(
            *seen.borrow(),
            [
                "a/1/items/s",
                "gone/x/ping",
                "full/x/items/s",
                "late/x/ping",
                "a/3/items/s"
            ]
        );
    }

    #[test]
    fn batch_the_limit_is_the_hosts_and_an_empty_batch_calls_nothing() {
        let t = fake(|_, _| json_answer(200, "null"));
        let c = Client::new(&t);
        assert!(c.batch().send().is_empty());
        assert_eq!(t.many.get(), 0);
        let mut b = c.batch();
        let hs: Vec<_> = (0..MAX_BATCH)
            .map(|_| b.add("a/b", &cart::small(), &()))
            .collect();
        let mut r = b.send();
        assert_eq!(t.many.get(), 1);
        assert!(hs.into_iter().all(|h| r.take(h).is_ok()));
        let mut b = c.batch();
        let hs: Vec<_> = (0..=MAX_BATCH)
            .map(|_| b.add("a/b", &cart::small(), &()))
            .collect();
        let mut r = b.send();
        assert_eq!(r.len(), MAX_BATCH + 1);
        assert!(
            hs.into_iter()
                .all(|h| matches!(r.take(h), Err(Failure::Transport(e)) if e.kind == "failed"))
        );
        assert_eq!(t.many.get(), 1);
        assert_eq!(MAX_BATCH, 16);
    }

    #[test]
    fn a_hostile_envelope_in_one_batch_entry_does_not_cost_the_others_their_results() {
        let hostile = format!(
            r#"{{"v":1,"code":"x","message":"m","extra":{}0{}}}"#,
            "[".repeat(8000),
            "]".repeat(8000)
        );
        let t = fake(move |address, _| {
            if address == "evil/x" {
                answer(400, problem::CONTENT_TYPE, &hostile)
            } else {
                json_answer(200, r#"{"count":1}"#)
            }
        });
        let c = Client::new(&t);
        let mut b = c.batch();
        let a = b.add("a/1", &cart::add_item(), &item("s", 1));
        let e = b.add("evil/x", &cart::add_item(), &item("s", 1));
        let z = b.add("a/2", &cart::add_item(), &item("s", 1));
        let mut r = b.send();
        assert!(r.take(a).is_ok() && r.take(z).is_ok());
        assert!(matches!(r.take(e), Err(Failure::MalformedEnvelope { .. })));
    }

    #[test]
    fn the_shared_problem_vectors_map_to_the_same_failure_kinds() {
        // Copied by reference from the TypeScript SDK's fixture, as problem.rs does.
        const VECTORS: &str =
            include_str!("../../../sdks/typescript/test/fixtures/problem-vectors.json");
        let all = json::parse(VECTORS).expect("vectors are JSON");
        let decode = all.get("decode").and_then(Value::as_array).unwrap();
        assert!(!decode.is_empty());
        for v in decode {
            let s = |k: &str| v.get(k).and_then(Value::as_str).unwrap().to_owned();
            let status = match v.get("status") {
                Some(Value::Number(n)) => *n as u16,
                _ => panic!("status"),
            };
            let name = s("name");
            let (ct, body) = (s("content_type"), s("body"));
            let want = v
                .get("expect")
                .and_then(|e| e.get("kind"))
                .and_then(Value::as_str)
                .unwrap();
            let t = fake(move |_, _| {
                Ok(WireAnswer {
                    status,
                    headers: vec![("content-type".into(), ct.clone())],
                    body: body.clone().into_bytes(),
                })
            });
            let got = cart::Client::with_transport(&t, "a/b").ping();
            let kind = match &got {
                Err(f) => f.kind(),
                Ok(_) => "success",
            };
            let expected = match want {
                "problem" => "problem",
                "malformed" => "malformed-envelope",
                "unsupported-version" => "unsupported-envelope",
                // A 200 with the envelope type is a success status: its body is not `()`.
                "plain" if status < 300 => "invalid-response",
                "plain" => "http",
                other => panic!("{other}"),
            };
            assert_eq!(kind, expected, "{name}");
            if let (Err(Failure::Problem { problem, .. }), Some(code)) = (
                &got,
                v.get("expect")
                    .and_then(|e| e.get("code"))
                    .and_then(Value::as_str),
            ) {
                assert_eq!(problem.code(), code, "{name}");
            }
        }
    }

    #[test]
    fn paths_are_encoded_as_one_segment_and_dot_segments_are_refused() {
        assert_eq!(
            url_path("/a/{}/b/{}", &[&"x y", &5]).unwrap(),
            "/a/x%20y/b/5"
        );
        assert_eq!(url_path("/{}", &[&"a/b?c#d"]).unwrap(), "/a%2Fb%3Fc%23d");
        assert_eq!(
            url_path("/{}", &[&"it's (!)*"]).unwrap(),
            "/it%27s%20%28%21%29%2A"
        );
        assert_eq!(url_path("/{}", &[&"é"]).unwrap(), "/%C3%A9");
        assert!(url_path("/{}", &[&"."]).is_err());
        assert!(url_path("/{}", &[&".."]).is_err());
        assert!(url_path("/{}/{}", &[&"a"]).is_err());
        assert_eq!(
            with_query("/", &[("limit", &5), ("tag", &"a b"), ("tag", &"c")]).unwrap(),
            "/?limit=5&tag=a%20b&tag=c"
        );
        assert_eq!(
            with_query("/x?a=1", &[("b", &true)]).unwrap(),
            "/x?a=1&b=true"
        );
        for bad in [
            "x",
            "/a b",
            "/a#b",
            "/a\\b",
            "/a/../b",
            "/a/%2e%2E/b",
            "/./",
            "/a\u{7f}",
        ] {
            assert!(check_path(bad).is_err(), "{bad}");
        }
        assert!(check_path("/a/b..c/%2e%2ex?q=/../").is_ok());
    }
}
