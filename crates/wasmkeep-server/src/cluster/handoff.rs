//! Messages that hand an alarm or an actor to another node (ADR 0014,
//! ADR 0017).

use super::*;

/// The body of an alarm hand-off: its due time.
pub(crate) fn alarm_handoff_body(tenant: &str, at: u64) -> String {
    format!("wasmkeep-alarm 1\n{tenant}\n{at}\n")
}

/// The tenant and due time in an alarm hand-off's body.
pub(crate) fn parse_alarm_handoff(body: &[u8]) -> Option<(String, u64)> {
    let text = std::str::from_utf8(body).ok()?;
    let mut lines = text.lines();
    (lines.next()? == "wasmkeep-alarm 1").then_some(())?;
    let tenant = lines.next()?.to_owned();
    let at = lines.next()?.parse().ok()?;
    lines.next().is_none().then_some((tenant, at))
}

/// What a take call got (ADR 0017).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TakeAnswer {
    /// The node holds the actor now.
    Held,
    /// Another node holds it (the acquire lost): its id.
    HeldBy(String),
    /// No such actor (never created, or a class the node does not serve).
    NoSuchActor,
    /// Refused (draining, secrets blocked, another tenant), the bucket did
    /// not answer, or the call failed.
    Failed(String),
}

/// How long a take may take (a restore can take a few seconds).
pub(super) const TAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The body of a take call.
pub(crate) fn take_body(tenant: &str) -> String {
    format!("wasmkeep-take 1\n{tenant}\n")
}

/// The tenant in a take call's body.
pub(crate) fn parse_take(body: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(body).ok()?;
    let mut lines = text.lines();
    (lines.next()? == "wasmkeep-take 1").then_some(())?;
    let tenant = lines.next()?.to_owned();
    lines.next().is_none().then_some(tenant)
}

/// Why an alarm hand-off did not go through.
#[derive(Debug)]
pub(crate) enum HandOffError {
    /// The peer does not serve this actor (403, 404).
    Refused(String),
    /// The peer is down, unreachable, slow, or draining itself.
    Failed(String),
}

impl std::fmt::Display for HandOffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(m) | Self::Failed(m) => f.write_str(m),
        }
    }
}

/// How long an alarm hand-off may take.
pub(super) const HAND_OFF_TIMEOUT: Duration = Duration::from_secs(2);
