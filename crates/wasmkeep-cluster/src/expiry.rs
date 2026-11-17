//! Idle expiry ([ADR 0028](../../../docs/adr/0028-actors-that-end.md),
//! section 4): how long a class's actors may stay idle, and which actors
//! one pass of the collecting node expires.
//!
//! An actor's ownership record is written when it is acquired and when it
//! hibernates (the release). A record that names nobody and was last
//! modified more than the class's limit ago is an actor idle at least that
//! long, so one listing of `owners/<class>/` (keys, last-modified times and
//! versions, no bodies) gives every candidate, and one listing of `alarms/`
//! the actors with an alarm, which are left alone. The listing cannot tell
//! a released record from a held one: the expiry's own acquisition
//! ([`crate::node_lease::Owners::acquire_unchanged`]) reads the record and
//! takes it only if it names nobody and is still the version listed.
//!
//! Pure functions of strings and numbers, so the simulation in `sim/` makes
//! the same decisions as the server.

use std::collections::BTreeSet;

/// The shortest limit a class may have: one hour.
pub const MIN_IDLE_SECS: u64 = 60 * 60;

/// The longest limit: about 100 years, so that milliseconds never overflow.
pub const MAX_IDLE_SECS: u64 = 100 * 365 * 24 * 60 * 60;

/// How often the collecting node runs a pass (ms).
pub const PASS_EVERY_MS: u64 = 10 * 60 * 1000;

/// Expiries started per pass, at most.
pub const MAX_PER_PASS: usize = 100;

/// Expiries running at once on the collecting node.
pub const PARALLEL: usize = 4;

/// Parses a limit: a whole number and a unit, `s`, `m`, `h` or `d`
/// (`"7d"`, `"36h"`, `"90m"`, `"3600s"`), at least [`MIN_IDLE_SECS`] and
/// at most [`MAX_IDLE_SECS`]. In seconds.
///
/// # Errors
/// Why it is not a limit, for the operator.
pub fn parse_idle(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("{text:?}: give a unit (s, m, h or d), as in 7d"))?;
    let (number, unit) = text.split_at(split);
    let per = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        _ => {
            return Err(format!(
                "{text:?}: a whole number and one unit, s, m, h or d, as in 7d"
            ));
        }
    };
    let n: u64 = number
        .parse()
        .map_err(|_| format!("{text:?}: a whole number and one unit, as in 7d"))?;
    let secs = n.saturating_mul(per);
    if secs < MIN_IDLE_SECS {
        return Err(format!("{text:?}: the shortest limit is one hour (1h)"));
    }
    if secs > MAX_IDLE_SECS {
        return Err(format!("{text:?}: the longest limit is 36500d"));
    }
    Ok(secs)
}

/// A limit in the largest unit that divides it exactly (`604800` gives
/// `7d`); [`parse_idle`] reads it back.
#[must_use]
pub fn format_idle(secs: u64) -> String {
    for (per, unit) in [(24 * 60 * 60, "d"), (60 * 60, "h"), (60, "m")] {
        if secs.is_multiple_of(per) {
            return format!("{}{unit}", secs / per);
        }
    }
    format!("{secs}s")
}

/// One ownership record, as a listing of `owners/<class>/` shows it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Listed {
    /// The key after `owners/<class>/`: the actor's name, encoded as the
    /// record's key encodes it.
    pub name: String,
    /// When it was last written, by the bucket's clock (ms).
    pub modified_ms: u64,
    /// Its version as listed (the `ETag`), which the expiry's acquisition
    /// must find unchanged; empty if the listing gave none, and then the
    /// record is never expired.
    pub version: String,
    /// The idle origin a version 2 record carries (`idle-since`, ADR
    /// 0035), read separately: earlier than `modified_ms` when refused
    /// cycles rewrote the record without activity. `None`: the record's
    /// last-modified time is its idle origin.
    pub idle_since_ms: Option<u64>,
}

impl Listed {
    /// Since when the actor has been idle.
    pub fn idle_origin_ms(&self) -> u64 {
        self.idle_since_ms
            .map_or(self.modified_ms, |since| since.min(self.modified_ms))
    }
}

/// The records of one listing an expiry pass acts on: last written more
/// than `idle_ms` before `now_ms`, with a version, not among `alarmed`
/// (names with an entry in the alarm index, encoded as record keys are),
/// and not `skip`ped (records this node knows to be held, or that an
/// earlier pass found changed). Oldest first, at most `max`.
pub fn candidates(
    listed: Vec<Listed>,
    alarmed: &BTreeSet<String>,
    skip: impl Fn(&Listed) -> bool,
    now_ms: u64,
    idle_ms: u64,
    max: usize,
) -> Vec<Listed> {
    let mut out: Vec<Listed> = listed
        .into_iter()
        .filter(|l| {
            now_ms.saturating_sub(l.idle_origin_ms()) > idle_ms
                && !l.version.is_empty()
                && !alarmed.contains(&l.name)
                && !skip(l)
        })
        .collect();
    out.sort_by(|a, b| (a.idle_origin_ms(), &a.name).cmp(&(b.idle_origin_ms(), &b.name)));
    out.truncate(max);
    out
}

/// Whether node `me` is the one that collects: the lowest id among the
/// `live` nodes that may collect (serving the app, not draining, announcing
/// the capability word `ends`), itself included.
pub fn collects<'a>(me: &str, live: impl IntoIterator<Item = &'a str>) -> bool {
    live.into_iter().all(|other| other >= me)
}

/// The version part to compare of an `ETag`: without the quotes and the
/// weak marker some stores add, so a listing's and a read's agree.
#[must_use]
pub fn etag(raw: &str) -> &str {
    raw.trim().trim_start_matches("W/").trim_matches('"')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_parse_with_one_unit_and_a_floor_of_an_hour() {
        assert_eq!(parse_idle("7d"), Ok(7 * 86_400));
        assert_eq!(parse_idle("36h"), Ok(36 * 3_600));
        assert_eq!(parse_idle("90m"), Ok(5_400));
        assert_eq!(parse_idle("3600s"), Ok(3_600));
        assert_eq!(parse_idle(" 1h "), Ok(3_600));
        for bad in [
            "",
            "7",
            "d",
            "7w",
            "1.5h",
            "-1h",
            "59m",
            "3599s",
            "0d",
            "7 d",
            "7dd",
            "36501d",
            "99999999999999999999d",
        ] {
            assert!(parse_idle(bad).is_err(), "{bad:?}");
        }
        for secs in [3_600, 5_400, 86_400, 7 * 86_400, 3_601] {
            assert_eq!(parse_idle(&format_idle(secs)), Ok(secs));
        }
        assert_eq!(format_idle(7 * 86_400), "7d");
        assert_eq!(format_idle(3_601), "3601s");
    }

    fn listed(name: &str, modified_ms: u64, version: &str) -> Listed {
        Listed {
            name: name.into(),
            modified_ms,
            version: version.into(),
            idle_since_ms: None,
        }
    }

    #[test]
    fn candidates_are_old_records_without_an_alarm_oldest_first() {
        let all = vec![
            listed("fresh", 9_500, "v1"),
            listed("old", 2_000, "v2"),
            listed("older", 1_000, "v3"),
            listed("alarmed", 1_000, "v4"),
            listed("unversioned", 1_000, ""),
            listed("skipped", 1_000, "v6"),
            listed("edge", 9_000, "v7"),
        ];
        let alarmed = BTreeSet::from(["alarmed".to_owned()]);
        let got = candidates(
            all.clone(),
            &alarmed,
            |l| l.name == "skipped",
            10_000,
            1_000,
            10,
        );
        let names: Vec<&str> = got.iter().map(|l| l.name.as_str()).collect();
        // Exactly the limit is not past it.
        assert_eq!(names, ["older", "old"]);
        let got = candidates(all, &alarmed, |_| false, 10_000, 1_000, 1);
        assert_eq!(got.len(), 1);
    }

    /// ADR 0035 S3: a record rewritten by refused cycles keeps its idle
    /// origin in `idle-since`; it is a candidate by that, not by the
    /// rewrite's time.
    #[test]
    fn a_carried_idle_origin_makes_a_candidate() {
        let mut refused = listed("refused", 9_900, "v1");
        refused.idle_since_ms = Some(1_000);
        let got = candidates(
            vec![refused],
            &BTreeSet::new(),
            |_| false,
            10_000,
            1_000,
            10,
        );
        assert_eq!(got.len(), 1);
    }

    #[test]
    fn the_lowest_live_id_collects() {
        assert!(collects("a", ["a", "b", "c"]));
        assert!(!collects("b", ["a", "b", "c"]));
        assert!(collects("b", ["b", "c"]));
        assert!(collects("a", []));
    }

    #[test]
    fn etags_compare_without_quotes() {
        assert_eq!(etag("\"abc\""), "abc");
        assert_eq!(etag("W/\"abc\""), "abc");
        assert_eq!(etag("abc"), "abc");
    }
}
