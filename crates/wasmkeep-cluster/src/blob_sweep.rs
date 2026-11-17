//! Ending a deleted actor's files ([ADR 0030](../../../docs/adr/0030-files-and-static-assets.md),
//! section 5): the actor's incarnation, the sweep record a delete writes
//! before its reset, and what the sweep pass does with each record.
//!
//! Every file of an actor is under `blobs/<epoch>/<class>/<name id>/<incarnation>/`.
//! A delete draws a new incarnation and writes it in the empty database
//! its reset carries, so the next actor at the address starts in a
//! namespace no file of the old one is in. Before the reset is uploaded it
//! writes `blob-sweeps/<class>/<name id>/<old incarnation>` as `pending`;
//! once the bucket acknowledged the reset under a valid hold it rewrites it
//! `swept` and removes the old namespace. The pass judges a `pending`
//! record older than [`PENDING_JUDGED_AFTER_MS`] by restoring the actor:
//! another incarnation means the delete happened, the same one that it did
//! not, and only then is the record dropped with no file touched. A `swept`
//! record's namespace is listed again an hour later and a day after that
//! (uploads started before a link expired may land late), then dropped.
//!
//! Pure functions of strings and numbers, so the simulation in `sim/`
//! makes the same decisions as the server.

/// An actor's incarnation before any delete since files came.
pub const FIRST: &str = "0";

/// How old a `pending` record must be before it is judged: younger, its
/// delete may still be running.
pub const PENDING_JUDGED_AFTER_MS: u64 = 10 * 60 * 1000;

/// When a `swept` record's namespace is listed again, and a `listed` one's
/// the last time (by the record's last-modified time).
pub const RELIST_AFTER_MS: u64 = 60 * 60 * 1000;
pub const LAST_LIST_AFTER_MS: u64 = 24 * 60 * 60 * 1000;

/// How long a record nobody can judge (the actor has no history to fence,
/// no ownership record, or its class is gone) is kept before it is dropped:
/// dropping a record never deletes a file, so it is always safe.
pub const AGE_OUT_MS: u64 = 25 * 60 * 60 * 1000;

/// How often the sweep node runs a pass.
pub const PASS_EVERY_MS: u64 = 60 * 1000;

/// Whether `s` is an incarnation: [`FIRST`], or 16 lowercase hex digits.
pub fn is_incarnation(s: &str) -> bool {
    s == FIRST || (s.len() == 16 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
}

/// The incarnation a database's host meta value gives: [`FIRST`] when
/// there is none.
///
/// # Errors
/// A value that is not an incarnation: never read as [`FIRST`], which
/// would put the actor in the files of an earlier one.
pub fn incarnation_of(meta: Option<&str>) -> Result<String, String> {
    match meta {
        None => Ok(FIRST.to_owned()),
        Some(v) if is_incarnation(v) && v != FIRST => Ok(v.to_owned()),
        Some(v) => Err(format!("the actor's blob incarnation {v:?} is not one")),
    }
}

/// What an instance of the actor uses as its incarnation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Start {
    /// This one.
    Use(String),
    /// A fresh one, drawn and written into the database before any file
    /// call: the database carries a delete's mark (`replaced` starting
    /// `delete `) but no incarnation, a reset of a node without `blobs`.
    /// Running it at [`FIRST`] would put it in the files of the actor
    /// before that delete, or under a record that sweeps them.
    Draw,
}

/// The incarnation an instance starts with, from the database's host meta
/// `blob_incarnation` and `replaced`.
///
/// # Errors
/// An incarnation value that is not one.
pub fn start(incarnation: Option<&str>, replaced: Option<&str>) -> Result<Start, String> {
    match incarnation {
        Some(_) => incarnation_of(incarnation).map(Start::Use),
        None if replaced.is_some_and(|r| r.starts_with("delete ")) => Ok(Start::Draw),
        None => Ok(Start::Use(FIRST.to_owned())),
    }
}

/// Whether `s` is a name id: 64 lowercase hex digits.
pub fn is_name_id(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Whether `s` is an epoch: 16 lowercase hex digits.
pub fn is_epoch(s: &str) -> bool {
    s.len() == 16 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn is_class(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// A record's key under `blob-sweeps/`, `<class>/<name id>/<incarnation>`,
/// checked, or `None` (a key nothing of WasmKeep wrote: left alone).
pub fn parse_key(tail: &str) -> Option<(&str, &str, &str)> {
    let mut parts = tail.split('/');
    let (class, id, inc) = (parts.next()?, parts.next()?, parts.next()?);
    (parts.next().is_none() && is_class(class) && is_name_id(id) && is_incarnation(inc))
        .then_some((class, id, inc))
}

/// Where a record stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Written before the reset: the delete may or may not have happened.
    Pending,
    /// The reset was acknowledged: the namespace was removed once.
    Swept,
    /// Listed again an hour later: one listing to go.
    Listed,
}

impl State {
    fn word(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Swept => "swept",
            Self::Listed => "listed",
        }
    }
}

/// A sweep record's body: its state, the epoch its files are under, and
/// the actor's name (nothing needs to reverse the hash).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub state: State,
    pub epoch: String,
    pub name: String,
}

const HEADER: &str = "wasmkeep-blob-sweep 1";

impl Record {
    pub fn encode(&self) -> String {
        format!(
            "{HEADER}\n{}\n{}\n{}\n",
            self.state.word(),
            self.epoch,
            self.name
        )
    }

    /// The record `body` holds, or `None`: a body this version does not
    /// know is never acted on.
    pub fn decode(body: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(body).ok()?;
        let mut lines = text.strip_suffix('\n')?.split('\n');
        if lines.next()? != HEADER {
            return None;
        }
        let state = match lines.next()? {
            "pending" => State::Pending,
            "swept" => State::Swept,
            "listed" => State::Listed,
            _ => return None,
        };
        let epoch = lines.next()?;
        let name = lines.next()?;
        (lines.next().is_none() && is_epoch(epoch) && !name.is_empty()).then(|| Self {
            state,
            epoch: epoch.to_owned(),
            name: name.to_owned(),
        })
    }
}

/// What a pass does with one record, `age_ms` old by the bucket's clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Nothing yet.
    Wait,
    /// Judge the delete: restore the actor and compare incarnations.
    Judge,
    /// Remove what is under the namespace, then rewrite the record `listed`.
    Relist,
    /// Remove what is under the namespace, then drop the record.
    Finish,
}

/// The ages a pass acts at (ms): the constants above, shorter in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ages {
    pub judge_after: u64,
    pub relist_after: u64,
    pub last_after: u64,
    pub age_out: u64,
}

impl Default for Ages {
    fn default() -> Self {
        Self {
            judge_after: PENDING_JUDGED_AFTER_MS,
            relist_after: RELIST_AFTER_MS,
            last_after: LAST_LIST_AFTER_MS,
            age_out: AGE_OUT_MS,
        }
    }
}

/// What to do with a record in `state`, `age_ms` old.
pub fn action(state: State, age_ms: u64) -> Action {
    action_at(state, age_ms, Ages::default())
}

/// [`action`] at `ages`.
pub fn action_at(state: State, age_ms: u64, ages: Ages) -> Action {
    match state {
        State::Pending if age_ms > ages.judge_after => Action::Judge,
        State::Swept if age_ms > ages.relist_after => Action::Relist,
        State::Listed if age_ms > ages.last_after => Action::Finish,
        _ => Action::Wait,
    }
}

/// A judgement of a `pending` record of `recorded` incarnation, against the
/// incarnation a verified, fenced restore of the actor holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Judged {
    /// The delete happened: sweep the old namespace.
    Deleted,
    /// It did not, and no late reset can make it happen now: drop the
    /// record, touch no file.
    NotDeleted,
}

/// `current` is `None` for an actor whose next instance draws a fresh
/// incarnation ([`Start::Draw`]): no file of it can be under any record.
pub fn judge(recorded: &str, current: Option<&str>) -> Judged {
    if current == Some(recorded) {
        Judged::NotDeleted
    } else {
        Judged::Deleted
    }
}

/// What a pass does once it acted on a record: every action that removes
/// files is a judged one, at the second and last listing too (an
/// incarnation can come back, through a node without `blobs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Nothing now: the next pass asks again.
    Skip,
    /// Remove the namespace, then rewrite the record in this state, or
    /// drop it (`None`).
    Sweep(Option<State>),
    /// Drop the record, touch no file: the actor is at that incarnation.
    Drop,
    /// Drop the record, touch no file: nobody could judge it for
    /// [`AGE_OUT_MS`].
    AgeOut,
}

/// The outcome of `action` on a record `age_ms` old, given the judgement
/// (`None`: none could be made).
pub fn outcome(action: Action, judged: Option<Judged>, age_ms: u64, ages: Ages) -> Outcome {
    match (action, judged) {
        (Action::Wait, _) => Outcome::Skip,
        (_, None) if age_ms > ages.age_out => Outcome::AgeOut,
        (_, None) => Outcome::Skip,
        (_, Some(Judged::NotDeleted)) => Outcome::Drop,
        (Action::Judge, Some(Judged::Deleted)) => Outcome::Sweep(Some(State::Swept)),
        (Action::Relist, Some(Judged::Deleted)) => Outcome::Sweep(Some(State::Listed)),
        (Action::Finish, Some(Judged::Deleted)) => Outcome::Sweep(None),
    }
}

/// The folder of an incarnation's files under a root's `blobs/`, with its
/// trailing `/`: every part checked, so a prefix never names more than one
/// namespace.
pub fn namespace_folder(
    epoch: &str,
    class: &str,
    name_id: &str,
    incarnation: &str,
) -> Option<String> {
    (is_epoch(epoch) && is_class(class) && is_name_id(name_id) && is_incarnation(incarnation))
        .then(|| format!("{epoch}/{class}/{name_id}/{incarnation}/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "2bd806c97f0e00af1a1fc3328fa763a9269723c8db8fac4f93af71db186d6e90";

    #[test]
    fn incarnations_are_checked_and_never_defaulted_from_a_bad_value() {
        assert_eq!(incarnation_of(None).unwrap(), "0");
        assert_eq!(
            incarnation_of(Some("0123456789abcdef")).unwrap(),
            "0123456789abcdef"
        );
        for bad in [
            "",
            "0",
            "1",
            "0123456789ABCDEF",
            "0123456789abcde",
            "x".repeat(16).as_str(),
        ] {
            assert!(incarnation_of(Some(bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_delete_mark_without_an_incarnation_draws_a_fresh_one() {
        assert_eq!(start(None, None), Ok(Start::Use("0".into())));
        assert_eq!(
            start(None, Some("restore 4 1 a")),
            Ok(Start::Use("0".into()))
        );
        assert_eq!(start(None, Some("delete 5 1 2")), Ok(Start::Draw));
        assert_eq!(
            start(Some("0123456789abcdef"), Some("delete 5 1 2")),
            Ok(Start::Use("0123456789abcdef".into()))
        );
        assert!(start(Some("bad"), None).is_err());
    }

    #[test]
    fn records_round_trip_and_odd_ones_are_left_alone() {
        for state in [State::Pending, State::Swept, State::Listed] {
            let r = Record {
                state,
                epoch: "6e340b9cffb37a98".into(),
                name: "a b/ç".into(),
            };
            assert_eq!(Record::decode(r.encode().as_bytes()), Some(r));
        }
        for bad in [
            "",
            "pending\n",
            "wasmkeep-blob-sweep 1\npending\n6e340b9cffb37a98\nn",
            "wasmkeep-blob-sweep 1\ngone\n6e340b9cffb37a98\nn\n",
            "wasmkeep-blob-sweep 1\npending\nE\nn\n",
            "wasmkeep-blob-sweep 1\npending\n6e340b9cffb37a98\n\n",
            "wasmkeep-blob-sweep 2\npending\n6e340b9cffb37a98\nn\n",
        ] {
            assert_eq!(Record::decode(bad.as_bytes()), None, "{bad:?}");
        }
        assert_eq!(parse_key(&format!("c/{ID}/0")), Some(("c", ID, "0")));
        for bad in [
            format!("c/{ID}"),
            format!("c/{ID}/0/x"),
            format!("C/{ID}/0"),
            format!("c/{}/0", &ID[1..]),
            format!("c/{ID}/00"),
            format!("../{ID}/0"),
        ] {
            assert_eq!(parse_key(&bad), None, "{bad}");
        }
    }

    #[test]
    fn a_pass_acts_by_age_and_judges_by_incarnation() {
        assert_eq!(
            action(State::Pending, PENDING_JUDGED_AFTER_MS),
            Action::Wait
        );
        assert_eq!(
            action(State::Pending, PENDING_JUDGED_AFTER_MS + 1),
            Action::Judge
        );
        assert_eq!(action(State::Swept, RELIST_AFTER_MS), Action::Wait);
        assert_eq!(action(State::Swept, RELIST_AFTER_MS + 1), Action::Relist);
        assert_eq!(action(State::Listed, RELIST_AFTER_MS + 1), Action::Wait);
        assert_eq!(
            action(State::Listed, LAST_LIST_AFTER_MS + 1),
            Action::Finish
        );
        assert_eq!(judge("0", Some("0")), Judged::NotDeleted);
        assert_eq!(judge("0", Some("0123456789abcdef")), Judged::Deleted);
        assert_eq!(judge("0", None), Judged::Deleted);
        let ages = Ages::default();
        for action in [Action::Judge, Action::Relist, Action::Finish] {
            // The actor came back to the record's incarnation: no file goes.
            assert_eq!(
                outcome(action, Some(Judged::NotDeleted), 0, ages),
                Outcome::Drop
            );
            assert_eq!(outcome(action, None, AGE_OUT_MS, ages), Outcome::Skip);
            assert_eq!(outcome(action, None, AGE_OUT_MS + 1, ages), Outcome::AgeOut);
        }
        assert_eq!(
            outcome(Action::Finish, Some(Judged::Deleted), 0, ages),
            Outcome::Sweep(None)
        );
    }

    #[test]
    fn a_namespace_folder_names_one_namespace() {
        assert_eq!(
            namespace_folder("6e340b9cffb37a98", "c", ID, "0").unwrap(),
            format!("6e340b9cffb37a98/c/{ID}/0/")
        );
        assert!(namespace_folder("", "c", ID, "0").is_none());
        assert!(namespace_folder("6e340b9cffb37a98", "", ID, "0").is_none());
        assert!(namespace_folder("6e340b9cffb37a98", "c", "", "0").is_none());
        assert!(namespace_folder("6e340b9cffb37a98", "c", ID, "").is_none());
    }
}
