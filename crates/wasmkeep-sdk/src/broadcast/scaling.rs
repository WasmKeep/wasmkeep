//! Durable direct-edge scaling. All external effects are replayable.
use super::*;
use crate::directory::{Change, Keep};
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScaleChild {
    pub name: String,
    pub lo: u64,
    pub hi: Option<u64>,
    pub sockets: Option<u64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScaleKind {
    Boot,
    Split,
    Merge,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScaleOp {
    pub id: String,
    pub kind: ScaleKind,
    pub step: u64,
    pub a: ScaleChild,
    pub b: ScaleChild,
    pub expect: u64,
    pub started: u64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScaleState {
    pub children: Vec<ScaleChild>,
    pub next: u64,
    pub version: u64,
    pub split: Option<u64>,
    pub merge: Option<u64>,
    pub op: Option<ScaleOp>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScaleCommand {
    Init(ScaleChild),
    Range(ScaleChild),
    Drain(ScaleChild),
    End(ScaleChild, bool),
    Create(ScaleChild),
    Change(Change),
}
pub trait ScaleIo {
    fn load(&self) -> Result<ScaleState, String>;
    fn save(&mut self, state: &ScaleState) -> Result<(), String>;
    fn execute(&mut self, command: ScaleCommand) -> Result<(), String>;
}
pub fn scale_decision(
    c: &[ScaleChild],
    cap: u64,
    now: u64,
    split: Option<u64>,
    merge: Option<u64>,
) -> Option<(ScaleKind, usize, usize)> {
    if merge.is_none_or(|t| now.saturating_sub(t) >= 60000) && c.len() < 16 {
        for (i, a) in c.iter().enumerate() {
            if a.sockets.is_some_and(|n| n * 100 >= cap * 80)
                && a.hi.map_or(1u128 << 64, u128::from) - u128::from(a.lo) > 1
            {
                return Some((ScaleKind::Split, i, i));
            }
        }
    }
    if split.is_some_and(|t| now.saturating_sub(t) < 60000) {
        return None;
    }
    for (i, a) in c.iter().enumerate() {
        let Some(n) = a.sockets.filter(|n| n * 100 < cap * 30) else {
            continue;
        };
        let neighbor = [i.checked_sub(1), (i + 1 < c.len()).then_some(i + 1)]
            .into_iter()
            .flatten()
            .filter(|j| c[*j].sockets.is_some())
            .min_by_key(|j| c[*j].sockets);
        if let Some(j) = neighbor
            && c[j].sockets.is_some_and(|m| (n + m) * 100 <= cap * 60)
        {
            return Some((ScaleKind::Merge, i, j));
        }
    }
    None
}
#[derive(Debug)]
pub struct Scaling<B: ScaleIo = ScaleHost> {
    pub io: B,
    prefix: String,
    capacity: u64,
}
impl<B: ScaleIo> Scaling<B> {
    pub fn new(io: B, prefix: impl Into<String>, capacity: u64) -> Result<Self, String> {
        if capacity == 0 || capacity > 500 {
            return Err("invalid edge capacity".into());
        }
        Ok(Self {
            io,
            prefix: prefix.into(),
            capacity,
        })
    }
    pub fn state(&self) -> Result<ScaleState, String> {
        self.io.load()
    }
    pub fn begin(&mut self, s: &mut ScaleState, now: u64) -> Result<bool, String> {
        if s.op.is_some() {
            return Ok(true);
        }
        let d = scale_decision(&s.children, self.capacity, now, s.split, s.merge);
        if !s.children.is_empty() && d.is_none() {
            return Ok(false);
        }
        if s.next > u64::MAX - 2 {
            return Err("scale child counter exhausted".into());
        }
        let id = format!("{}.op{}", self.prefix, s.next);
        s.next += 1;
        let mut a = if let Some((_, i, _)) = d {
            s.children[i].clone()
        } else {
            let name = format!("{}.e{}", self.prefix, s.next);
            s.next += 1;
            ScaleChild {
                name,
                lo: 0,
                hi: None,
                sockets: None,
            }
        };
        a.sockets = None;
        let mut b = match d {
            Some((ScaleKind::Split, _, _)) => {
                let name = format!("{}.e{}", self.prefix, s.next);
                s.next += 1;
                ScaleChild {
                    name,
                    lo: (u128::from(a.lo)
                        + (a.hi.map_or(1u128 << 64, u128::from) - u128::from(a.lo)) / 2)
                        as u64,
                    hi: a.hi,
                    sockets: None,
                }
            }
            Some((_, _, j)) => s.children[j].clone(),
            None => a.clone(),
        };
        b.sockets = None;
        s.op = Some(ScaleOp {
            id,
            kind: d.map_or(ScaleKind::Boot, |d| d.0),
            step: 0,
            a,
            b,
            expect: s.version,
            started: now,
        });
        self.io.save(s)?;
        Ok(true)
    }
    pub fn step(&mut self, s: &mut ScaleState, now: u64) -> Result<(), String> {
        let o = s.op.clone().ok_or("no pending scale operation")?;
        match (o.kind, o.step) {
            (ScaleKind::Boot, 0) => {
                self.io.execute(ScaleCommand::Init(o.a.clone()))?;
                s.children = vec![o.a.clone()];
            }
            (ScaleKind::Boot, _) => {
                self.io.execute(ScaleCommand::Create(o.a.clone()))?;
                s.version = 1;
                s.op = None;
            }
            (ScaleKind::Split, 0) => {
                self.io.execute(ScaleCommand::Init(o.b.clone()))?;
                if !s.children.iter().any(|c| c.name == o.b.name) {
                    s.children.push(o.b.clone());
                }
            }
            (ScaleKind::Split, 1) => {
                self.io.execute(ScaleCommand::Change(Change::Split {
                    lo: o.a.lo,
                    at: o.b.lo,
                    target: o.b.name.clone(),
                    meta: vec![],
                    expect: o.expect,
                    op: o.id.clone(),
                }))?;
                s.version = o.expect + 1;
            }
            (ScaleKind::Split, _) => {
                let a = ScaleChild {
                    hi: Some(o.b.lo),
                    ..o.a.clone()
                };
                self.io.execute(ScaleCommand::Range(a.clone()))?;
                for c in &mut s.children {
                    if c.name == a.name {
                        *c = a.clone();
                    }
                }
                s.split = Some(now);
                s.op = None;
            }
            (ScaleKind::Merge, 0) => {
                let b = ScaleChild {
                    lo: o.a.lo.min(o.b.lo),
                    hi: o.a.hi.zip(o.b.hi).map(|(a, b)| a.max(b)),
                    ..o.b.clone()
                };
                self.io.execute(ScaleCommand::Range(b.clone()))?;
                for c in &mut s.children {
                    if c.name == b.name {
                        *c = b.clone();
                    }
                }
            }
            (ScaleKind::Merge, 1) => {
                self.io.execute(ScaleCommand::Change(Change::Merge {
                    lo: o.a.lo.max(o.b.lo),
                    keep: if o.b.lo < o.a.lo {
                        Keep::Left
                    } else {
                        Keep::Right
                    },
                    expect: o.expect,
                    op: o.id.clone(),
                }))?;
                s.version = o.expect + 1;
            }
            (ScaleKind::Merge, 2) => self.io.execute(ScaleCommand::Drain(o.a.clone()))?,
            (ScaleKind::Merge, _) => {
                self.io.execute(ScaleCommand::End(
                    o.a.clone(),
                    now.saturating_sub(o.started) >= 30000,
                ))?;
                s.children.retain(|c| c.name != o.a.name);
                s.merge = Some(now);
                s.op = None;
            }
        }
        for c in &mut s.children {
            c.sockets = None;
        }
        s.children.sort_by_key(|c| c.lo);
        if let Some(o) = &mut s.op {
            o.step += 1;
        }
        self.io.save(s)
    }
}
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    #[test]
    fn r4_hysteresis_and_pair_thresholds() {
        let mut c = vec![
            ScaleChild {
                name: "edge/a".into(),
                lo: 0,
                hi: Some(1 << 63),
                sockets: Some(8),
            },
            ScaleChild {
                name: "edge/b".into(),
                lo: 1 << 63,
                hi: None,
                sockets: Some(2),
            },
        ];
        assert_eq!(scale_decision(&c, 10, 60000, None, Some(1)), None);
        assert_eq!(
            scale_decision(&c, 10, 60001, None, Some(1)),
            Some((ScaleKind::Split, 0, 0))
        );
        c[0].sockets = Some(4);
        assert_eq!(scale_decision(&c, 10, 60000, Some(1), None), None);
        assert_eq!(
            scale_decision(&c, 10, 60001, Some(1), None),
            Some((ScaleKind::Merge, 1, 0))
        );
        c[1].sockets = Some(3);
        assert_eq!(scale_decision(&c, 10, 60001, None, None), None);
        c[1].sockets = Some(2);
        c[0].sockets = Some(5);
        assert_eq!(scale_decision(&c, 10, 60001, None, None), None);
    }
    #[derive(Debug, Default)]
    struct Fake {
        saved: ScaleState,
        effects: Vec<ScaleCommand>,
        fail: bool,
    }
    impl ScaleIo for Fake {
        fn load(&self) -> Result<ScaleState, String> {
            let mut s = decode_state(&encode_state(&self.saved))?;
            s.op = self
                .saved
                .op
                .as_ref()
                .map(|o| decode_op(&encode_op(o)))
                .transpose()?;
            Ok(s)
        }
        fn save(&mut self, s: &ScaleState) -> Result<(), String> {
            if self.fail {
                self.fail = false;
                return Err("crash".into());
            }
            self.saved = s.clone();
            Ok(())
        }
        fn execute(&mut self, c: ScaleCommand) -> Result<(), String> {
            self.effects.push(c);
            Ok(())
        }
    }
    #[test]
    fn r4_every_step_replays_after_lost_checkpoint() {
        for kind in [ScaleKind::Boot, ScaleKind::Split, ScaleKind::Merge] {
            let steps = match kind {
                ScaleKind::Boot => 2,
                ScaleKind::Split => 3,
                ScaleKind::Merge => 4,
            };
            for cut in 0..steps {
                let a = ScaleChild {
                    name: "edge/a".into(),
                    lo: 0,
                    hi: None,
                    sockets: Some(8),
                };
                let mut saved = ScaleState {
                    next: 3,
                    version: 1,
                    ..ScaleState::default()
                };
                if kind == ScaleKind::Split {
                    saved.children = vec![a.clone()];
                }
                if kind == ScaleKind::Merge {
                    saved.version = 2;
                    saved.children = vec![
                        ScaleChild {
                            hi: Some(1 << 63),
                            sockets: Some(1),
                            ..a.clone()
                        },
                        ScaleChild {
                            name: "edge/b".into(),
                            lo: 1 << 63,
                            hi: None,
                            sockets: Some(2),
                        },
                    ];
                }
                let mut scale = Scaling::new(
                    Fake {
                        saved,
                        ..Fake::default()
                    },
                    "edge/r",
                    10,
                )
                .unwrap();
                let mut s = scale.io.saved.clone();
                assert!(scale.begin(&mut s, 60000).unwrap());
                for _ in 0..cut {
                    scale.step(&mut s, 60000).unwrap();
                }
                scale.io.fail = true;
                assert!(scale.step(&mut s, 60000).is_err());
                let mut scale = Scaling::new(scale.io, "edge/r", 10).unwrap();
                let mut s = scale.state().unwrap();
                while s.op.is_some() {
                    scale.step(&mut s, 60000).unwrap();
                }
                assert_eq!(scale.io.effects.len(), steps + 1);
                assert_eq!(scale.io.effects[cut], scale.io.effects[cut + 1]);
                assert_eq!(
                    s.children.len(),
                    if kind == ScaleKind::Split { 2 } else { 1 }
                );
                assert!(s.children.iter().all(|c| c.sockets.is_none()));
            }
        }
    }
}

use crate::{
    storage,
    wire::{Reader, Writer},
};
#[derive(Debug)]
pub struct ScaleHost {
    pub directory: String,
    pub room: String,
    pub capacity: u64,
    pub head: Position,
}
fn put_child(w: &mut Writer, c: &ScaleChild) {
    w.bytes(c.name.as_bytes())
        .u64(c.lo)
        .u64(u64::from(c.hi.is_some()))
        .u64(c.hi.unwrap_or(0));
}
fn num(r: &mut Reader<'_>) -> Result<u64, String> {
    r.u64().ok_or("invalid scale row".into())
}
fn txt(r: &mut Reader<'_>) -> Result<String, String> {
    String::from_utf8(r.bytes().ok_or("invalid scale row")?.to_vec())
        .map_err(|_| "invalid scale row".into())
}
fn get_child(r: &mut Reader<'_>) -> Result<ScaleChild, String> {
    let name = txt(r)?;
    let lo = num(r)?;
    let has = num(r)?;
    let hi = num(r)?;
    Ok(ScaleChild {
        name,
        lo,
        hi: (has != 0).then_some(hi),
        sockets: None,
    })
}
fn encode_state(s: &ScaleState) -> Vec<u8> {
    let mut w = Writer::default();
    w.u64(s.next)
        .u64(s.version)
        .u64(u64::from(s.split.is_some()))
        .u64(s.split.unwrap_or(0))
        .u64(u64::from(s.merge.is_some()))
        .u64(s.merge.unwrap_or(0))
        .u64(s.children.len() as u64);
    for c in &s.children {
        put_child(&mut w, c);
    }
    w.finish()
}
fn decode_state(b: &[u8]) -> Result<ScaleState, String> {
    let mut r = Reader::new(b);
    let next = num(&mut r)?;
    let version = num(&mut r)?;
    let has = num(&mut r)?;
    let split = num(&mut r)?;
    let split = (has != 0).then_some(split);
    let has = num(&mut r)?;
    let merge = num(&mut r)?;
    let merge = (has != 0).then_some(merge);
    let n = num(&mut r)?;
    if n > 16 {
        return Err("invalid scale children".into());
    }
    let mut children = vec![];
    for _ in 0..n {
        children.push(get_child(&mut r)?);
    }
    Ok(ScaleState {
        children,
        next,
        version,
        split,
        merge,
        op: None,
    })
}
fn encode_op(o: &ScaleOp) -> Vec<u8> {
    let mut w = Writer::default();
    w.bytes(o.id.as_bytes())
        .u64(match o.kind {
            ScaleKind::Boot => 0,
            ScaleKind::Split => 1,
            ScaleKind::Merge => 2,
        })
        .u64(o.step)
        .u64(o.expect)
        .u64(o.started);
    put_child(&mut w, &o.a);
    put_child(&mut w, &o.b);
    w.finish()
}
fn decode_op(b: &[u8]) -> Result<ScaleOp, String> {
    let mut r = Reader::new(b);
    let id = txt(&mut r)?;
    let kind = match num(&mut r)? {
        0 => ScaleKind::Boot,
        1 => ScaleKind::Split,
        2 => ScaleKind::Merge,
        _ => return Err("invalid scale kind".into()),
    };
    Ok(ScaleOp {
        id,
        kind,
        step: num(&mut r)?,
        expect: num(&mut r)?,
        started: num(&mut r)?,
        a: get_child(&mut r)?,
        b: get_child(&mut r)?,
    })
}
impl ScaleHost {
    pub fn init(&self) -> Result<(), String> {
        storage::exec("CREATE TABLE IF NOT EXISTS wasmkeep_room_scale(id INTEGER PRIMARY KEY CHECK(id=1),data BLOB NOT NULL)",&[]).map_err(|e|e.to_string())?;
        storage::exec("CREATE TABLE IF NOT EXISTS wasmkeep_room_ops(id TEXT PRIMARY KEY,kind TEXT NOT NULL,step INTEGER NOT NULL,args BLOB NOT NULL)",&[]).map_err(|e|e.to_string())?;
        Ok(())
    }
    fn config(&self, c: &ScaleChild, drain: bool) -> String {
        let mut b = "{\"room\":".to_owned();
        crate::json::write_str(&mut b, &self.room);
        b.push_str(&format!(
            ",\"lo\":\"{:016x}\",\"state\":\"{}\",\"cap\":{},\"e\":{},\"n\":{}",
            c.lo,
            if drain { "draining" } else { "active" },
            self.capacity,
            self.head.e,
            self.head.n
        ));
        if let Some(hi) = c.hi {
            b.push_str(&format!(",\"hi\":\"{hi:016x}\""));
        }
        b.push('}');
        b
    }
}
impl ScaleIo for ScaleHost {
    fn load(&self) -> Result<ScaleState, String> {
        let rows = storage::query("SELECT data FROM wasmkeep_room_scale WHERE id=1", &[])
            .map_err(|e| e.to_string())?;
        let mut s = rows
            .values
            .first()
            .map(|r| decode_state(r[0].as_bytes().ok_or("invalid scale row")?))
            .transpose()?
            .unwrap_or_default();
        let rows =
            storage::query("SELECT args FROM wasmkeep_room_ops", &[]).map_err(|e| e.to_string())?;
        s.op = rows
            .values
            .first()
            .map(|r| decode_op(r[0].as_bytes().ok_or("invalid scale op")?))
            .transpose()?;
        Ok(s)
    }
    fn save(&mut self, s: &ScaleState) -> Result<(), String> {
        storage::exec("BEGIN", &[]).map_err(|e| e.to_string())?;
        let result = (|| {
            storage::exec(
                "INSERT INTO wasmkeep_room_scale(id,data) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET data=excluded.data",
                &[encode_state(s).into()],
            )?;
            storage::exec("DELETE FROM wasmkeep_room_ops", &[])?;
            if let Some(o) = &s.op {
                storage::exec(
                    "INSERT INTO wasmkeep_room_ops(id,kind,step,args) VALUES(?1,?2,?3,?4)",
                    &[
                        o.id.clone().into(),
                        format!("{:?}", o.kind).into(),
                        (o.step as i64).into(),
                        encode_op(o).into(),
                    ],
                )?;
            }
            storage::exec_durable("COMMIT", &[])?;
            Ok::<(), crate::SqlError>(())
        })();
        if result.is_err() {
            let _ = storage::exec("ROLLBACK", &[]);
        }
        result.map_err(|e| e.to_string())
    }
    fn execute(&mut self, c: ScaleCommand) -> Result<(), String> {
        let (op, child, drain, force) = match c {
            ScaleCommand::Create(c) => {
                crate::directory::Client::new(&self.directory)
                    .create(&c.name, &[])
                    .map_err(|e| e.to_string())?;
                return Ok(());
            }
            ScaleCommand::Change(c) => {
                crate::directory::Client::new(&self.directory)
                    .change(&c)
                    .map_err(|e| e.to_string())?;
                return Ok(());
            }
            ScaleCommand::Init(c) => ("init", c, false, false),
            ScaleCommand::Range(c) => ("range", c, false, false),
            ScaleCommand::Drain(c) => ("drain", c, true, false),
            ScaleCommand::End(c, force) => ("end", c, true, force),
        };
        let body = if op == "end" {
            if force { "force".into() } else { String::new() }
        } else {
            self.config(&child, drain)
        };
        let r = crate::wire::call(
            &child.name,
            "POST",
            format!("/wasmkeep-edge/v1/{op}"),
            vec![],
            body.into_bytes(),
        )?;
        if r.status != 200 {
            return Err(format!("scale {op} answered {}", r.status));
        }
        Ok(())
    }
}
impl Scaling<ScaleHost> {
    /// Called only by the alarm. Poke itself performs no calls.
    pub fn run<S: Store>(
        &mut self,
        round: &mut Round,
        core: &mut Core<S>,
        now: u64,
    ) -> Result<(), String> {
        self.io.init()?;
        let e = core.wake()?;
        self.io.head = Position {
            e: e.e,
            n: core.messages()?.last().map_or(e.base, |m| m.position.n),
        };
        let mut s = self.state()?;
        let mut operations = 0;
        if s.children.is_empty() {
            self.finish(&mut s, now)?;
            operations += 1;
        }
        round.sync(&s.children);
        round.run(core, now)?;
        for c in &mut s.children {
            c.sockets = round.count(&c.name);
        }
        if s.op.is_some() {
            self.finish(&mut s, now)?;
            operations += 1;
        }
        while operations < 4
            && scale_decision(&s.children, self.capacity, now, s.split, s.merge).is_some()
        {
            self.finish(&mut s, now)?;
            operations += 1;
        }
        Ok(())
    }
    fn finish(&mut self, s: &mut ScaleState, now: u64) -> Result<(), String> {
        crate::alarms::set_alarm(now.saturating_add(2000));
        self.begin(s, now)?;
        while s.op.is_some() {
            self.step(s, now)?;
        }
        crate::alarms::set_alarm(now.saturating_add(20));
        Ok(())
    }
}
