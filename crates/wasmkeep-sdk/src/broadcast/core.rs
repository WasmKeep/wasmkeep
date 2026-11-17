use super::*;
use crate::{Value, storage};
/// Storage seam used by the host adapter and crash tests. Durable execution
/// must not return success until uploaded. An alarm is an absolute millisecond.
pub trait Store {
    fn exec(&mut self, sql: &str, params: &[Value], durable: bool) -> Result<(), String>;
    fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>, String>;
    fn alarm(&mut self, at: u64) -> Result<(), String>;
    fn flush(&mut self) -> Result<(), String>;
}
#[derive(Debug)]
pub struct Host;
impl Store for Host {
    fn exec(&mut self, sql: &str, params: &[Value], durable: bool) -> Result<(), String> {
        if durable {
            storage::exec_durable(sql, params)
        } else {
            storage::exec(sql, params)
        }
        .map(|_| ())
        .map_err(|e| e.to_string())
    }
    fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        storage::query(sql, &[])
            .map(|r| r.values)
            .map_err(|e| e.to_string())
    }
    fn alarm(&mut self, at: u64) -> Result<(), String> {
        crate::alarms::set_alarm(at);
        Ok(())
    }
    fn flush(&mut self) -> Result<(), String> {
        storage::flush().map_err(|e| e.to_string())
    }
}
/// R2 sequencer. `before_push` is the confirmation barrier for R3's round;
/// publishing never calls a child. Join/poke and scale operations belong to R4.
#[derive(Debug)]
pub struct Core<S: Store = Host> {
    db: S,
    epoch: Option<Epoch>,
    pending: bool,
    pub(super) confirm: bool,
}
impl<S: Store> Core<S> {
    pub fn new(db: S, confirm: bool) -> Self {
        Self {
            db,
            epoch: None,
            pending: false,
            confirm,
        }
    }
    pub fn init(&mut self) -> Result<(), String> {
        self.db.exec("CREATE TABLE IF NOT EXISTS wasmkeep_room_history (n INTEGER PRIMARY KEY,e INTEGER NOT NULL,at INTEGER NOT NULL,id TEXT UNIQUE,body TEXT NOT NULL)", &[], false)?;
        self.db.exec("CREATE TABLE IF NOT EXISTS wasmkeep_room_epochs (e INTEGER PRIMARY KEY,base INTEGER NOT NULL)", &[], false)
    }
    fn messages(&self) -> Result<Vec<Message>, String> {
        self.db
            .query("SELECT n,e,at,id,body FROM wasmkeep_room_history ORDER BY n")?
            .into_iter()
            .map(|r| {
                Ok(Message {
                    position: Position {
                        n: integer(&r[0])?,
                        e: integer(&r[1])?,
                    },
                    at: integer(&r[2])?,
                    id: r[3].as_str().map(str::to_owned),
                    body: r[4].as_str().ok_or("invalid room row")?.into(),
                })
            })
            .collect()
    }
    fn epochs(&self) -> Result<Vec<Epoch>, String> {
        self.db
            .query("SELECT e,base FROM wasmkeep_room_epochs ORDER BY e")?
            .iter()
            .map(|r| {
                Ok(Epoch {
                    e: integer(&r[0])?,
                    base: integer(&r[1])?,
                })
            })
            .collect()
    }
    fn wake(&mut self) -> Result<Epoch, String> {
        if let Some(e) = self.epoch {
            return Ok(e);
        }
        let epochs = self.epochs()?;
        let e = epochs
            .last()
            .map_or(0, |e| e.e)
            .checked_add(1)
            .filter(|e| *e <= i64::MAX as u64)
            .ok_or("epoch exhausted")?;
        let base = self.messages()?.last().map_or(0, |m| m.position.n);
        self.db.exec("BEGIN", &[], false)?;
        let result = (|| {
            self.db.exec(
                "INSERT INTO wasmkeep_room_epochs(e,base) VALUES(?1,?2)",
                &[int(e)?, int(base)?],
                false,
            )?;
            self.db.exec(
                "DELETE FROM wasmkeep_room_epochs WHERE e <= ?1",
                &[int(e.saturating_sub(64))?],
                false,
            )?;
            self.db.exec("COMMIT", &[], true)
        })();
        if result.is_err() {
            let _ = self.db.exec("ROLLBACK", &[], false);
        }
        result?;
        let epoch = Epoch { e, base };
        self.epoch = Some(epoch);
        Ok(epoch)
    }
    pub fn publish(&mut self, input: &[Publish], now: u64) -> Result<Vec<Position>, String> {
        if input.is_empty()
            || input.len() > 64
            || input
                .iter()
                .any(|m| m.body.len() > 65536 || m.id.as_ref().is_some_and(|i| i.len() > 64))
            || input.iter().map(|m| m.body.len()).sum::<usize>() > 262144
        {
            return Err("invalid publish".into());
        }
        let at = int(now)?;
        let epoch = self.wake()?;
        let mut rows = self.messages()?;
        let mut n = rows.last().map_or(epoch.base, |m| m.position.n);
        let mut positions = Vec::new();
        self.db.exec("BEGIN", &[], false)?;
        let result = (|| {
            for m in input {
                if let Some(old) = rows.iter().find(|r| m.id.is_some() && r.id == m.id) {
                    positions.push(old.position);
                    continue;
                }
                n = n
                    .checked_add(1)
                    .filter(|n| *n <= i64::MAX as u64)
                    .ok_or("number exhausted")?;
                let p = Position { e: epoch.e, n };
                self.db.exec(
                    "INSERT INTO wasmkeep_room_history(n,e,at,id,body) VALUES(?1,?2,?3,?4,?5)",
                    &[
                        int(n)?,
                        int(epoch.e)?,
                        at.clone(),
                        m.id.clone().into(),
                        m.body.clone().into(),
                    ],
                    false,
                )?;
                rows.push(Message {
                    position: p,
                    at: now,
                    id: m.id.clone(),
                    body: m.body.clone(),
                });
                positions.push(p);
            }
            let mut bytes: usize = rows.iter().map(|r| r.body.len()).sum();
            let mut drop = 0;
            while rows.len() - drop > 2000 || bytes > 8 * 1024 * 1024 {
                bytes -= rows[drop].body.len();
                drop += 1;
            }
            if drop > 0 {
                self.db.exec(
                    "DELETE FROM wasmkeep_room_history WHERE n <= ?1",
                    &[int(rows[drop - 1].position.n)?],
                    false,
                )?;
            }
            self.db.exec("COMMIT", &[], false)
        })();
        if result.is_err() {
            let _ = self.db.exec("ROLLBACK", &[], false);
        }
        result?;
        if !self.pending {
            self.db.alarm(now.saturating_add(20))?;
            self.pending = true;
        }
        Ok(positions)
    }
    /// Flush before every push, even a header-only round. False means no push
    /// is permitted; the retry alarm has been set for 50ms later.
    pub fn before_push(&mut self, now: u64) -> Result<bool, String> {
        self.wake()?;
        if self.confirm
            && let Err(e) = self.db.flush()
        {
            if e.contains("not durable: still uploading") {
                self.db.alarm(now.saturating_add(50))?;
                self.pending = true;
                return Ok(false);
            }
            return Err(e);
        }
        self.pending = false;
        Ok(true)
    }
    pub fn history(
        &mut self,
        after: Option<Position>,
        limit: usize,
        last: Option<usize>,
    ) -> Result<History, String> {
        if limit == 0
            || limit > 2000
            || last.is_some_and(|n| n > 2000)
            || (after.is_some() && last.is_some())
        {
            return Err("invalid history request".into());
        }
        let epoch = self.wake()?;
        let rows = self.messages()?;
        let epochs = self.epochs()?;
        let head = Position {
            e: epoch.e,
            n: rows.last().map_or(epoch.base, |m| m.position.n),
        };
        let mut h = History {
            head,
            lost: None,
            reset: None,
            messages: vec![],
            more: false,
        };
        let mut start = rows.len();
        if let Some(p) = after {
            if p.e > epoch.e || p.e == 0 || (p.e == epoch.e && p.n > head.n) {
                return Err("invalid position".into());
            }
            let oldest = epochs.first().ok_or("missing epoch")?.e;
            let boundary = epochs
                .iter()
                .filter(|e| e.e > p.e)
                .min_by_key(|e| e.base)
                .copied();
            let mut n = p.n;
            if p.e >= oldest
                && let Some(b) = boundary
                && p.n > b.base
            {
                h.lost = Some(b);
                n = b.base;
            }
            if p.e < oldest
                || rows
                    .first()
                    .is_some_and(|m| n.saturating_add(1) < m.position.n)
            {
                h.reset = Some(rows.first().map_or(head, |m| m.position));
                start = 0;
            } else {
                start = rows
                    .iter()
                    .position(|m| m.position.n > n)
                    .unwrap_or(rows.len());
            }
        } else if let Some(last) = last {
            start = rows.len().saturating_sub(last);
        }
        // Fixed v1 response fields, including optional lost/reset positions.
        let mut bytes =
            48 + usize::from(h.lost.is_some()) * 16 + usize::from(h.reset.is_some()) * 16;
        for row in &rows[start..] {
            let encoded = 36 + row.body.len() + row.id.as_ref().map_or(0, |id| 4 + id.len());
            if h.messages.len() == limit || bytes + encoded > 262144 {
                h.more = true;
                break;
            }
            bytes += encoded;
            h.messages.push(row.clone());
        }
        Ok(h)
    }
}
fn integer(v: &Value) -> Result<u64, String> {
    v.as_i64()
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| "invalid room integer".into())
}
fn int(n: u64) -> Result<Value, String> {
    i64::try_from(n)
        .map(Value::Integer)
        .map_err(|_| "integer overflow".into())
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
