use super::*;
use crate::{Value, storage};
/// Storage seam used by the host adapter and crash tests. Durable execution
/// must not return success until uploaded. An alarm is an absolute millisecond.
pub trait Store {
    fn exec(&mut self, sql: &str, params: &[Value], durable: bool) -> Result<(), String>;
    fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, String>;
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
    fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, String> {
        storage::query(sql, params)
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
    /// The whole window, oldest first: tests only, never a per-event path.
    #[cfg(test)]
    pub(super) fn messages(&self) -> Result<Vec<Message>, String> {
        self.db
            .query(
                "SELECT n,e,at,id,body FROM wasmkeep_room_history ORDER BY n",
                &[],
            )?
            .iter()
            .map(|r| row(r))
            .collect()
    }
    fn end(&self, newest: bool) -> Result<Option<Position>, String> {
        let order = if newest { "DESC" } else { "ASC" };
        self.db
            .query(
                &format!("SELECT n,e FROM wasmkeep_room_history ORDER BY n {order} LIMIT 1"),
                &[],
            )?
            .first()
            .map(|r| {
                Ok(Position {
                    n: integer(&r[0])?,
                    e: integer(&r[1])?,
                })
            })
            .transpose()
    }
    /// The highest number in the window, if any.
    pub(super) fn last(&self) -> Result<Option<u64>, String> {
        Ok(self.end(true)?.map(|p| p.n))
    }
    /// Up to `count` rows from number `from` on, at most one page past
    /// `bytes` of bodies: what one push needs, never the whole window.
    pub(super) fn rows(
        &self,
        mut from: u64,
        count: usize,
        bytes: usize,
    ) -> Result<Vec<Message>, String> {
        let mut out = Vec::new();
        let mut seen = 0;
        while out.len() < count && seen <= bytes {
            let page = PAGE.min(count - out.len());
            let rows = self.db.query(
                "SELECT n,e,at,id,body FROM wasmkeep_room_history WHERE n >= ?1 ORDER BY n LIMIT ?2",
                &[int(from)?, int(page as u64)?],
            )?;
            for r in &rows {
                let m = row(r)?;
                seen += m.body.len();
                from = m.position.n.saturating_add(1);
                out.push(m);
            }
            if rows.len() < page {
                break;
            }
        }
        Ok(out)
    }
    pub(super) fn epochs(&self) -> Result<Vec<Epoch>, String> {
        self.db
            .query("SELECT e,base FROM wasmkeep_room_epochs ORDER BY e", &[])?
            .iter()
            .map(|r| {
                Ok(Epoch {
                    e: integer(&r[0])?,
                    base: integer(&r[1])?,
                })
            })
            .collect()
    }
    pub(super) fn wake(&mut self) -> Result<Epoch, String> {
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
        let base = self.last()?.unwrap_or(0);
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
        let mut n = self.last()?.unwrap_or(epoch.base);
        let mut positions = Vec::new();
        self.db.exec("BEGIN", &[], false)?;
        let result = (|| {
            for m in input {
                if let Some(id) = &m.id {
                    let old = self.db.query(
                        "SELECT n,e FROM wasmkeep_room_history WHERE id=?1",
                        &[id.clone().into()],
                    )?;
                    if let Some(r) = old.first() {
                        positions.push(Position {
                            n: integer(&r[0])?,
                            e: integer(&r[1])?,
                        });
                        continue;
                    }
                }
                n = n
                    .checked_add(1)
                    .filter(|n| *n <= i64::MAX as u64)
                    .ok_or("number exhausted")?;
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
                positions.push(Position { e: epoch.e, n });
            }
            self.db.exec(TRIM, &[], false)?;
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
        let epochs = self.epochs()?;
        let first = self.end(false)?;
        let head = Position {
            e: epoch.e,
            n: self.last()?.unwrap_or(epoch.base),
        };
        let mut h = History {
            head,
            lost: None,
            reset: None,
            messages: vec![],
            more: false,
        };
        let mut from = None;
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
            let kept = || -> Result<bool, String> {
                Ok(!self
                    .db
                    .query(
                        "SELECT 1 FROM wasmkeep_room_history WHERE n=?1 AND e=?2",
                        &[int(p.n)?, int(p.e)?],
                    )?
                    .is_empty())
            };
            if (p.e < oldest && !kept()?) || first.is_some_and(|f| n.saturating_add(1) < f.n) {
                h.reset = Some(first.unwrap_or(head));
                from = first.map(|f| f.n);
            } else {
                from = Some(n.saturating_add(1));
            }
        } else if let Some(last) = last.filter(|l| *l > 0) {
            let r = self.db.query(
                "SELECT n FROM wasmkeep_room_history ORDER BY n DESC LIMIT 1 OFFSET ?1",
                &[int(last as u64 - 1)?],
            )?;
            from = match r.first() {
                Some(r) => Some(integer(&r[0])?),
                None => first.map(|f| f.n),
            };
        }
        // Fixed v1 response fields, including optional lost/reset positions.
        let mut bytes =
            48 + usize::from(h.lost.is_some()) * 16 + usize::from(h.reset.is_some()) * 16;
        while let Some(start) = from {
            let rows = self.db.query(
                "SELECT n,e,at,id,body FROM wasmkeep_room_history WHERE n >= ?1 ORDER BY n LIMIT ?2",
                &[int(start)?, int(PAGE as u64)?],
            )?;
            from = None;
            for r in &rows {
                let row = row(r)?;
                let encoded = 36 + row.body.len() + row.id.as_ref().map_or(0, |id| 4 + id.len());
                if h.messages.len() == limit || bytes + encoded > 262144 {
                    h.more = true;
                    break;
                }
                bytes += encoded;
                from = Some(row.position.n.saturating_add(1));
                h.messages.push(row);
            }
            if rows.len() < PAGE || h.more {
                break;
            }
        }
        Ok(h)
    }
}
/// Rows a bounded read takes at a time.
const PAGE: usize = 32;
/// The window: the newest 2,000 rows within 8 MiB of bodies; every older
/// row goes, in one statement SQLite runs.
const TRIM: &str = "DELETE FROM wasmkeep_room_history WHERE n <= (SELECT n FROM (SELECT n, row_number() OVER w AS k, sum(length(CAST(body AS BLOB))) OVER w AS tail FROM wasmkeep_room_history WINDOW w AS (ORDER BY n DESC ROWS UNBOUNDED PRECEDING)) WHERE k > 2000 OR tail > 8388608 ORDER BY n DESC LIMIT 1)";
fn row(r: &[Value]) -> Result<Message, String> {
    Ok(Message {
        position: Position {
            n: integer(&r[0])?,
            e: integer(&r[1])?,
        },
        at: integer(&r[2])?,
        id: r[3].as_str().map(str::to_owned),
        body: r[4].as_str().ok_or("invalid room row")?.into(),
    })
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
