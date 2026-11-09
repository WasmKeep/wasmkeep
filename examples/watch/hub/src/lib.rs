//! Watch's hub: one actor (`hub/main`) that knows every monitor, sends the
//! alerts, and serves the dashboard.
//!
//! - `GET /`: the dashboard (HTML).
//! - `PUT /monitors/<id>` `{"url", "every_s", "timeout_ms"?}`: creates or
//!   updates monitor `monitor/<id>` and starts it.
//! - `DELETE /monitors/<id>`: stops and deletes it.
//! - `GET /monitors`: every monitor's status (one call to each).
//! - `PUT /webhook` with a URL as the body: where alerts are POSTed (JSON
//!   with `text` and `content`, so Slack and Discord webhooks take it as is).
//! - `GET /alerts`: the latest alerts and their delivery.
//! - `POST /alert` (from monitors only): records a state change. Delivery
//!   happens in the hub's own alarm event, so a slow webhook never holds a
//!   monitor up; failed deliveries are retried with backoff.
//!
//! Configuration (`wasmkeep serve --env hub=FILE`):
//!
//! - `ADMIN_TOKEN`: when set, every change (`PUT`, `DELETE`) and `GET
//!   /webhook` need `Authorization: Bearer <token>`. The dashboard and the
//!   status reads stay open.
//! - `WEBHOOK_URL`: the webhook, instead of `PUT /webhook`.

use serde::Deserialize;
use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::{Actor, Value, actors, alarms, env, now_ms, params, storage};

const MONITOR_CLASS: &str = "monitor";

struct Hub;

#[derive(Deserialize)]
struct MonitorSpec {
    url: String,
    every_s: u64,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
struct Alert {
    /// Ignored: the id is the calling monitor's name, so a monitor can
    /// only report on itself.
    #[serde(default)]
    id: String,
    url: String,
    state: String,
    at: i64,
    #[serde(default)]
    detail: String,
}

fn call(
    address: &str,
    method: &str,
    path: &str,
    body: Vec<u8>,
) -> Result<actors::Response, String> {
    actors::call(
        address,
        &actors::Request {
            method: method.into(),
            path: path.into(),
            headers: vec![("content-type".into(), "application/json".into())],
            body,
        },
    )
    .map_err(|e| format!("{e:?}"))
}

/// The hub's own path, for the dashboard's URLs: `/actor/<class>/<name>`,
/// the name percent-encoded.
fn self_path() -> String {
    let me = actors::self_address();
    let mut name = String::with_capacity(me.name.len());
    for b in me.name.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            name.push(char::from(b));
        } else {
            name.push_str(&format!("%{b:02X}"));
        }
    }
    format!("/actor/{}/{name}", me.class)
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

fn webhook() -> Option<String> {
    if let Some(url) = env::get("WEBHOOK_URL").filter(|u| !u.is_empty()) {
        return Some(url);
    }
    let rows = storage::query("SELECT v FROM settings WHERE k = 'webhook'", &[]).ok()?;
    rows.get(0, "v").and_then(Value::as_str).map(str::to_owned)
}

fn put_monitor(id: &str, body: &[u8]) -> Result<serde_json::Value, (u16, String)> {
    if !valid_id(id) {
        return Err((400, "ids are [a-z0-9_-], up to 64 bytes\n".into()));
    }
    let spec: MonitorSpec = serde_json::from_slice(body)
        .map_err(|_| (400, "expected {\"url\", \"every_s\"}\n".into()))?;
    if !(spec.url.starts_with("http://") || spec.url.starts_with("https://")) {
        return Err((400, "url must be http:// or https://\n".into()));
    }
    let every_s = spec.every_s.clamp(1, 86_400);
    let timeout_ms = spec.timeout_ms.unwrap_or(5_000).clamp(100, 20_000);
    let config = serde_json::json!({
        "url": spec.url, "every_s": every_s, "timeout_ms": timeout_ms,
    });
    let response = call(
        &format!("{MONITOR_CLASS}/{id}"),
        "PUT",
        "/config",
        serde_json::to_vec(&config).unwrap_or_default(),
    )
    .map_err(|e| (502, format!("configuring the monitor: {e}\n")))?;
    if response.status != 200 {
        return Err((502, format!("the monitor answered {}\n", response.status)));
    }
    storage::exec(
        "INSERT INTO monitors (id, url, every_s) VALUES (?1, ?2, ?3)
         ON CONFLICT (id) DO UPDATE SET url = ?2, every_s = ?3",
        &params![id, spec.url, i64::try_from(every_s).unwrap_or(i64::MAX)],
    )
    .map_err(|e| (500, e.message))?;
    Ok(serde_json::from_slice(&response.body).unwrap_or(serde_json::Value::Null))
}

fn delete_monitor(id: &str) -> Result<(), (u16, String)> {
    call(&format!("{MONITOR_CLASS}/{id}"), "DELETE", "/", Vec::new())
        .map_err(|e| (502, format!("deleting the monitor: {e}\n")))?;
    storage::exec("DELETE FROM monitors WHERE id = ?1", &params![id])
        .map_err(|e| (500, e.message))?;
    Ok(())
}

fn monitors() -> serde_json::Value {
    let ids: Vec<String> = storage::query("SELECT id FROM monitors ORDER BY id", &[])
        .map(|rows| {
            (0..rows.len())
                .filter_map(|i| rows.get(i, "id").and_then(Value::as_str).map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    // One call per monitor: fine for tens of monitors, slow for thousands
    // (see docs/dogfood.md).
    let list: Vec<serde_json::Value> = ids
        .iter()
        .map(
            |id| match call(&format!("{MONITOR_CLASS}/{id}"), "GET", "/", Vec::new()) {
                Ok(r) if r.status == 200 => {
                    let mut v: serde_json::Value =
                        serde_json::from_slice(&r.body).unwrap_or(serde_json::Value::Null);
                    if let Some(o) = v.as_object_mut() {
                        o.remove("recent");
                    }
                    v
                }
                Ok(r) => serde_json::json!({"id": id, "error": format!("HTTP {}", r.status)}),
                Err(e) => serde_json::json!({"id": id, "error": e}),
            },
        )
        .collect();
    serde_json::Value::Array(list)
}

fn alerts() -> serde_json::Value {
    let rows = storage::query(
        "SELECT seq, at, id, url, state, detail, delivered, attempts, last_error
         FROM alerts ORDER BY seq DESC LIMIT 50",
        &[],
    );
    let Ok(rows) = rows else {
        return serde_json::Value::Array(Vec::new());
    };
    let s = |i, c| rows.get(i, c).and_then(Value::as_str).map(str::to_owned);
    let n = |i, c| rows.get(i, c).and_then(Value::as_i64);
    serde_json::Value::Array(
        (0..rows.len())
            .map(|i| {
                serde_json::json!({
                    "seq": n(i, "seq"), "at": n(i, "at"), "id": s(i, "id"), "url": s(i, "url"),
                    "state": s(i, "state"), "detail": s(i, "detail"),
                    "delivered": n(i, "delivered") == Some(1),
                    "attempts": n(i, "attempts"), "last_error": s(i, "last_error"),
                })
            })
            .collect(),
    )
}

/// Records an alert durably (a state change must not be lost), then
/// schedules delivery in the hub's own event. A retried monitor alarm can
/// report the same state with a new timestamp: compare the last recorded state
/// for this monitor, not the timestamp. Keep this durable record before sending
/// the webhook; a duplicate report must not enqueue another delivery.
fn record_alert(alert: &Alert) -> Result<(), String> {
    storage::exec_durable(
        "INSERT INTO alerts (at, id, url, state, detail, delivered, attempts)
         SELECT ?1, ?2, ?3, ?4, ?5, 0, 0
         WHERE ?4 IS NOT (SELECT state FROM alerts WHERE id = ?2 ORDER BY seq DESC LIMIT 1)",
        &params![
            alert.at,
            alert.id.as_str(),
            alert.url.as_str(),
            alert.state.as_str(),
            alert.detail.as_str()
        ],
    )
    .map_err(|e| e.message)?;
    alarms::set_alarm(now_ms());
    Ok(())
}

/// Sends every undelivered alert, oldest first; schedules a retry for what
/// failed (backoff by attempts, 5 s doubling to 10 min).
fn deliver() -> Result<(), String> {
    let pending = storage::query(
        "SELECT seq, at, id, url, state, detail, attempts FROM alerts WHERE delivered = 0 ORDER BY seq LIMIT 20",
        &[],
    )
    .map_err(|e| e.message)?;
    if pending.is_empty() {
        return Ok(());
    }
    let Some(target) = webhook() else {
        // Nowhere to send: keep them, delivered once a webhook is set.
        return Ok(());
    };
    let mut retry_in: Option<u64> = None;
    for i in 0..pending.len() {
        let seq = pending.get(i, "seq").and_then(Value::as_i64).unwrap_or(0);
        let id = pending.get(i, "id").and_then(Value::as_str).unwrap_or("");
        let url = pending.get(i, "url").and_then(Value::as_str).unwrap_or("");
        let state = pending
            .get(i, "state")
            .and_then(Value::as_str)
            .unwrap_or("");
        let detail = pending
            .get(i, "detail")
            .and_then(Value::as_str)
            .unwrap_or("");
        let attempts = pending
            .get(i, "attempts")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let text = if state == "down" {
            format!("🔴 {id} is DOWN ({url}): {detail}")
        } else {
            format!("🟢 {id} is up again ({url})")
        };
        let body = serde_json::json!({
            "text": text, "content": text,
            "monitor": id, "url": url, "state": state, "detail": detail,
            "at": pending.get(i, "at").and_then(Value::as_i64),
        });
        let sent = http::fetch(
            "POST",
            &target,
            &[("content-type", "application/json")],
            &serde_json::to_vec(&body).unwrap_or_default(),
            std::time::Duration::from_secs(10),
            4096,
        );
        match sent {
            Ok(r) if r.status < 300 => {
                storage::exec_durable("UPDATE alerts SET delivered = 1, attempts = attempts + 1, last_error = NULL WHERE seq = ?1", &params![seq])
                    .map_err(|e| e.message)?;
            }
            other => {
                let error = match other {
                    Ok(r) => format!("HTTP {}", r.status),
                    Err(e) => e,
                };
                storage::exec(
                    "UPDATE alerts SET attempts = attempts + 1, last_error = ?2 WHERE seq = ?1",
                    &params![seq, error],
                )
                .map_err(|e| e.message)?;
                let wait = (5_000u64 << attempts.clamp(0, 7)).min(600_000);
                retry_in = Some(retry_in.map_or(wait, |w| w.min(wait)));
            }
        }
    }
    let more = storage::query("SELECT 1 FROM alerts WHERE delivered = 0 LIMIT 1", &[])
        .map(|r| !r.is_empty())
        .unwrap_or(false);
    if let Some(wait) = retry_in {
        alarms::set_alarm(now_ms() + wait);
    } else if more {
        alarms::set_alarm(now_ms()); // the next batch
    }
    Ok(())
}

fn respond_json(out: ResponseOutparam, status: u16, value: &serde_json::Value) {
    http::respond(
        out,
        status,
        "application/json",
        &serde_json::to_vec(value).unwrap_or_default(),
    );
}

impl Actor for Hub {
    fn on_init() -> Result<(), String> {
        for sql in [
            "CREATE TABLE IF NOT EXISTS monitors (id TEXT PRIMARY KEY, url TEXT NOT NULL, every_s INTEGER NOT NULL)",
            "CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS alerts (seq INTEGER PRIMARY KEY, at INTEGER NOT NULL, id TEXT NOT NULL, url TEXT NOT NULL, state TEXT NOT NULL, detail TEXT NOT NULL, delivered INTEGER NOT NULL, attempts INTEGER NOT NULL, last_error TEXT)",
        ] {
            storage::exec(sql, &[]).map_err(|e| e.message)?;
        }
        Ok(())
    }

    fn on_alarm() -> Result<(), String> {
        deliver()
    }
}

/// Whether the request may proceed: with `ADMIN_TOKEN` set, it must carry
/// `Authorization: Bearer <token>`.
fn authorized(request: &IncomingRequest) -> bool {
    let Some(token) = env::get("ADMIN_TOKEN").filter(|t| !t.is_empty()) else {
        return true;
    };
    request
        .headers()
        .get("authorization")
        .first()
        .and_then(|v| v.strip_prefix(b"Bearer "))
        .is_some_and(|given| same(given, token.as_bytes()))
}

/// Compares without stopping at the first difference, so the time taken
/// does not tell how much of a guess was right.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl http::Guest for Hub {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let full = request.path_with_query().unwrap_or_default();
        let path = full.split('?').next().unwrap_or("/").to_owned();
        let method = request.method();
        // Monitors' alerts are checked by caller instead (below).
        let guarded = path == "/webhook" || !matches!(method, Method::Get | Method::Head);
        if guarded && path != "/alert" && !authorized(&request) {
            return http::respond(
                out,
                401,
                "text/plain",
                b"needs Authorization: Bearer <ADMIN_TOKEN>\n",
            );
        }
        if let Some(id) = path.strip_prefix("/monitors/") {
            let id = id.to_owned();
            return match method {
                Method::Put => {
                    let body = http::read_body(&request, 16 * 1024).unwrap_or_default();
                    match put_monitor(&id, &body) {
                        Ok(v) => respond_json(out, 200, &v),
                        Err((s, m)) => http::respond(out, s, "text/plain", m.as_bytes()),
                    }
                }
                Method::Delete => match delete_monitor(&id) {
                    Ok(()) => http::respond(out, 200, "text/plain", b"deleted\n"),
                    Err((s, m)) => http::respond(out, s, "text/plain", m.as_bytes()),
                },
                _ => http::respond(out, 405, "text/plain", b"use PUT or DELETE\n"),
            };
        }
        match (method, path.as_str()) {
            (Method::Get, "/") => http::respond(
                out,
                200,
                "text/html; charset=utf-8",
                DASHBOARD.replace("{SELF}", &self_path()).as_bytes(),
            ),
            (Method::Get, "/monitors") => respond_json(out, 200, &monitors()),
            (Method::Get, "/alerts") => respond_json(out, 200, &alerts()),
            (Method::Get, "/webhook") => {
                http::respond(
                    out,
                    200,
                    "text/plain",
                    webhook().unwrap_or_default().as_bytes(),
                );
            }
            (Method::Put, "/webhook") if env::get("WEBHOOK_URL").is_some_and(|u| !u.is_empty()) => {
                http::respond(
                    out,
                    409,
                    "text/plain",
                    b"the webhook is set by the operator (WEBHOOK_URL)\n",
                );
            }
            (Method::Put, "/webhook") => {
                let body = http::read_body(&request, 4096).unwrap_or_default();
                let url = String::from_utf8(body)
                    .unwrap_or_default()
                    .trim()
                    .to_owned();
                if !(url.starts_with("http://") || url.starts_with("https://")) {
                    return http::respond(out, 400, "text/plain", b"send an http(s) URL\n");
                }
                match storage::exec(
                    "INSERT INTO settings (k, v) VALUES ('webhook', ?1) ON CONFLICT (k) DO UPDATE SET v = ?1",
                    &params![url],
                ) {
                    Ok(_) => {
                        alarms::set_alarm(now_ms()); // deliver anything waiting
                        http::respond(out, 200, "text/plain", b"ok\n");
                    }
                    Err(e) => http::respond(out, 500, "text/plain", e.message.as_bytes()),
                }
            }
            (Method::Post, "/alert") => {
                // Only monitors may raise alerts: the host sets this header on
                // calls between actors and strips it from outside requests.
                // "<tenant>/<class>/<name>", set by the host: a monitor
                // reports on itself only. Monitor ids need no decoding.
                let caller = request.headers().get("x-wasmkeep-caller");
                let caller = caller
                    .first()
                    .map(|v| String::from_utf8_lossy(v).into_owned());
                let monitor = caller.as_deref().and_then(|c| {
                    let mut parts = c.splitn(3, '/');
                    let (_, class, name) = (parts.next()?, parts.next()?, parts.next()?);
                    (class == MONITOR_CLASS && valid_id(name)).then(|| name.to_owned())
                });
                let Some(monitor) = monitor else {
                    return http::respond(out, 403, "text/plain", b"monitors only\n");
                };
                let body = http::read_body(&request, 16 * 1024).unwrap_or_default();
                match serde_json::from_slice::<Alert>(&body)
                    .map_err(|e| e.to_string())
                    .and_then(|a| record_alert(&Alert { id: monitor, ..a }))
                {
                    Ok(()) => http::respond(out, 200, "text/plain", b"recorded\n"),
                    Err(e) => http::respond(out, 500, "text/plain", e.as_bytes()),
                }
            }
            _ => http::respond(out, 404, "text/plain", b"no route\n"),
        }
    }
}

const DASHBOARD: &str = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Watch</title>
<style>
:root{--bg:#fff;--fg:#1a1a1a;--muted:#6b6b6b;--line:#e5e5e5;--up:#1a7f37;--down:#cf222e;--unk:#9a6700}
@media (prefers-color-scheme:dark){:root{--bg:#0d1117;--fg:#e6edf3;--muted:#8b949e;--line:#30363d;--up:#3fb950;--down:#f85149;--unk:#d29922}}
body{font:15px/1.5 system-ui,sans-serif;background:var(--bg);color:var(--fg);margin:0;padding:24px;max-width:960px;margin:auto}
h1{font-size:20px;margin:0 0 16px}table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:8px;border-bottom:1px solid var(--line)}
th{color:var(--muted);font-weight:500;font-size:13px}.up{color:var(--up)}.down{color:var(--down)}.unknown{color:var(--unk)}
.bar{display:flex;gap:1px;height:18px;align-items:flex-end}.bar i{width:4px;height:100%;background:var(--up)}.bar i.f{background:var(--down)}
code{font-size:13px}h2{font-size:16px;margin:32px 0 8px}.muted{color:var(--muted)}
</style></head><body>
<h1>Watch</h1>
<table><thead><tr><th>Monitor</th><th>State</th><th>Uptime</th><th>Last check</th><th>Recent</th></tr></thead><tbody id="rows"><tr><td colspan="5" class="muted">Loading…</td></tr></tbody></table>
<h2>Alerts</h2><table><thead><tr><th>When</th><th>Monitor</th><th>State</th><th>Detail</th><th>Delivered</th></tr></thead><tbody id="alerts"></tbody></table>
<script>
const SELF="{SELF}";const sockets={};const bars={};
const esc=s=>String(s??"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const ago=t=>{if(!t)return"–";const s=Math.round((Date.now()-t)/1000);return s<60?s+"s ago":s<3600?Math.round(s/60)+"m ago":Math.round(s/3600)+"h ago"};
function bar(id){return (bars[id]||[]).slice(-40).map(c=>`<i class="${c.ok?"":"f"}" title="${esc(new Date(c.at).toLocaleTimeString())} ${c.ok?c.status:esc(c.error||c.status)} ${c.ms}ms"></i>`).join("")}
function live(id){if(sockets[id])return;const p=location.protocol==="https:"?"wss:":"ws:";
 const ws=new WebSocket(`${p}//${location.host}/actor/monitor/${encodeURIComponent(id)}`);sockets[id]=ws;
 ws.onmessage=e=>{const c=JSON.parse(e.data);(bars[id]=bars[id]||[]).push(c);const el=document.getElementById("bar-"+id);if(el)el.innerHTML=bar(id)};
 ws.onclose=()=>{delete sockets[id];setTimeout(()=>live(id),3000)}}
async function load(){
 const ms=await (await fetch(SELF+"/monitors")).json();
 document.getElementById("rows").innerHTML=ms.length?ms.map(m=>`<tr><td><b>${esc(m.id)}</b><br><code class="muted">${esc(m.url)}</code></td>
 <td class="${esc(m.state)}">${esc(m.state||m.error)}<br><span class="muted">${m.since?ago(m.since):""}</span></td>
 <td>${m.uptime!=null?(m.uptime*100).toFixed(2)+"%":"–"}<br><span class="muted">${m.checks||0} checks</span></td>
 <td>${m.last?(m.last.ok?m.last.status:esc(m.last.error||m.last.status))+" · "+m.last.ms+"ms<br><span class=muted>"+ago(m.last.at)+"</span>":"–"}</td>
 <td><div class="bar" id="bar-${esc(m.id)}">${bar(m.id)}</div></td></tr>`).join(""):`<tr><td colspan="5" class="muted">No monitors yet: PUT ${SELF}/monitors/&lt;id&gt; {"url":"https://…","every_s":30}</td></tr>`;
 ms.forEach(m=>m.id&&live(m.id));
 const al=await (await fetch(SELF+"/alerts")).json();
 document.getElementById("alerts").innerHTML=al.map(a=>`<tr><td>${new Date(a.at).toLocaleString()}</td><td>${esc(a.id)}</td><td class="${esc(a.state)}">${esc(a.state)}</td><td>${esc(a.detail)}</td><td>${a.delivered?"yes":"no ("+a.attempts+" tries"+(a.last_error?": "+esc(a.last_error):"")+")"}</td></tr>`).join("")||`<tr><td colspan="5" class="muted">None yet.</td></tr>`}
load();setInterval(load,10000);
</script></body></html>
"##;

wasmkeep_sdk::export!(Hub);
