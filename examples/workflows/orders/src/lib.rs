//! The workflow class of the workflows example, deployed as class `orders`:
//! each actor is one order, named by its id, run by the SDK's workflow
//! runner ([ADR 0019]).
//!
//! The input is `key=value` pairs separated by `;`: `item` (what is
//! ordered), `cooloff_ms` (how long to wait after reserving, default
//! 1000) and `approve_within_ms` (how long to wait for the `approved`
//! event, default one day). Every side effect goes through `ledger/main`,
//! which records it with the step's idempotency key.
//!
//! Timings come from the class's configuration (`--env orders=FILE`):
//! `RETENTION_MS` (default 7 days), `BACKOFF_MS` (first retry delay,
//! default 1 s; it doubles up to 30 times that), `ATTEMPTS` (default 5) and
//! `RUN_BUDGET_MS` (time a run spends on new steps before continuing in a
//! new event, default 20 s).
//!
//! [ADR 0019]: https://github.com/wasmkeep/wasmkeep/blob/main/docs/adr/0019-workflows-as-an-sdk-pattern.md

use wasmkeep_sdk::http::{self, IncomingRequest, ResponseOutparam};
use wasmkeep_sdk::workflow::{self, Config, Run, Stop};
use wasmkeep_sdk::{Actor, actors, env};

struct Orders;

fn number(name: &str, default: u64) -> u64 {
    env::get(name)
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn config() -> Config {
    let backoff = number("BACKOFF_MS", 1_000);
    Config::new()
        .retention_ms(number("RETENTION_MS", 7 * 86_400_000))
        .backoff(backoff, backoff.saturating_mul(30))
        .max_attempts(u32::try_from(number("ATTEMPTS", 5)).unwrap_or(5))
        .run_budget_ms(number("RUN_BUDGET_MS", 20_000))
}

fn field<'a>(input: &'a str, name: &str) -> Option<&'a str> {
    input
        .split(';')
        .find_map(|pair| pair.trim().strip_prefix(name)?.strip_prefix('='))
}

/// Records `what` at the ledger, with the step's idempotency key.
fn ledger(what: &str, key: &str) -> Result<String, String> {
    let response = actors::call(
        "ledger/main",
        &actors::Request {
            method: "POST".into(),
            path: format!("/record?what={what}&key={key}"),
            headers: Vec::new(),
            body: Vec::new(),
        },
    )
    .map_err(|e| format!("{e:?}"))?;
    let body = String::from_utf8_lossy(&response.body).trim().to_owned();
    // The ledger's way of making a step trap (see its `/fail`).
    assert!(response.status != 599, "{body}");
    if response.status == 200 {
        Ok(body)
    } else {
        Err(format!("ledger answered {}: {body}", response.status))
    }
}

/// The order: reserve, cool off, wait for approval, charge, notify.
fn order(wf: &mut Run, input: &[u8]) -> Result<Vec<u8>, Stop> {
    let input = String::from_utf8_lossy(input).into_owned();
    let item = field(&input, "item").unwrap_or("something").to_owned();
    let cooloff = field(&input, "cooloff_ms")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000);
    let approve_within = field(&input, "approve_within_ms")
        .and_then(|v| v.parse().ok())
        .unwrap_or(86_400_000);

    // The ledger's answer (`reserve:<item>#n`) is the step's recorded result.
    let reservation: String =
        wf.step_keyed("reserve", |key| ledger(&format!("reserve:{item}"), key))?;
    wf.sleep("cool-off", cooloff)?;
    if wf.wait_for_event("approved", approve_within)?.is_none() {
        let _: String = wf.step_keyed("release", |key| ledger(&format!("release:{item}"), key))?;
        return Err(Stop::fail(format!(
            "not approved in time: {reservation} was released"
        )));
    }
    let _: String = wf.step_keyed("charge", |key| ledger(&format!("charge:{item}"), key))?;
    // The same step name in a loop: each call is its own position.
    for who in ["buyer", "warehouse"] {
        let _: String = wf.step_keyed("notify", |key| ledger(&format!("notify:{who}"), key))?;
    }
    Ok(format!("shipped {item}").into_bytes())
}

impl Actor for Orders {
    fn on_init() -> Result<(), String> {
        workflow::store::init().map_err(|e| e.to_string())
    }

    fn on_alarm() -> Result<(), String> {
        workflow::store::on_alarm(&config(), order);
        Ok(())
    }
}

impl http::Guest for Orders {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        workflow::store::handle(&config(), request, out);
    }
}

wasmkeep_sdk::export!(Orders);
