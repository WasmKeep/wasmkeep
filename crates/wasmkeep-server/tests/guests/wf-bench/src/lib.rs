//! A workflow for measuring replay cost (`benches/workflow.rs`): actor
//! `wf/n-<N>` records N steps, each returning its index, then parks on a
//! sleep that never ends. `POST /bench/replay` runs the runner once, as its
//! alarm would: N recorded steps replayed, the park found, the alarm set.
//! Everything else is the workflow protocol.

use wasmkeep_sdk::http::{self, IncomingRequest, ResponseOutparam};
use wasmkeep_sdk::workflow::{self, Config, Run, Stop};
use wasmkeep_sdk::{Actor, actors};

struct Bench;

/// The park's wake time: never, in practice.
const PARK: u64 = 4_000_000_000_000;

fn config() -> Config {
    Config::new().backoff(10, 100)
}

fn steps() -> u64 {
    actors::self_address()
        .name
        .strip_prefix("n-")
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

fn flow(wf: &mut Run, _input: &[u8]) -> Result<Vec<u8>, Stop> {
    for i in 0..steps() {
        let _: u64 = wf.step("s", || Ok(i))?;
    }
    wf.sleep_until("park", PARK)?;
    Ok(Vec::new())
}

impl Actor for Bench {
    fn on_init() -> Result<(), String> {
        workflow::store::init().map_err(|e| e.to_string())
    }

    fn on_alarm() -> Result<(), String> {
        workflow::store::on_alarm(&config(), flow);
        Ok(())
    }
}

impl http::Guest for Bench {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        if request.path_with_query().as_deref() == Some("/bench/replay") {
            workflow::store::on_alarm(&config(), flow);
            // Parked once every step is recorded: its alarm is the park's.
            let parked = wasmkeep_sdk::alarms::get_alarm() == Some(PARK);
            http::respond(
                out,
                200,
                "text/plain",
                if parked { b"parked" } else { b"busy" },
            );
        } else {
            workflow::store::handle(&config(), request, out);
        }
    }
}

wasmkeep_sdk::export!(Bench);
