//! The queue class of the jobs example, deployed as class `queue`. Each
//! actor is one queue; its settings depend on its name:
//!
//! - `queue/jobs` pushes batches to `app/worker` from its alarm;
//! - any other name (`queue/inbox`) is pull-only: consumers lease and ack.
//!
//! Both dead-letter a message after three attempts. The short backoff
//! (100 ms doubling to 1 s) keeps the example quick to watch; the SDK's
//! defaults are 1 s to 5 min.

use wasmkeep_sdk::http::{self, IncomingRequest, ResponseOutparam};
use wasmkeep_sdk::queue::{self, Config};
use wasmkeep_sdk::{Actor, actors};

struct Queue;

fn config() -> Config {
    let base = Config::new()
        .max_attempts(3)
        .retry_backoff(100, 1_000)
        .visibility_ms(5_000);
    match actors::self_address().name.as_str() {
        "jobs" => base.consumer("app/worker").batch(10),
        _ => base,
    }
}

impl Actor for Queue {
    fn on_init() -> Result<(), String> {
        queue::store::init().map_err(|e| e.to_string())
    }

    fn on_alarm() -> Result<(), String> {
        queue::store::on_alarm(&config()).map_err(|e| e.to_string())
    }
}

impl http::Guest for Queue {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        queue::store::handle(&config(), request, out);
    }
}

wasmkeep_sdk::export!(Queue);
