# Dogfood log

What building real applications on WasmKeep taught us: every friction point,
with what we did about it. Newest first within each app.

## Watch (uptime monitor), [`examples/watch`](../examples/watch)

One `monitor` actor per URL, checking on its alarm; one `hub` actor that
creates monitors, records alerts durably and delivers them to a webhook with
retries, and serves a live dashboard. About 600 lines of Rust in two
components.

### What worked well

- **The actor model fits.** Each monitor is a few dozen lines of state and
  one alarm. Scheduling, persistence and isolation came for free; nothing
  needed a lock, a queue or a cron daemon.
- **Alarms as the schedule.** "Check, record, set the next alarm" is the
  whole loop, and it survives hibernation, restarts and (since ADR 0009)
  the death of the node.
- **`exec-durable` where it matters.** The hub records alerts durably, so an
  acknowledged state change survives a crash, while ordinary bookkeeping
  stays fast.
- **Calls with a trusted caller header.** The hub accepts alerts only from
  monitors (`x-wasmkeep-caller`), with no shared secret to manage.
- **WebSockets from the actor.** The dashboard streams each monitor's checks
  live with five lines of guest code.

### Friction, and what we did

| # | Friction | Status |
|---|---|---|
| 1 | **No outbound HTTP helper.** Every app re-implemented the `wasi:http` client plumbing (about 70 lines). | Fixed: `wasmkeep_sdk::http::fetch` (method, URL, headers, body, one timeout, body limit), sending a known `Content-Length`. |
| 2 | **Actors can reach any address over outbound HTTP**, including `localhost`, private networks and cloud metadata (`169.254.169.254`): server-side request forgery for any tenant's code, and a way to reach other nodes' internal ports. A monitor that checks arbitrary URLs makes this obvious. (The restriction had been planned but not yet built.) | Fixed: the host resolves names itself, refuses every non-public address (IPv4 and IPv6 special ranges, IPv4 inside IPv6), connects to the checked address (no DNS rebinding), and verifies TLS against the host name. `--allow-outbound CIDR` opens specific ranges. It also refuses the ports browsers block (mail on 25, 465 and 587, FTP, SSH, IRC and so on, 83 in all) whatever the address; `--allow-outbound-port PORT` opens one. |
| 3 | **An actor does not know its own name or class.** Requests arrive with `/actor/<class>/<name>` removed and alarms carry nothing. The hub hard-codes its own path for the dashboard; each monitor is told its id in its configuration. | Fixed: `actors.self-address` in the WIT, in all five SDKs. The hub builds its links from it, monitors take their id from it, and the hub takes an alert's id from the caller header, so a monitor can only report on itself. |
| 4 | **No configuration or secrets.** The webhook URL is stored through a request; an API key would have to be too, or be compiled in. | Fixed: the `env` interface, in all five SDKs. Values come from files on the node (`--env CLASS=FILE`), per class, never in the component or the bucket. The hub takes `ADMIN_TOKEN` and `WEBHOOK_URL` from it. |
| 5 | **No way to list actors.** By design (actors are islands), so the hub keeps a registry; but `GET /monitors` makes one call per monitor, in sequence: fine for tens, slow for thousands. | Pattern documented here; batch or parallel calls are a possible SDK addition. |
| 6 | **No authentication story.** Anyone who can reach the hub can add monitors or change the webhook. Watch v1 leaves this to a reverse proxy. | Partly fixed: the pattern is documented (deploy guide), and the hub follows it: with `ADMIN_TOKEN` set, changes and the webhook URL need `Authorization: Bearer`, compared without stopping at the first differing byte. Helpers in the SDKs would save each app those 20 lines. |
| 7 | **Testing an app means writing an HTTP client and a fake server.** The end-to-end test (`crates/wasmkeep-server/tests/watch.rs`) needed both, and a bug in the fake server (it ignored chunked bodies) looked like a delivery bug at first. | Open: a test harness crate for apps. |
| 8 | **Authorization on the callee is easy to forget.** The monitor accepted `PUT /config` and `DELETE` from anyone, so the hub's checks could be skipped by calling `/actor/monitor/<id>/config` directly: point a monitor at any URL, or delete it. Found while adding the hub's token. | Fixed: monitors take changes only from a `hub` caller (`x-wasmkeep-caller`). A class-level rule ("only reachable from these classes") in the host would make this a declaration instead of code in every actor. |
| 9 | **Any request creates an actor.** `GET /actor/monitor/<anything>` creates a database for that name. Fine for rooms and counters; for Watch, anyone can fill the disk with empty monitors. | Fixed: `--access CLASS=existing` ([ADR 0010](adr/0010-who-may-create-an-actor.md)): an unknown name answers 404 and leaves nothing on disk or in the bucket; `internal` refuses outside requests. Watch serves monitors as `existing`. The hub runs as `existing:main`. |
| 10 | **"Up" is what the site answers the checker, not a browser.** A check is ok for any status below 400, and a site can answer a client it does not know with an error (a 404, say) although it is up: the monitor reports it down. | Partly there: a check names itself, `User-Agent: wasmkeep-watch/<version>`, which a test now reads on the wire, so a site's operator can see it and allow it. Open: a monitor cannot set its own request headers or the statuses it accepts. |

### Alert delivery semantics

The hub durably records each monitor's state change before sending a webhook.
Repeated reports of its last recorded state are skipped, even if an alarm retry
uses a new timestamp. A later up/down transition is a new alert, and each
monitor is deduplicated separately. Existing alert history supplies the last
state, so existing databases need no migration.

Webhook delivery remains at least once: a crash after the receiver accepts a
webhook but before the hub durably records delivery can send it again. A lost
HTTP response has the same ambiguity. This is not an exactly-once guarantee;
a receiver requiring one needs its own idempotency handling. Failed deliveries
remain pending and retry normally.
