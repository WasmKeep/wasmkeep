//! Load generator for one big WebSocket room (`bench/room`, driven by
//! `bench/room.sh`; see docs/benchmarks.md, "One big room").
//!
//! ```text
//! roomload run    --url ws://HOST:PORT/actor/room/NAME --sockets N --senders S
//!                 --rate MSGS_PER_S --duration SECS --out FILE [...]
//! roomload sample --pid PID --metrics http://HOST:PORT/metrics --out FILE [...]
//! roomload merge  --prefix room/n1000/plain/r100 [--sample FILE] FILE...
//! ```
//!
//! `run` opens N sockets to the room, at most `--ramp` new ones a second,
//! spread over the `--source-ip` addresses (one client address reaches one
//! server address from at most the ephemeral port range, about 28,000 by
//! default). It waits for `--start-at` (Unix milliseconds) if given, so
//! several processes, on one machine or several, start sending together,
//! then sends `--rate` messages a second in total through its first S
//! sockets (or by HTTP `POST` with `--http-senders`) for `--duration`
//! seconds, waits `--drain` seconds for the last deliveries and closes.
//!
//! Each message is about `--payload` bytes and carries the process, the
//! sender, its index and its send time (wall clock, nanoseconds). The room
//! numbers it (`seq`). Every socket checks the numbers it receives for
//! gaps, duplicates and order, and the process records, for each `seq`,
//! when the first and the last of its sockets received it. `run` writes
//! all of it to `--out`, a text file of one record per line.
//!
//! `sample` runs on the node's machine: every `--interval-ms` it reads the
//! node's CPU time, resident memory and open descriptors from `/proc/PID`,
//! and the open sockets and refusals from its metrics page.
//!
//! `merge` reads the files of every process (and the node's samples), takes
//! for each `seq` the last receipt over all of them, and prints CSV rows
//! `bench,value,unit` for `bench/room.sh`. Latency across machines is only
//! as good as their clocks agree (NTP: typically well under a millisecond
//! on one network).

// A benchmark harness: a broken setup should stop the run loudly.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;
use std::io::Write as _;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Args, Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

#[derive(Parser)]
#[command(about = "Load generator for one big WebSocket room")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Opens the sockets, sends, records what every socket received.
    Run(RunArgs),
    /// Samples the node's process and metrics page.
    Sample(SampleArgs),
    /// Merges the files of every process into CSV rows.
    Merge(MergeArgs),
}

#[derive(Args)]
struct RunArgs {
    /// The room: `ws://HOST:PORT/actor/room/NAME`.
    #[arg(long)]
    url: String,
    /// Sockets this process opens.
    #[arg(long)]
    sockets: usize,
    /// How many of them send (the first ones).
    #[arg(long, default_value_t = 10)]
    senders: usize,
    /// Messages a second this process sends, over all its senders.
    #[arg(long)]
    rate: f64,
    /// Seconds of sending.
    #[arg(long, default_value_t = 10.0)]
    duration: f64,
    /// Seconds to wait after the last send for the last deliveries.
    #[arg(long, default_value_t = 5.0)]
    drain: f64,
    /// Bytes per message, about (the room adds its `seq`).
    #[arg(long, default_value_t = 200)]
    payload: usize,
    /// New sockets a second at most, during the ramp-up.
    #[arg(long, default_value_t = 2000.0)]
    ramp: f64,
    /// Handshakes in flight at most.
    #[arg(long, default_value_t = 256)]
    connect_concurrency: usize,
    /// Source addresses, used in turn (repeat, or separate with commas).
    #[arg(long, value_delimiter = ',')]
    source_ip: Vec<IpAddr>,
    /// When to start sending, in Unix milliseconds (several processes).
    #[arg(long)]
    start_at: Option<u64>,
    /// This process's number, unique over every process of a run.
    #[arg(long, default_value_t = 0)]
    id: u32,
    /// Processes in the run, to size the tables (`seq` up to
    /// rate x procs x duration x 2).
    #[arg(long, default_value_t = 1)]
    procs: u32,
    /// Senders say it with `POST` instead of their socket (they still
    /// receive on it): a full actor queue then answers 429.
    #[arg(long)]
    http_senders: bool,
    /// Also read the room's counters (`GET`) at the end; one process does.
    #[arg(long)]
    room_stats: bool,
    /// Where to write the records.
    #[arg(long)]
    out: PathBuf,
}

#[derive(Args)]
struct SampleArgs {
    /// The node's process.
    #[arg(long)]
    pid: u32,
    /// The node's metrics page: `http://HOST:PORT/metrics`.
    #[arg(long)]
    metrics: String,
    #[arg(long, default_value_t = 500)]
    interval_ms: u64,
    /// Stop after this many seconds (and when the process ends).
    #[arg(long, default_value_t = 3600.0)]
    duration: f64,
    #[arg(long)]
    out: PathBuf,
}

#[derive(Args)]
struct MergeArgs {
    /// The rows' name prefix, such as `room/n1000/plain/r100`.
    #[arg(long)]
    prefix: String,
    /// The node's samples (`roomload sample`).
    #[arg(long)]
    sample: Option<PathBuf>,
    /// Set when the node exited during the run (`bench/room.sh` checks).
    #[arg(long)]
    node_died: bool,
    /// The files of every `run` process.
    files: Vec<PathBuf>,
}

fn main() {
    match Cli::parse().command {
        Command::Run(args) => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(run(args));
        }
        Command::Sample(args) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(sample(args));
        }
        Command::Merge(args) => merge(&args),
    }
}

fn now_ns() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap()
}

// ---------------------------------------------------------------- run

/// What this process's sockets received, by `seq`.
struct Tables {
    send_ns: Vec<AtomicU64>,
    first_ns: Vec<AtomicU64>,
    last_ns: Vec<AtomicU64>,
    receivers: Vec<AtomicU32>,
    /// Deliveries whose `seq` is past the tables.
    overflow: AtomicU64,
    /// Senders' own messages seen back: the room stored and broadcast them.
    acked: AtomicU64,
}

impl Tables {
    fn new(capacity: usize) -> Self {
        let zeros = || (0..capacity).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        Self {
            send_ns: zeros(),
            first_ns: (0..capacity).map(|_| AtomicU64::new(u64::MAX)).collect(),
            last_ns: zeros(),
            receivers: (0..capacity).map(|_| AtomicU32::new(0)).collect(),
            overflow: AtomicU64::new(0),
            acked: AtomicU64::new(0),
        }
    }
}

/// One socket's view of the sequence.
#[derive(Default, Clone, Copy)]
struct SocketStats {
    first: u64,
    last: u64,
    unique: u64,
    /// Numbers skipped between two it received.
    missing: u64,
    duplicates: u64,
    /// Numbers received after a higher one (not equal to the last).
    reordered: u64,
    /// The close code the server sent, 0 if it did not close it.
    server_close: u16,
    /// The connection failed without a close frame.
    broken: bool,
}

#[derive(Default)]
struct Outcomes {
    handshakes: BTreeMap<String, u64>,
    http: BTreeMap<String, u64>,
    sockets: Vec<SocketStats>,
}

fn bump(map: &mut BTreeMap<String, u64>, key: impl Into<String>) {
    *map.entry(key.into()).or_default() += 1;
}

/// `ws://host:port/path` into the address and the path.
fn split_url(url: &str) -> (String, String) {
    let rest = url
        .strip_prefix("ws://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or_else(|| panic!("{url}: expected ws://HOST:PORT/PATH"));
    let (host, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    let path = if path.is_empty() { "/" } else { path };
    (host.to_owned(), path.to_owned())
}

async fn connect_tcp(server: SocketAddr, source: Option<IpAddr>) -> std::io::Result<TcpStream> {
    let socket = if server.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    if let Some(ip) = source {
        socket.bind(SocketAddr::new(ip, 0))?;
    }
    let stream = socket.connect(server).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

/// One message, padded to about `payload` bytes.
fn message(proc_id: u32, sender: usize, index: u64, payload: usize) -> String {
    let mut text = format!("p{proc_id} s{sender} i{index} t{} ", now_ns());
    while text.len() < payload {
        text.push('x');
    }
    text
}

/// Parses `<seq> p<proc> s<sender> i<index> t<ns> ...`.
fn parse_line(line: &str) -> Option<(u64, u32, usize, u64)> {
    let mut words = line.split(' ');
    let seq = words.next()?.parse().ok()?;
    let proc_id = words.next()?.strip_prefix('p')?.parse().ok()?;
    let sender = words.next()?.strip_prefix('s')?.parse().ok()?;
    let _index = words.next()?;
    let sent = words.next()?.strip_prefix('t')?.parse().ok()?;
    Some((seq, proc_id, sender, sent))
}

struct SocketTask {
    index: usize,
    proc_id: u32,
    tables: Arc<Tables>,
    outgoing: Option<mpsc::Receiver<String>>,
    stop: watch::Receiver<bool>,
}

impl SocketTask {
    async fn drive(
        mut self,
        ws: tokio_tungstenite::WebSocketStream<TcpStream>,
        sender: bool,
    ) -> SocketStats {
        let (mut sink, mut stream) = ws.split();
        let mut stats = SocketStats::default();
        let mut outgoing = self.outgoing.take();
        loop {
            tokio::select! {
                frame = stream.next() => match frame {
                    Some(Ok(Message::Text(text))) => {
                        let at = now_ns();
                        for line in text.as_str().split('\n') {
                            self.receive(line, at, sender, &mut stats);
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        stats.server_close = frame.map_or(1005, |f| u16::from(f.code));
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => {
                        stats.broken = true;
                        break;
                    }
                },
                text = async {
                    match outgoing.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => match text {
                    Some(text) => {
                        if sink.send(Message::Text(text.into())).await.is_err() {
                            stats.broken = true;
                            break;
                        }
                    }
                    None => outgoing = None,
                },
                _ = self.stop.changed() => {
                    let _ = sink.send(Message::Close(None)).await;
                    // Read until the server's close answers ours.
                    let _ = tokio::time::timeout(Duration::from_secs(2), async {
                        while let Some(Ok(frame)) = stream.next().await {
                            if matches!(frame, Message::Close(_)) { break; }
                        }
                    }).await;
                    break;
                }
            }
        }
        stats
    }

    fn receive(&self, line: &str, at: u64, sender: bool, stats: &mut SocketStats) {
        let Some((seq, proc_id, from, sent)) = parse_line(line) else {
            return;
        };
        if stats.unique == 0 {
            stats.first = seq;
            stats.last = seq;
            stats.unique = 1;
        } else if seq == stats.last {
            stats.duplicates += 1;
            return;
        } else if seq < stats.last {
            stats.reordered += 1;
            return;
        } else {
            stats.missing += seq - stats.last - 1;
            stats.last = seq;
            stats.unique += 1;
        }
        let t = &self.tables;
        let Ok(i) = usize::try_from(seq) else { return };
        if i >= t.send_ns.len() {
            t.overflow.fetch_add(1, Ordering::Relaxed);
            return;
        }
        t.send_ns[i].store(sent, Ordering::Relaxed);
        t.first_ns[i].fetch_min(at, Ordering::Relaxed);
        t.last_ns[i].fetch_max(at, Ordering::Relaxed);
        t.receivers[i].fetch_add(1, Ordering::Relaxed);
        if sender && proc_id == self.proc_id && from == self.index {
            t.acked.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// A sender that says it over HTTP: one keep-alive connection, one request
/// at a time.
async fn http_sender(
    server: SocketAddr,
    source: Option<IpAddr>,
    host: String,
    path: String,
    mut rx: mpsc::Receiver<String>,
    outcomes: Arc<Mutex<Outcomes>>,
) {
    let mut conn: Option<BufReader<TcpStream>> = None;
    while let Some(body) = rx.recv().await {
        let status = async {
            if conn.is_none() {
                conn = Some(BufReader::new(connect_tcp(server, source).await.ok()?));
            }
            let c = conn.as_mut()?;
            let head = format!(
                "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            c.get_mut().write_all(head.as_bytes()).await.ok()?;
            c.get_mut().write_all(body.as_bytes()).await.ok()?;
            let mut line = String::new();
            c.read_line(&mut line).await.ok()?;
            let status = line.split(' ').nth(1)?.to_owned();
            let mut length = 0usize;
            let mut close = false;
            loop {
                line.clear();
                c.read_line(&mut line).await.ok()?;
                let header = line.trim_end();
                if header.is_empty() {
                    break;
                }
                let lower = header.to_ascii_lowercase();
                if let Some(v) = lower.strip_prefix("content-length:") {
                    length = v.trim().parse().ok()?;
                }
                if lower.starts_with("connection:") && lower.contains("close") {
                    close = true;
                }
            }
            let mut skip = vec![0; length];
            c.read_exact(&mut skip).await.ok()?;
            if close {
                conn = None;
            }
            Some(status)
        }
        .await;
        if status.is_none() {
            conn = None;
        }
        bump(
            &mut outcomes.lock().unwrap().http,
            status.unwrap_or_else(|| "error".to_owned()),
        );
    }
}

async fn run(args: RunArgs) {
    let (host, path) = split_url(&args.url);
    let server: SocketAddr = tokio::net::lookup_host(&host)
        .await
        .unwrap_or_else(|e| panic!("{host}: {e}"))
        .next()
        .unwrap_or_else(|| panic!("{host}: no address"));
    let senders = args.senders.min(args.sockets);
    assert!(
        senders > 0 || args.rate == 0.0,
        "--senders must be at least 1"
    );
    let total_rate = args.rate * f64::from(args.procs);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let capacity = ((total_rate * args.duration * 2.0) as usize).max(10_000) + 1;
    let tables = Arc::new(Tables::new(capacity));
    let outcomes = Arc::new(Mutex::new(Outcomes::default()));
    let (stop_tx, stop_rx) = watch::channel(false);
    let config = WebSocketConfig::default()
        .read_buffer_size(4096)
        .write_buffer_size(0);

    // Sender channels: to a socket, or to an HTTP connection.
    let mut sender_tx = Vec::new();
    let mut socket_rx: Vec<Option<mpsc::Receiver<String>>> = Vec::new();
    for i in 0..senders {
        let (tx, rx) = mpsc::channel::<String>(1024);
        sender_tx.push(tx);
        if args.http_senders {
            let source =
                (!args.source_ip.is_empty()).then(|| args.source_ip[i % args.source_ip.len()]);
            tokio::spawn(http_sender(
                server,
                source,
                host.clone(),
                path.clone(),
                rx,
                outcomes.clone(),
            ));
            socket_rx.push(None);
        } else {
            socket_rx.push(Some(rx));
        }
    }

    // Ramp-up: at most `ramp` new sockets a second, `connect_concurrency`
    // handshakes at once.
    let ramp_start = std::time::Instant::now();
    let ramp_start_ns = now_ns();
    let permits = Arc::new(Semaphore::new(args.connect_concurrency.max(1)));
    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel::<bool>();
    let mut tasks = Vec::with_capacity(args.sockets);
    for i in 0..args.sockets {
        #[allow(clippy::cast_precision_loss)]
        let due = Duration::from_secs_f64(i as f64 / args.ramp.max(1.0));
        tokio::time::sleep_until(tokio::time::Instant::from_std(ramp_start + due)).await;
        let permit = permits.clone().acquire_owned().await.unwrap();
        let source = (!args.source_ip.is_empty()).then(|| args.source_ip[i % args.source_ip.len()]);
        let request = args.url.clone();
        let task = SocketTask {
            index: i,
            proc_id: args.id,
            tables: tables.clone(),
            outgoing: socket_rx.get_mut(i).and_then(Option::take),
            stop: stop_rx.clone(),
        };
        let is_sender = i < senders;
        let outcomes = outcomes.clone();
        let ready = ready_tx.clone();
        tasks.push(tokio::spawn(async move {
            let opened = async {
                let tcp = connect_tcp(server, source)
                    .await
                    .map_err(|e| format!("tcp_{:?}", e.kind()))?;
                tokio_tungstenite::client_async_with_config(request, tcp, Some(config))
                    .await
                    .map_err(|e| match e {
                        WsError::Http(response) => response.status().as_u16().to_string(),
                        WsError::Io(e) => format!("io_{:?}", e.kind()),
                        other => format!("ws_{}", short(&other.to_string())),
                    })
            }
            .await;
            drop(permit);
            match opened {
                Ok((ws, _)) => {
                    bump(&mut outcomes.lock().unwrap().handshakes, "101");
                    let _ = ready.send(true);
                    let stats = task.drive(ws, is_sender).await;
                    outcomes.lock().unwrap().sockets.push(stats);
                }
                Err(e) => {
                    bump(&mut outcomes.lock().unwrap().handshakes, e);
                    let _ = ready.send(false);
                }
            }
        }));
    }
    drop(ready_tx);
    let mut connected = 0u64;
    for _ in 0..args.sockets {
        if ready_rx.recv().await == Some(true) {
            connected += 1;
        }
    }
    let ramp_ms = ramp_start.elapsed().as_millis();
    eprintln!(
        "roomload {}: {connected}/{} sockets open in {ramp_ms} ms",
        args.id, args.sockets
    );
    let mut late_start = false;
    if let Some(at) = args.start_at {
        let now_ms = now_ns() / 1_000_000;
        if at > now_ms {
            tokio::time::sleep(Duration::from_millis(at - now_ms)).await;
        } else {
            late_start = true;
        }
    }

    // Open loop: message k is due at start + k / rate, whatever the room
    // answers, so a slow room shows as latency, not as fewer messages.
    let send_start_ns = now_ns();
    let cpu_start = proc_sample(std::process::id()).map_or(0, |s| s.0);
    let start = tokio::time::Instant::now();
    let mut sent = 0u64;
    let mut skipped = 0u64;
    if args.rate > 0.0 && senders > 0 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let total = (args.rate * args.duration).round() as u64;
        for k in 0..total {
            #[allow(clippy::cast_precision_loss)]
            let due = start + Duration::from_secs_f64(k as f64 / args.rate);
            tokio::time::sleep_until(due).await;
            let s = usize::try_from(k).unwrap() % senders;
            match sender_tx[s].try_send(message(args.id, s, k, args.payload)) {
                Ok(()) => sent += 1,
                Err(_) => skipped += 1,
            }
        }
    }
    let send_end_ns = now_ns();
    drop(sender_tx);
    tokio::time::sleep(Duration::from_secs_f64(args.drain)).await;
    // This process's own CPU over the sends and the drain: a load
    // generator near 100% per core measures itself, not the room.
    let cpu_ticks = proc_sample(std::process::id()).map_or(0, |s| s.0) - cpu_start;
    let stop_ns = now_ns();
    let _ = stop_tx.send(true);
    for task in tasks {
        let _ = task.await;
    }
    let room = if args.room_stats {
        room_counters(server, &host, &path).await
    } else {
        Vec::new()
    };

    let outcomes = outcomes.lock().unwrap();
    let mut out = std::io::BufWriter::new(std::fs::File::create(&args.out).unwrap());
    let t = &tables;
    writeln!(out, "roomload 1").unwrap();
    for (k, v) in [
        ("proc", u64::from(args.id)),
        ("sockets_wanted", args.sockets as u64),
        ("sockets_connected", connected),
        ("senders", senders as u64),
        ("ramp_ms", u64::try_from(ramp_ms).unwrap_or(u64::MAX)),
        ("late_start", u64::from(late_start)),
        ("ramp_start_ns", ramp_start_ns),
        ("send_start_ns", send_start_ns),
        ("send_end_ns", send_end_ns),
        ("sent", sent),
        ("skipped", skipped),
        ("acked", t.acked.load(Ordering::Relaxed)),
        ("cpu_ticks", cpu_ticks),
        ("stop_ns", stop_ns),
        ("overflow", t.overflow.load(Ordering::Relaxed)),
    ] {
        writeln!(out, "meta {k} {v}").unwrap();
    }
    for (k, v) in &outcomes.handshakes {
        writeln!(out, "handshake {k} {v}").unwrap();
    }
    for (k, v) in &outcomes.http {
        writeln!(out, "http {k} {v}").unwrap();
    }
    for s in &outcomes.sockets {
        writeln!(
            out,
            "socket {} {} {} {} {} {} {} {}",
            s.first,
            s.last,
            s.unique,
            s.missing,
            s.duplicates,
            s.reordered,
            s.server_close,
            u8::from(s.broken)
        )
        .unwrap();
    }
    for i in 1..capacity {
        let n = t.receivers[i].load(Ordering::Relaxed);
        if n > 0 {
            writeln!(
                out,
                "seq {i} {} {} {} {n}",
                t.send_ns[i].load(Ordering::Relaxed),
                t.first_ns[i].load(Ordering::Relaxed),
                t.last_ns[i].load(Ordering::Relaxed)
            )
            .unwrap();
        }
    }
    for (k, v) in room {
        writeln!(out, "room {k} {v}").unwrap();
    }
    out.flush().unwrap();
}

fn short(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .take(40)
        .collect()
}

/// One HTTP/1.1 `GET` with `Connection: close`; the body, or `None`.
async fn http_get(server: SocketAddr, host: &str, path: &str) -> Option<String> {
    let mut tcp = connect_tcp(server, None).await.ok()?;
    let head = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    tcp.write_all(head.as_bytes()).await.ok()?;
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), tcp.read_to_end(&mut raw))
        .await
        .ok()?
        .ok()?;
    let text = String::from_utf8_lossy(&raw);
    let (head, body) = text.split_once("\r\n\r\n")?;
    if !head.starts_with("HTTP/1.1 200") {
        return None;
    }
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        // Chunk sizes on their own lines; keep the rest.
        let mut plain = String::new();
        let mut lines = body.split("\r\n");
        while let Some(size) = lines.next() {
            if usize::from_str_radix(size.trim(), 16).ok()? == 0 {
                break;
            }
            plain.push_str(lines.next()?);
        }
        return Some(plain);
    }
    Some(body.to_owned())
}

async fn room_counters(server: SocketAddr, host: &str, path: &str) -> Vec<(String, String)> {
    let Some(body) = http_get(server, host, path).await else {
        return vec![("unreadable".to_owned(), "1".to_owned())];
    };
    body.lines()
        .filter_map(|l| l.split_once(' '))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

// ---------------------------------------------------------------- sample

/// The sum of a metric's series whose labels contain every `filter`.
fn metric(page: &str, name: &str, filter: &[&str]) -> u64 {
    let mut sum = 0.0;
    for line in page.lines() {
        let Some(rest) = line.strip_prefix(name) else {
            continue;
        };
        if !(rest.starts_with('{') || rest.starts_with(' ')) {
            continue;
        }
        if !filter.iter().all(|f| rest.contains(f)) {
            continue;
        }
        if let Some(v) = rest.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) {
            sum += v;
        }
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let n = sum as u64;
    n
}

/// CPU ticks (user and system), resident kB, open descriptors.
fn proc_sample(pid: u32) -> Option<(u64, u64, u64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Fields after the command, which is in parentheses and may hold spaces.
    let after = &stat[stat.rfind(')')? + 2..];
    let fields: Vec<&str> = after.split(' ').collect();
    let ticks = fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?;
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let rss = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))?
        .trim()
        .trim_end_matches(" kB")
        .trim()
        .parse()
        .ok()?;
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?.count() as u64;
    Some((ticks, rss, fds))
}

async fn sample(args: SampleArgs) {
    let (host, path) = split_url(&args.metrics);
    let server: SocketAddr = tokio::net::lookup_host(&host)
        .await
        .unwrap()
        .next()
        .unwrap();
    let mut out = std::io::BufWriter::new(std::fs::File::create(&args.out).unwrap());
    // USER_HZ: 100 on every Linux the project targets.
    writeln!(out, "roomsample 1\nmeta clk_tck 100").unwrap();
    let end = tokio::time::Instant::now() + Duration::from_secs_f64(args.duration);
    let mut tick = tokio::time::interval(Duration::from_millis(args.interval_ms));
    while tokio::time::Instant::now() < end {
        tick.tick().await;
        let Some((ticks, rss, fds)) = proc_sample(args.pid) else {
            writeln!(out, "exited {}", now_ns()).unwrap();
            break;
        };
        let page = http_get(server, &host, &path).await.unwrap_or_default();
        writeln!(
            out,
            "sample {} {ticks} {rss} {fds} {} {} {} {}",
            now_ns(),
            metric(&page, "wasmkeep_websockets_open", &[]),
            metric(
                &page,
                "wasmkeep_admission_refused_total",
                &["reason=\"overloaded\""]
            ),
            metric(
                &page,
                "wasmkeep_admission_refused_total",
                &["reason=\"capacity\""]
            ),
            metric(&page, "wasmkeep_events_total", &["kind=\"websocket\""]),
        )
        .unwrap();
        out.flush().unwrap();
    }
}

// ---------------------------------------------------------------- merge

#[derive(Default)]
struct Seq {
    send: u64,
    first: u64,
    last: u64,
    receivers: u64,
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let i = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[i]
}

#[allow(clippy::too_many_lines)]
fn merge(args: &MergeArgs) {
    let mut meta: BTreeMap<String, u64> = BTreeMap::new();
    let mut send_start = u64::MAX;
    let mut ramp_start = u64::MAX;
    let mut send_end = 0u64;
    let mut handshakes: BTreeMap<String, u64> = BTreeMap::new();
    let mut http: BTreeMap<String, u64> = BTreeMap::new();
    // Each socket with the time its process began closing.
    let mut sockets: Vec<([u64; 8], u64)> = Vec::new();
    let mut seqs: BTreeMap<u64, Seq> = BTreeMap::new();
    let mut room: BTreeMap<String, u64> = BTreeMap::new();
    for file in &args.files {
        let first_socket = sockets.len();
        let mut stop = u64::MAX;
        let text =
            std::fs::read_to_string(file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
        for line in text.lines() {
            let w: Vec<&str> = line.split(' ').collect();
            let num = |i: usize| w.get(i).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
            match w[0] {
                "meta" => match w[1] {
                    "send_start_ns" => send_start = send_start.min(num(2)),
                    "ramp_start_ns" => ramp_start = ramp_start.min(num(2)),
                    "send_end_ns" => send_end = send_end.max(num(2)),
                    "proc" => {}
                    "stop_ns" => stop = num(2),
                    k => *meta.entry(k.to_owned()).or_default() += num(2),
                },
                "handshake" => *handshakes.entry(w[1].to_owned()).or_default() += num(2),
                "http" => *http.entry(w[1].to_owned()).or_default() += num(2),
                "socket" => {
                    let mut s = [0u64; 8];
                    for (i, v) in s.iter_mut().enumerate() {
                        *v = num(i + 1);
                    }
                    sockets.push((s, 0));
                }
                "seq" => {
                    let e = seqs.entry(num(1)).or_insert(Seq {
                        first: u64::MAX,
                        ..Seq::default()
                    });
                    e.send = e.send.max(num(2));
                    e.first = e.first.min(num(3));
                    e.last = e.last.max(num(4));
                    e.receivers += num(5);
                }
                "room" => {
                    room.insert(w[1].to_owned(), num(2));
                }
                _ => {}
            }
        }
        for socket in &mut sockets[first_socket..] {
            socket.1 = stop;
        }
    }
    let p = &args.prefix;
    let row = |name: &str, value: u64, unit: &str| println!("{p}/{name},{value},{unit}");

    let max_seq = seqs.keys().next_back().copied().unwrap_or(0);
    // Sockets the server did not close and that did not break: every one
    // should have received every number from 1 to the last.
    let mut lost = 0u64;
    let mut duplicates = 0u64;
    let mut reordered = 0u64;
    let mut closed: BTreeMap<u64, u64> = BTreeMap::new();
    let mut broken = 0u64;
    let mut healthy = 0u64;
    // A socket is expected to have every number the room had broadcast a
    // second before its process began closing: a room still working off a
    // backlog broadcasts later numbers to sockets of processes still open.
    let firsts: Vec<(u64, u64)> = seqs.iter().map(|(k, v)| (v.first, *k)).collect();
    let expected = |stop: u64| {
        firsts
            .iter()
            .filter(|(first, _)| first.saturating_add(1_000_000_000) <= stop)
            .map(|(_, seq)| *seq)
            .max()
            .unwrap_or(0)
    };
    let mut by_stop: BTreeMap<u64, u64> = BTreeMap::new();
    let mut cutoffs = Vec::new();
    for (s, stop) in &sockets {
        let [first, last, unique, missing, dups, reord, close, brk] = *s;
        let max_seq = *by_stop.entry(*stop).or_insert_with(|| expected(*stop));
        duplicates += dups;
        reordered += reord;
        if close != 0 {
            *closed.entry(close).or_default() += 1;
            continue;
        }
        if brk != 0 {
            broken += 1;
            continue;
        }
        healthy += 1;
        cutoffs.push(max_seq);
        lost += if unique == 0 {
            max_seq
        } else {
            missing + (first - 1) + max_seq.saturating_sub(last)
        };
    }
    cutoffs.sort_unstable();
    let mut last_lat: Vec<u64> = Vec::new();
    let mut first_lat: Vec<u64> = Vec::new();
    let mut deliveries = 0u64;
    let mut last_receipt = 0u64;
    let mut incomplete = 0u64;
    // Messages whose last socket had them more than a second after they
    // were sent: the room fell behind, even if it caught up in the drain.
    let mut late = 0u64;
    for (seq, s) in &seqs {
        deliveries += s.receivers;
        last_receipt = last_receipt.max(s.last);
        if s.send > 0 {
            last_lat.push(s.last.saturating_sub(s.send));
            if s.last.saturating_sub(s.send) > 1_000_000_000 {
                late += 1;
            }
            first_lat.push(s.first.saturating_sub(s.send));
        }
        // Fewer receipts than the sockets that should have had it.
        let should = cutoffs.len() - cutoffs.partition_point(|c| c < seq);
        if s.receivers < should as u64 {
            incomplete += 1;
        }
    }
    last_lat.sort_unstable();
    first_lat.sort_unstable();

    for k in [
        "sockets_wanted",
        "sockets_connected",
        "senders",
        "sent",
        "skipped",
        "acked",
        "late_start",
    ] {
        row(k, meta.get(k).copied().unwrap_or(0), "count");
    }
    row(
        "ramp_ms_sum",
        meta.get("ramp_ms").copied().unwrap_or(0),
        "ms",
    );
    // Load processes' CPU, percent of one core, summed over processes.
    let span = last_receipt.max(send_end).saturating_sub(send_start);
    if span > 0 {
        let ticks = u128::from(meta.get("cpu_ticks").copied().unwrap_or(0));
        let pct = ticks * 10_000_000 * 100 / u128::from(span);
        row("load_cpu_pct", u64::try_from(pct).unwrap_or(0), "percent");
    }
    for (k, v) in &handshakes {
        row(&format!("handshake_{k}"), *v, "count");
    }
    for (k, v) in &http {
        row(&format!("http_{k}"), *v, "count");
    }
    for (k, v) in &closed {
        row(&format!("server_close_{k}"), *v, "count");
    }
    row("sockets_broken", broken, "count");
    row("messages", seqs.len() as u64, "count");
    row("deliveries", deliveries, "count");
    row("lost_deliveries", lost, "count");
    row("duplicates", duplicates, "count");
    row("reordered", reordered, "count");
    row("incomplete_messages", incomplete, "count");
    // With HTTP senders, a message is acknowledged by its answer (200, or
    // 202 in a batch room); otherwise by its echo on the sender's socket.
    let acked = if http.is_empty() {
        meta.get("acked").copied().unwrap_or(0)
    } else {
        http.get("200").copied().unwrap_or(0) + http.get("202").copied().unwrap_or(0)
    };
    let unacked = meta.get("sent").copied().unwrap_or(0).saturating_sub(acked);
    row("unacked", unacked, "count");
    row("late_messages", late, "count");
    row("latency_last_p50", percentile(&last_lat, 0.5), "ns");
    row("latency_last_p99", percentile(&last_lat, 0.99), "ns");
    row(
        "latency_last_max",
        last_lat.last().copied().unwrap_or(0),
        "ns",
    );
    row("latency_first_p50", percentile(&first_lat, 0.5), "ns");
    let window = last_receipt.saturating_sub(send_start);
    let per_s = if window > 0 {
        u64::try_from(u128::from(deliveries) * 1_000_000_000 / u128::from(window)).unwrap_or(0)
    } else {
        0
    };
    row("delivered_per_s", per_s, "msg/s");
    for (k, v) in &room {
        row(&format!("room_{k}"), *v, "count");
    }
    // Acknowledged (echoed to its sender) but not in the room's table: the
    // room's numbers have no holes, so the highest echo must be stored.
    let stored_max = room.get("stored_max_seq").copied();
    // And holes in what it stored: numbers below its highest, missing.
    let holes = room
        .get("stored")
        .map(|stored| stored_max.unwrap_or(0).saturating_sub(*stored));
    let lost_acked = stored_max.map(|m| max_seq.saturating_sub(m) + holes.unwrap_or(0));
    if let Some(n) = lost_acked {
        row("lost_acked", n, "count");
    }

    // The node, during the sends (and the drain).
    let mut node = String::new();
    if let Some(file) = &args.sample {
        let text = std::fs::read_to_string(file).unwrap();
        let mut samples: Vec<[u64; 9]> = Vec::new();
        for line in text.lines() {
            let w: Vec<&str> = line.split(' ').collect();
            if w[0] == "sample" {
                let mut s = [0u64; 9];
                for (i, v) in s.iter_mut().enumerate() {
                    *v = w.get(i + 1).and_then(|v| v.parse().ok()).unwrap_or(0);
                }
                samples.push(s);
            }
        }
        // A node may serve several runs (a fleet run): only the samples
        // from this run's ramp-up to its last receipt count.
        let end = last_receipt.max(send_end);
        let run: Vec<&[u64; 9]> = samples
            .iter()
            .filter(|s| s[0] >= ramp_start && s[0] <= end)
            .collect();
        let sending: Vec<&&[u64; 9]> = run.iter().filter(|s| s[0] >= send_start).collect();
        if let (Some(a), Some(b)) = (sending.first(), sending.last()) {
            let wall = b[0].saturating_sub(a[0]);
            if wall > 0 {
                // Percent of one core: ticks are hundredths of a second.
                let pct =
                    u128::from(b[1].saturating_sub(a[1])) * 10_000_000 * 100 / u128::from(wall);
                row("node_cpu_pct", u64::try_from(pct).unwrap_or(0), "percent");
            }
        }
        let peak = |i: usize| run.iter().map(|s| s[i]).max().unwrap_or(0);
        row("node_rss_peak", peak(2) * 1024, "bytes");
        row("node_fds_peak", peak(3), "count");
        row("node_websockets_open_peak", peak(4), "count");
        // Counters: from the last sample before the run to its last one.
        let before = samples
            .iter()
            .rev()
            .find(|s| s[0] < ramp_start)
            .or(samples.first());
        let delta = |i: usize| {
            run.last()
                .map_or(0, |l| l[i])
                .saturating_sub(before.map_or(0, |f| f[i]))
        };
        row("node_refused_overloaded", delta(5), "count");
        row("node_refused_capacity", delta(6), "count");
        row("node_websocket_events", delta(7), "count");
        if text.lines().any(|l| l.starts_with("exited")) {
            node.push_str("node exited; ");
        }
    }

    // The breaking point's kind, for a reader of the rows.
    let refused = handshakes
        .iter()
        .filter(|(k, _)| k.as_str() != "101")
        .map(|(_, v)| v)
        .sum::<u64>()
        + closed
            .iter()
            .filter(|(k, _)| matches!(**k, 1013 | 1008 | 1012))
            .map(|(_, v)| v)
            .sum::<u64>()
        + http.get("429").copied().unwrap_or(0)
        + http.get("503").copied().unwrap_or(0);
    let mut kinds = Vec::new();
    if args.node_died || !node.is_empty() {
        kinds.push("crash");
    }
    if lost > 0 || lost_acked.unwrap_or(0) > 0 || duplicates > 0 || reordered > 0 {
        kinds.push("loss");
    }
    let stalled = unacked > 0
        || late > 0
        || incomplete > 0
        || broken > 0
        || closed.keys().any(|k| *k == 1011)
        || meta.get("skipped").copied().unwrap_or(0) > 0;
    if stalled {
        kinds.push("stall");
    }
    if refused > 0 {
        kinds.push("refused");
    }
    if kinds.is_empty() {
        kinds.push("ok");
    }
    for k in &kinds {
        row(&format!("outcome_{k}"), 1, "flag");
    }
    eprintln!(
        "{p}: {} sockets, {} messages, last-socket latency p50 {:.1} ms p99 {:.1} ms max {:.1} ms, {per_s} deliveries/s, lost {lost}, dup {duplicates}, unacked {unacked}: {}",
        healthy,
        seqs.len(),
        ms(percentile(&last_lat, 0.5)),
        ms(percentile(&last_lat, 0.99)),
        ms(last_lat.last().copied().unwrap_or(0)),
        kinds.join("+")
    );
}

#[allow(clippy::cast_precision_loss)]
fn ms(ns: u64) -> f64 {
    ns as f64 / 1e6
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_line_parses_back() {
        let text = message(3, 7, 42, 200);
        assert!(text.len() >= 200);
        let (seq, proc_id, sender, sent) = parse_line(&format!("9 {text}")).unwrap();
        assert_eq!((seq, proc_id, sender), (9, 3, 7));
        assert!(sent > 0);
    }

    #[test]
    fn metrics_sum_the_matching_series() {
        let page = "# HELP x\nwasmkeep_websockets_open 12\n\
            wasmkeep_admission_refused_total{reason=\"overloaded\"} 3\n\
            wasmkeep_admission_refused_total{reason=\"capacity\"} 4\n\
            wasmkeep_websockets_open_other 99\n";
        assert_eq!(metric(page, "wasmkeep_websockets_open", &[]), 12);
        assert_eq!(
            metric(
                page,
                "wasmkeep_admission_refused_total",
                &["reason=\"capacity\""]
            ),
            4
        );
    }

    #[test]
    fn urls_split_into_host_and_path() {
        let (host, path) = split_url("ws://127.0.0.1:8080/actor/room/plain-1");
        assert_eq!(host, "127.0.0.1:8080");
        assert_eq!(
            path.split('/').collect::<Vec<_>>(),
            ["", "actor", "room", "plain-1"]
        );
    }
}
