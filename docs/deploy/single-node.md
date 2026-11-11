# One server (VPS)

WasmKeep on one Linux machine, behind a reverse proxy that terminates TLS,
run by systemd. This is the right setup for most projects: one node serves
thousands of requests per second (see [benchmarks](../benchmarks.md)), and
adding a bucket gives it continuous off-machine backups.

> **Pre-alpha.** Fine for side projects and staging; keep a backup of any
> data you care about.

## 1. Install the binary

Each [release on GitHub](https://github.com/WasmKeep/wasmkeep/releases)
carries, for x86_64 and aarch64 Linux (glibc 2.28 or newer: RHEL 8,
Debian 10, Ubuntu 20.04 and later), a tarball with the binary and its
third-party notices, a CycloneDX SBOM, `SHA256SUMS`, and a Sigstore bundle
for each file. CI builds and signs them (`.github/workflows/release.yml`);
no key is involved, the signature's certificate names the workflow and
the tag. To check a download with cosign 3 (the version that signs them),
here for `v0.1.0`:

```bash
f=wasmkeep-0.1.0-x86_64-unknown-linux-gnu.tar.gz
cosign verify-blob --bundle "$f.sigstore.json" \
  --certificate-identity https://github.com/WasmKeep/wasmkeep/.github/workflows/release.yml@refs/tags/v0.1.0 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com "$f"
tar xzf "$f" && cd "${f%.tar.gz}" && sudo install -m 755 wasmkeep /usr/local/bin/wasmkeep
```

`SHA256SUMS` lists every file of the release, for tools that check
checksums; it is signed like the rest, so verify its own bundle the same
way before trusting it.

Or build on the server, or on a machine with the same OS and CPU
architecture:

```bash
git clone https://github.com/wasmkeep/wasmkeep && cd wasmkeep
cargo build --release -p wasmkeep-cli
sudo install -m 755 target/release/wasmkeep /usr/local/bin/wasmkeep
```

Then a system user and its directories:

```bash
sudo useradd --system --home-dir /var/lib/wasmkeep --shell /usr/sbin/nologin wasmkeep
sudo install -d -o wasmkeep -g wasmkeep /var/lib/wasmkeep/{classes,data,cache}
```

Copy your components (built as in each SDK's README) into
`/var/lib/wasmkeep/classes/`.

Configuration and secrets for a class (API keys, webhook URLs: see
[Configuration and secrets](../concepts.md#configuration-and-secrets)) go in
a file only the service can read:

```bash
sudo install -d -m 750 -o root -g wasmkeep /etc/wasmkeep
sudo install -m 640 -o root -g wasmkeep /dev/null /etc/wasmkeep/chat.env
sudoedit /etc/wasmkeep/chat.env       # NAME=value lines
```

## 2. Run it with systemd

`/etc/systemd/system/wasmkeep.service`:

```ini
[Unit]
Description=WasmKeep
After=network-online.target
Wants=network-online.target

[Service]
User=wasmkeep
Group=wasmkeep
ExecStart=/usr/local/bin/wasmkeep serve \
  --actor chat=/var/lib/wasmkeep/classes/chat_room.wasm \
  --actor counter=/var/lib/wasmkeep/classes/counter.wasm \
  --data-dir /var/lib/wasmkeep/data \
  --cache-dir /var/lib/wasmkeep/cache \
  --env chat=/etc/wasmkeep/chat.env \
  --listen 127.0.0.1:8080
Environment=RUST_LOG=info
Restart=on-failure
# SIGTERM: in-flight requests get up to 10 s, then actors finish their
# queued events (up to 10 s more) and, in a cluster, hand over. Give it room.
TimeoutStopSec=30
LimitNOFILE=65536
# Hardening. Do not add MemoryDenyWriteExecute (Wasmtime maps compiled code
# executable) or a LimitAS (the instance pool reserves a lot of virtual
# address space up front; it is not resident memory).
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/lib/wasmkeep

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now wasmkeep
journalctl -u wasmkeep -f          # logs; RUST_LOG=debug for more
curl localhost:8080/_wasmkeep/health   # ok
curl localhost:8080/_wasmkeep/ready    # ready (see docs/monitoring.md)
curl -X POST localhost:8080/actor/counter/hello
```

The first start compiles each component and caches the result in
`--cache-dir`; later starts take a fraction of a second.

## 3. Put a reverse proxy in front

WasmKeep speaks plain HTTP/1.1 and listens on localhost by default. Let a
reverse proxy handle TLS and public traffic. With
[Caddy](https://caddyserver.com) (automatic HTTPS, WebSockets included):

```caddyfile
actors.example.com {
    reverse_proxy 127.0.0.1:8080
}
```

With nginx, WebSockets need the upgrade headers:

```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 1h;   # idle WebSockets
}
```

Every actor is reachable by anyone who knows its address, and a request to
a new name creates that actor. Put authentication in your actors, or in the
proxy. In an actor: give the class a token with `--env`, and compare the
`Authorization` header with it in `fetch` without stopping at the first
differing byte (Watch's hub does this, `examples/watch/hub`). Calls between
actors carry `x-wasmkeep-caller: <tenant>/<class>/<name>`, which the host sets
and strips from outside requests, so an actor can accept some changes only
from another class.

Classes whose actors only other actors should create (a per-user actor
created at sign-up, a worker the scheduler starts) take `--access
CLASS=existing`: a request for an unknown name answers 404 and creates
nothing, so no one can fill the disk one name at a time. `--access
CLASS=internal` keeps a class away from outside requests entirely.

Actors' outbound HTTP reaches public addresses only. If they must call a
service on this machine or your private network, allow it explicitly, as
narrowly as you can: `--allow-outbound 10.0.5.20` (repeatable; ranges like
`10.0.5.0/24` work too). Never allow `169.254.169.254`: on a cloud VM it
hands out the machine's credentials. If that service's HTTPS certificate
comes from a private CA, add `--outbound-ca /etc/wasmkeep/internal-ca.pem`.
Ports other protocols use (mail on 25, 465 and 587, SSH, FTP and the rest
of the list browsers block) are refused too; `--allow-outbound-port 587`
opens one, for a mail relay your actors call over HTTP.

## 4. Back up the data

Each actor is one SQLite file:
`<data-dir>/actors/<tenant>/<class>/<name>.db`. Two ways to keep copies:

**Continuous, to a bucket (recommended).** Run the node as a one-node
cluster: every event's writes are uploaded to an S3-compatible bucket
(AWS S3, Cloudflare R2, MinIO, anything with conditional writes), and a
lost disk is restored from it on the next request to each actor. Add to
`ExecStart`:

```text
  --node-id vps-1 --advertise http://127.0.0.1:8080 --bucket s3://my-bucket/wasmkeep \
  --cluster-listen 127.0.0.1:7443 --cluster-advertise https://127.0.0.1:7443
```

With `--node-id`, a start also checks its node id in the bucket before it
listens, for up to 15 s: all of it the first time or after a crash, and
after a clean stop only what is left of 15 s since the node stopped
([why](../cluster.md#flags)).

The last two flags give the node a cluster listener on loopback: nodes
talk to each other over mutual TLS, and a node in a cluster refuses to
start without one (or without `--cluster-plaintext accept`). Alone, it
carries nothing; when a second server joins, move it to an address the
other nodes reach.

Then give the unit the bucket credentials (better: from an
`EnvironmentFile=` readable only by root):

```ini
Environment=AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=auto
Environment=AWS_ENDPOINT=https://<account>.r2.cloudflarestorage.com
```

Use `exec-durable` for writes that must survive the machine. Growing to
more servers later means starting them on the same bucket, with
`--advertise` set to an address the others can reach
([three nodes](three-nodes.md)).

**Snapshots.** Without a bucket, copy the files with SQLite's online
backup, which is safe while the server runs:

```bash
cd /var/lib/wasmkeep/data
find actors -name '*.db' | while read -r db; do
  mkdir -p "/backup/$(dirname "$db")"
  sqlite3 "$db" ".backup '/backup/$db'"
done
```

A plain `cp` of a live database can produce a corrupt copy.

## 5. Updating

- **New component version**: replace the `.wasm` and
  `systemctl restart wasmkeep`. Each actor runs `on-init` again on its next
  event, so migrations happen lazily, per actor.
- **No-restart deploys** need a bucket: `wasmkeep deploy --app acme/shop ...`
  uploads a version, and nodes started with `--app acme/shop` switch to it
  within seconds, actor by actor (see [Running a cluster](../cluster.md#deploying)).
- **New WasmKeep binary**: install it and restart. Open WebSockets are
  closed; clients reconnect.

## Sizing

Memory is spent on *resident* actors only: about 250 KB each for a Rust
actor and 740 KB for a TypeScript one on QuickJS, the default engine (see
[benchmarks](../benchmarks.md));
hibernated actors (idle for 30 s) cost nothing but disk. These figures
assume transparent huge pages off, which `wasmkeep serve` arranges for
itself ([monitoring](../monitoring.md#memory)); with
`--transparent-huge-pages system` on a kernel set to `always`, expect
about twice the memory. At most
`--max-instances` actors (1,000 by default) are resident at once; raise it
if more are active within the idle timeout, and budget the memory. CPU
scales with request rate.
