# Three nodes with Docker Compose

Three WasmKeep nodes (`a`, `b`, `c`) sharing a MinIO bucket, with mutual TLS
between them and plain HTTP between nodes refused, as in
[Three nodes](../../docs/deploy/three-nodes.md). It serves the
[counter example](../../examples/counter). Use it to try the cluster or as a
test bed; for production, follow the guide (one node per machine, a real
bucket, a load balancer).

```text
  127.0.0.1:8081   127.0.0.1:8082   127.0.0.1:8083    (clients)
        │                │                │
     node a ◄─ mTLS ─► node b ◄─ mTLS ─► node c       (port 7443, compose network only)
        └────────────────┼────────────────┘
                 MinIO, bucket `wasmkeep`, prefix `demo`
```

## Start it

From this directory, with Docker (Compose v2) or Podman:

```bash
docker compose up -d --build
```

```bash
podman compose up -d --build
```

The first build compiles the server and the counter (a few minutes). Then
MinIO starts, `bucket` creates the bucket, `deploy` runs `wasmkeep deploy
--app demo/counter` with the counter component, and the three nodes serve
`--app demo/counter`. These steps converge by retrying rather than by
waiting on each other (Podman's Compose cannot wait for a one-shot service
to finish): until the bucket exists `deploy` fails and is restarted, and
until the deploy lands each node exits with `nothing is deployed for
demo/counter yet` and is restarted. That takes a few seconds; after it, each
node reports not ready (`GET /_wasmkeep/ready` answers 503) for a few more,
while its certificate fingerprint reaches the others.

## Try it

Any node answers for any actor; `x-wasmkeep-node` says which node runs it:

```bash
curl -i -X POST http://127.0.0.1:8081/actor/counter/alice
```

```bash
curl -i http://127.0.0.1:8083/actor/counter/alice
```

Stop the node that answered (say `b`) and ask again: another node answers
with the value preserved, since a clean stop hands its actors over.

```bash
docker compose stop b
```

```bash
curl -i -X POST http://127.0.0.1:8081/actor/counter/alice
```

`docker compose kill b` instead shows a crash: the actor comes back on
another node 15 to about 21 s after the node's last heartbeat (10 s
lease plus 5 s grace, up to a watch period more, then a restore from the
bucket).
`docker compose start b` brings the node back. Logs:
`docker compose logs -f a b c` (each node logs `cluster enabled` with its
cluster address at start).

The MinIO console is at <http://127.0.0.1:9001> (user `wasmkeep-demo`).

To deploy a new version, rebuild the demo image and rerun the one-shot
service; the nodes switch within a few seconds (rerunning it unchanged is a
no-op):

```bash
docker compose build deploy
```

```bash
docker compose run --rm deploy
```

## With Prometheus and Grafana

`monitoring.yaml` adds each node's metrics (`--metrics-listen`, on the
compose network only), Prometheus scraping them every 5 s, and Grafana
with the [WasmKeep dashboard](../grafana/wasmkeep.json) already loaded:

```bash
docker compose -f compose.yaml -f monitoring.yaml up -d --build
```

```bash
podman compose -f compose.yaml -f monitoring.yaml up -d --build
```

Grafana is at <http://127.0.0.1:3000> (the dashboard is its home page;
anonymous viewing, `admin`/`admin` to edit), Prometheus at
<http://127.0.0.1:9090>; both listen on 127.0.0.1 only. Send some traffic
(the counter from "Try it", in a loop over a few names and the three
ports), then kill a node: within about 15 s "Nodes ready" drops to 2,
"Failovers (range)" counts the actors the other nodes restored, and the
counters keep their values. Stop everything, monitoring included, with the
same `-f` options and `down`.

Prometheus also loads the alert rules of
[`deploy/prometheus/alerts.yml`](../prometheus/alerts.yml): its Alerts
page (<http://127.0.0.1:9090/alerts>) lists them with their state. The
example runs no Alertmanager, so nothing is sent anywhere.

## What is in it

| Service | Image | Role |
| --- | --- | --- |
| `minio` | `cgr.dev/chainguard/minio` | The bucket. MinIO no longer publishes official images; Chainguard's is built from source |
| `bucket` | `curlimages/curl` | Creates the bucket once (MinIO's images have no shell or client) |
| `deploy` | `wasmkeep-demo` (the `demo` stage of [`deploy/Dockerfile`](../Dockerfile)) | `wasmkeep deploy` of the counter, then exits |
| `a`, `b`, `c` | `wasmkeep` (the release stage) | `wasmkeep serve --app demo/counter` |

- Images are pinned by digest. Chainguard's free tier only tags `latest`, so
  its pin goes stale as they rebuild; refresh it by resolving the new
  digest of `cgr.dev/chainguard/minio:latest`.
- The nodes advertise their compose service names (`http://a:8080`,
  `https://a:7443`); the cluster port is not published to the host.
- `stop_grace_period: 30s`: a clean stop can take about 29 s, and
  Compose's default of 10 s would turn it into a crash.
- Each node and MinIO keep their data in a named volume. Start over with
  `docker compose down -v`.
- The credentials in `compose.yaml` are demo credentials, readable by
  anyone with the repository. Whoever can write the bucket can run code on
  the cluster: replace them, and keep MinIO's ports on localhost, before
  storing anything that matters.

## The image on its own

[`deploy/Dockerfile`](../Dockerfile) builds the server image on its own,
from the repository root:

```bash
docker build -f deploy/Dockerfile -t wasmkeep .
```

It is distroless (no shell, no package manager), runs as user 65532,
keeps data under `/var/lib/wasmkeep` (mount a volume there), and its entrypoint is `wasmkeep`:
`docker run --rm wasmkeep serve ...` takes the same flags as the binary.
There is no `HEALTHCHECK` (nothing in the image could run one); health-check
`GET /_wasmkeep/ready` from the orchestrator or load balancer instead.
LICENSE, NOTICE and, in released images, the third-party notices are in
`/usr/share/doc/wasmkeep/`.

For each release, CI publishes the image for linux/amd64 and
linux/arm64 as `ghcr.io/wasmkeep/wasmkeep:<version>`, signed with keyless
cosign like the release binaries. To check it before running it (cosign 3),
here for `v0.2.0`:

```bash
cosign verify ghcr.io/wasmkeep/wasmkeep:0.2.0 \
  --certificate-identity https://github.com/WasmKeep/wasmkeep/.github/workflows/release.yml@refs/tags/v0.2.0 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

It prints the verified digest; pin that digest (`ghcr.io/wasmkeep/wasmkeep@sha256:...`)
in your deployment rather than the tag. The `dev` tag is a test build of
the release process, overwritten without notice: do not deploy it.

To serve one component with it (the release's `counter.wasm`, for
instance), mount the component and a volume for the data, and listen on
every address of the container:

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/counter.wasm:/counter.wasm:ro" -v wasmkeep-data:/var/lib/wasmkeep \
  ghcr.io/wasmkeep/wasmkeep:0.2.0 serve --listen 0.0.0.0:8080 --actor counter=/counter.wasm
```

The component must be readable by user 65532 (on SELinux hosts, add `,Z`
to its mount).

`scripts/sbom.sh` writes a CycloneDX SBOM of the binary (every crate
compiled into it, from `Cargo.lock`) to `target/sbom/`.

## What was tested

On Linux with Podman 6.1 and podman-compose 1.6 (x86-64): the image
builds, the cluster starts with mutual TLS and plain HTTP refused, the
counter answers on every node, a stopped owner hands its actor over with
the value kept, and a killed one's actor comes back on another node after
about 15 s. CI builds the image with Docker's BuildKit for linux/amd64 and
linux/arm64 and checks each one starts; the cluster itself is not run
there. Not tested: the cluster on arm64, Docker Compose, and `kill` with
Docker (whose restart policy may differ from Podman's).

