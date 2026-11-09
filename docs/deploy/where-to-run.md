# Where to run it

WasmKeep is a long-lived process that keeps actors, leases, WebSockets and
timers in memory, writes many small transactions to local SQLite files,
holds three open files per resident actor, and reserves a large virtual
address space for its instance pool. That shapes where it runs well.

| Option | Pros | Cons | Use it for |
| --- | --- | --- | --- |
| **Dedicated server** (bare metal) | Best price per performance; local NVMe makes `fsync` fast and predictable; no noisy neighbours | You handle hardware failure and OS upkeep; slower to resize | Production on a budget, clusters of three or more, benchmarks you publish |
| **Virtual machine** | Easy to create, resize and snapshot; private networks built in | Noisy neighbours (our shared-VM benchmark runs vary by up to ±30%); network-attached disks add `fsync` latency: prefer instance-local NVMe or fast volumes | The default, from one VPS to a three-node cluster |
| **Container** (Docker, Compose, Kubernetes) | Reproducible packaging and rollout; a distroless image and a three-node Compose example in [`deploy/`](../../deploy/compose/README.md) | One more layer; see the rules below | Teams that standardise on containers |
| **Serverless containers** (Cloud Run, Fargate, Lambda) | No servers to manage | Scale-to-zero kills resident actors and drops leases; instances are ephemeral, with no stable node identity; CPU may be throttled between requests; WebSockets and alarms fight the model | Not recommended |

A container is packaging, not isolation: actors are already sandboxed by
WebAssembly. Isolating tenants who may be hostile needs a stronger tier
(a process or microVM per tenant), which WasmKeep does not have yet
([security model](../security.md)).

## Rules that apply everywhere

- **A real disk for `--data-dir`**: local NVMe or a fast volume; never a
  container's overlay filesystem or a network file system (SQLite relies
  on local locking and `fsync`).
- **Open files**: give the process a high limit (`LimitNOFILE=65536` with
  systemd, `--ulimit nofile=65536` with Docker). WasmKeep raises its soft
  limit to the hard one and caps resident actors to fit
  ([monitoring](../monitoring.md#open-files)).
- **Virtual memory**: no `LimitAS` or `ulimit -v`; do not use
  `MemoryDenyWriteExecute` (Wasmtime maps compiled code executable). A
  memory limit on resident memory (cgroups) is fine.
- **Transparent huge pages** need nothing: WasmKeep turns them off for
  its own process on Linux, which halves memory per actor where the
  kernel sets them to `always` ([monitoring](../monitoring.md#memory)).
  Leave the system setting alone for other software on the machine.
- **A stable node identity** in a cluster: one `--node-id` per machine (a
  StatefulSet on Kubernetes).
- **A private network** between nodes, in the same region as the bucket;
  only the load balancer reaches them ([front door](front-door.md)).
- **Local disk durability**: on one node, the local disk *is* the
  database, so back it up ([one server](single-node.md#4-back-up-the-data));
  in a cluster the bucket is the source of truth and a node's disk can be
  lost.

## What is measured so far

The numbers in [Benchmarks](../benchmarks.md) come from a shared cloud
container: good for comparing commits and languages, not for capacity
claims. A three-node soak ran against R2 and OVHcloud Object Storage,
from a desktop and from three VPS next to the bucket
([results](../benchmarks.md#three-nodes-against-a-real-bucket)): nothing
lost over 27 kills. Put the nodes in the same region as the bucket: a
durable write waits for one PUT (54 ms next to it, 587 ms from a desktop
to R2), and a failover for a restore of many GETs. The suite has not
yet run on dedicated hardware.
