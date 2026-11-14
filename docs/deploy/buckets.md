# Buckets

Everything a WasmKeep cluster shares lives in one S3-compatible bucket:
the actors' databases, who owns which actor, the nodes' leases, deploys
and secrets. The cluster is only as safe as the bucket's guarantees, and
a store that accepts a request but quietly ignores one of its conditions
loses data with no error anywhere. `wasmkeep cluster diagnose` checks
those guarantees on a bucket before you trust a cluster to it.

## What WasmKeep needs from a bucket

| Guarantee | What relies on it |
| --- | --- |
| **Create if absent** (`PUT` with `If-None-Match: *`) is refused when the key exists, and exactly one of several simultaneous creates wins | The first ownership record of an actor, the alarm index, deploys: two nodes must never both believe they created the same record |
| **Replace if the version matches** (`PUT` with `If-Match: <etag>`) is refused on a stale version or a deleted object, and accepted with the current version, whether it came from the write or from a read | Node leases and ownership records ([ADR 0020](../adr/0020-one-lease-per-node.md)): a renewal, a hand-over or a fence lands only if nothing was written in between |
| **Read after write**: a read right after a create, replace, overwrite or delete sees it | A node reads its own lease, records and snapshots back |
| **Listings** show a new key and stop showing a deleted one within seconds, and return every key past the first page (S3: 1,000 keys) | Heartbeats and peer discovery, the alarm index, garbage collection, `cluster migrate` |
| **Deleting an absent key** succeeds and removes nothing else | Garbage collection and the alarm index delete keys that may already be gone |
| **Large objects** (one `PUT` of a database snapshot) come back whole | Restores |
| **Last-modified times**, whole seconds at least, never earlier for a later write, the same in a head and a listing | `cluster migrate` compares them to tell writes after its commit from writes before; `cluster fence` checks this machine's clock against the bucket's |
| **The clock**: the bucket's time and each node's within the lease's allowed skew (1 s with the default timings) | Leases are measured on each node's clock ([how](../cluster.md#how-it-works)) |

Every node also proves the first two at start, on a throwaway object
under `probe/`, and refuses to run on a store that ignores them. `cluster
diagnose` goes further, and measures what the start check cannot.

## Running `cluster diagnose`

Run it from a node, or any machine with the nodes' `AWS_*` settings
([by provider](three-nodes.md#connection-settings-by-provider)) and
credentials that can read, write, list and delete:

```bash
wasmkeep cluster diagnose --bucket s3://wasmkeep-demo
wasmkeep cluster diagnose --bucket s3://wasmkeep-demo/prod --json      # for scripts
wasmkeep cluster diagnose --bucket s3://wasmkeep-demo --prefix-for-test diagnose/today
wasmkeep cluster diagnose --bucket s3://wasmkeep-demo --skip-paging   # no 1,001-key listing
```

It writes only under a scratch prefix, `diagnose/run-<time>-<pid>/`
below the URL's prefix (or `--prefix-for-test`, also below it), never at
the prefix itself, so it can run beside a live cluster without touching
its keys. It refuses a scratch prefix that already holds an object, and
deletes everything it wrote when it ends, whether the checks passed or
not; if that cleanup fails it says how many objects are left and where.
A run takes a few seconds next to the bucket (the paging check writes
and deletes 1,001 empty objects; `--skip-paging` leaves it out).

```text
Bucket s3://wasmkeep-demo, writing only under s3://wasmkeep-demo/diagnose/run-1794637469714-478230/

pass   start-check           the check every node runs at start passes
pass   create-if-absent      create over an existing object refused; create after a delete accepted
pass   concurrent-creates    1 of 8 simultaneous creates won, 7 refused
pass   replace-if-match      current version (written or read) accepted; stale and deleted refused
pass   read-after-write      5 of 5 reads right after a create were current (2 ms per write and read)
pass   read-after-overwrite  10 of 10 reads right after a replace or overwrite were current
pass   list-new-key          listed after 1 ms
pass   list-deleted-key      gone from the listing after 1 ms
pass   delete-absent         absent keys delete without error; a deleted key reads absent at once
pass   large-object          16 MiB round trip intact (write 0.0 s, read 0.0 s)
pass   last-modified         whole-second precision, in write order; head and listing agree within 0 ms
pass   clock-skew            bucket minus this machine between -0.0 s and +1.0 s (nodes tolerate 1.0 s)
pass   list-paging           1001 of 1001 keys listed

cleanup: s3://wasmkeep-demo/diagnose/run-1794637469714-478230/ is empty again
safe for WasmKeep
```

Each line is one check: `pass`, `warn` (it holds, with a value worth
knowing, such as a listing that lags more than a second), `FAIL` (a
guarantee is broken), `error` (the check could not finish: no answer, an
unexpected error) or `skip`. The last line is the verdict:

| Exit code | Verdict | Meaning |
| --- | --- | --- |
| 0 | `safe for WasmKeep` | Every check passed (warnings included) |
| 1 | `not safe: <checks> failed` | The bucket broke a guarantee: do not run a cluster on it |
| 2 | `could not finish: ...`, or an error before any check | No verdict: the bucket did not answer, refused the credentials, or the scratch prefix was not empty. Try again |

A bucket that does not answer is never reported as unsafe. The clock
check brackets one write between two readings of this machine's clock
and compares the bucket's last-modified time with them; it fails only
when the whole interval is beyond the allowed skew, and then the clock
to fix may be this machine's: run it again from another. `--json`
prints the same as one object: `checks` (`name`, `status`, `value`),
`cleanup`, `verdict` (`safe`, `unsafe`, `incomplete`), `summary` and
`exit_code`.

Checking that each live node answers on its health and cluster ports is
not part of it yet; `cluster status` shows the nodes the bucket knows.

## Providers checked

Each row is a run of `cluster diagnose` (all checks, paging included)
from the WasmKeep version shown. Other providers may well work; they
have not been checked this way yet.

| Provider | Date | WasmKeep | Verdict |
| --- | --- | --- | --- |
| moto 5.2.5 (the S3 server of the test suites, in memory) | 2026-11-14 | `main` after 0.3.0 | safe for WasmKeep |
| OVHcloud Object Storage (S3 API), from a VPS in the same region | 2026-11-14 | `main` after 0.3.0 | safe for WasmKeep: listings within 14 ms, 16 MiB written in 0.7 s and read in 0.2 s, whole-second last-modified times |
| Cloudflare R2 | | | not checked yet |
| AWS S3 | | | not checked yet |
| MinIO | | | not checked yet |

R2 and OVHcloud also passed the storage, failover and cluster suites and
a soak ([three nodes](three-nodes.md#connection-settings-by-provider));
that is a separate test from this one.
