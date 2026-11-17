# Local inspector

Run `wasmkeep inspect --app TENANT/APP --bucket s3://BUCKET/PREFIX`.
The bucket can instead come from `WASMKEEP_BUCKET`; credentials, endpoint
and region use the same `AWS_*` settings as `actor sql`. Open the printed
URL locally. `--port 8765` selects a port; the default is a free port.
`--bind ::1` selects IPv6 loopback. Every other address except `127.0.0.1`
is refused. Stop the command with Ctrl-C.

The embedded page shows the cluster's nodes, processes and apps, deployed
classes and classes with retained history, actors in pages of 50, and a
selected actor's head, retained ancestry and recent history objects. Actor
names are shown in their encoded bucket form. Classes and actors use a
lexical cursor; concurrent bucket changes can affect later pages. Listings stop with an explicit error above 10,000 objects, including
objects scanned to discover classes and apps. Metadata bodies (current,
manifest and heartbeat) are capped at 4 MiB before buffering.
There is no CDN, framework, node endpoint or new listener on nodes.

## Read-only SQL

The SQL box uses the same verified, lease-free bucket export and constrained
SQLite reader as [`actor sql`](actor-data-access.md). It neither wakes an
actor nor writes to the bucket. Use bucket credentials restricted to LIST
and GET. SQL runs on a temporary local plaintext copy, removed when the
query completes. Interruption while a SQL worker owns the copy, or forcibly
killing the process, can leave scratch files. Only one bucket operation runs
at a time.

One statement may return at most 10,000 rows and 16 MiB of results, with
1 MiB per value. Execution stops after five seconds or ten million SQLite
VM instructions. Writes, PRAGMAs, extension loading, host tables and schema
tables are denied. Export is capped at 2 GiB and uses the existing replay
checks and retry deadlines. Integers are JSON decimal strings to preserve
64-bit precision; blobs are base64 objects. Hashed long actor names cannot
be reversed: use `actor sql` with the original name for those actors.

These are independent bucket reads, not a consistent cluster-wide snapshot.
A head may contain unacknowledged uploads. Ownership generation is only a
hint, not proof that the head is settled. Collection can remove history
while it is being read; the exporter retries or reports failure. See
[ADR 0033, section 1](adr/0033-data-access-export-and-online-backup.md#1-reading-an-actor-without-its-owner).

## Local access

The launch URL contains a fresh 256-bit random, one-time bootstrap token.
The page immediately removes it from the current browser history entry with
`history.replaceState`, then exchanges it once for an independent session
bearer. The bearer stays in the page script's memory and is sent only in the
Authorization header, never a cookie or URL. Reloading requires restarting
the inspector for a new bootstrap URL. A server on another loopback port
receives no inspector credential from the browser.

Treat the launch URL as a secret: it remains exposed in terminal output,
terminal recording and copied logs, and browser-launch command arguments
(argv). Browser tooling or history synchronization may observe it before
replacement; history replacement is not guaranteed erasure from those
systems. A stolen unused token can win the first exchange. Restarting the
inspector invalidates both credentials.

The page requires a valid bootstrap or session bearer; API requests require
the session bearer. Scripts are authorized by a CSP SHA-256 hash.
The exact loopback address and bound port must appear in Host; `localhost` and DNS
names are rejected. Cross-origin requests are rejected, no CORS headers
are emitted, and responses use `Cache-Control: no-store` and
`Referrer-Policy: no-referrer`. This is a local operator tool, not a public
service; do not expose it through a proxy or tunnel.
