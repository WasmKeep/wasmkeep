# 0036. Per-node ingress rate limits

- Status: proposed
- Date: 2026-11-16
- Release: 0.5.0

## Decision

An app may declare up to 32 `[limits.<name>]` tables in `wasmkeep.toml`.
Names follow `[a-z][a-z0-9-]{0,63}`. Each table contains `match`, `key`,
`rate`, `burst`, and, only with `key = "header"`, `header`. Unknown fields
are errors. `match` is an optional uppercase standard HTTP method and one
space, followed by an absolute path: either exact or ending in `/*`.
Matching uses the encoded URI path without the query; `/api/*` matches
`/api/` and its descendants, not `/api`. Paths are at most 1024 bytes and
contain no whitespace, query, fragment, or other wildcard. `rate` is a
positive decimal integer followed by `/s`, `/m`, or `/h`; `burst` is a
positive integer. Both integers are at most 1,000,000. `key` is `ip` or
`header`; header names are canonical lowercase HTTP field names, excluding
hop-by-hop, forwarding and host-internal names.

A bucket belongs to one app, named rule and client on one node. IP means
the TCP peer, never an untrusted forwarding header. This release has no
trusted-proxy address configuration: proxies share their peer quota; use
an edge-overwritten header key when appropriate. Header keys require one
nonempty non-whitespace printable ASCII value of at most 256 bytes. Invalid or missing
keys return 400. A header key is not authentication.

Admission uses token buckets with monotonic time and integer fractional
credits. All matched rules are checked together; only successful admission
spends credits. Rejection returns 429, a short text body, and `Retry-After`
as the largest required wait rounded up to seconds. Admission precedes
body reads, front slots and actor work, including direct actor routes and
WebSocket handshakes. Authenticated cluster traffic is not charged again;
public header claims cannot mark a request as internal. Legacy plaintext
forwarding carries a domain-separated, single-use signed admission receipt;
it never grants actor caller privileges.

There are at most 4,096 client buckets per app and 65,536 per process.
Only fully refilled buckets may be reclaimed. If space is unavailable,
admission returns 429 with a bounded retry delay and a separate capacity
counter. Client header values are hashed; they never appear in logs or
metric labels. Counters distinguish admission, quota rejection, invalid
keys and capacity rejection; a gauge reports retained buckets.

Code, routing and policies are published together. Unrelated deployments
retain credits. Changed rules retain at most their previous available
whole tokens under the new burst, never gaining a new burst merely through
a policy edit. Full project deployments replace all policies; partial
legacy deployments preserve them. Budgets reset on process restart and
are not shared across nodes. Listener-level overload still needs edge
protection.

## Compatibility

ADR 0034 reserves manifest format 5. Nonempty policies therefore require
format 6 and the `rate_limits` capability, in addition to older reader
capabilities. Policies are serialized in name order and participate in
version identity. Each record is `limit NAME METHOD PATH KEY RATE BURST`,
with `*` for any method and `ip` or `header:FIELD` for the key. This keeps
all valid header names distinct from the IP key. Empty policies add no
bytes to legacy manifests.
Deployment and rollback refuse any eligible live reader lacking support,
without a force override. When a current or pending deployment has policies, a node unable to apply
it refuses public intake, including direct actor requests. An undecodable
manifest is treated as potentially requiring policies. Legacy front-only
failures without policies retain their previous behavior.

## Verification

Parser errors include source positions. Tests cover fractional refill,
atomic overlap, capacity pressure, concurrent admission, policy updates,
legacy bytes, capability refusal, and ingress rejection before guest work.
The strict-write golden trace remains unchanged.
