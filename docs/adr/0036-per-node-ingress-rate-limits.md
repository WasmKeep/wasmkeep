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
Paths are at most 1024 bytes and
contain no whitespace, query, fragment, or other wildcard. `rate` is a
positive decimal integer followed by `/s`, `/m`, or `/h`; `burst` is a
positive integer. Both integers are at most 1,000,000. `key` is `ip` or
`header`; header names are canonical lowercase HTTP field names, excluding
hop-by-hop, forwarding and host-internal names.

Matching is case-sensitive, excludes the query, decodes unreserved percent
escapes, canonicalizes remaining escape hex to uppercase, and collapses repeated
slashes and dot segments (`.` and `..`, including percent-encoded forms). Exact rules ignore trailing slashes. A `GET` rule also covers `HEAD`;
a `HEAD` rule covers only `HEAD`. Actor names use the router's full percent
and UTF-8 decoding, then canonical percent encoding to preserve segment boundaries.
Other reserved escapes stay encoded: the paths in `https://example.test/a%2Fb`
and `https://example.test/a/b` differ. Prefix rules retain their final slash;
a prefix covers that slash and descendants, not the path without the slash. This normalization is only for policy matching; it does not rewrite
the URI delivered to routing or guest code.

A bucket belongs to one app, named rule and client on one node. IP means
the TCP peer, with IPv6 aggregated to 64-bit subnet prefixes and IPv4-mapped IPv6 treated as IPv4,
never an untrusted forwarding header. This release has no
trusted-proxy address configuration: proxies share their peer quota; use
an edge-overwritten header key when appropriate. Header keys require one
nonempty non-whitespace printable ASCII value of at most 256 bytes. Invalid or missing
keys return 400. A header key is not authentication.

Admission uses token buckets with monotonic time and integer fractional
credits. All matched rules are checked together; only successful admission
spends credits. Rejection returns 429, a short text body, and `Retry-After`
as the largest required wait rounded up to seconds. Admission precedes
body reads, front slots and guest actor work, including direct actor routes and
WebSocket handshakes. Authenticated cluster traffic is not charged again;
public header claims cannot mark a request as internal.

Actor ownership resolution may precede admission; guest execution and body reads
still follow it. A forwarding node does not charge public requests or WebSocket handshakes. When
its cluster secret is already loaded, it replaces client-supplied proofs with a
domain-separated signed client identity: the normalized TCP IP key (IPv6 by its 64-bit prefix,
IPv4-mapped addresses equivalent to IPv4), bound to the destination owner, method,
path and query, app, timestamp, and end-to-end header values. The owner verifies
this proof and charges its own policies using that IP or the policy's header key.
Replays are charged again; there is no ingress replay store or receipt throughput
ceiling. The proof grants no actor privileges and does not authenticate the body.
Missing, invalid or expired proofs on forwarded requests return 503 when the
owner has policies. Without policies, unsigned forwards retain their previous
behavior. Signing never fetches a cold secret. Clock skew outside the signature
window still invalidates a proof on a policy-enforcing owner.
Forwarded public traffic is charged on both plaintext and mutual-TLS transports;
authenticated internal actor calls remain exempt. Already-spent internal call
signatures return 403 before body reads in plaintext mode; fresh signatures still
undergo body and atomic replay checks before actor work.

Only app services with policies share the 65,536-bucket process budget, capped
at 4,096 buckets per service. Publishing or removing policies and dropping a
service rebalance shares. Under pressure, fully refilled buckets are evicted
first; only then are depleted buckets evicted in least-recently-used order.
Current request buckets are protected. Refill eligibility is computed without
changing unrelated buckets' credits or clocks. Rejected quota attempts count as
uses. Evicted identities get a fresh burst if they return, so cycling more keys
than the actual policy-service share can still weaken enforcement; these are
bounded admission controls, not durable quotas.
If an admission needs more simultaneous rule buckets than its app share, it
returns 429 with `Retry-After: 1` and the capacity outcome. Header values are hashed and never appear in logs
or metric labels. Counters distinguish admission, quota, invalid-key and capacity
outcomes; a gauge reports retained buckets.

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

Offline or later-joining older binaries are not protected by the live-reader
check: they may treat format 6 as corrupt and serve without limits. Before
enabling policies, stop and upgrade every node, including offline nodes;
prevent an older binary from rejoining. Fail-closed handling of undecodable
manifests is a guarantee of rate-limit-aware binaries only.

With neither current nor pending policies, ingress admission returns immediately:
it does not change response ordering, capture a front snapshot, or increment rate
counters. Bucket read errors do not invent pending policies; corrupt manifests do.
The metrics endpoint still registers `wasmkeep_rate_limit_requests_total` and
`wasmkeep_rate_limit_active_keys` even when no policy exists. These metrics are the
intentional exception to no-policy compatibility.

## Verification

Parser errors include source positions. Tests cover fractional refill,
atomic overlap, capacity pressure, concurrent admission, policy updates,
legacy bytes, capability refusal, and ingress rejection before guest work.
The strict-write golden trace remains unchanged.
