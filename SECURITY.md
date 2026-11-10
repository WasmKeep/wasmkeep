# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately, through this repository's
**Security → Report a vulnerability** (GitHub private vulnerability
reporting), not in a public issue or pull request.

Include what you can: the affected version or commit, how to reproduce,
and the impact you see.

WasmKeep is an experimental, pre-alpha project, not ready for production.
Reports are handled on a best-effort basis: we address them as fast as we
can, but there is no guaranteed response or fix time. We disclose in
coordination with you and credit you unless you prefer otherwise.

## Supported versions

WasmKeep is pre-alpha: fixes land on `main` and ship in the next release;
earlier releases receive none.

## Scope

In scope: anything that breaks the guarantees in
[docs/security.md](docs/security.md), for example an actor escaping its
sandbox or reaching another actor's data, a client creating actors of an
`existing` class or reaching an `internal` one, a client passing for a node
or an actor, or a way to exhaust a node past the documented limits.

Out of scope while WasmKeep is pre-alpha: side channels between tenants in
one process (see the security model), and attacks that need write access to
the cluster's bucket (whoever has it controls the cluster).
