# Rate limiting

Run `wasmkeep dev` in this directory. The project file adds a host token
bucket to the example's actor-based fixed windows. The host admits up to
20 requests immediately and refills 10 tokens per second for each TCP
peer on each node, across `/actor/limit/*`.

The host rejects excess requests with 429 and `Retry-After`, before the
actor exists or reads a body. The guest remains an example of persistent
application-level windows using an actor per key and alarms; it can also
return 429 according to its own window. These are independent policies.

Host budgets reset at process restart and are not shared between nodes.
Behind a proxy, the peer is the proxy; forwarded IP headers do not change
it. Header keys require an edge that overwrites client input and prevents
clients reaching the node directly if the key is meant to be trusted.

See [project-file policies](../../docs/project-file.md#ingress-limits) for
matching, overlapping rules, capacity bounds and deployment compatibility.
