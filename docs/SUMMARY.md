# Summary

[What is WasmKeep?](overview.md)
[Who it is for, and use cases](use-cases.md)
[WasmKeep and Cloudflare Workers](compared-with-workers.md)
[WasmKeep and celld](compared-with-celld.md)

# Getting started

- [Your first actor](tutorials/first-actor.md)
- [From one node to three](tutorials/one-node-to-three.md)
- [Languages](languages.md)

# Guide

- [Concepts](concepts.md)
- [Running a cluster](cluster.md)
- [When a node dies, and how it scales](failover-and-scaling.md)
- [Key-value and queues](kv-and-queues.md)
- [Workflows](workflows.md)

# Deploying

- [Where to run it](deploy/where-to-run.md)
- [One server (VPS)](deploy/single-node.md)
- [Three nodes](deploy/three-nodes.md)
- [Load balancer, WAF and routing](deploy/front-door.md)
- [Back up and restore the bucket](deploy/backup-and-restore.md)
- [Upgrade a cluster to 0.4.0](deploy/upgrading-to-0.4.md)
- [Monitoring](monitoring.md)

# Reference

- [API reference](reference/api.md)
- [Benchmarks](benchmarks.md)
- [Security model](security.md)
- [Licensing and compliance](legal.md)
- [Usage ping](telemetry.md)
- [Dogfood log](dogfood.md)
- [Releasing](releasing.md)

# Design decisions

- [0001 WASI 0.2 and plain cargo](adr/0001-wasi-0.2-and-plain-cargo-for-guests.md)
- [0002 The actor WIT, v0.1](adr/0002-wasmkeep-actor-wit-v0.1.md)
- [0003 WebSockets, WIT v0.2](adr/0003-websockets-wit-v0.2.md)
- [0004 CLA for dual licensing](adr/0004-cla-for-dual-licensing.md)
- [0005 Replication format: LTX v3](adr/0005-replication-format-ltx-v3.md)
- [0006 Simulation tool](adr/0006-simulation-tool.md)
- [0007 Replication protocol](adr/0007-replication-protocol.md)
- [0008 Deploy and rolling upgrades](adr/0008-deploy-and-rolling-upgrades.md)
- [0009 Alarms in a cluster](adr/0009-alarms-in-a-cluster.md)
- [0010 Who may create an actor](adr/0010-who-may-create-an-actor.md)
- [0011 Mutual TLS between nodes](adr/0011-node-to-node-mtls.md)
- [0012 Garbage collection of actor history](adr/0012-bucket-garbage-collection.md)
- [0013 Access rules and config in deploys](adr/0013-access-rules-and-config-in-deploys.md)
- [0014 Draining a node and moving an actor](adr/0014-draining-a-node-and-moving-an-actor.md)
- [0015 KV and queues as SDK patterns](adr/0015-kv-and-queues-as-sdk-patterns.md)
- [0016 Secrets in the bucket](adr/0016-secrets-in-the-bucket.md)
- [0017 Moving one actor](adr/0017-moving-one-actor.md)
- [0018 QuickJS, the default TypeScript engine](adr/0018-quickjs-the-default-typescript-engine.md)
- [0019 Workflows as an SDK pattern](adr/0019-workflows-as-an-sdk-pattern.md)
- [0020 One lease per node](adr/0020-one-lease-per-node.md)
- [0021 The app model](adr/0021-the-app-model.md)
- [0022 App-owned state and the one-time migration](adr/0022-app-owned-state-and-the-one-time-migration.md)
- [0023 The front](adr/0023-the-front.md)
- [0024 Socket home](adr/0024-socket-home.md)
- [0025 Calling several actors at once](adr/0025-calling-several-actors-at-once.md)
