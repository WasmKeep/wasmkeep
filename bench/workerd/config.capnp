# The counter Durable Object for the comparison benchmark (bench/compare).
# The harness copies this directory, sets the port and runs
# `workerd serve config.capnp --experimental`.
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = .counterWorker),
    (name = "do-disk", disk = (path = "do", writable = true)),
  ],
  sockets = [ (name = "http", address = "127.0.0.1:PORT", http = (), service = "main") ],
);

const counterWorker :Workerd.Worker = (
  modules = [ (name = "counter.js", esModule = embed "counter.js") ],
  compatibilityDate = "2026-10-01",
  durableObjectNamespaces = [ (className = "Counter", uniqueKey = "wasmkeep-bench-counter", enableSql = true) ],
  durableObjectStorage = (localDisk = "do-disk"),
  bindings = [ (name = "COUNTER", durableObjectNamespace = "Counter") ],
);
