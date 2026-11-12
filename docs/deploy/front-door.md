# The front door: load balancer, WAF and routing

How traffic reaches a cluster on dedicated servers or virtual machines.

```
clients ── HTTPS, WebSockets
   │
   ▼
WAF / CDN (optional)        Cloudflare, or Coraza/ModSecurity at the proxy
   │
   ▼
load balancer               HAProxy, nginx, Caddy or a cloud LB: terminates TLS
   │  plain HTTP to any node's --listen port, on the private network
   ▼
node a     node b     node c       any node takes any request;
   └── forwards to the actor's owner, at most one hop ──┘
                    │
                 bucket            leases, heartbeats, replication
```

- **TLS ends at the load balancer.** Nodes serve plain HTTP on `--listen`;
  bind it to the private network.
- **Any node will do.** If another node owns the actor, the request is
  forwarded there (once, never twice), and the response says which node
  served it (`x-wasmkeep-node`).
- **Health-check `GET /_wasmkeep/ready`**: `503` while a node shuts down,
  cannot reach the bucket or is nearly out of disk
  ([monitoring](../monitoring.md)). `/_wasmkeep/health` only says the
  process answers.
- **WebSockets** need the upgrade passed through and a tunnel or idle
  timeout above 90 s (WasmKeep pings an idle socket after 60 s and closes
  it 30 s later if there is no answer).
- **A server timeout above the failover wait.** When a node dies, the
  others keep a `GET`, a `HEAD` or a WebSocket upgrade for one of its
  actors until they took the actor over, up to 25 s by default
  (`--failover-wait-secs`, see [what a caller sees when a node
  dies](../cluster.md#when-a-node-dies-what-a-caller-sees)), and a kept
  request sent on to the node that took the actor can wait 30 s more
  there. A load balancer that gives up on the node sooner turns that wait
  into its own 504: give it 60 s or more for the node's answer, or lower
  the wait on the nodes to fit.
- **Tell the node when the client is gone.** A kept request is dropped,
  and never run, when its connection closes. If the load balancer keeps
  its connection to the node open after its own client left, the node
  cannot know, and runs the request for nobody once the actor is taken
  over.
- **Never expose** the metrics listener (`--metrics-listen`) or the
  node-to-node traffic to the internet.

## Route by actor (recommended)

Hash the actor's path, `/actor/<class>/<name>`, so that every request for
an actor reaches the same node. The first node to get a request for an
actor nobody owns takes its lease, so actors settle where their traffic
arrives and forwarding all but disappears. It is only a hint: when nodes
come and go, ownership still follows the leases, and a request that lands
elsewhere is forwarded.

**HAProxy**: `balance uri depth 3` hashes the first three path segments
(`/actor`, the class, the name):

```haproxy
frontend public
    bind :443 ssl crt /etc/haproxy/site.pem alpn http/1.1
    # Client IP for actors: replace whatever the client sent.
    http-request del-header X-Forwarded-For
    option forwardfor
    default_backend wasmkeep

backend wasmkeep
    balance uri depth 3
    hash-type consistent
    option httpchk GET /_wasmkeep/ready
    http-check expect status 200
    # Above the failover wait (25 s) plus a forward to the new owner.
    timeout server 60s
    timeout tunnel 1h
    # A node that refuses the connection: try another one.
    retries 2
    option redispatch
    # The client left while its request waited: close the node's side too.
    option abortonclose
    default-server inter 2s fall 2 rise 2
    server a 10.0.0.11:8080 check
    server b 10.0.0.12:8080 check
    server c 10.0.0.13:8080 check
```

`timeout server` is how long HAProxy waits for a node's answer: it must
be longer than the nodes' failover wait, or HAProxy answers 504 while the
node still holds the request. `option redispatch` with `retries` sends a
request whose connection a node refused (it just died, and its health
check has not failed yet) to another node, where it can be kept, instead
of failing it; it applies to connections that could not be opened, so no
request is sent twice. `option abortonclose` asks HAProxy to give up a
request whose client closed its connection before an answer, and so to
close its connection to the node: that is what lets a node drop a kept
request nobody waits for. This example was not run against HAProxy with a
kept request: check with your version that a client's departure closes
the node's side while HAProxy waits for the answer (the node then counts
`outcome="left"` in `wasmkeep_failover_waits_total`). Another proxy needs
its own equivalent; without one, a kept `GET` runs after the takeover for
a client that has left.

**nginx**: hash a variable holding the actor's path:

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}
map $uri $wasmkeep_actor {
    ~^(/actor/[^/]+/[^/]+)  $1;
    default                 $uri;
}
upstream wasmkeep {
    hash $wasmkeep_actor consistent;
    server 10.0.0.11:8080;
    server 10.0.0.12:8080;
    server 10.0.0.13:8080;
}
server {
    listen 443 ssl;
    location / {
        proxy_pass http://wasmkeep;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_set_header X-Forwarded-For $remote_addr;     # replace, never append
        proxy_read_timeout 1h;
    }
}
```

Open-source nginx has no active health checks: pair it with an external
check of `/_wasmkeep/ready`, or use HAProxy. **Caddy** is the simplest for
one node (see [one server](single-node.md#3-put-a-reverse-proxy-in-front));
for a cluster its `lb_policy uri_hash` hashes the whole URI, so requests to
different paths of one actor may land on different nodes (correct, with a
forwarding hop).

## Client addresses

WasmKeep passes request headers through to actors unchanged, except its own
`x-wasmkeep-*` (stripped or set) and, when a node forwards a request to the
actor's owner, hop-by-hop headers (`Connection`, `Keep-Alive`, `TE`,
`Upgrade` and the like, and any header `Connection` names; RFC 9110
section 7.6.1). An actor that reads
`X-Forwarded-For` trusts whatever the load balancer put there: the load
balancer must **replace** the header, not append to it, and nodes must be
reachable only through it.

## Static files

Serve static files (HTML, JavaScript, CSS, images, fonts, video,
documents) from the front door, not from actors: the reverse proxy itself,
or object storage behind a CDN. Route actor paths (`/actor/...`) to
WasmKeep, and everything else to the static origin.

Actors are for state with an identity: one instance per name, one event
at a time, a lease and a database. A file has neither. Served by an actor,
every request would go through the instance pool and the Wasm sandbox,
take the actor's lease (so one node serves it, not all of them), and the
guest would have to implement caching headers (`ETag`, 304 answers) and
range requests (needed for video) itself. A web server or CDN does all of
that from disk or cache at almost no CPU cost.

**Caddy**, files from a directory and actors on the nodes:

```caddy
example.com {
    @actors path /actor/* /_wasmkeep/*
    handle @actors {
        reverse_proxy 10.0.0.11:8080 10.0.0.12:8080 10.0.0.13:8080 {
            health_uri /_wasmkeep/ready
        }
    }
    handle {
        root * ./public
        file_server
    }
}
```

With a CDN, the same split works: the static paths go to a bucket or
static host, the rest to your load balancer.

A few small files embedded in a component (an admin page, a favicon) are
fine; anything larger, changing often, or large media belongs on the
static origin. Serving assets directly from the nodes, without an actor,
is a possible built-in for later ([roadmap](../../ROADMAP.md#later-on-demand)).

## WAF and rate limits

Filter and rate-limit at the edge: a CDN such as Cloudflare, or a WAF at
the proxy (OWASP Core Rule Set with Coraza or ModSecurity), and per-client
rate limits in the load balancer (HAProxy stick tables, nginx
`limit_req`). WasmKeep's own limits are a backstop, not a WAF: a connection
cap, 30 s to send headers, 16 MB request bodies, WebSocket message and
socket caps ([security model](../security.md)). Keep the edge's body limit
at or below 16 MB.

## Without a separate load balancer

Every WasmKeep node already routes: whichever node a request reaches, it is
served by the actor's owner. What a load balancer adds is one address that
survives a node dying, TLS, and filtering. The three servers can provide
those themselves.

| Setup | Pros | Cons |
| --- | --- | --- |
| **DNS round robin, Caddy on every node** | No extra machine; automatic certificates; the simplest | A dead node keeps getting a share of new connections until DNS caches expire (browsers usually retry another address, many API clients do not); no health checks; no routing by actor, so more forwarding |
| **A floating IP moved by keepalived, HAProxy on the nodes** | No extra machine; failover in seconds; health checks and routing by actor | One node takes all incoming traffic at a time; keepalived to set up |
| **A managed load balancer** (Hetzner, DigitalOcean, AWS...) | Nothing to run; health checks | A small monthly cost; some cannot hash on a path prefix |

### DNS round robin with Caddy

Publish one name with every node's public address (three `A` or `AAAA`
records, short TTL), and run Caddy beside WasmKeep on each node. WasmKeep
listens on the node's private address (`--listen 10.0.0.11:8080`, the
address the other nodes forward to); Caddy takes the public one:

```caddyfile
app.example.com {
    reverse_proxy 10.0.0.11:8080 {
        header_up X-Forwarded-For {remote_host}   # replace, never append
    }
}
```

Each node gets its own certificate. Requests for an actor owned elsewhere
are forwarded by WasmKeep over the private network.

### A floating IP with keepalived

Most providers sell an address you can move between servers (Hetzner,
OVH, Scaleway, DigitalOcean reserved IPs, or a VRRP address on your own
network). keepalived on each node holds it on one healthy node and moves
it within seconds when that node or its WasmKeep stops answering. HAProxy
on every node (configured as above, with routing by actor and
`/_wasmkeep/ready` checks against all three nodes) terminates TLS on the
floating address:

```keepalived
vrrp_script wasmkeep_ready {
    script "/usr/bin/curl -fsS --max-time 2 http://10.0.0.11:8080/_wasmkeep/ready"
    interval 2
    fall 2
    rise 2
}

vrrp_instance front {
    state BACKUP              # all nodes start as backup; priority decides
    interface eth1            # the private network
    virtual_router_id 51
    priority 100              # 100, 90, 80 on the three nodes
    advert_int 1
    unicast_peer { 10.0.0.12 10.0.0.13 }   # the other nodes
    virtual_ipaddress { 203.0.113.10/32 dev eth0 }
    track_script { wasmkeep_ready }
}
```

On providers whose floating IP is moved through an API (Hetzner Cloud,
DigitalOcean), call it from keepalived's `notify_master` script instead of
announcing the address with `virtual_ipaddress`. Bind HAProxy to the
floating address with `net.ipv4.ip_nonlocal_bind = 1`, so every node can
start it before holding the address.

### Plain DNS round robin, no proxy at all

Not recommended: nodes speak plain HTTP, so there is no TLS without a proxy
in front, and WasmKeep has no rate limiting or filtering of its own beyond
its protective limits.


