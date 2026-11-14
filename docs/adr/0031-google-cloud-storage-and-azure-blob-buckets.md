# 0031. Google Cloud Storage and Azure Blob buckets

- Status: proposed (2026-11-14). On paper only: nothing here has been run
  against either provider. The property table was built from each
  provider's documentation and from the source of `object_store` 0.14.2,
  read on 2026-11-14. Only the real test in Verification, run from
  inside each provider's network, can accept it.
- Date: 2026-11-14
- Milestone: [Next](../../ROADMAP.md#next), `0.5.0`, item 6, "More buckets"
- Follows: [ADR 0005](0005-replication-format-ltx-v3.md) and
  [ADR 0007](0007-replication-protocol.md) (replication),
  [ADR 0009](0009-alarms-in-a-cluster.md) (the alarm index),
  [ADR 0012](0012-bucket-garbage-collection.md) (garbage collection),
  [ADR 0016](0016-secrets-in-the-bucket.md) (secrets),
  [ADR 0020](0020-one-lease-per-node.md) (node leases and ownership
  records, with the listing amendments of 2026-11-14), and
  [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md)
  (`cluster migrate`).
- Touches: [ADR 0030](0030-files-and-static-assets.md), files and static
  assets (proposed).
  Its presigned links are S3-specific; section 7 says what each new
  backend can offer.

## Context

A cluster keeps everything it shares in one bucket, opened only through
`object_store`'s `aws` feature. `open_bucket_unchecked` in
`crates/wasmkeep-cli/src/main.rs` builds an `AmazonS3Builder::from_env()`,
and `parse_bucket` accepts only `s3://BUCKET[/PREFIX]`. `diagnose.rs`
rebuilds its scratch URL from `s3://` too. Google Cloud Storage can be
reached that way through its S3-interop XML API, but conditional writes do
not carry over (section 1). Azure Blob has no S3 API at all. The adapter,
`crates/wasmkeep-cluster/src/object_store.rs`, already speaks
`object_store`'s generic `PutMode::Create` and `PutMode::Update`, and its
opaque `Version` carries both the ETag and the store's version id. Most of
the work is therefore opening the store and proving it, not changing the
protocol.

`object_store` is pinned at `=0.14.2` (workspace `Cargo.toml`). Every
statement below about what it sends is from that version's source.

### What WasmKeep relies on, by property

Taken from the ADRs above, `docs/deploy/buckets.md` and the code:

| # | Property | Relied on by |
| --- | --- | --- |
| P1 | **Create if absent** is refused over an existing object, and exactly one of several simultaneous creates wins | First ownership record, LTX segments (ADR 0007: "all writes are conditional creates"), alarm index entries, deploys, sealed secrets (ADR 0016), the start probe |
| P2 | **Replace if the version matches** is refused on a stale version, a deleted object, or a version from before a plain overwrite. It is accepted with the current version, whether that came from the write or from a read. Exactly one of several simultaneous replaces wins | Node leases and ownership records (ADR 0020), `current` pointers for deploys and secrets (ADR 0016), the migration lock (ADR 0022) |
| P3 | **A version token** is returned by a write and by a read, and is what the store's conditional replace takes | `ObjectStoreBucket::{get, create, replace}` (`encode_version` and `decode_version`) |
| P4 | **Read after write, overwrite and delete** | A node reads back its own lease, records and snapshots |
| P5 | **Listing after write and delete** within seconds, every key past the first page | Heartbeats and peer discovery (ADR 0020 amendment), intents (ADR 0014), the alarm sweep (ADR 0009), garbage collection (ADR 0012), `cluster migrate` |
| P6 | **Listing in byte order, and listing from an offset** (`list_with_offset`, exclusive) | The alarm sweep resumes from a listed key and stops at the first entry not yet due (`crates/wasmkeep-server/src/cluster.rs`, ADR 0009). Fixed-width hex keys make listing order transaction order (ADR 0007) and `seq` order (ADR 0016). Other listings are sorted after the fact (`object_store.rs` sorts them) |
| P7 | **Deleting an absent key** succeeds and removes nothing else | Garbage collection, the alarm index |
| P8 | **Large objects** in one PUT come back whole | Snapshots, restores |
| P9 | **Last-modified**: whole seconds at least, in order for writes a second apart, the same in a head and a listing | `cluster migrate`, ADR 0012's retention window, ADR 0014's stale move keys, `cluster fence` |
| P10 | **The clock**: the bucket's time within the lease's allowed skew (1 s) of each node's | Leases are measured on each node's clock (ADR 0020) |
| P11 | **Request rate**: a node lease rewritten every 3 s, records rewritten on every hand-over, without throttling that outlasts a lease attempt (2 s at the defaults, ADR 0020 section 1) | Node leases |
| P12 | **Presigned links** that bind the method, the key, the expiry and, for uploads, the size, the type and create-if-absent | ADR 0030 (proposed) |
| P13 | **Credentials from the environment**, kept in `/etc/wasmkeep/env` | Every node and CLI command |

## Decision

Add two backends beside `s3://`: `gs://` through `object_store`'s `gcp`
feature, and `az://` through its `azure` feature. They are native APIs,
not S3 emulation. The protocol does not change. Each new backend must pass
the same start probe as an S3 bucket, adjusted where its API differs
(section 3), and is listed as supported only after the real test
(Verification).

### 1. What each provider gives, property by property

Sources are numbered at the end of this record ([G*] for Google, [A*] for
Microsoft, [O*] for `object_store`). Every page was fetched on 2026-11-14.

| # | Google Cloud Storage via `object_store` `gcp` | Verdict | Azure Blob via `object_store` `azure` | Verdict |
| --- | --- | --- | --- | --- |
| P1 | `PutMode::Create` sends `x-goog-if-generation-match: 0` [O1]. The value 0 means the request "only proceeds if no object with the specified name exists" (noncurrent versions do not count). A failed match is 412 [G1]. `object_store` maps the 412 to `AlreadyExists` [O1] | **Holds** (documented) | `PutMode::Create` sends `If-None-Match: *` [O2], which fails the operation if the blob exists [A1]. Writes that fail a condition return 412 [A1][A3]. `object_store` maps 412, and Azurite's 409, to `AlreadyExists` [O2] | **Holds** (documented) |
| P2 | `PutMode::Update` sends `x-goog-if-generation-match: <generation>`, taken from `UpdateVersion::version`. Without one it fails with `MissingVersion` before sending [O1]. A generation is unique per object version and changes whenever the data is replaced [G8][G2]. A mismatch is 412 [G1] | **Holds**, through the generation rather than the ETag | `PutMode::Update` sends `If-Match: <etag>`, taken from `UpdateVersion::e_tag`. Without one it fails with `MissingETag` [O2]. The ETag is "updated every time a write operation is performed" [A3]. A mismatch is 412 [A1][A3] | **Holds**, through the ETag |
| P3 | Reads and writes take the generation from `x-goog-generation` [O1]. A listing (XML `ListBucketResult`) has no generation, so a `Version` built from a listing could not be used for a replace. No code does that today | **Holds** for reads and writes. Whether a PUT response carries `x-goog-generation` is checked by the probe (open question 1) | Reads and writes carry ETag. `x-ms-version-id` appears only with blob versioning on [A2][A20], and Update does not use it | **Holds** |
| P4 | Strongly consistent read-after-write, read-after-overwrite (new objects and replacements) and read-after-delete [G5]. Exceptions: cached publicly readable objects, IAM changes (about a minute), bucket configuration changes [G5]. WasmKeep's bucket is private | **Holds** | Strong consistency: "subsequent read or list operations return the latest update" [A3]. LRS and ZRS writes are synchronous. Geo-redundant secondaries lag the primary [A5], and WasmKeep never reads them | **Holds** (primary endpoint only) |
| P5 | Object listing is strongly consistent [G5]. XML listings with `list-type=2` page by `continuation-token`, and `IsTruncated` must be checked because a page can be short [G6]. The JSON API recommends at most 1,000 per page [G7] | **Holds** | List Blobs is strongly consistent [A3]. `maxresults` is capped at 5,000, a page may be short with a `NextMarker`, and the listing loops until the marker is empty [A6] | **Holds** |
| P6 | Results are "ordered lexicographically by name" [G6]. `object_store` sends `start-after` (exclusive) for `list_with_offset` [O1][G6] | **Holds** on flat buckets (hierarchical-namespace buckets: open question 5) | "Blobs are listed in alphabetical order ... with upper-case letters listed first" (byte order for the ASCII keys WasmKeep writes). With a **hierarchical namespace**, "`/` is treated as the lowest sort order" [A6], which breaks byte order. `object_store` sends `startFrom` (inclusive, version 2023-05-03 and later [A6]; it sends 2023-11-03 [O6]) and drops the offset key itself. On Azurite it lists everything and filters on the client [O3] | **Holds on flat-namespace accounts only**. HNS accounts are refused (section 4) |
| P7 | Deleting an absent object is a 404, which `object_store` treats as success, as it does for S3 (to be confirmed by `delete-absent`) | **Expected**; diagnose checks it | Same: 404 `BlobNotFound`, treated as success (to be confirmed by `delete-absent`) | **Expected**; diagnose checks it |
| P8 | Objects up to 5 TiB [G9]. A snapshot is one PUT | **Holds** | One Put Blob up to 5,000 MiB [A2] | **Holds** (above any snapshot today) |
| P9 | `Last-Modified` is "the date and time that the object was last modified". Listings report the time the *metadata* was last modified [G6], which for objects WasmKeep never patches should be the same. Precision is not documented | **Unconfirmed**; diagnose measures it (`last-modified`) | RFC 1123 times, so whole seconds [A7]. Any write, metadata included, changes it [A2] | **Holds** |
| P10 | No documented statement about server time | **Measured only** (`clock-skew`) | No documented statement. SAS allows up to 15 minutes of skew [A10], which is unrelated | **Measured only** (`clock-skew`) |
| P11 | "One write per second" to the same object name, after which writes may be throttled [G9]. About 1,000 writes and 5,000 reads per second per bucket at first, doubling no faster than every 20 minutes. Retry 408, 429 and 5xx with backoff [G10] | **Holds at idle** (a lease every 3 s). **At risk** for the start probe, the diagnose races, and a record handed over twice within a second (section 3, risks) | A single block blob up to 3,000 requests per second [A8]. 20,000 to 40,000 requests per second per account, depending on region [A9]. Throttling is 503 or 500 [A8] | **Holds** |
| P12 | V4 signed URLs, at most 7 days [G11][O4]. `signed_url_opts` signs extra headers, which can include `x-goog-if-generation-match: 0`, since only `x-goog-*` *query* parameters are reserved [O4]. Without a key file, signing calls IAM `signBlob` [G12][O1]. `If-None-Match` is not honored on PUT [G2] | **Partly**: ADR 0030's proof must be rewritten for `x-goog-` headers | A SAS signs permissions, times, resource, IP, protocol and the response overrides (`rscd`, `rsct`, ...), never request headers [A12]. So size, type and create-if-absent cannot be bound. `object_store`'s Azure signer has no `signed_url_opts`, so not even `rscd` and `rsct` can be signed [O3], and a PUT link gets `sp=w` (write, overwrite included) [O6] | **No**: node links only (section 7) |
| P13 | `from_env` reads every `GOOGLE_*` variable it knows, including `GOOGLE_APPLICATION_CREDENTIALS` and `GOOGLE_SERVICE_ACCOUNT(_PATH, _KEY)` [O5]. With none set it falls back to Application Default Credentials and the metadata server [O5][G14] | **Holds** | `from_env` reads `AZURE_*`: `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_STORAGE_SAS_TOKEN`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`, with managed identity last [O5]. It does **not** read the Azure CLI's own `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY` [A16][O5] | **Holds**, with one naming trap (section 2) |

**GCS through `s3://` (the S3-interop XML API).** GCS accepts SigV4 and
HMAC keys, and "processes `x-amz-*` headers that have an `x-goog-*`
equivalent" [G3][G4]. It documents `If-Match` and `If-None-Match` only
for GET and HEAD Object [G2], and the precondition page points writers to
generation preconditions [G1]. `object_store`'s `aws` backend sends
`If-None-Match: *` and `If-Match: <etag>` on PUT, so **conditional writes
through `s3://` against `storage.googleapis.com` are not supported by
WasmKeep**. If GCS ignores those headers, P1 and P2 are lost silently. The
start probe already refuses such a store ("it ignores
`If-None-Match: *`"), so the failure mode is a node that will not start,
not lost data. This has not been tried live. The docs will say: use
`gs://`, never `s3://`, for GCS.

### 2. URLs and credentials

```text
--bucket gs://BUCKET[/PREFIX]
--bucket az://ACCOUNT/CONTAINER[/PREFIX]
--bucket s3://BUCKET[/PREFIX]          # unchanged
```

- `gs://` is `object_store`'s own form [O5]: `GoogleCloudStorageBuilder::from_env().with_bucket_name(BUCKET)`.
- `az://` carries the account. `object_store` (following fsspec) reads
  `az://CONTAINER/PATH` and takes the account from the environment [O5].
  A WasmKeep bucket URL names the whole store, because it is printed by
  `cluster diagnose`, compared by `cluster status`, and stored in the
  project file's `bucket`. An operator with two storage accounts must not
  reach the wrong one by changing a variable. So WasmKeep parses
  `az://ACCOUNT/CONTAINER/PREFIX` itself and builds
  `MicrosoftAzureBuilder::from_env().with_account(ACCOUNT).with_container_name(CONTAINER)`.
  If `AZURE_STORAGE_ACCOUNT_NAME` is set to another account, it refuses
  to start rather than choosing one.
- `abfs://`, `https://…blob.core.windows.net` and `adl://` are refused,
  with a message naming `az://`. One spelling per store keeps the URL
  comparable.
- Credentials stay in `/etc/wasmkeep/env`, as `AWS_*` do today, under
  each provider's standard names:

| Provider | Preferred on the provider's own VMs | Elsewhere, or for the CLI |
| --- | --- | --- |
| GCS | The VM's attached service account through the metadata server. Nothing in the file; Google advises against key files [G14] | `GOOGLE_APPLICATION_CREDENTIALS=/etc/wasmkeep/gcs-key.json` (a service-account key or a workload identity federation file) [G14][O5] |
| Azure | A managed identity through IMDS [A17]. Nothing in the file, or `AZURE_CLIENT_ID` for a user-assigned identity | `AZURE_STORAGE_ACCOUNT_KEY`, or `AZURE_STORAGE_SAS_TOKEN`, or a service principal (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`) [O5] |

- The Azure CLI's `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY`
  [A16] are not `object_store`'s names [O5]. If either is set without
  its `object_store` counterpart, `wasmkeep` refuses with "did you mean
  `AZURE_STORAGE_ACCOUNT_KEY`". Silently falling through to managed
  identity would make the error a confusing 403.
- Permissions: GCS needs object create, get, list and delete on the
  bucket (for example `roles/storage.objectUser`), plus
  `iam.serviceAccounts.signBlob` on itself only if presigned links are
  used without a key file (open question 7). Azure needs *Storage Blob
  Data Contributor* on the container. Overwriting needs `blobs/write`, not
  only `blobs/add/action` [A2].
- `project.rs`'s `HIDDEN_FROM_BUILDS` (the credentials a build command
  never sees) grows from three `AWS_*` names to every `GOOGLE_*` and
  `AZURE_*` variable and the file `GOOGLE_APPLICATION_CREDENTIALS` names.

### 3. The start probe, per backend

`bucket_check::check_conditional_writes` stays as it is: create, create
over it (refused), replace with the current version, replace with the
stale one (refused), read back. It runs on a fresh key under `probe/` and
proves P1 and P2 on any backend. Per backend:

- **GCS.** The probe makes four writes to one object within milliseconds.
  GCS documents one write per second per object [G9], and a 429 is
  retried by `object_store` with backoff [O8]. On `gs://` the probe
  spaces its writes by one second, which adds about 3 s to a start, so a
  throttled answer is never mistaken for a broken condition. It also
  checks that the create's result carried a generation: a replace from a
  write's version without one fails with `MissingVersion`.
- **Azure.** No change in steps. The probe checks that the create's
  result carried an ETag.
- **Both.** `put_error` maps `MissingVersion` and `MissingETag` to
  `Unavailable` today, so a node would retry forever. They become a
  distinct error that the probe reports as unsafe: "the bucket returned no
  generation (or ETag) for a write; conditional replaces cannot work".
  Starting is refused, with that sentence in the log.
- **Azure, hierarchical namespace.** A node lists a scratch prefix it has
  just written to under `probe/` (two keys, `probe/<id>/a-b` and
  `probe/<id>/a/b`) and refuses to start if they do not come back in byte
  order. This costs two creates, one LIST and two deletes per start.
  Running on an HNS account would quietly break the alarm sweep's order
  (P6), so a start check is warranted, not only diagnose.

### 4. `cluster diagnose`, additions

Every current check runs on every backend unchanged, through the same
`Bucket` and `ObjectStore` traits. The header line names the backend and
what carries its conditions:

```text
Bucket gs://wasmkeep-demo (Google Cloud Storage, XML API, generation preconditions), writing only under gs://wasmkeep-demo/diagnose/run-…/
```

New checks, on all backends:

| Check | What it does | Fails when |
| --- | --- | --- |
| `version-token` | A create, a replace and a read each return the token the backend's replace takes (generation on GCS, ETag on Azure and S3). A replace with a read's token is accepted | A token is missing (`MissingVersion` or `MissingETag`) |
| `list-order` | Writes `o/A`, `o/a`, `o/a-b`, `o/a/b`, `o/a0`, `o/a~` and lists `o/` | The order is not byte order. On Azure the message names the hierarchical namespace |
| `list-offset` | `list_with_offset` from `o/a-b` returns exactly the keys after it, the offset excluded | Any key missing or extra (Azure's `startFrom` is inclusive, and the filter in `object_store` must drop it) |
| `same-object-rate` | Ten rewrites of one key as fast as the bucket answers, reporting answers per second and any 429 or 503 | Never fails. A **warning** if any rewrite was throttled, with the rate reached (expected on GCS above 1 per second [G9]) |

Changed on GCS: the races (`concurrent-creates`, `concurrent-replaces`)
are eight writes to one object at once. GCS may answer some of them 429
rather than 412. The check already reads the object back after a retry
that gets a 412. It also counts throttled answers and reports them as a
value, so that only two winners fail the check.

When ADR 0030 lands, its `presigned-*` checks run per backend (section 7):
the `x-goog-` variant on GCS, and on Azure a single `skip` line saying that
node links are used.

The "Providers checked" table in `docs/deploy/buckets.md` gains rows for
GCS and Azure, each blank until the real test fills it.

### 5. Code changes

- `Cargo.toml` (`wasmkeep-cli`, and `wasmkeep-cluster`'s dev-dependencies
  for its tests): `object_store` features `["aws", "gcp", "azure"]`.
  **No new crate**: `gcp` and `azure` pull in the same
  `reqwest`/`rustls`/`aws-lc-rs`/`cloud-base` set as `aws`. `azure` adds
  only `httparse`, which is already in `Cargo.lock` through `hyper` [O9].
  Run `cargo deny` and the licence check as for any feature change. Each
  feature could be optional at build time; one release binary that opens
  all three is simpler, and the size cost is to be measured.
- `main.rs`: `parse_bucket` returns a `BucketUrl { scheme, account,
  bucket, prefix }`, and `open_bucket_unchecked` matches on the scheme to
  build `AmazonS3`, `GoogleCloudStorage` or `MicrosoftAzure`. The error
  for an unknown scheme lists the three forms. `diagnose.rs` builds its
  scratch URL from the same struct instead of `trim_start_matches("s3://")`.
- `wasmkeep-cluster/src/object_store.rs`: the module comment already
  names GCS and Azure. `put_error` gains the missing-token case
  (section 3). A unit test asserts that a `Version` without a generation
  is refused before sending, not retried.
- `bucket_check.rs`: the write spacing on GCS and the order check
  (section 3), chosen by a `BucketKind` the CLI passes in.
- `counted_store.rs` (`wasmkeep_bucket_requests_total`) counts by method
  as today. Its doc names the provider's billing class for each method,
  in a table in `docs/monitoring.md`.
- `project.rs`: `HIDDEN_FROM_BUILDS` (section 2). The project file's
  `bucket` accepts the new schemes.
- Clap help: "credentials from the AWS_* environment variables" becomes
  "from the environment, by scheme (see docs/deploy/buckets.md)".
- ADR 0030's link endpoint and signer (`--blob-link-endpoint`, the second
  signer-only `AmazonS3`) become per backend once it is merged
  (section 7).

### 6. Docs

- `docs/deploy/buckets.md`: the guarantees table gains P3 (the version
  token), P6 (listing order) and P11 (rate per object). A section per
  backend covers the URL, the variables, the permissions, flat namespace
  only on Azure, soft delete (below), and why `s3://` must not be used
  against GCS.
- `docs/deploy/three-nodes.md`, "Connection settings by provider": a GCS
  row and an Azure row, marked "not checked yet" until the real test.
- `docs/cluster.md`, "Try it locally": `gs://` and `az://` named beside
  `s3://`. The Azurite note says what it does not prove (Verification).
- `docs/cluster.md`, "What an idle cluster asks of the bucket": the
  monthly cost per provider (Cost, below).
- `docs/deploy/backup-and-restore.md`: soft delete is not a backup. The
  sizing below shows what it costs on a bucket that rewrites a lease
  every 3 s.
- `ROADMAP.md`: item 6 points to this record.

### 7. ADR 0030's links on these backends

- **GCS.** Presigned links can be built: `signed_url_opts` with signed
  headers `content-length`, `content-type` and
  `x-goog-if-generation-match: 0` for an upload. A download uses the
  `response-content-disposition` and `response-content-type` query
  parameters, which must be honored by GCS signed URLs (open question 6).
  ADR 0030's start proof is written in `x-amz-` terms ("an unsigned
  `x-amz-acl`"). On GCS it needs a variant with an unsigned `x-goog-acl`.
  With no key file, every link costs one IAM `signBlob` call [O1][G12],
  which the proof also exercises.
- **Azure.** A SAS cannot bind request headers [A12], so it cannot hold
  an upload to one size, one type, or create-if-absent. A `PUT` link from
  `object_store` grants `sp=w`, write including overwrite [O6][A12].
  Downloads could carry `rscd` and `rsct` in a SAS [A12], but
  `object_store`'s signer does not expose them [O3]. Azure nodes therefore
  use **node links** only. `--blob-links presigned` refuses to start on
  `az://`, and `auto` picks node links without running the proof. Writing
  WasmKeep's own SAS signer for downloads is possible later, and is not
  part of this record.

## Cost

Rates at rest, per node at the default 3 s heartbeat, since 0.4.2
(`docs/cluster.md`, "What an idle cluster asks of the bucket"): 0.33 PUT,
about 0.1 LIST and 2.33 GET per second (1.0 heartbeats, 0.33
`control/changed`, 1.0 deploy, tombstone and secrets). For three nodes and
30 days that is about **2.59 million PUTs, 0.78 million LISTs and
18.1 million GETs**. `docs/benchmarks.md`, "Bucket requests at rest",
measured the 0.4.1 code at 0.334 PUT, 1.10 LIST and 1.33 GET per node
(2.6 million, 8.6 million and 13 million a month). It is shown for
comparison only; 0.4.2 is current.

| Monthly, three idle nodes | GCS Standard, single region [G13] | Azure Hot LRS, East US [A13] |
| --- | --- | --- |
| How a PUT is billed | Class A, $0.005 per 1,000 | Write, $0.05 per 10,000 |
| How a LIST is billed | Class A (XML "GET Bucket when listing objects") | "List and Create Container", $0.05 per 10,000 [A14] |
| How a GET is billed | Class B, $0.0004 per 1,000 | Read, $0.004 per 10,000 (a HEAD is "Other", same price [A14]) |
| How a DELETE is billed | Free | Free on GPv2 [A14] |
| **0.4.2 at rest** | 3.37 M × Class A = $16.84, 18.1 M × Class B = $7.25: **about $24** | PUT $12.95, LIST $3.89, GET $7.25: **about $24** |
| 0.4.1 at rest, for comparison | 11.2 M × Class A = $56, 13 M × Class B = $5.2: about $61 | PUT $13, LIST $43, GET $5.2: about $61 |

- On both providers a LIST is billed like a PUT, so the 0.4.2 listing
  cadence matters as much there as on S3.
- On GCS, dual-region and multi-region buckets bill Class A at $0.01 per
  1,000 [G13], about $41 a month at rest.
- Azure prices come from Microsoft's Retail Prices API
  (`https://prices.azure.com/api/retail/prices`, filtered to service
  Storage, region `eastus`, SKU "Hot LRS", product "General Block Blob
  v2"), because the pricing page renders its numbers by script [A13]. The
  hierarchical-namespace rates are higher, and it is refused anyway.
- Prices change; the docs will cite the pages, not copy the numbers.

**Soft delete.** GCS buckets get 7 days of soft delete by default, and it
covers overwritten objects, billed like live ones [G15][G13]. An Azure
account created in the portal has blob soft delete on, and "overwriting a
blob automatically creates a soft-deleted snapshot", billed at the active
rate. Microsoft names checkpoint-style overwrites as a source of
"unexpected cost" [A18][A19]. Three node leases rewritten every 3 s leave
about 86,000 soft-deleted copies a day, about 600,000 over 7 days. The
bodies are small (an estimate: hundreds of bytes), so cents a month.
Garbage-collected segments and snapshots (ADR 0012) are also kept for
7 more days, and that grows with write traffic. The docs will recommend
turning soft delete off (GCS retention 0, Azure blob soft delete off) for a
WasmKeep bucket and relying on the backup procedure. They will leave the
choice to the operator.

**Egress.** Nodes outside the provider pay internet egress on every GET
and on every restore. Azure: inbound is free, the first 100 GB a month
out is free, then about $0.087 per GB from the Retail Prices API [A15]
(to be confirmed in the calculator). GCS: free within the same location,
for example a VM and a bucket in one region [G13]; its internet rates
could not be read from the page (open question 8). Bandwidth at rest is
small, since the bodies are small. A restore of a large database or a
move between nodes outside the provider is billed by the gigabyte. The
higher latency also eats into the 2 s lease attempt (P11). The docs will
say to keep the nodes in the bucket's region, as they already say for S3.

## Consequences

- An operator on Google Cloud or Azure runs a cluster on the native object
  store, with the VM's own identity and no long-lived key in
  `/etc/wasmkeep/env`.
- `s3://` against GCS is documented as unsupported, and the start probe
  stands behind that.
- Azure clusters get node links only for ADR 0030 files: more node
  bandwidth, and no presigned downloads, until a signer of WasmKeep's own.
- Azure storage accounts with a hierarchical namespace (ADLS Gen2) are
  refused at start.
- A start on GCS takes about 3 s longer (the spaced probe).
- The binary grows by two backends. The size is to be measured, not
  guessed.
- Nothing changes for `s3://` clusters: same probe steps, same code path.

## Risks

- **ETag against generation.** On GCS the token that guards a replace is
  the generation, not the ETag. The XML ETag is an MD5 for simple objects
  [G8], so it does not tell two writes of the same bytes apart. The
  adapter already keeps both and hands both back. The risk is future code
  that builds a `Version` from a listing (no generation on GCS) or
  compares listed ETags to decide "unchanged". ADR 0020's planned "read
  a body only when its listed ETag changed" is safe only because every
  live node rewrites `written_at`. A unit test pins that `Update` without
  a generation is refused, not retried, and the docs of `Version` say
  where a token may come from.
- **Consistency corners.** Both providers document strong consistency for
  what WasmKeep does [G5][A3]. The corners are outside its path:
  publicly cached GCS objects (WasmKeep's are private; ADR 0030 downloads
  set `private`), IAM and HMAC changes that take minutes [G5], and Azure
  secondary endpoints [A5]. A credential rotated on a running cluster may
  be refused for a minute: the nodes treat that as an outage, not a
  conflict, and the docs say to rotate one node at a time.
- **Per-object rate on GCS.** One write per second per object [G9]. At
  rest a node lease is one write in 3 s. A record handed back and forth
  twice within a second (a drain racing a wake), or a burst of
  `control/changed` rewrites by scripts, could be throttled. A 429 is
  retried with backoff [O8], but a lease write that misses its 2 s
  attempt counts toward a lapse. Only the soak with kills, on GCS itself,
  can show whether this happens. The node already counts failed lease writes
  (`wasmkeep_node_lease_failures_total`), and the soak reads them.
- **Request costs.** Both bill a LIST like a PUT. A cluster left idle
  costs about $24 a month on either at 0.4.2's rates, and roughly
  2.5 times that on a 0.4.1 binary. The docs will give the arithmetic and
  point at `wasmkeep_bucket_requests_total`.
- **Egress** for nodes outside the provider (Cost). The docs will say:
  same region, or expect a bill per restore.
- **Emulators prove less than they seem** (Verification). A green run on
  Azurite is not a verdict on Azure.
- **`object_store` drift.** The mapping of `PutMode` to headers is read
  from 0.14.2's source. An upgrade must re-read `gcp/client.rs` and
  `azure/client.rs`, and run `cluster diagnose` on both again, before the
  pin moves.

## Verification

Nothing in this record is accepted until steps 3 and 4 pass on both
providers.

1. **Unit and mock tests** (no network). The URL parser covers `gs://`
   and `az://`, refuses `abfs://` and a conflicting
   `AZURE_STORAGE_ACCOUNT_NAME`, and refuses `AZURE_STORAGE_ACCOUNT` or
   `AZURE_STORAGE_KEY` alone. A mock HTTP server checks the headers each
   builder sends for `Create` and `Update` (`x-goog-if-generation-match:
   0` and `<generation>`, `If-None-Match: *`, `If-Match`). The
   missing-token error is a probe failure. `HIDDEN_FROM_BUILDS` covers the
   new variables. These prove only what WasmKeep sends, never what a
   provider does with it.
2. **Local emulators**, in CI where they help.
   - **Azurite** [A21][A22] supports conditional headers ("Access
     control based on conditional headers") and blob versioning. It is
     worth running the storage suites and `cluster diagnose` against it,
     with `object_store`'s emulator mode. **It does not prove**:
     - real Azure's status codes (Azurite answers some preconditions with
       409, per `object_store`'s own test [O2]);
     - `startFrom`, since `object_store` filters on the client under
       emulation [O3], so P6's offset path is not exercised;
     - soft delete, which Azurite does not support [A22];
     - throttling, races at scale, or latency, since it is "not a scalable
       storage service" [A22];
     - the hierarchical namespace, which it does not have [A21];
     - managed identity.
   - **fake-gcs-server** [G16], read at commit `3c29d20`, implements
     generation preconditions only as JSON API query parameters. Its XML
     handlers read no `x-goog-if-generation-match` and do not parse
     `list-type=2`, `continuation-token` or `start-after`. `object_store`'s
     `gcp` backend uses exactly those, so **fake-gcs-server cannot test
     it**, and a run against it would be misleading. Google's
     `storage-testbench` is "not an officially supported Google product"
     with "limited support for the XML API" [G17]. There is **no faithful
     GCS emulator** for this backend. Only step 1 covers GCS until step 3.
     This is a finding from reading source, not from running it.
3. **The real test, per provider, inside its network.** The owner noted
   that a fair test needs servers inside each provider's network: test
   VPS elsewhere would measure the internet, and pay egress. For each
   provider, three VMs in one region (GCE in one region with a
   single-region Standard bucket there; Azure VMs in one region with a
   flat-namespace Hot LRS or ZRS account there). Each uses its own
   identity (attached service account; managed identity), so no key sits
   in the env file. Steps:
   1. `cluster diagnose` three times, races included, from each node.
      Record the results in the "Providers checked" table.
   2. The S3-gated suites (`./scripts/check.sh --s3`), made
      backend-generic as `--bucket-url gs://…` and `az://…`: storage,
      failover, cluster.
   3. A three-node cluster with one app, as in `docs/deploy/three-nodes.md`.
   4. Idle for ten minutes. Compare `wasmkeep_bucket_requests_total` with
      the provider's own request metrics (Cloud Monitoring; Azure Storage
      metrics), and price the run.
   5. Also once, from a VPS outside the provider, `cluster diagnose` only,
      to see the latency and the lease attempt margin. This does not
      count toward a verdict.
4. **The soak with kills**, as for R2 and OVHcloud: 30 minutes of
   traffic, nodes killed and restarted in turns, a drain and a move.
   Checked afterwards: no actor owned twice (ADR 0020's invariants, from
   the logs and the records), no write lost, lease lapses and 429s counted
   (GCS above all), and the request bill against the counters. The result
   is a row in `docs/benchmarks.md` and in the "Providers checked" table
   for each provider.
5. **ADR 0030**, if merged first: its start proof's GCS variant on the
   same fleet, and `presigned` refused on Azure.

## Alternatives

- **GCS through `s3://` only.** No code at all, but conditional writes are
  not documented for PUT [G2]. At best the probe refuses it, at worst it
  is unsafe. Rejected.
- **`object_store`'s `az://CONTAINER` form, with the account from the
  environment.** It matches fsspec, and it is shorter. Rejected because a
  bucket URL that does not name the account can point at another store
  when one variable changes (section 2). Revisit if users ask.
- **One generic `object_store::parse_url_opts`** for every scheme. Fewer
  lines, but it accepts `abfs://`, `https://`, `file://` and `memory://`,
  each with its own spelling of the same store. An explicit match on
  three schemes is clearer about what has been checked.
- **A gateway (S3Proxy or similar) in front of Azure.** One more moving
  part whose handling of conditions would itself need proving. Rejected.
- **Build-time features per backend.** Smaller binaries for those who
  need one store. Deferred until the size is measured.

## Implementation

Not started. In order, each in its own PR, none merged before the step
before it passes:

1. URL parsing, builders and features, the missing-token error, the probe
   per backend, `HIDDEN_FROM_BUILDS` (Verification 1).
2. `cluster diagnose` additions (section 4), and Azurite in CI with its
   limits written next to it.
3. Docs (section 6), with both providers marked "not checked yet".
4. The real test and the soak (Verification 3 and 4). The "Providers
   checked" rows and this record's status follow from them.

## Open questions

1. Does a GCS XML `PUT` response carry `x-goog-generation`? `object_store`
   reads it if present [O1]. If it is absent, every replace from a
   write's version fails, and the probe catches it. Not confirmed in the
   docs read.
2. Does GCS honor or ignore `If-None-Match: *` on PUT through `s3://`?
   Only GET and HEAD are documented [G2]. A one-off test in step 3 would
   say whether the probe refuses it or whether the request fails outright.
3. What GCS answers to eight simultaneous replaces of one object: 412s,
   429s, or both. This decides whether `concurrent-replaces` needs more
   than the counting in section 4.
4. Whether real Azure ever answers `If-None-Match: *` with 409
   (`BlobAlreadyExists` [A4]) rather than the documented 412 [A1].
   `object_store` maps both, so this is for the docs only.
5. GCS buckets with a hierarchical namespace: their listing order and
   conditional writes were not researched. Refuse them (as Azure HNS) or
   check them? `list-order` would catch a wrong order either way.
6. Whether GCS V4 signed URLs honor `response-content-disposition` and
   `response-content-type`, and bind `content-length` as a signed header.
   This decides ADR 0030's GCS variant.
7. The exact permission and quota for IAM `signBlob` when signing without
   a key file, and its latency per link.
8. GCS internet egress prices (not readable from the pricing page as
   fetched), and whether Azure's inter-zone transfer ($0.01 per GB in the
   Retail Prices API) applies between zones of one region. The bandwidth
   page says same-region transfer is free [A15].
9. Whether the portal turns on Azure *container* soft delete by default
   (blob soft delete it does [A18]).
10. The binary's growth with the two features, to settle build-time
    features (Alternatives).

## Sources

All fetched 2026-11-14. Google's `cloud.google.com/storage/docs/*` pages
now redirect to `docs.cloud.google.com`; the URLs are the pages read.

- [G1] https://docs.cloud.google.com/storage/docs/request-preconditions
- [G2] https://docs.cloud.google.com/storage/docs/xml-api/reference-headers
- [G3] https://docs.cloud.google.com/storage/docs/aws-simple-migration
- [G4] https://docs.cloud.google.com/storage/docs/interoperability
- [G5] https://docs.cloud.google.com/storage/docs/consistency
- [G6] https://docs.cloud.google.com/storage/docs/xml-api/get-bucket-list
- [G7] https://docs.cloud.google.com/storage/docs/json_api/v1/objects/list
- [G8] https://docs.cloud.google.com/storage/docs/metadata
- [G9] https://docs.cloud.google.com/storage/quotas
- [G10] https://docs.cloud.google.com/storage/docs/request-rate
- [G11] https://docs.cloud.google.com/storage/docs/access-control/signed-urls
- [G12] https://docs.cloud.google.com/storage/docs/access-control/signing-urls-manually
- [G13] https://cloud.google.com/storage/pricing (operation classes, Standard single-region and dual- or multi-region prices, free deletes, same-location transfer, soft-deleted objects billed)
- [G14] https://docs.cloud.google.com/docs/authentication/application-default-credentials
- [G15] https://docs.cloud.google.com/storage/docs/soft-delete
- [G16] https://github.com/fsouza/fake-gcs-server (README, and `fakestorage/preconditions.go`, `upload.go`, `server.go`, `object.go` at commit `3c29d20`)
- [G17] https://github.com/googleapis/storage-testbench (README)
- [A1] https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations
- [A2] https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob
- [A3] https://learn.microsoft.com/en-us/azure/storage/blobs/concurrency-manage
- [A4] https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-error-codes
- [A5] https://learn.microsoft.com/en-us/azure/storage/common/storage-redundancy
- [A6] https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs
- [A7] https://learn.microsoft.com/en-us/rest/api/storageservices/representation-of-date-time-values-in-headers
- [A8] https://learn.microsoft.com/en-us/azure/storage/blobs/scalability-targets
- [A9] https://learn.microsoft.com/en-us/azure/storage/common/scalability-targets-standard-account
- [A10] https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview
- [A11] https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas (a user delegation key and its SAS last at most seven days)
- [A12] https://learn.microsoft.com/en-us/rest/api/storageservices/create-service-sas
- [A13] https://azure.microsoft.com/en-us/pricing/details/storage/blobs/ and the Retail Prices API, https://prices.azure.com/api/retail/prices (query in Cost)
- [A14] https://learn.microsoft.com/en-us/azure/storage/blobs/map-rest-apis-transaction-categories
- [A15] https://azure.microsoft.com/en-us/pricing/details/bandwidth/
- [A16] https://learn.microsoft.com/en-us/azure/storage/blobs/authorize-data-operations-cli
- [A17] https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token
- [A18] https://learn.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-enable
- [A19] https://learn.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview
- [A20] https://learn.microsoft.com/en-us/azure/storage/blobs/versioning-overview
- [A21] https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite
- [A22] https://github.com/Azure/Azurite (README)
- [O1] https://docs.rs/crate/object_store/0.14.2/source/src/gcp/client.rs (`put`: `x-goog-if-generation-match`, `MissingVersion`; `list_request`: `list-type=2`, `start-after`; `x-goog-generation`; `signBlob`)
- [O2] https://docs.rs/crate/object_store/0.14.2/source/src/azure/client.rs (`put_blob`: `If-None-Match: *`, `If-Match`, `MissingETag`; 409 and 412 mapped to `AlreadyExists` for `Create`; `startFrom`)
- [O3] https://docs.rs/crate/object_store/0.14.2/source/src/azure/mod.rs (`list_with_offset` under emulation; the `Signer` without `signed_url_opts`)
- [O4] https://docs.rs/crate/object_store/0.14.2/source/src/gcp/mod.rs (`signed_url_opts`, the 7-day limit, reserved `x-goog-` query parameters)
- [O5] https://docs.rs/crate/object_store/0.14.2/source/src/gcp/builder.rs and https://docs.rs/crate/object_store/0.14.2/source/src/azure/builder.rs (`from_env`, the configuration keys, URL forms, credential order)
- [O6] https://docs.rs/crate/object_store/0.14.2/source/src/azure/credential.rs (API version 2023-11-03; SAS `sp=w` for `PUT`)
- [O7] https://docs.rs/object_store/0.14.2/object_store/trait.ObjectStore.html ("the order of returned ObjectMeta is not guaranteed", the generic contract; the providers' own order is in P6)
- [O8] https://docs.rs/crate/object_store/0.14.2/source/src/client/retry.rs (5xx, 429 and 408 retried with backoff)
- [O9] https://docs.rs/crate/object_store/0.14.2/source/Cargo.toml (features `gcp`, `azure`, `gcp-base`, `azure-base`)
