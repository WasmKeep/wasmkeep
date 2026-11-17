# 0030. Files and static assets

- Status: proposed (2026-11-14); revised after an adversarial review
  (asset contents refreshed before reuse, a start proof of every link
  condition, fixed-length namespaces, the sweep judged only after a
  delete's outcome, stored headers overridden on download, indexes
  content-addressed, the fallback index the one served before);
  section 9 amended 2026-11-17 for `0.6.0` (manifest format 7; the
  hostname picks the app's assets; a bucket layout with separate
  contents and indexes; collection guarded by deploy and collection
  markers instead of refreshed ages; slices A1 to A7)
- Date: 2026-11-14
- Milestone: [Next](../../ROADMAP.md#next), `0.6.0`, after data access
  (`0.5.0`) and several apps per process
  ([ADR 0038](0038-several-apps-per-process.md), earlier in `0.6.0`)
- Follows: [ADR 0021](0021-the-app-model.md) (the app is the unit),
  [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md) (the
  app's root, `state/<tenant>/<app>/`), [ADR 0023](0023-the-front.md)
  (the front, routing), [ADR 0026](0026-the-project-file.md) (the project
  file), and [ADR 0028](0028-actors-that-end.md) (actors that end: the
  delete routine, idle expiry and `app delete`).
- Amends: [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md)
  (two families under an app's root, `blobs/` and `blob-sweeps/`, and one
  beside its versions, `apps/<t>/<a>/_assets/`),
  [ADR 0023](0023-the-front.md) (routing: assets before the front, and
  `/_wasmkeep/blobs/`; `front-core` imports `blobs`),
  [ADR 0026](0026-the-project-file.md) (an `[assets]` section),
  [ADR 0012](0012-bucket-garbage-collection.md) (collecting unused
  assets), and [ADR 0028](0028-actors-that-end.md) (a delete also ends
  the actor's files).

## Context

An actor has one SQLite database, capped at 1 GB, replicated by shipping
its pages to the bucket (ADR 0007). That is the wrong place for a photo,
a PDF or a video: every byte written becomes history to upload, keep and
restore, and a restore downloads it again. Today an app that stores files
does one of two things, both poor: it puts them in the database, or it
calls a storage service of its own with credentials in its configuration.

The front (ADR 0023) serves every path outside `/actor/`. A site's HTML,
scripts and images reach the browser only through it: each request
instantiates the front, and its component carries the files or fetches
them. Nothing a node knows makes a request for `app.js` cheap.

The roadmap ("Now", files and databases) asks for:

- **files** for actors and the front, kept in the app's part of the
  bucket, with links a browser uploads to and downloads from directly;
- **static assets** shipped with `wasmkeep deploy`, served by every node
  without waking any code, with a rule for paths that match no file;
- redirects and routes left to the front's own code.

What the code gives us to build on:

- Every node opens the bucket with the `object_store` crate (0.14.2),
  through `AmazonS3Builder::from_env` and an `Arc<dyn ObjectStore>`
  (`crates/wasmkeep-cli/src/main.rs`, `crates/wasmkeep-cluster/src/object_store.rs`).
  The concrete `AmazonS3` implements `object_store::signer::Signer`,
  whose `signed_url_opts` presigns one request with SigV4 and binds
  given headers and query parameters to the signature. The trait object
  does not expose it.
- The node's credentials cover the whole bucket prefix: every app's state,
  deploys, the sealed secrets and `cluster/secret`. They must never reach
  a guest, and a guest must never name a full object key.
- `Manifest::parse` (`crates/wasmkeep-server/src/apps.rs`) returns `None`
  on any line it does not know, and a node that cannot read a version
  stays not ready. New lines need a new format and a capability word, as
  the front had (ADR 0023, "Compatibility").
- Deploy collection (`crates/wasmkeep-server/src/apps/collect.rs`) follows
  only plain version ids (lowercase letters and digits): a key beginning
  with `_` beside the versions is never taken for one.
- `/_wasmkeep/` on the public port is the server's, and no component ever
  sees it (`App::route`).
- ADR 0015, 0019 and 0029 built key-value, queues, workflows and a shared
  database as SDK patterns over actors, with no host function. This
  record does not: see "Why a host interface" under Alternatives.

## Decision

### 1. Who owns a file: one namespace per caller

A file (a **blob** in the interface) belongs to exactly one namespace,
and a caller reaches only its own:

- **An actor's namespace** is the actor's. Only that actor's code reads,
  writes, lists and deletes it, as only it reads its database. It ends
  with the actor (section 5).
- **The front's namespace** is the app's front, `_front/main`. Only the
  front reaches it. It ends only by its own deletes or with the app.

No caller names another namespace: the host derives it from where the
code runs (the actor's own address, or the front), never from an
argument. To share a file, its owner hands out a **link** (section 4), or
answers a call with its bytes. A file many actors read belongs to one
actor that serves it, as a database many actors use is kept by one
actor (ADR 0029).

Why not one app-wide namespace: every class could then read and
overwrite every other class's files, a file would have no owner whose
end ends it (an actor's delete could not remove "its" files without a
reference count the host does not keep), and an `internal` class's data
would be readable by any code of the app without a call that its own
code can refuse. Why not let the front read actors' files: the front can
already ask any actor for a link, and the actor's code decides; a direct
path would bypass that code. See "Not decided here" for a read-only link
the front could sign for an actor's file.

### 2. Keys

Under the app's root `R = <prefix>state/<tenant>/<app>/` (ADR 0022):

| Family | Key |
| --- | --- |
| an actor's files | `R blobs/<class>/<name id>/<incarnation>/<key>` |
| the front's files | `R blobs/_front/main/0/<key>` |
| files to remove after a delete | `R blob-sweeps/<class>/<name id>/<incarnation>` |
| the app's last count (section 7) | `R blob-usage` |

- `<name id>` is the SHA-256 of the actor's name, 64 lowercase hex
  digits: a fixed length whatever the name (up to 256 bytes, so up to 512
  hex digits written out, past S3's 1,024-byte key and a file system's
  255-byte name), never `.` or `..`. The name itself is written in the
  sweep record's body, so nothing needs to reverse the hash. `_front` is
  not a class, so the front cannot collide with one.
- **`<incarnation>`** is 16 lowercase hex digits, or `0` for an actor
  never deleted since this release. The delete routine (ADR 0028,
  section 1) draws a new one and writes it in the host meta of the empty
  database the reset segment carries, `blob_incarnation`. It is therefore
  durable exactly when the delete is, and every later holder reads it
  with the restored database. A new actor at a deleted address starts in
  a namespace no file of the old one can be in, whatever is still being
  swept or uploaded there.
- **`<key>`**, the part the guest gives: 1 to 512 bytes of
  `[A-Za-z0-9._~-]` and `/`, made of segments of at most 255 bytes
  separated by single `/`, none empty, none `.` or `..`, not starting or
  ending with `/`. Anything else is `invalid-key`, never `unavailable`.
  With the tenant, app and class at most 64 bytes each, the rest of a key
  is at most 790 bytes; a node with an app that imports `blobs` refuses to
  start on a bucket prefix longer than 200 bytes, so no object key passes
  1,024 bytes. The alphabet is the one S3 and `object_store`'s
  `Path` take without escaping, so a key has one spelling in the bucket
  and in a link, and no key can name a parent. A file's display name (with spaces, accents) is the app's data,
  given back in a download link's `filename` (section 4).
- **A node without a bucket** (`serve` with a data directory only, and
  `wasmkeep dev`) does not map keys to paths: a file system cannot hold
  both `a` and `a/b`, and some fold case. Each namespace is a directory
  `<data_dir>/state/<t>/<a>/blobs/<class>/<name id>/<incarnation>/` holding
  a small SQLite index (`index.db`: key, file id, size, type, ETag, time)
  and the contents as `f/<random id>`, written to `tmp/` beside them,
  fsynced, renamed into `f/`, then committed in the index; conditions are
  checked in the index's transaction. Files in `f/` or `tmp/` that no row
  names are removed when the node starts. No path is built from a key, so
  nothing below the namespace can be named by one.

The app is still two prefixes, `apps/<t>/<a>/` and `state/<t>/<a>/`, for
deleting, exporting, backups and access policies (ADR 0022).

### 3. The interface: `wasmkeep:actor/blobs`

A new interface in package `wasmkeep:actor@0.2.0`, imported by
`actor-core` and `front-core`. The package version stays: an interface
is added and nothing existing changes, so a component built before links
as it did ([languages](../languages.md#versions)). All five WIT copies
carry it.

```wit
/// Files kept in the app's part of the bucket, in the caller's own
/// namespace: an actor's, or the front's (docs/adr/0030-files-and-static-assets.md).
interface blobs {
    use wasi:io/streams@0.2.12.{input-stream, output-stream};

    record blob-info {
        key: string,
        size: u64,
        /// As given when written; `application/octet-stream` if none was.
        content-type: string,
        /// The bucket's ETag, opaque: compare, never parse.
        etag: string,
        /// Milliseconds since the epoch, by the bucket's clock.
        modified-ms: u64,
    }

    variant condition {
        /// Write whatever is there.
        any,
        /// Write only if no file has this key.
        absent,
        /// Write only if the file's ETag is this one.
        etag(string),
    }

    record write-options {
        /// `type/subtype` with optional parameters, at most 256 bytes.
        content-type: option<string>,
        condition: condition,
    }

    variant disposition {
        /// Downloaded under this name, or the key's last segment.
        attachment(option<string>),
        /// Shown by the browser: only for the types in section 8.
        inline,
    }

    record download-options {
        /// Default 300, at most 3,600.
        expires-in-secs: option<u32>,
        disposition: disposition,
    }

    record upload-options {
        /// The exact size the browser will send.
        size: u64,
        content-type: string,
        /// Default 900, at most 3,600.
        expires-in-secs: option<u32>,
    }

    /// What the browser must send: `method` to `url`, with exactly these
    /// headers, and a body of exactly the size given.
    record upload-link {
        url: string,
        method: string,
        headers: list<tuple<string, string>>,
        expires-at-ms: u64,
    }

    record listing {
        entries: list<blob-info>,
        /// Pass as `after` for the next page; none at the end.
        next: option<string>,
    }

    variant blob-error {
        invalid-key(string),
        /// Past a size limit (section 7).
        too-large,
        /// The app is over its stored-bytes limit (section 7).
        quota-exceeded,
        /// The write's condition did not hold.
        conflict,
        /// Refused as asked (an inline link for an unsafe type, a write
        /// without a valid hold, a closed upload).
        not-allowed(string),
        /// The bucket or disk failed or did not answer in time: not known
        /// to have happened. Safe to retry.
        unavailable(string),
    }

    head: func(key: string) -> result<option<blob-info>, blob-error>;

    /// The whole file, up to 16 MiB (else `too-large`; use `open-read`).
    get: func(key: string) -> result<option<tuple<blob-info, list<u8>>>, blob-error>;

    /// Writes the whole file, up to 16 MiB. Returns once it is durable.
    put: func(key: string, body: list<u8>, options: write-options) -> result<blob-info, blob-error>;

    /// Streams a file, or the bytes `[start, start + length)` of it.
    open-read: func(key: string, range: option<tuple<u64, u64>>)
        -> result<option<tuple<blob-info, input-stream>>, blob-error>;

    /// Starts a streamed write; nothing is visible until `finish`.
    open-write: func(key: string, options: write-options) -> result<upload, blob-error>;

    resource upload {
        /// The stream to write the body to; only once.
        body: func() -> result<output-stream, blob-error>;
        /// Ends the body and makes the file durable, with its condition.
        /// Dropping an upload without `finish` abandons it.
        finish: static func(this: upload) -> result<blob-info, blob-error>;
    }

    /// Removes the file if it exists. Durable when it returns.
    delete: func(key: string) -> result<_, blob-error>;

    /// Files whose key starts with `prefix` as a string (`up` finds
    /// `uploads/a`; `uploads/` finds what is under it), in key order,
    /// after the key `after`, at most `limit` (1 to 1,000). `prefix` is
    /// empty or over the key alphabet and may end with `/`; `after` is a
    /// key.
    list: func(prefix: string, after: option<string>, limit: u32) -> result<listing, blob-error>;

    /// A link a browser downloads the file from, without this code.
    link-download: func(key: string, options: download-options) -> result<string, blob-error>;

    /// A link a browser uploads one new file to, without this code.
    link-upload: func(key: string, options: upload-options) -> result<upload-link, blob-error>;
}
```

- **Durability.** `put`, `finish` and `delete` return once the bucket
  acknowledged the write (on a node without a bucket, once fsynced). A
  conditional write uses the bucket's `If-None-Match: *` and `If-Match`,
  which every node already proves at start (`bucket_check.rs`).
- **When.** In any event of an actor (`on-init`, requests, calls,
  alarms, socket events) and in any front request. Each call waits on the
  bucket and counts against the event's wall clock, not its CPU budget.
  A streamed read or write is bounded by the event's wall (30 s for the
  front): files larger than an event can move go through links. Streams
  and uploads still open when the event ends are closed and abandoned.
- **Writes need a valid hold.** An actor's `put`, `open-write`, `finish`,
  `delete` and `link-upload` are refused with `not-allowed` unless the
  node holds the actor (ADR 0020), checked when the call starts; a
  `finish` or `delete` whose acknowledgement arrives after the hold
  lapsed returns `unavailable`, as `exec-durable` does. The write may
  still have landed: a plain PUT is not fenced (section 6).
- **Listing** uses the bucket's listing of the prefix's last full
  segment (`object_store` matches whole segments) with an offset, and
  filters the rest of the prefix on the node.
- **Not transactional.** Blob calls are not part of the database's
  transaction, may run inside one, and survive its rollback.
- **Streams** use `wasi:io`, which every actor and front already links
  (it comes with `wasi:http`), so a front can splice a file into its
  response body without holding it in memory. A streamed write uses the
  bucket's multipart upload; an abandoned one is aborted, and the
  bucket's own rule for incomplete multipart uploads is the backstop the
  docs ask for.
- **SDKs.** Rust and TypeScript first: `blobs::put`, `get`, `head`,
  `delete`, `list` (with an iterator over pages), `link_download`,
  `link_upload`, and in Rust `open_read` and `open_write` (an `io::Read`
  and `io::Write`). TypeScript gets the buffered functions and the links
  first, and streams once its bindings carry `wasi:io` resources (to be
  checked when built). The other SDKs come later; the API reference
  (`crates/wasmkeep-wit/tests/api_reference.rs`) lists them as missing
  until then.

### 4. Links

A link is a URL that allows one kind of request on one file until it
expires. It is a **capability**: whoever holds it can use it, nothing
ties it to a user, and it cannot be revoked before it expires except by
deleting the file (a download) or rotating the credentials that signed
it. So links are short-lived, narrow, and never logged by the host (logs
name the key and the namespace).

**Presigned links** (a node with a bucket that passed the start proof
below). `signed_url_opts` builds the URL from the store's own endpoint,
so the node builds a second, signer-only `AmazonS3` for the link
endpoint (below), with the same credential provider, region, bucket and
addressing style (path or virtual-hosted) as its own store, and signs
with it (SigV4 query signing):

- **Download**: `GET` (a browser's `HEAD` is refused by the signature;
  links are for GET), the exact object key, the expiry, and the signed
  query parameters that override whatever headers the object was stored
  with: `response-content-disposition` (section 8),
  `response-content-type` (the stored type for an inline link,
  `application/octet-stream` for an attachment), `response-cache-control`
  (`private, max-age=<seconds left>`) and `response-content-encoding`
  (`identity`). The browser gets the bytes from the bucket, with the host
  out of the path.
- **Upload**: `PUT` to the exact object key, with signed headers
  `content-length` (the exact `size`), `content-type` (exactly as
  given), `if-none-match: *` and `cache-control: private, no-store`. The
  browser sends the body with those headers (`content-length` is set by
  the browser from the body). A different size, a different type, a
  missing header or a key that already exists is refused by the bucket.
  Other headers the uploader adds (`Content-Encoding`, `x-amz-meta-*`)
  may be stored with the object; downloads override every one that
  matters, and the start proof checks that unsigned `x-amz-` headers are
  refused. Upload links are **create-only**: while the file exists the
  link fails. They are not single-use: if the app deletes the file
  before the link expires, the link can create it again. So each upload
  gets a key never used before (`uploads/<random>`), the app records the
  key as used once confirmed, and deletes any file that reappears at a
  key it already deleted. To replace a file, an app uploads to a new key
  and deletes the old one.
- **Expiry**: download 300 s by default, upload 900 s, at most 3,600 s
  for both. SigV4's own ceiling (7 days) is never approached. With
  temporary credentials (a role), a link dies when they do, which may be
  sooner: `object_store` does not say when they expire, so the host
  cannot shorten the link to match, and the docs say to give the node
  credentials that outlive the longest link.
- **The signed host** is the bucket's endpoint as browsers reach it:
  `--blob-link-endpoint URL` on `serve` (default: the endpoint the node
  uses, `AWS_ENDPOINT` or the provider's). A node that reaches its
  bucket on a private address must set it; the signature binds the host,
  so a link to an unreachable address is useless, not unsafe.
- **CORS** for uploads from a page is the bucket's configuration, the
  operator's to set (allowed origin the app's, method `PUT`, headers
  `content-type` and `if-none-match`). The docs give it per provider;
  `cluster diagnose --origin` checks it (section 10).

**Node links** (a node without a bucket, a bucket that failed the start
proof, or `--blob-links node`). The URL is the node's own public port,
`/_wasmkeep/blobs/v1/<token>`:

- `<token>` is the base64url of a record (version, tenant, app,
  namespace, incarnation, key, method, expiry, and for an upload the
  size and type; for a download the disposition) followed by an
  HMAC-SHA256 over it. The MAC key is derived from `cluster/secret` with
  HKDF and the label `wasmkeep blob links v1 <tenant>/<app>`; a node
  without a bucket keeps a random key in its data directory. Any node of
  the app verifies a token; a node of another app refuses it (`403`)
  before reading a body, as its key and its app differ, and a node that
  has seen the app's tombstone (ADR 0028, section 5) refuses every token.
- A download streams the file from the bucket or disk with the response
  headers of section 8, never the stored ones.
- An upload refuses, before reading, a `Content-Length` other than the
  signed size, a chunked body, or a `Content-Type` other than the signed
  one. It streams exactly the signed number of bytes to the bucket as a
  create-if-absent write and commits only if the body then ends: a short
  body, or one that does not end there, aborts the write and answers
  `400`. An existing key answers `412`. The whole upload must end by the
  link's expiry plus one hour, or it is aborted.
- They cost the node's bandwidth and a request slot each, and are
  counted (section 7). Rotating `cluster/secret` ends every node link.

`--blob-links auto|presigned|node` (default `auto`): `auto` presigns on a
bucket whose start proof passed, and uses node links otherwise; `presigned`
refuses to start when the proof fails. **The start proof**, beside the
conditional-write check every node runs on `probe/`, sends to the link
endpoint, with links signed for it, every case a link relies on:

1. an upload of the right size, type and headers lands;
2. the same link again is refused (the key exists);
3. uploads with another size, another type, without `if-none-match`, or
   with an unsigned `x-amz-acl: public-read` header, each to a fresh key,
   are refused;
4. a link signed for one key writes no other;
5. a download link returns the bytes with the signed
   `response-content-type` and `response-content-disposition`.

About ten small requests at start. Any case failing, or the link
endpoint not answering from the node, makes the node use node links,
said once in the log and in `wasmkeep_blob_links_mode`. A store that
ignores any one of these conditions would let a link holder store
another size, type or key than the app allowed, or overwrite a confirmed
file: the proof is what lets a node trust links at all. `cluster
diagnose` runs the same cases (section 10).

### 5. Lifecycle

**An actor's files end with the actor.** ADR 0028's delete routine
(`delete-self`, `delete-self-durable`, `actor delete`, idle expiry)
gains two steps:

1. **Before the reset is uploaded**, the node writes the sweep record
   `R blob-sweeps/<class>/<name id>/<old incarnation>`, whose body is the
   actor's name and the word `pending`.
   If it cannot be written, the delete fails as an upload failure does
   (ADR 0028: given up, or `internal` for the durable delete). The reset
   then carries the new `blob_incarnation`.
2. **Once the reset is acknowledged** while the hold is valid (ADR 0028:
   only then is the delete done), the node rewrites the record's body as
   `swept` (creating the record again if it is missing), then removes
   everything under `R blobs/<class>/<name id>/<old incarnation>/`, in the
   background, at most 1,000 keys per delete request. Download links to
   those files fail from then on. A delete given up (the upload failed)
   leaves the record `pending` for the pass to judge.

**The sweep pass.** One node per app (the live node with the lowest id,
as idle expiry chooses it, ADR 0028, section 4) lists `R blob-sweeps/`
once a minute and, for each record:

- **Pending**, and last modified more than 10 minutes ago (by the
  bucket's clock; a younger one may belong to a delete still running):
  a judging job is queued as the operator delete's job is (ADR 0028,
  section 3: the owner runs it in the actor's mailbox, so after any
  delete routine already there has its outcome; or any serving node
  acquires the actor if nobody holds it). It restores, **fences** (ADR
  0028, section 3, step 3: what a restore read is the state only once
  this hold's upload is in), and reads `blob_incarnation`. If it differs
  from the record's, the delete happened: step 2. If it is the same, the
  delete did not happen, and no late reset can make it happen after the
  fence: the record is deleted and no file is touched.
- **Swept**: the prefix is listed again, and what is there removed, when
  the record is one hour old and again at 25 hours (by its last-modified
  time): an upload started before its link expired may land well after,
  and a node link aborts an upload one hour past expiry. Then the record
  is deleted. An upload to a presigned link that is still running a day
  later leaks its file; the docs give a bucket lifecycle rule for
  `state/*/*/blobs/` as the backstop for such strays.

**Idle expiry** (ADR 0028, section 4) counts events only. A file
downloaded through a link every day does not keep its actor alive: no
code runs. The docs say so beside `expire-after-idle`.

**`app delete`** (ADR 0028, section 5) already removes every key under
`state/<t>/<a>/`, files included. Nodes stop issuing links when they see
the app's tombstone (within 3 s), and refuse node links after it. The
command lists `state/<t>/<a>/blobs/` again one hour and 25 hours after
the tombstone (it waits for the first, or with `--no-wait` prints the
command that finishes each), so an upload link issued just before leaves
nothing behind. `deploy --recreate` is refused until both listings have
run and found nothing: a re-created app's actors start at incarnation
`0`, the same keys as the old app's, and an old link must not reach
them.

**Garbage the host does not collect.** The host knows owners, not
references. A file an actor wrote but never recorded (the event failed
after `put`), or uploaded through a link nobody confirmed, stays until
its owner deletes it or ends. The docs give the pattern: write the file
first and the row after, give upload links unique keys
(`uploads/<random>`), record each pending upload with its expiry, check
it with `head` when the browser says it is done, and sweep pending ones
past their expiry from an alarm.

**Restoring history to a point before a delete** (from a bucket that
keeps versions, or a backup) brings back the old incarnation's rows but
not its files, which were swept. Such a restore takes the files from the
same backup.

**Versioning is not assumed.** WasmKeep never reads object versions. A
bucket that keeps old versions keeps deleted files as old versions too:
deleting from the app does not erase them from such a bucket, and the
operator's lifecycle rule decides when they go. Backups follow the
app's two prefixes (ADR 0022).

### 6. Consistency

- **Read after write** within a namespace: a `head`, `get` or `list`
  after an acknowledged write sees it (the bucket's guarantee that
  `cluster diagnose` checks; listings within seconds).
- **Single writer per actor, not per file.** An actor's events run one at
  a time, but a browser holding an upload link writes at any time, and
  the front's requests run at once on every node. Conditions (`absent`,
  `etag`) are how concurrent writers agree; upload links are
  create-only for that reason.
- **A deposed holder's write.** A node that lost the actor's hold
  (ADR 0020) and does not know it yet may still complete a PUT started
  while it held it. The host refuses to start a write without a valid
  hold and never acknowledges one after the hold lapsed, but cannot
  unsend it. Apps that cannot tolerate a stale write use write-once keys
  or `etag` conditions. Within one incarnation this is the only window;
  across a delete, the incarnation keeps a stale write out of the new
  actor's namespace, and the delayed sweep removes it.
- **Moves and failover** move nothing: the files are in the bucket, and
  any holder reaches them. On a node without a bucket, files stay on
  that node's disk with the database.

### 7. Limits, quotas and metrics

**Enforced by the host:**

| Limit | Value |
| --- | --- |
| key | 512 bytes, the alphabet of section 2 |
| `get`, `put` | 16 MiB, as a call's body |
| one file (streamed write, upload link) | `--max-blob-bytes`, default 1 GiB, at most 5 GiB (one presigned PUT's ceiling on S3) |
| `list` page | 1,000 |
| content type | 256 bytes, `type/subtype` and `;`-parameters of RFC 9110 token and quoted-string characters only (no CR, LF or control characters) |
| download filename | 255 bytes after the host strips `"`, `\`, `/`, CR, LF and control characters |
| link expiry | 3,600 s |
| links per event | 1,000 |

**Measured, and enforced with a lag.** S3 gives no size for a prefix,
and links bypass the host, so nothing can refuse a write by a total
known at that moment. Once an hour the sweep node lists
`state/<t>/<a>/blobs/` and writes the app's stored bytes and file count
to `R blob-usage`, which every node of the app reads once a minute. With `--max-app-blob-bytes`, writes and upload links are refused
with `quota-exceeded` while the last count is over it, until a count is
under it again. The quota can be exceeded by up to an hour of writes
plus every upload link outstanding; the docs say so. Per-actor totals
are the app's to keep (in its database), not the host's. On a node
without a bucket the same count walks the directory, and the disk's own
readiness check (`/_wasmkeep/ready`) stays the hard stop.

**Metrics** (with `tenant` and `app`, like the other series of an app's
work):

- `wasmkeep_blob_ops_total{op, outcome}`: `op` one of `head`, `get`,
  `put`, `read`, `write`, `delete`, `list`; `outcome` one of `ok`,
  `not_found`, `conflict`, `too_large`, `quota`, `not_allowed`,
  `unavailable`.
- `wasmkeep_blob_bytes_total{direction}`: bytes through the host (`in`,
  `out`), node links included.
- `wasmkeep_blob_links_total{kind}` (`download`, `upload`) and
  `wasmkeep_blob_links_mode` (1 for `presigned`, 0 for `node`).
- `wasmkeep_blob_node_link_requests_total{outcome}`: `ok`, `expired`,
  `bad_token`, `wrong_app`, `exists`, `wrong_size`.
- `wasmkeep_blob_stored_bytes` and `wasmkeep_blob_stored_files`: the last
  hourly count.
- `wasmkeep_blob_sweeps_total{outcome}`: `swept`, `not_deleted`,
  `final`, `failed`.

### 8. Security

- **No credentials in guests.** Guests give keys; the host builds every
  object key from its own app, the caller's namespace and incarnation,
  and the validated key. Links carry only what SigV4 or the HMAC binds.
- **Cross-app and cross-actor.** A namespace is never an argument, so no
  call reaches another actor's or app's files. A presigned link names one
  object key; a node link names its app and is refused by any other app's
  node. Neither can be widened: SigV4 binds the key, method, signed
  headers and expiry, and the token's MAC covers every field.
- **Links in the wild.** A URL leaks through history, logs, `Referer` and
  screenshots. Short defaults, create-only uploads, the expiry ceiling
  and the sweep after a delete bound the damage; the docs advise
  `Referrer-Policy: no-referrer` on pages that show download links. A
  link discloses the bucket's endpoint and name, the app's prefix layout,
  the access key id that signed it (and, with temporary credentials, the
  session token, valid only with the secret it never carries) and the
  actor's class; the name is hashed.
- **What a browser renders.** A file uploaded by one user and opened by
  another must not run as the app's page.
  - Downloads are `attachment` unless asked `inline`, and `inline` is
    refused (`not-allowed`) except for `image/png`, `image/jpeg`,
    `image/gif`, `image/webp`, `image/avif`, `audio/*`, `video/*` and
    `text/plain`. Not `text/html`, `image/svg+xml`, `application/pdf` or
    XML: they can run script or be read as a document.
  - The host builds `Content-Disposition` itself (RFC 6266:
    `attachment; filename="<ASCII fallback>"; filename*=UTF-8''<percent-encoded>`).
  - **Presigned downloads come from the bucket's origin**, not the app's:
    a file that does run script there has no access to the app's
    cookies or storage. The docs require that the link endpoint is not
    a host under the app's own site (no shared parent domain whose
    cookies it would receive).
  - **Node-link downloads come from the app's origin**, so the host adds
    `X-Content-Type-Options: nosniff`, `Content-Security-Policy: sandbox;
    default-src 'none'`, `Cross-Origin-Resource-Policy: same-origin` and
    `Cache-Control: private, max-age=<seconds left>` to every one. A
    sandbox and an attachment do not stop a page from loading the URL as
    a subresource (`<script src>`, a stylesheet): so an attachment is
    always sent as `application/octet-stream`, which `nosniff` keeps from
    running as script or style, and an inline one only has a type of the
    list above, none of which a browser runs. Presigned attachments get
    the same type through `response-content-type`.
- **No path traversal.** The key alphabet and segment rule make `..`,
  `\`, `%2F` and empty segments impossible; on disk, no path is built
  from a key (section 2).
- **SSRF does not apply.** The host never fetches a URL a guest gives:
  links point at the bucket endpoint the operator configured, or at the
  node itself. Outbound HTTP keeps its egress policy (`egress.rs`).

### 9. Static assets

**Deploy.** A version may carry a directory of files served as they are:

```
wasmkeep deploy --app acme/shop --bucket s3://b/p --assets web/dist \
  --assets-not-found spa --assets-front-first api/,auth/ --assets-immutable assets/
wasmkeep deploy --app acme/shop --bucket s3://b/p --no-assets
```

and in the project file (ADR 0026), as the whole truth like every other
section (no `[assets]`, no assets):

```toml
[assets]
dir = "web/dist"                       # required
build = "cd web && npm run build"      # optional, run before the front's
not-found = "spa"                      # "front" (default), "spa" or "404"
front-first = ["api/", "auth/"]        # path prefixes the front answers first
immutable = ["assets/"]                # name prefixes cached for good
watch = ["web/src"]                    # `wasmkeep dev`
```

- `front-first` and `immutable` entries are prefixes of whole segments:
  each ends with `/` (`api/` matches `api/x`, never `apiary.png`); an
  entry without the `/` is refused.
- Flags: `--assets DIR` sets them, `--no-assets` removes them, neither
  keeps the current ones (no copy: the new version names the same
  index, which is content-addressed). `--assets-not-found`, `--assets-front-first` and
  `--assets-immutable` take what the keys take and need `--assets`.
  `--rollback` conflicts with all of them. With a project file they are
  refused, as the other flags that say what the version is (ADR 0026).
- Builds run in the order assets, front, classes, so that a front can
  read the built files' names.

**What is taken from `DIR`.** Every regular file below it, recursively,
with its path relative to `DIR` as its name:

- **Refused** (the deploy fails before writing, naming each): a symbolic
  link (followed nowhere: a link to a home directory must not be uploaded by
  accident), a name that is not valid UTF-8, holds a control character,
  `\`, or a segment `.` or `..`; a top-level `actor/` or `_wasmkeep/`
  (those paths never reach assets); more than 20,000 files; a file over
  25 MiB; more than 512 MiB in all; a name over 1,024 bytes; a name under
  an `immutable` prefix that the previous version has with other
  contents (an immutable name must keep its bytes, or browsers keep the
  old ones for a year). A name that is not in Unicode NFC is warned
  about: names match as bytes, and some systems write NFD.
- **Skipped**, and listed in the plan: names with a segment starting
  with `.`, except under `.well-known/`.
- **Precompressed files.** For a file `X`, a sibling `X.br` or `X.gz` is
  its Brotli or gzip variant, not a file of its own: the deploy checks
  that it decompresses to `X` exactly and refuses it otherwise (an `X.gz`
  meant as a download of its own is shipped under another name). For text
  types without one (HTML, CSS, JavaScript, JSON, SVG, XML, WebAssembly,
  plain text, source maps), the deploy makes both, kept when they save
  at least 10%. `X.br` without `X` is a file of its own.
- **Types** come from the extension, by a fixed table in the CLI
  (documented with the deploy), `; charset=utf-8` for text types,
  `application/octet-stream` for the rest. The index records them, so a
  later CLI with a longer table changes nothing already deployed.

**Storage: content-addressed, beside the versions.**

```
apps/<t>/<a>/_assets/content/<sha256>     one object per distinct content (a file, or a
                                          compressed variant); immutable
apps/<t>/<a>/_assets/index/<sha256>       one object per distinct set of assets; immutable
apps/<t>/<a>/_assets/deploying/<id>       a deploy with assets is running (below)
apps/<t>/<a>/_assets/collecting/<id>      a collection of assets is running (below)
```

`<sha256>` is the 64 lowercase hex digits of the object's bytes; `<id>`
is 16 random lowercase hex digits. Nothing else is written under
`_assets/`, and no command deletes a key there that it does not
recognise.

- **The index** is text, at most 8 MiB (a deploy whose index would be
  larger is refused), with lines ending in LF:

  ```
  wasmkeep-assets 1
  not-found spa
  front-first api/
  front-first auth/
  immutable assets/
  file index.html text/html;charset=utf-8 identity:<sha256>:<size> br:<sha256>:<size> gzip:<sha256>:<size>
  file logo.png image/png identity:<sha256>:<size>
  ```

  `not-found` once (`front`, `spa` or `404`); `front-first` and
  `immutable` lines sorted, without duplicates; then one `file` line per
  name, sorted by the name's bytes: the name with every byte outside
  `[A-Za-z0-9._~/-]` percent-encoded, the type without spaces, then
  `identity` and, when the variant exists, `br` and `gzip`, in that
  order, each with its content's digest and size in bytes. The form is
  canonical, so one set of assets and rules has one index and one
  digest. A node refuses (as a corrupt version) an index whose digest is
  not the manifest's, with a first line or a line it does not know, out
  of order, or over 8 MiB. A later index form takes a new first line and
  a new manifest format.
- **A deploy names its index by the index's digest.** **Manifest format
  7**, written only for a version with assets: format 6's lines (ADR
  0036) plus one, `assets <sha256 of the index>`, after the `front`
  line and before `access`, in the version's identity, so a deploy that
  changes only a file, or only a rule of `[assets]`, makes a new
  version. The line appears at most once. A version without assets is
  written exactly as before, in formats 1 to 6, with the same bytes and
  id. Two versions with the same assets share one index object.
- `_assets` is not a version id (an `_`), so version collection never
  treats it as one, and a CLI from before this release never touches it.

**What a deploy writes, in order.** Every deploy whose new manifest has
an `assets` line does this, a deploy that keeps the current assets
included (it has no files, so it uploads nothing in step 4):

1. It scans `DIR` and builds the index. It checks the capability
   word (section 11), prints the plan and waits for its confirmation.
   Nothing is written before this point, and no deadline runs while
   the prompt waits. If the new version's identity is the current
   one's, the deploy is **unchanged**: it asks nothing, still runs
   steps 2 to 5 and 7 as a **repair**, skips step 6, and ends with
   `unchanged`, or `unchanged, N repaired` when it uploaded N objects
   (the index counts as one).
2. It writes its marker, `_assets/deploying/<id>`, and notes the
   marker's last-modified time `T0`, which is the bucket's clock. From
   here to step 7 it writes the marker again every 10 minutes, through
   the wait and the uploads.
3. It lists `_assets/collecting/`. While a marker there is younger than
   15 minutes, it says `waiting for a collection of assets to finish`,
   waits 2 seconds and lists again. A marker's age is `T0`, plus the
   time passed since on the deploy's monotonic clock, minus the
   marker's last-modified time: so a marker an interrupted collection
   left holds a deploy until it is 15 minutes old, never longer.
4. Only then, it lists `_assets/content/` once, and uploads the contents
   its index names that are not there, then the index if it is not
   there, each with `If-None-Match: *` (an existing one is the same
   bytes by its name, and "already exists" is success), eight at a time.
   A deploy that changes one file uploads one content and one index.
5. It heads the index and every content the index names. One that is
   missing after step 4 stops the deploy. One whose size is not the
   index's (for the index object, not the size of the index the deploy
   built) holds other bytes under that digest: the deploy deletes it
   and uploads it again, the only delete a deploy makes under
   `_assets/`, and safe beside nodes, which check every digest. With
   `--assets-verify` it also reads each object back and checks its
   digest, and replaces a wrong one the same way; that costs a download
   of the whole site, so it is not the default, and it is what to run
   when a node counts `wasmkeep_asset_fetches_total{outcome="digest_mismatch"}`.
   A deploy that keeps the current assets has no bytes: if an object is
   missing or wrong it stops and says to deploy again with `--assets`.
6. It writes the classes, the front, the manifest and `current` as
   today (ADR 0013). It refuses to write the manifest, or to switch
   `current`, more than 30 minutes after it began the last write of the
   marker that the bucket acknowledged, measured on its own monotonic
   clock.
7. It deletes its marker, whatever the outcome, and before the
   collection the same run starts (or that collection would stop on
   its own deploy's marker). A marker left by a deploy that was killed
   is ignored once it is an hour old.

Contents are never refreshed, and written again only by a repair
(step 5): what protects
them is the marker, not their age.

**Collection** (ADR 0012). `wasmkeep deploy` runs it after version
collection, in the same run, after every deploy and rollback. Nodes
never delete an asset. The rule:

> An object under `_assets/content/` or `_assets/index/` is deleted only
> if (a) no manifest of any version directory of the app names it,
> directly (an index) or through its index (a content), (b) it was last
> modified more than the retention window before the collection began,
> and (c) no deploy marker younger than one hour existed when the
> collection began. Otherwise it stays.

The steps, whose order matters:

1. The collector writes its marker, `_assets/collecting/<id>`, notes
   its last-modified time `Tc` (the bucket's clock), and starts a
   five-minute timer on its own monotonic clock from before it sent
   that write. This is the first step of the whole collection run,
   before version collection, whenever the current version has an
   `assets` line or anything is under `_assets/`: version collection
   then takes `Tc` as its "now" too, in place of the workstation's
   clock that it compares to last-modified times today, and stops at
   the same five minutes. For an app that never had assets no marker
   is written and version collection is unchanged.
2. It lists `_assets/deploying/`. It deletes the markers last modified
   an hour or more before `Tc`. If any other remains, a deploy is
   running: it deletes its own marker and stops, having deleted
   nothing, and says `assets not collected: a deploy is in progress`.
   The next deploy or rollback collects.
3. It lists `_assets/content/` and `_assets/index/`, with ages: these
   are the only candidates, so nothing written after this listing can
   be deleted by this pass. It also deletes the markers under
   `_assets/collecting/` older than 15 minutes at `Tc`.
4. It finds what is in use: it lists the app's version directories,
   **every** one that has a `manifest`, in the lineage or not (a version
   a rollback stepped back from, or one a failed deploy left, still
   counts), reads each manifest, and for each `assets` line reads the
   index and checks its digest. A manifest or an index it cannot read
   (a format or a first line it does not know, a digest that does not
   match, a bucket error) stops the step with nothing deleted, and says
   which: a newer CLI collects. **An index the bucket says is not
   there** is handled so that collection never halts for good:
   - of the **current** version: the step stops with nothing deleted
     and says `assets not collected: the index of the current version
     is missing; run the deploy again to repair it`;
   - of **any other** version: that version cannot be loaded any more
     and its index cannot be rebuilt, since the files it was made from
     are gone. The step names it (`version <id> has lost its assets and
     cannot be rolled back to`), counts it as naming nothing, and goes
     on. Its contents then go like any unused ones.
5. It deletes the candidates that are not in use, whose name is 64 hex
   digits, and that were last modified at or before `Tc` minus the
   retention window (`--version-retention-secs`; 0 is allowed and means
   no wait). Before each delete it checks its timer, and stops once
   five minutes have passed; every 100 deletes it reads `current` again
   and stops if it changed, as version collection does. What is left is
   for the next pass.
6. It deletes its marker, on success or failure.

What this keeps, and why:

- **The current deploy and the ones before it.** Version collection
  keeps the current version and the five before it (ADR 0012), and
  every kept version's manifest is read in step 4, so their indexes and
  contents are in use. That covers the index a node served before its
  current one (below), unless six deploys ran inside the retention
  window. Collection of assets runs after version collection: the
  contents only a version deleted in this same pass named are no longer
  in use, and go in the same pass if they are old enough.
- **A deploy in progress.** Either the deploy wrote its marker before
  the collector listed the markers (step 2 of collection), and the
  collector deletes nothing; or it wrote it after, and then the
  collector's own marker, written before that listing, is there when
  the deploy lists `_assets/collecting/` (step 3 of the deploy): the
  deploy waits until that marker is gone or 15 minutes old, and the
  collector stopped deleting at five. The deploy lists the contents
  only after that, so whatever the collector removed, the deploy sees
  missing and uploads. From then until its manifest exists, its marker
  stops every later collector; once its manifest exists, step 4 finds
  it. A deploy that keeps the current assets is covered the same way,
  and also by `current`: its switch is conditional on the `current` it
  read, whose version is kept.
- **A rollback** writes no asset and takes no marker: it points
  `current` at a kept version, whose manifest was in use throughout.
- **Two collectors at once** are harmless: both delete only what no
  manifest names, and a delete of a key already gone succeeds.

What it assumes, said plainly:

- The bucket lists its own writes at once (the read-after-write
  listing `cluster diagnose` checks). A marker a listing does not show
  yet is a marker not seen.
- In a pass that collects assets, ages compare two times of the
  bucket's own clock (`Tc`, `T0` and last-modified times), for versions
  and for assets: the workstation's wall clock is not compared to the
  bucket's. The two deadlines (five minutes, 30 minutes) run on the
  local monotonic clock.
- **The remaining race.** A delete the collector sent before its five
  minutes ended, which the bucket applies more than ten minutes later,
  could remove a content that a deploy, having waited the 15 minutes,
  then found present. Likewise a deploy that stalls for more than 30
  minutes between its last deadline check and the write of its
  manifest could name a content a collector removed in between. Bucket
  requests time out long before either bound, and nothing fences a
  plain delete, so this is bounded, not excluded. Where the bucket and
  the client offer a conditional delete (`If-Match` with the ETag the
  listing of step 3 gave), step 5 uses it; that narrows the race and
  is not relied on.
- **What a lost object costs, and the repair.** A lost **content**:
  nodes answer `503` for that file only and count
  `wasmkeep_asset_fetches_total{outcome="unavailable"}`. A lost
  **index** is worse: a node that already holds it in memory goes on
  serving, but a node that must load the version (a restart, a new
  node, a rollback to it) cannot: its follower fails with `unreadable
  _assets/index/<sha256> (missing)`, and the node is not ready for the
  whole app, actors and front included, as for any version it cannot
  read. Both are repaired by running the same deploy again from the
  same files: an unchanged deploy runs steps 2 to 5 under a marker,
  uploads whatever is missing, the index included, and says
  `unchanged, N repaired`. No flag is needed, and a deploy that
  changes something repairs the objects it shares the same way.
- **A rollback** to a version whose index is missing is refused before
  `current` is touched.
- **Collection can be starved**: it runs at the end of a deploy or a
  rollback, and stops when another deploy's marker is there. `wasmkeep
  assets collect --app <tenant>/<app> --bucket <url>
  [--version-retention-secs SECS]` runs the same pass, versions then
  assets, on its own, for a moment when no deploy runs; a pipeline that
  deploys without pause schedules it.
- A directory a failed deploy left, with a manifest, keeps its assets
  until someone removes it; the step names such directories. Contents a
  failed deploy uploaded before it had a manifest are not in use, and
  go once they are older than the window.
- `app delete` (ADR 0028) removes every key under the app's two
  prefixes but its tombstone, assets and markers included. A backup copies the indexes and contents
  that the versions it keeps name, never a marker.

**Serving.** The hostname picks the app, then the path picks what
answers. With several apps in a process
([ADR 0038](0038-several-apps-per-process.md), section 6), the request's
authority is looked up in the node file's table before anything else:
an unknown host answers `421`, a stopped app's host `503`, and no
request falls into another app's assets. Each app runtime holds its own
index; nothing below looks at the host again, and `Forwarded` and
`X-Forwarded-Host` are never read. With one app and no table, no `Host`
is needed, as today. Inside the app, `App::route` becomes:

1. `/_wasmkeep/...`: the server's, now with `/_wasmkeep/blobs/v1/`
   (section 4).
2. Admission (ADR 0036): an app's ingress limits match asset paths as
   any other, and a refused request answers `429` before any lookup.
3. `/actor/<class>/<name>...`: the actors. No asset is ever served
   under `/actor/`, and the deploy refuses a top-level `actor/`.
4. A request that arrived on the cluster port: `404`, as today.
5. A path whose decoded form (below) is under one of `front-first`:
   the front.
6. **A `GET` or `HEAD` without `Upgrade`** whose path names an asset:
   the asset, answered by the host. No code runs, no front slot is
   taken, and a request body, if one is sent, is not read (one declared
   larger than the node's limit is answered `413` before routing, as
   today).
7. A `GET` or `HEAD` without `Upgrade` that names none: the `not-found`
   rule.
8. Everything else (other methods, `OPTIONS` included, and upgrades per
   ADR 0027): the front, or `404 not found` without one.

So the precedence is: the server's paths, then actors, then
`front-first`, then assets, then the front. A version without an
`assets` line skips steps 5 to 7.

Assets are served on the public port only, never on the cluster port,
never forwarded. A path that fails the decoding below goes straight to
step 8, whatever `not-found` says. A path whose decoded form starts with
`_wasmkeep/` or `actor/` while its raw form does not (`/%5Fwasmkeep/`,
`/%61ctor/`) answers `400`: the reserved prefixes have one spelling. The
front receives the raw path, as today.

**Matching.** The path (the query is ignored) is percent-decoded once
and must then be valid UTF-8 with no control character, no `\`, no empty
segment, no `.` or `..` segment, and no `/` that came from `%2F`;
anything else names no asset. Then, in order: the name itself; for a
path ending in `/`, its `index.html`; for a path not ending in `/` whose
`<path>/index.html` exists and that names no file, a `308` whose
`Location` is the raw path with `/` added (query kept, one leading `/`),
with `Cache-Control: no-cache` (a cached `308` would outlive a version
that drops the `index.html`). Matching is case-sensitive and a lookup in
the index held in memory: no file system is involved, so no request
can reach a file outside the version's table.

**`not-found`**, for an unmatched `GET` or `HEAD`:

- `front` (the default): to the front, or `404 not found` without one.
- `spa`: a **navigation** gets the root `index.html` with `200`;
  anything else goes to the front (or `404`). A navigation is a request
  with `Sec-Fetch-Mode: navigate`, or without that header, one whose
  `Accept` names `text/html` and whose last path segment has no `.`. So a
  missing `app.4f9c2e.js` is never answered with HTML. The deploy
  refuses `spa` without a root `index.html`.
- `404`: the root `404.html` with status `404`, or `404 not found`
  without one. Unmatched paths never reach the front, but through
  `front-first` and other methods.

Every not-found answer the host makes carries `Cache-Control: no-store`,
and so does the front's answer, if it is not a `2xx`, to an unmatched
path under an `immutable` prefix (the host replaces the front's
`Cache-Control` there): during a rollout a node not yet switched does
not have a new file, and a cache must not keep that answer.

**Responses.**

- `HEAD` answers as `GET` does, with the same status and headers
  (`Content-Length` included) and no body.
- `Content-Type` from the index; `X-Content-Type-Options: nosniff`.
- `ETag`: the strong tag `"<first 32 hex digits of the representation's
  digest>"`, so each encoding has its own. `If-None-Match` (a list, `*`,
  weak comparison) answers `304` with the `ETag`, `Cache-Control` and
  `Vary` the `200` would carry. No `Last-Modified`, and
  `If-Modified-Since` is ignored.
- `Cache-Control`: `public, max-age=31536000, immutable` for a name
  under an `immutable` prefix; `public, max-age=0, must-revalidate` for
  every other. The rule is the prefixes the app declares, never a guess
  from the name: a name that only looks fingerprinted is not cached for
  good. The docs give the prefix per build tool.
- `Accept-Encoding`: `br`, then `gzip`, then the plain file, honouring
  `q=0`; `Content-Encoding` as chosen, and `Vary: Accept-Encoding` on
  every response of a file that has a variant.
- `Range`: one range of the plain file (`206`, `Accept-Ranges: bytes`),
  with `If-Range` honoured only for the plain file's ETag. A `206`
  always carries bytes of the plain file and the plain file's `ETag`,
  with no `Content-Encoding`. An `If-Range` that holds anything else
  (the ETag of the `br` or `gzip` variant, a date, another version's
  tag) makes the request an ordinary one: a full `200`, with the
  encoding chosen from `Accept-Encoding` as usual. A request with a
  usable `Range` gets the plain file whatever its `Accept-Encoding`; an
  unsatisfiable one gets `416` with a `Content-Range` giving the
  file's size and no range (RFC 9110, section 14.4).
  Several ranges get the whole file.
- No compression at request time: only the variants the deploy stored.
- Bodies are streamed from the node's cache on disk, never held whole
  in memory. The size limits are the deploy's (25 MiB a file, 512 MiB
  and 20,000 files a version, an 8 MiB index); the node enforces the
  index limit and each content's size from the index when it fetches.
- The `spa` fallback answers with `Cache-Control: no-cache` and
  `Vary: Sec-Fetch-Mode, Accept`, and the host adds that `Vary` to the
  front's answers to unmatched `GET` and `HEAD` requests under `spa`, so
  a cache between the client and the node keeps a navigation's answer
  apart from a script's.
- Nothing else: no header from the request (Host, `X-Forwarded-*`,
  cookies) changes an asset's answer, which is what keeps a shared cache
  from storing one client's variant for another. Headers the app wants
  on its pages (a `Content-Security-Policy`, say) are "Not decided here":
  until then, a `<meta>` tag in the HTML, the proxy, or `front-first`.

**What the front can override: nothing for a matched asset.** The host
answers it before any code. An app that wants code on a path does not
ship a file there, or lists its prefix in `front-first`, which is part of
the version and so reviewed, deployed and rolled back with it. This is
the point of assets: their answers do not depend on code.

**On the node.** At a version switch the node reads the index (digest
checked against the manifest line) under the same lock as the classes and the
front (ADR 0023), and swaps it with them. Contents are fetched on first
use, one fetch per digest at a time, verified against their digest
before they are served or kept, and kept on disk in
`<data_dir>/assets/<tenant>/<app>/<sha256>` up to `--assets-cache-bytes`
(default 1 GiB, least recently used out first). With several apps the
directory is each app's own, never shared by digest: an index could
name the digest of a file its app never uploaded, and must not be
answered from another app's copy. The byte limit is the process's, one
least-recently-used order across its apps. In the background the node
fetches the rest of the version's contents while they fit. A content
the bucket cannot give is `503` with `Retry-After: 1`. Since every
byte fetched is checked against a digest the manifest chains to, a
corrupt or tampered object, or a partial download, is never served. The
cache on disk is the node's own, checked once when filled and again when
the node starts (a size check, and the digest of any file whose size or
time changed); bytes changed on disk afterwards are the node's disk
failing, outside this check.

**Versions side by side.** Nodes switch a few seconds apart, and a
browser keeps a page for hours: a page from the new version can ask an
old node for a new file, and an open tab can ask for a file a new
version dropped. A node therefore keeps the index it served before its
current one (whatever the switch was: a deploy or a rollback, so after
a rollback from B to A it keeps B's), in memory and in its data
directory so that a restart keeps it; a node that never served another
takes the index of the manifest's `previous`. For a name under an
`immutable` prefix that the current index lacks, it answers from that
one. Only immutable names: the deploy refuses to change their contents
(above). Collection keeps the contents of the versions it keeps (the
current one and the five before it, ADR 0012), which includes the one a
node served before unless six deploys ran within the retention window. A new
file asked of a node not yet switched falls to the `not-found` rule
(never HTML for a script under `spa`), for those few seconds.

**Rollback** re-points `current` at the earlier version, whose index and
contents collection kept: the assets come back with the front and the
classes, with no upload. It first heads the index the target version
names, and is refused if it is missing.

**`wasmkeep dev`** serves `[assets] dir` from disk with the same rules:
the same routing order, matching, `not-found` modes, `front-first`,
types, `ETag`, ranges and `Cache-Control` values, and the deploy's
refusals and skips (no symbolic link is followed, no dotfile served; a
refusal is printed and the name is not served, the session goes on). It
runs `[assets] build` at start and when a path under `[assets] watch`
changes, builds its table from the directory, and rebuilds it when the
directory changes. It differs in four ways: no compressed variant is
made or served (a sibling `X.br` or `X.gz` is still hidden as a
variant, unchecked); the immutable-name check against a previous
version does not apply; there is no previous index; and files are read
from the directory at each request, with the digest for the `ETag`
computed when the table is built. It writes nothing to a bucket.

**Mixed versions.** A node of this release announces the capability
word `assets` on heartbeat line 11, after the words it has today, and
reads manifest formats 1 to 7.

- **The deploy** of a version with an `assets` line is refused, before
  any write, while a live node of the app does not announce `assets`:
  `live nodes of <tenant>/<app> cannot read a version with assets
  (manifest format 7) (<node ids>): upgrade them first`
  (`DeployError::NodesWithoutAssets`, beside `NodesWithoutEnds`). There
  is no `--force`. A rollback to such a version is checked the same
  way. Format 7 also inherits the checks of the formats below it.
- **A node without the word** that meets a format 7 manifest all the
  same (it started after the check) cannot parse it: `Manifest::parse`
  returns nothing, the deploy follower fails with
  `DeployError::Corrupt`, logged as `checking for a new deploy failed`
  with `unreadable <version>/manifest`, and marks the node behind the
  deploy: `/_wasmkeep/ready` answers `503` until it can apply the
  version, and it retries with a growing wait, as for any version it
  cannot read (ADR 0013). It never serves a format 7 version without
  its assets. The same holds for a build of this release from before
  slice A4: until the node serves assets, its loader refuses a manifest
  with an `assets` line the same way, and does not announce the word.
- **An older CLI** cannot read a format 7 manifest: it stops on
  `current`, deploys nothing and collects nothing.
- **Rolling back the release**: deploy a version without assets
  (`--no-assets`, or no `[assets]`), which is written in format 6 or
  below, then downgrade the nodes. The objects under `_assets/` stay
  until a CLI of this release collects them.

**Guests.** No guest reads, lists or writes an asset: there is no host
function for them, `wasmkeep:actor/blobs` reaches only the caller's
own namespace under `state/`, never `apps/<t>/<a>/_assets/`, and no
guest names a bucket key (section 8). A front that needs a built file's
bytes or name carries it in its component; the build order (assets,
front, classes) is for that.

**Security, for assets.**

- **Path traversal.** At deploy, names come from walking `DIR` and are
  refused with a `.` or `..` segment, `\`, a control character or
  invalid UTF-8. At a request, the decoded path is refused the same
  segments, and matching is a lookup in the index: no path of a file
  system is ever built from a request. The cache on disk is named by
  digests only.
- **Symbolic links** are refused at deploy and by `dev`; only `DIR`
  itself may be one, resolved once. Other non-regular files (devices, sockets, pipes)
  are refused the same way. Hard links are regular files and are taken.
- **No directory listing.** A path ending in `/` answers its
  `index.html` or falls to `not-found`; the host never makes a listing.
- **No header of the request picks the app or the file** but the
  authority and the path.
- **Integrity.** Every byte served was checked against a digest that
  chains to the manifest, which chains to the version id.
- Whoever can deploy the app decides its assets, as it decides its
  code; the deploy's credentials are the check (ADR 0008).

**Metrics**: `wasmkeep_asset_requests_total{outcome}` (`ok`,
`not_modified`, `partial`, `redirect`, `spa`, `not_found`, `to_front`,
`unavailable`), `wasmkeep_asset_bytes_total{encoding}`,
`wasmkeep_asset_cache_bytes`, and
`wasmkeep_asset_fetches_total{outcome}` (`ok`, `digest_mismatch`,
`unavailable`), each with `tenant` and `app`.

**Slices.** Each is one change with its proof; A1 to A3 deploy and
collect, A4 to A7 serve. Order: A1, A2, A3, A4, A5, then A6 and A7 in
either order. The word `assets` is announced only from A4,
so until then every format 7 deploy to a cluster is refused, and tests
of A2 and A3 work on the bucket alone.

| Slice | What | Where | Proof |
| --- | --- | --- | --- |
| A1 | The scan of `DIR` (refusals, skips, variants, types), the index text and its digest, `[assets]` in the project file and the `--assets*` flags, the plan's listing | new `crates/wasmkeep-cli/src/assets.rs`, `crates/wasmkeep-cli/src/{main,project}.rs` | CLI unit tests: every refusal and skip named above, a symbolic link, a variant that does not decompress to its file, an index that is byte-identical for the same directory walked in another order, an entry of `front-first` without `/` |
| A2 | Format 7 (`assets` line, parse and write, identity), the index parser, the refusal while a node lacks `assets`, the deploy's steps 1 to 7 with the marker, keeping the current assets, `--no-assets` | `crates/wasmkeep-server/src/apps.rs`, new `crates/wasmkeep-server/src/assets.rs`, `crates/wasmkeep-cluster/src/membership.rs` | `apps.rs` unit tests: format 7 round trip, `assets` refused in format 6 and twice, a version without assets keeps its bytes and id, the fixtures that use `wasmkeep-manifest 7` as the unknown format moved to 8, `format_7_needs_assets_and_every_word_below_it`; on an in-memory bucket, a second deploy uploads only the changed content, a marker is there during the deploy and gone after, a deploy waits for a fresh collection marker and not for a stale one, a missing kept content refuses |
| A3 | Collection of assets after version collection, version collection on `Tc`, the repair run of an unchanged deploy and `--assets-verify`, `wasmkeep assets collect`, the rollback's index check | `crates/wasmkeep-server/src/apps/collect.rs`, `crates/wasmkeep-server/src/apps.rs`, `crates/wasmkeep-cli/src/main.rs` | `collect.rs` unit tests on an in-memory bucket: version collection's cutoff taken from the marker's time (a store whose clock is an hour off the test's), a missing index of the current version deletes nothing, one of another version is named and the pass goes on, an unchanged deploy re-uploads a deleted content and a deleted index and says `unchanged, 2 repaired`, a content of another size replaced, a content of the right size and other bytes replaced only with `--assets-verify`, a rollback to a version without its index refused; unused contents and indexes deleted, used ones kept, a version outside the lineage keeps its assets, an unknown manifest format or a missing index deletes nothing, a fresh deploy marker deletes nothing and a stale one is removed, a key that is not 64 hex digits is never deleted, the five-minute stop; **the race**: a store wrapper pauses the collector after its listing of contents, a deploy that reuses an unused old content runs meanwhile and must wait, the collector resumes and deletes, the deploy then uploads the content again, and the deployed version has every content; the same with the deploy first, where the collector deletes nothing |
| A4 | The node reads the index at a version switch, fetches and verifies contents, the cache on disk at its per-app path, the routing order, matching and responses, the metrics with their `tenant` and `app` labels, the word `assets` announced | `crates/wasmkeep-server/src/{assets,http,apps,metrics}.rs`, `crates/wasmkeep-server/src/actor/deploys.rs`, `crates/wasmkeep-cluster/src/membership.rs` | unit tests of matching and of every response header; new `crates/wasmkeep-server/tests/cluster/assets.rs` (registered in `main.rs`): a file, `index.html`, the `308`, `HEAD`, `304`, a range and `416`, `br` and `gzip` choice and `Vary`, escaped reserved prefixes `400`, an asset never under `/actor/`, a `206` with the plain file's `ETag` and an `If-Range` holding a variant's tag answered `200`, a tampered content object refused by digest and `503`, a node restarted after its version's index was deleted not ready until an unchanged deploy repairs it, no asset on the cluster port, a deploy refused while a node lacks the word |
| A5 | `not-found` modes, `front-first`, `immutable` and the cache headers, the previous index and its persistence, rollback | `crates/wasmkeep-server/src/{assets,http}.rs` | `assets.rs`: `front`, `spa` (a navigation, a script, the `Vary`) and `404` with and without `404.html`; a `front-first` prefix reaching the front over a file; the previous version's immutable names served after a deploy and after a restart; `no-store` on not-found answers; rollback serving the old files with no upload |
| A6 | Several apps: the hostname picks the index. **Blocked** on ADR 0038's hostname table (its hostname-routing slice); A1 to A5 and A7 do not wait for it | `crates/wasmkeep-server/src/assets.rs` and the app runtime of ADR 0038 | `crates/wasmkeep-server/tests/cluster/two_apps.rs` (ADR 0038): two apps with the same asset names and other bytes each answer their own by host, an unknown host `421`, an index naming a digest only the other app holds answers `503`; each app's metrics carry its own labels |
| A7 | `wasmkeep dev`, the docs (deploy guide, front page, upgrade notes, the type table), and the end-to-end collection proof. Needs A5, not A6 | `crates/wasmkeep-cli/src/dev.rs`, `docs/` | a `dev` test serving a directory, a changed file and a symbolic link; `assets.rs`: seven deploys with retention 0 collect the first version's contents while every kept version still answers each of its files from every node, and **a deploy running during a collection**, both orders as in A3, against the cluster's bucket, after which every node answers every file of the new version |

### 10. `cluster diagnose`

One new group of checks, `blob-links`, run unless `--skip-links`, under
the scratch prefix, signed with the node's credentials for
`--link-endpoint URL` (default as for `serve`):

- `presigned-upload`: an upload link with a size, type and
  `if-none-match: *` lands with exactly those.
- `presigned-upload-conditions`: the same link with another size,
  another type, no `if-none-match`, an unsigned `x-amz-acl`, or over an
  existing key, is refused each time; a link signed for one key does not
  write another (the cases of the node's start proof, section 4).
- `presigned-download`: a download link returns the bytes with the
  signed `Content-Disposition` and `Content-Type`, and a link for one key
  reads no other.
- `presigned-expiry`: a link signed for 2 s is refused after 4 s.
- `cors` (with `--origin URL` only): a preflight `PUT` from that origin
  with `content-type` and `if-none-match` is allowed.

A failure in the first four makes the verdict `warn`, not `FAIL`: the
cluster is still safe, but its nodes will fall back to node links, and
the output says `presigned links: not safe on this bucket; nodes use
node links (--blob-links node)`. `cors` failing is `warn` with the rule
to add. The existing checks and their verdict do not change.

### 11. Compatibility

- **Built components** link unmodified: `blobs` is added to
  `actor-core` and `front-core`, and nothing they import changes.
- **Capability words** beside `take` and `front` (ADR 0023): `blobs` and
  `assets`.
  - A deploy of a class or front that imports `wasmkeep:actor/blobs` is
    refused while a live node of the app lacks `blobs`: such a node
    could not link it, and would stay not ready. A deploy writing
    format 7 is refused while a live node lacks `assets` (section 9,
    "Mixed versions"). Neither has a
    `--force`, as for `front`. A rollback to such a version is checked
    the same way.
  - **Deletes need the word too.** A delete of an actor whose class
    imports `blobs` must write the sweep record and a new incarnation;
    a node without this release would not, and the next actor would
    inherit the old files at incarnation `0`. Such a node cannot link the
    class, so it does not serve it, and ADR 0028 leaves a request to the
    nodes that serve the class; the delete job and idle expiry also
    check, on the node that runs them, that it has the word.
  - The TypeScript engine (ADR 0018) must import `blobs` only in builds
    that use it, or every TypeScript deploy would wait for the word
    during an upgrade; checked when built.
  - A node and CLI of this release write and read formats 1 to 7. An
    older CLI cannot read a format 7 manifest and stops, as with format
    3 before 0.4.0.
- **Older deploys** are unchanged: a version without assets is served as
  before, and its manifest keeps its bytes and id.
- **Behaviour changes**, for the release notes:
  - a decoded path that spells `/_wasmkeep/` or `/actor/` with escapes
    answers `400`;
  - a `GET` or `HEAD` to a path that names an asset no longer reaches the
    front;
  - `/_wasmkeep/blobs/v1/` answers on the public port;
  - the delete routine writes one more object before the reset, and the
    lowest live node of an app lists `blob-sweeps/` once a minute;
  - nodes run two more requests at start on `probe/` (the link proof).

## Consequences

- An app keeps files without a storage service of its own and without
  credentials in its configuration, and browsers move the bytes to and
  from the bucket directly.
- A site's files are served by every node with no instance, from a
  cache on disk, with correct caching headers, and roll back with the
  app.
- The node's bucket credentials sign links: whoever can make an actor
  issue a link can read or create one file. The actor's code is the
  check.
- Upload CORS and the link endpoint are new operator settings, and a
  bucket that does not enforce signed headers costs node bandwidth
  instead of failing.
- Quotas lag by up to an hour, and the host leaves unreferenced files to
  their owner.
- A deleted actor costs one more write and a sweep of its files; an app
  with many files costs one listing of them an hour.
- Deploys with assets list `_assets/content/` once and upload only new
  contents; collection reads every version directory's manifest and
  index, and skips a pass while a deploy with assets is running.

## Alternatives

- **Why a host interface, not an SDK pattern** (ADR 0015, 0019, 0029 chose
  patterns). A pattern can only keep bytes in actors' databases, which
  is the problem: 1 GB per actor, every byte replicated as history and
  downloaded by each restore. Links need credentials that guests must
  never hold, and only the host can sign with them. The same holds for
  assets: serving without waking code is the host's or nobody's.
- **One app-wide namespace.** Rejected in section 1: no owner, no
  lifecycle, and every class reaching every file.
- **Key spaces without an incarnation**, swept by prefix after a delete.
  A new actor at the address would write into the prefix being swept, or
  inherit a file an old upload link wrote after the sweep.
- **Presigned POST with a policy** (`content-length-range`). It allows a
  size range rather than an exact size, but not every S3-compatible store
  implements POST policies, and `object_store` does not sign them. An
  exact size, known by the app when it asks, works everywhere SigV4
  presigned PUT works, and the start proof checks it.
- **Proxying every transfer through the node.** Simpler, and what node
  links are, but it puts every byte through a node's bandwidth and a
  request slot, against the roadmap's "directly". Kept as the fallback.
- **Assets copied into each version directory**, as classes are. A
  deploy would copy every file server-side each time (thousands of
  requests for an unchanged site), and nothing would dedupe across
  versions. The pool costs one more collection step.
- **The front answering assets** (an SDK helper over files embedded in
  the component). Every request would instantiate the front, and a large
  site would make a large component, loaded on every node at each deploy.
- **Fingerprinting guessed from names** (`app.4f9c2e.js`). A name that
  only looks hashed would be cached for a year; the app knows its build
  tool's output directory.
- **The front able to override a matched asset at run time.** Its answer
  would then depend on code again, for every request, to save a
  `front-first` line.
- **Range requests and compression on the fly.** Precompressing at
  deploy costs nothing per request; compressing per request costs CPU no
  budget charges.

## Not decided here

- A read-only link the front could sign for an actor's file without
  waking it (for `<img>` tags on hot pages), and how the actor would
  allow it.
- Uploads larger than one presigned PUT (5 GiB) and resumable uploads:
  presigned multipart parts.
- Binding a content digest to upload links (`x-amz-checksum-sha256`)
  when the app knows it, and a node-side set of used node-link tokens:
  both would make a link truly single-use.
- Per-actor quotas kept by the host, and a hard app quota.
- Custom headers on assets (`Content-Security-Policy`,
  `Strict-Transport-Security`), and redirects or rewrites declared
  outside the front.
- Moving a node's local files into a bucket when it joins a cluster.
- Encryption of files by WasmKeep: files are stored as written, like the
  actors' history; the bucket's server-side encryption applies.
- A trusted-proxy setting for the authority that picks the app's assets
  (ADR 0038 leaves it open); until then only `Host` or `:authority`
  counts.
- Relying on a conditional delete (`If-Match` on the delete) to close
  the remaining race of asset collection: section 9 uses one where the
  bucket and the client offer it, but not every store does, and the
  rule does not depend on it.
- The SDKs other than Rust and TypeScript.

## Tests (to build)

- **`sim/`**: deletes with files among takeovers and late uploads:
  `deleted_actors_files_never_reach_the_next_incarnation`, and an unsafe
  variant without the incarnation that must be caught; the sweep record
  written, the delete failing after it, and the record dropped with no
  file touched.
- **Cluster** (`tests/cluster/blobs.rs`, plain and TLS): put, get, head,
  list and delete with conditions; a write refused without a hold; an
  actor's files swept after `actor delete`, a download link failing
  after; an upload link used twice, with a wrong size, a wrong type and
  after its expiry; node links on a store without signing, a token of
  another app refused; the hourly count and `quota-exceeded`.
- **Assets** (`tests/cluster/assets.rs`): matching, `index.html`, the
  `308`, every `not-found` mode, `front-first`, `304`, ranges, `br` and
  `gzip` choice and `Vary`, a tampered content object refused by digest,
  the previous version's immutable names served, rollback, and
  collection of unused contents with a concurrent deploy (section 9,
  slices A3 and A7).
- **CLI**: asset names refused and skipped, symbolic links, precompressed
  variants that do not match, format 7 and the capability refusals.
- **Diagnose**: the `blob-links` checks against a local S3 server that
  enforces signed headers, and a fake store that ignores them for the
  `warn` path.

## Plan

| Slice | Size |
| --- | --- |
| This record | S |
| WIT `blobs`, the host on a bucket and on disk, Rust and TypeScript SDKs | L |
| Links: presigned, node links, the start proof, `cluster diagnose` | M |
| Incarnations, sweep records and the sweep pass (with ADR 0028's routine) | M |
| Counting, `--max-app-blob-bytes`, metrics | S |
| Assets: deploy, index, format 7, collection (section 9, A1 to A3) | M |
| Assets: serving, cache, `not-found`, `dev` (section 9, A4 to A7) | M |
| Sim cases, cluster and CLI tests, adversarial review | M |
