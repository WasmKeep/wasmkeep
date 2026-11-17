# Files

An actor, or the front, can keep files: photos, documents, exports,
anything too large or too binary for a row. Files live in the app's part
of the cluster's bucket, or on the node's disk on a node without a
bucket. Each caller has its own namespace: an actor sees only its own
files, the front only the front's, and no call can name another's. The
design is [ADR 0030](adr/0030-files-and-static-assets.md); static files
served as they are deployed are [static assets](static-assets.md), a
separate feature.

## Writing and reading

Rust (`wasmkeep_sdk::blobs`, in actors and in fronts):

```rust
use wasmkeep_sdk::blobs::{self, PutOptions};

blobs::put("photos/1.jpg", &bytes, &PutOptions::new().content_type("image/jpeg").if_absent())?;
if let Some((info, body)) = blobs::get("photos/1.jpg")? {
    // up to 16 MiB in one call
}
for info in blobs::list_all("photos/") {
    let info = info?;
}
// Larger files: a stream each way.
let mut w = blobs::open_write("videos/1.mp4", &PutOptions::new())?;
std::io::Write::write_all(&mut w, &chunk)?;
let info = w.finish()?;
```

TypeScript (`@wasmkeep/sdk/blobs`, a module of its own):

```ts
import * as blobs from "@wasmkeep/sdk/blobs";

blobs.put("photos/1.jpg", bytes, { contentType: "image/jpeg", ifAbsent: true });
const file = blobs.get("photos/1.jpg"); // undefined if there is none
for (const info of blobs.listAll("photos/")) console.log(info.key, info.size);
```

The TypeScript calls are buffered (a whole file, up to 16 MiB); streams
are in the Rust SDK only for now. A component that never imports the
module imports no `wasmkeep:actor/blobs`, and runs on a node of any
release. Python has no files yet. Both SDKs' examples above are the ones
their contract tests run against the host.

A **key** is 1 to 512 bytes of `A-Z a-z 0-9 . _ ~ -` and `/`, in
segments that are not empty, `.` or `..`. A write takes a content type
(`application/octet-stream` by default) and a condition: `if_absent`
(create only) or `if_match(etag)` (replace that version only); a
condition that does not hold answers `conflict`. Every file has a size,
a content type, an `etag` and a modified time; `list` returns them in
key order, 1,000 at most per page.

## Limits

| Limit | Value |
| --- | --- |
| key | 512 bytes |
| `get`, `put` | 16 MiB, one call's body |
| one file, streamed | `--max-blob-bytes` (the process's), 1 GiB by default, at most 5 GiB |
| one upload link | 16 MiB on a bucket; `--max-blob-bytes` on a node without one |
| a listing page | 1,000 |
| link expiry | 3,600 s at most |
| links per event | 1,000 |
| the app's stored bytes | `--max-app-blob-bytes`, or the node file's `max-app-blob-bytes`: none by default |

**The app's limit lags.** A bucket gives no size for a prefix, and
links bypass the host, so nothing refuses a write by a total known at
that moment. Once an hour one node of the app counts its files and
writes the count to the bucket; every node reads it once a minute. While
the last count is over the limit, writes and upload links answer
`quota-exceeded` (a node link answers `507`); reads and deletes are
never refused, and a delete frees nothing until the next count. So the
app can go over by up to an hour of writes plus every upload link
outstanding. A restored or re-created app has no count until its first,
an hour at most after its nodes start, and is not refused meanwhile. On
a node without a bucket the count walks the directory, and
`/_wasmkeep/ready` (the disk check) stays the hard stop. Keep per-user
totals in the actor's own database: the host keeps only the app's.

## Links

A link is a URL that allows one request on one file until it expires,
for a browser to download or upload without the actor's code in the
path. Whoever holds a link can use it, and it cannot be revoked before
it expires except by deleting the file (a download), so links are short
(downloads 300 s by default, uploads 900 s, 3,600 s at most) and the
host never logs them.

```rust
let url = blobs::link_download("photos/1.jpg", &blobs::DownloadOptions::default())?;
let up = blobs::link_upload("uploads/7f3a9c", size, "image/jpeg", None)?;
// up.url, up.method ("PUT"), up.headers to send, up.expires_at_ms
```

```ts
const url = blobs.linkDownload("photos/1.jpg");
const up = blobs.linkUpload("uploads/7f3a9c", { size, contentType: "image/jpeg" });
```

- **Downloads** are `attachment` unless asked `inline`, which is
  allowed only for `image/png`, `image/jpeg`, `image/gif`, `image/webp`,
  `image/avif`, `audio/*`, `video/*` and `text/plain` (never HTML, SVG,
  PDF or XML, which can run script). On a bucket whose start proof
  passed, a download is a presigned URL of the bucket: the bytes come
  from the bucket, with the type, disposition and caching the link
  signed, whatever headers the object was stored with. Otherwise it is a
  node link (below).
- **Uploads are node links, in 0.6.0, on every node.** The library that
  signs bucket URLs cannot sign the `Content-Length` header, so a
  presigned upload could not hold the browser to the size the app
  allowed. A node link checks it: the node refuses, before reading the
  body, another `Content-Length`, a chunked body or another
  `Content-Type`, and writes the file only if it does not exist yet. On
  a bucket that write is one conditional `PUT`, which takes at most
  16 MiB, so an upload link allows 16 MiB at most there. For larger
  files, send them to the actor in chunks through a stream
  (`open_write`). Larger presigned uploads may come in a later release.
- **Node links** are `/_wasmkeep/blobs/v1/<token>` on the app's public
  port, under the app's own hostname: the node file's `blob-link-base`
  when set, else `https://` and the app's first hostname, else
  `--advertise`. The token carries an HMAC under a key derived per app,
  so any node of the app checks it and no other app's node accepts it.
  A node link's download carries `X-Content-Type-Options: nosniff` and
  a sandboxing `Content-Security-Policy`. Node links cost the node's
  bandwidth and a request slot each.
- **Upload links are create-only, not single-use.** While the file
  exists the link fails (`412`); if the app deletes the file before the
  link expires, the link can create it again. So give every upload a
  key never used before (`uploads/<random>`), and never reuse it.

`--blob-links auto` (the default) presigns downloads on a bucket whose
start proof passed and uses node links otherwise; `node` uses node
links only; `presigned` refuses to start when the proof fails.
`wasmkeep_blob_links_mode` says which a process uses.

**Links in the wild.** A URL leaks through history, logs, `Referer`
headers and screenshots: serve pages that show download links with
`Referrer-Policy: no-referrer`. A presigned link shows the bucket's
endpoint and name, the app's prefix layout and the access key id that
signed it (never the secret). With temporary credentials a presigned
link dies when they do: give nodes credentials that outlive the longest
link. Set `--blob-link-endpoint` to the bucket endpoint as browsers
reach it (a node on a private address must), and **never to a host
under the app's own site**: a file that runs script there must not
receive the site's cookies.

## A row and its file

A file call is durable when it returns; a plain database write is not
durable until its event's changes reach the bucket, and later still in
a class with [batched writes](adr/0034-batched-plain-writes.md). So a
row and its file can part. The host knows owners, not references, and
repairs neither case: the app does, in this order.

- **Recording a file**: write the file first, then the row that names
  it, and make the row durable at once: `storage.flush` after it, or
  `exec-durable` for the row itself ([what saved means](what-saved-means.md)). A node that dies in between leaves
  a file no row names, never a row that names no file.
- **Uploads from a browser**: give each link a fresh key
  (`uploads/<random>`); record the pending upload with its expiry and
  flush; when the browser says it is done, check it with `head` and
  record it as confirmed; from an alarm, delete pending uploads past
  their expiry, and any file that reappears at a key already deleted.
- **Deleting a file**: remove the row, make it durable, then delete
  the file; treat a `head` that finds nothing as a row to drop.

## When an actor ends

An actor's files end with it: `delete-self`, `delete-self-durable`,
`wasmkeep actor delete` and idle expiry all remove them. A new actor at
the same address starts with no file, even before the old ones are
gone: each life of an address has its own namespace (its
*incarnation*), and a sweep removes the old one's files within about
25 hours, checking first that the actor really moved on. `wasmkeep app
delete` removes every file of the app, and `deploy --recreate` starts
the app's files afresh at once.

**Idle expiry counts events only.** A file downloaded through a link
every day does not keep its actor alive: no code runs. An actor whose
files must outlive a quiet period needs an alarm, or no
`expire-after-idle`.

**`actor restore` restores rows, not files.** It rewinds one actor's
database; its files are as they are now. A restore to a point before a
delete gives the actor its old rows in a new, empty namespace. A row
may name a file deleted since: check with `head` before trusting a
restored row. To get the files of a past moment, restore the whole app
from a backup into a prefix of its own and copy them out
([backups](deploy/backup-and-restore.md#files-and-assets)).

## The bucket's own rules

Some files are left behind on purpose, and a lifecycle rule on the
bucket is the backstop:

- **Strays**: an upload still running a day after its actor was
  deleted, or a stream written before a crash, can leave a file in a
  namespace nothing names. Expire objects under `<prefix>state/*/*/blobs/`
  older than the longest a file must live only if the app keeps no file
  that long; otherwise leave them, they cost storage only.
- **Incomplete multipart uploads**: streamed writes larger than 16 MiB
  use multipart uploads. One the node abandons is aborted, and a rule
  that aborts incomplete multipart uploads after a day takes those a
  crash left. Every provider below supports it:
  AWS S3 and MinIO (`AbortIncompleteMultipartUpload`), Cloudflare R2
  (object lifecycle rules, "abort incomplete multipart uploads"),
  OVHcloud Object Storage (S3 lifecycle configuration).
- **Versioning**: WasmKeep never reads object versions. A bucket that
  keeps old versions keeps deleted files as old versions too, until its
  own rule removes them.

**CORS.** Upload links are node links on the app's own hostname: a page
served from that hostname needs no CORS rule. A page on another origin
cannot upload through them (the node answers no preflight). Presigned
downloads are plain navigations or `<img>`/`<video>` loads, which need
none either; a page that `fetch`es a presigned URL needs a CORS rule on
the bucket allowing `GET` from the app's origin.

## Several apps, backups, metrics

- In a process of several apps, each app's files, links and limit are
  its own; `--max-blob-bytes`, `--blob-links` and
  `--blob-link-endpoint` are the process's, and `max-app-blob-bytes`
  and `blob-link-base` are `[[app]]` fields of the
  [node file](reference/node-file.md).
- `backup create` copies files and assets, `backup restore` writes them
  back: [files and assets in backups](deploy/backup-and-restore.md#files-and-assets).
- The `wasmkeep_blob_*` series are listed in [monitoring](monitoring.md).
- Before deploying a version that uses files, upgrade every node:
  [upgrading to 0.6.0](deploy/upgrading-to-0.6.md#files).
