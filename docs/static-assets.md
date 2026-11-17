# Static assets

Ship HTML, scripts, styles and images with an app. Every node serves them
without running a front or waking an actor. Assets belong to the app:
with a [node file](reference/node-file.md), the hostname selects the app
before any file lookup; an unknown hostname answers 421.

## A site in the project file

```toml
[app]
tenant = "acme"
name = "site"

[assets]
dir = "web/dist"
build = "npm --prefix web run build"
watch = ["web/src"]
not-found = "spa"
front-first = ["api/", "auth/"]
immutable = ["assets/"]
```

Run `wasmkeep dev` locally, then `wasmkeep deploy --bucket s3://my-bucket`.
Paths are relative to the [project file](project-file.md). The assets build
runs before the front and class builds. A site can have assets and no code.
Removing `[assets]` removes the deployed assets on the next project deploy.

Without a project file:

```bash
wasmkeep deploy --no-project --app acme/site --bucket s3://my-bucket \
  --assets web/dist --assets-not-found spa --assets-immutable assets/
```

`--assets DIR` replaces assets; `--no-assets` removes them; neither keeps
the current index. `--assets-not-found`, `--assets-front-first` and
`--assets-immutable` require `--assets`. Prefixes end in `/` and match
whole segments. These version-setting flags conflict with a project file
and with `--rollback`. `--assets-verify` reads uploaded objects back to
check their digests, including on an unchanged deploy.

## Routes and browser caching

Server paths (`/_wasmkeep/`), admission limits and actor paths take
precedence. Next come `front-first` prefixes, assets, then the front.
Only GET and HEAD without Upgrade serve assets; other requests reach the
front, or answer 404 without one. Assets never serve on the cluster port.
`Forwarded` and `X-Forwarded-Host` do not change the selected app.

Names are case-sensitive. `https://site.example/guide/` serves
`guide/index.html`; `https://site.example/guide`
redirects there with 308, preserving the query and using `no-cache`.
Paths are decoded once; encoded slashes, traversal and invalid UTF-8
never name a file. Escaped reserved prefixes answer 400.

For a missing file, `not-found` selects:

- `front` (default): the front, or 404 without one.
- `spa`: root `index.html` for navigation, otherwise the front or 404.
  Navigation means `Sec-Fetch-Mode: navigate`, or, without that header,
  `Accept: text/html` and no dot in the last path segment. A missing script
  therefore does not receive HTML. Navigation responses use `no-cache`;
  selection varies on `Sec-Fetch-Mode, Accept`.
- `404`: `404.html`, if present, with status 404; otherwise a plain 404.
  These responses use `no-store`.

Ordinary files use `public, max-age=0, must-revalidate`. Files under
`immutable` prefixes use `public, max-age=31536000, immutable`: use
content-hashed filenames there. Deploy refuses changed bytes at a name
that the preceding version declared immutable. The previously served
index can supply missing immutable names after a switch, including after
restart. Missing immutable names are never cached as successful files.

Responses include a digest ETag, support If-None-Match, HEAD and single
byte ranges (206, or 416 for an unsatisfiable range). Ranges use identity
bytes. Content types and `X-Content-Type-Options: nosniff` come from the
index, never from bucket metadata. Compressed responses vary on
Accept-Encoding and have their own ETag.

## Files and types

Deploy refuses symlinks and non-regular files, reserved top-level `actor`
and `_wasmkeep` names, invalid UTF-8, control characters, backslashes and
traversal segments. DIR itself may be a symlink. Dotfiles are skipped,
except under `.well-known/`. Non-NFC Unicode names are warned about and
match as bytes. Limits: 20,000 files, 25 MiB per file, 512 MiB total,
1,024 bytes per name, and an 8 MiB index.

A sibling `X.br` or `X.gz` is a variant of X, verified to decompress to
exactly X. Without X it is an independent file. Deploy generates Brotli
and gzip for text types and Wasm when they save at least 10%.

| Extensions | Content-Type |
| --- | --- |
| html, htm | text/html;charset=utf-8 |
| css | text/css;charset=utf-8 |
| js, mjs | text/javascript;charset=utf-8 |
| json, map | application/json;charset=utf-8 |
| svg | image/svg+xml;charset=utf-8 |
| xml | application/xml;charset=utf-8 |
| txt | text/plain;charset=utf-8 |
| wasm | application/wasm |
| png, gif, webp, avif | image/png, image/gif, image/webp, image/avif respectively |
| jpg, jpeg | image/jpeg |
| ico | image/x-icon |
| woff, woff2 | font/woff, font/woff2 respectively |
| pdf, zip | application/pdf, application/zip respectively |
| other | application/octet-stream |

## Local development

`wasmkeep dev` serves the directory live. It builds assets at start,
reruns their build when a watched path changes, and rebuilds the table
when the directory changes. Files are opened from disk on each request;
the ETag changes when the table is rebuilt. Refused names are logged and
excluded without ending the session. Symlinks cannot escape the directory,
even if substituted after the table was built.

Routing, not-found modes, types, ETags, ranges and cache headers match
production. Dev creates and serves no compressed variants (siblings are
hidden, unchecked), does not enforce immutable names across versions,
has no previous index, and writes nothing to a bucket.

## Repair, rollback and collection

Assets use manifest format 7 and require every live node to announce
`assets`; [upgrade the nodes first](deploy/upgrading-to-0.6.md#static-assets).
A rollback restores assets together with the front and classes. Each app
has its own disk cache under `<data-dir>/assets/<tenant>/<app>/`; all apps
share the process's cache limit and eviction order. Asset
[metrics](monitoring.md) carry `tenant` and `app` labels.

A missing or corrupt content returns 503 for that file, with Retry-After
and `no-store`. A missing index prevents loading the version. Deploy the
same files again to repair missing objects, even when the version is
unchanged. Use `--assets-verify` to repair same-size corrupted objects.
A deploy keeping assets without local bytes cannot repair them: supply
`--assets` or the project's directory.

Deploy and rollback collect old versions, then unreferenced assets.
Every surviving manifest protects its index and contents, including
versions left by failed deploys or stepped back from by rollback. Deploy
and collection markers coordinate concurrent runs; a collection skips
while a deploy is active, or a deploy waits for an earlier collection.
To collect between frequent deployments:

```bash
wasmkeep assets collect --app acme/site --bucket s3://my-bucket \
  --version-retention-secs 3600
```

Retention defaults to 3600 seconds; zero permits immediate collection of
unreferenced objects. The current version and five predecessors remain.
Asset collection compares bucket timestamps, stops after five minutes,
and refuses unreadable manifests or indexes. See [ADR 0030, section
9](adr/0030-files-and-static-assets.md#9-static-assets) for the protocol
and its bounded timeout assumptions.

The compressed variants depend on the CLI version, including its pinned compression
crate versions and backend (gzip uses pure Rust `miniz_oxide`). A newer CLI can
turn an unchanged deploy into a changed deploy even when the source files are
identical: different compressed bytes produce a different index digest. To repair
assets with `unchanged, N repaired`, use the same CLI version that deployed them.
