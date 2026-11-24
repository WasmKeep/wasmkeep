/**
 * Files (ADR 0030): kept in the app's part of the bucket, or on the node's
 * disk without one, in the caller's own namespace, an actor's or the
 * front's. No call names another namespace.
 *
 * ```ts
 * import * as blobs from "@wasmkeep/sdk/blobs";
 *
 * blobs.put("photos/1.jpg", bytes, { contentType: "image/jpeg", ifAbsent: true });
 * const file = blobs.get("photos/1.jpg"); // undefined if there is none
 * for (const info of blobs.listAll("photos/")) console.log(info.key, info.size);
 * ```
 *
 * A module of its own, never re-exported by `@wasmkeep/sdk` or
 * `@wasmkeep/sdk/front`: a component that does not import it imports no
 * `wasmkeep:actor/blobs`, and links on a node of any release. The calls
 * are buffered (a whole file, up to 16 MiB); streams are not in this SDK
 * yet (the host's `open-read` streams count against a cap of 16 open at
 * once, a partly read one until its handle is dropped). A key is 1 to 512 bytes of `[A-Za-z0-9._~-]` and `/`, in segments
 * that are not empty, `.` or `..`.
 *
 * Every call throws a {@link BlobError} when the host refuses or fails it.
 *
 * @module
 */

import * as wk from "wasmkeep:actor/blobs@0.2.0";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** A file's information. */
export interface BlobInfo {
  key: string;
  size: number;
  /** As given when written; `application/octet-stream` if none was. */
  contentType: string;
  /** Opaque: compare, never parse. */
  etag: string;
  /** Milliseconds since the epoch, by the bucket's clock. */
  modifiedMs: number;
}

/** A whole file: its information and its bytes. */
export interface BlobFile {
  info: BlobInfo;
  body: Uint8Array;
  /** The body as UTF-8 text. */
  text(): string;
}

/** The kinds of {@link BlobError} (the WIT `blob-error` cases). */
export const BLOB_ERROR_KINDS = [
  "invalid-key",
  "too-large",
  "quota-exceeded",
  "conflict",
  "not-allowed",
  "unavailable",
] as const;

/** One of {@link BLOB_ERROR_KINDS}. */
export type BlobErrorKind = (typeof BLOB_ERROR_KINDS)[number];

/** A call the host refused or failed. `conflict`: a write's condition did
 * not hold. `unavailable`: the bucket or disk failed, not known to have
 * happened, safe to retry. `kind` is a `string`, so that a kind from a
 * newer host passes through; `known` narrows it. */
export class BlobError extends Error {
  constructor(
    readonly kind: string,
    readonly detail: string,
  ) {
    super(detail ? `${kind}: ${detail}` : kind);
    this.name = "BlobError";
  }

  /** `kind` when it is one of {@link BLOB_ERROR_KINDS}, else `undefined`. */
  get known(): BlobErrorKind | undefined {
    return (BLOB_ERROR_KINDS as readonly string[]).includes(this.kind)
      ? (this.kind as BlobErrorKind)
      : undefined;
  }
}

/** A host error as a {@link BlobError}; anything else as it is. */
function lifted(e: unknown): unknown {
  const p = (e as { payload?: { tag?: unknown; val?: unknown } }).payload;
  if (p && typeof p.tag === "string") {
    return new BlobError(p.tag, typeof p.val === "string" ? p.val : "");
  }
  return e;
}

function host<T>(f: () => T): T {
  try {
    return f();
  } catch (e) {
    throw lifted(e);
  }
}

/** `none` is `undefined` on one engine and `null` on the other. */
function some<T>(v: T | null | undefined): T | undefined {
  return v ?? undefined;
}

function info(i: wk.BlobInfo): BlobInfo {
  return {
    key: i.key,
    size: Number(i.size),
    contentType: i.contentType,
    etag: i.etag,
    modifiedMs: Number(i.modifiedMs),
  };
}

function bytes(body: string | Uint8Array): Uint8Array {
  return typeof body === "string" ? encoder.encode(body) : body;
}

/** What a write takes. */
export interface PutOptions {
  /** `type/subtype` with optional parameters; `application/octet-stream`
   * by default. */
  contentType?: string;
  /** Write only if no file has this key (`conflict` otherwise). */
  ifAbsent?: boolean;
  /** Write only if the file's ETag is this one (`conflict` otherwise). */
  ifMatch?: string;
}

/** The file's information, or `undefined` if there is none. */
export function head(key: string): BlobInfo | undefined {
  const i = some(host(() => wk.head(key)));
  return i === undefined ? undefined : info(i);
}

/** The whole file, up to 16 MiB (`too-large` past it), or `undefined`. */
export function get(key: string): BlobFile | undefined {
  const found = some(host(() => wk.get(key)));
  if (found === undefined) return undefined;
  const [i, body] = found;
  return { info: info(i), body, text: () => decoder.decode(body) };
}

/** Writes the whole file, up to 16 MiB; returns once it is durable. */
export function put(key: string, body: string | Uint8Array, options: PutOptions = {}): BlobInfo {
  if (options.ifAbsent && options.ifMatch !== undefined) {
    throw new TypeError("put: ifAbsent and ifMatch are exclusive");
  }
  const condition: wk.Condition =
    options.ifMatch !== undefined
      ? { tag: "etag", val: options.ifMatch }
      : options.ifAbsent
        ? { tag: "absent" }
        : { tag: "any" };
  return info(host(() => wk.put(key, bytes(body), { contentType: options.contentType, condition })));
}

/** Removes the file if it exists; durable when it returns. */
function remove(key: string): void {
  host(() => wk.delete(key));
}
export { remove as delete };

/** What a listing takes. */
export interface ListOptions {
  /** Keys starting with this, as a string (`up` finds `uploads/a`). */
  prefix?: string;
  /** Keys after this one: a page's `next`. */
  after?: string;
  /** At most this many entries, 1 to 1,000 (default 1,000). */
  limit?: number;
}

/** One page of a listing, in key order. */
export interface Listing {
  entries: BlobInfo[];
  /** Pass as `after` for the next page; `undefined` at the end. */
  next?: string;
}

/** One page of the files whose key starts with `prefix`. */
export function list(options: ListOptions = {}): Listing {
  const l = host(() => wk.list(options.prefix ?? "", options.after, options.limit ?? 1_000));
  return { entries: l.entries.map(info), next: some(l.next) };
}

/** Every file whose key starts with `prefix`, page after page. A page may
 * be short, or empty, with more after it: this follows `next`, not the
 * page's length. */
export function* listAll(prefix = "", pageSize = 1_000): Generator<BlobInfo, void, undefined> {
  let after: string | undefined;
  for (;;) {
    const page = list({ prefix, after, limit: pageSize });
    yield* page.entries;
    if (page.next === undefined) return;
    after = page.next;
  }
}

/** How a download link is served. */
export interface DownloadOptions {
  /** Seconds the link works: 300 by default, at most 3,600. */
  expiresInSecs?: number;
  /** Shown by the browser instead of downloaded: only for images, audio,
   * video and plain text (`not-allowed` otherwise). */
  inline?: boolean;
  /** The download's file name (an attachment's; by default the key's last
   * segment). */
  filename?: string;
}

/** A link a browser downloads the file from, without this code. */
export function linkDownload(key: string, options: DownloadOptions = {}): string {
  const disposition: wk.Disposition = options.inline
    ? { tag: "inline" }
    : { tag: "attachment", val: options.filename };
  return host(() => wk.linkDownload(key, { expiresInSecs: options.expiresInSecs, disposition }));
}

/** What an upload link allows. */
export interface UploadOptions {
  /** The exact size the browser will send. */
  size: number;
  contentType: string;
  /** Seconds the link works: 900 by default, at most 3,600. */
  expiresInSecs?: number;
}

/** What the browser must send: `method` to `url`, with exactly these
 * headers, and a body of exactly the size given. */
export interface UploadLink {
  url: string;
  method: string;
  headers: [string, string][];
  expiresAtMs: number;
}

/** A link a browser uploads one new file to, without this code. */
export function linkUpload(key: string, options: UploadOptions): UploadLink {
  const l = host(() =>
    wk.linkUpload(key, {
      size: BigInt(options.size),
      contentType: options.contentType,
      expiresInSecs: options.expiresInSecs,
    }),
  );
  return { url: l.url, method: l.method, headers: l.headers, expiresAtMs: Number(l.expiresAtMs) };
}
