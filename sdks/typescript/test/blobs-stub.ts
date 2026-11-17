// Stands in for `wasmkeep:actor/blobs` under node (scripts/test.mjs maps the
// import here): files in memory, with the host's shapes (u64 as bigint,
// errors thrown with a `payload` variant), and pages as short as a test
// asks, to check what follows `next`.

export interface BlobInfo {
  key: string;
  size: bigint;
  contentType: string;
  etag: string;
  modifiedMs: bigint;
}

type Condition = { tag: "any" } | { tag: "absent" } | { tag: "etag"; val: string };

const files = new Map<string, { info: BlobInfo; body: Uint8Array }>();
let counter = 0;
/** Each listing page holds at most this many entries (and an empty page
 * once, when `emptyPageOnce` is set), whatever the limit. */
export const pages = { max: Infinity, emptyPageOnce: false };
/** The last upload options seen. */
export const seen: { upload?: unknown; download?: unknown } = {};

class HostError extends Error {
  constructor(readonly payload: { tag: string; val?: string }) {
    super(payload.tag);
  }
}

function check(key: string): void {
  if (!/^[A-Za-z0-9._~-]+(\/[A-Za-z0-9._~-]+)*$/.test(key)) {
    throw new HostError({ tag: "invalid-key", val: `bad key ${key}` });
  }
}

export function reset(): void {
  files.clear();
  pages.max = Infinity;
  pages.emptyPageOnce = false;
}

export function head(key: string): BlobInfo | undefined {
  check(key);
  return files.get(key)?.info;
}

export function get(key: string): [BlobInfo, Uint8Array] | undefined {
  check(key);
  const f = files.get(key);
  return f && [f.info, f.body];
}

export function put(
  key: string,
  body: Uint8Array,
  options: { contentType?: string; condition: Condition },
): BlobInfo {
  check(key);
  const old = files.get(key);
  const c = options.condition;
  if ((c.tag === "absent" && old) || (c.tag === "etag" && old?.info.etag !== c.val)) {
    throw new HostError({ tag: "conflict" });
  }
  const info = {
    key,
    size: BigInt(body.length),
    contentType: options.contentType ?? "application/octet-stream",
    etag: `"${++counter}"`,
    modifiedMs: 1n,
  };
  files.set(key, { info, body });
  return info;
}

function remove(key: string): void {
  check(key);
  files.delete(key);
}
export { remove as delete };

export function list(prefix: string, after: string | undefined, limit: number) {
  if (limit < 1 || limit > 1000) throw new HostError({ tag: "not-allowed", val: "limit" });
  const keys = [...files.keys()].filter((k) => k.startsWith(prefix) && (after === undefined || k > after)).sort();
  if (pages.emptyPageOnce && keys.length > 0) {
    pages.emptyPageOnce = false;
    return { entries: [], next: after ?? "" };
  }
  const n = Math.min(limit, pages.max);
  const page = keys.slice(0, n);
  return {
    entries: page.map((k) => files.get(k)!.info),
    next: keys.length > n ? page[page.length - 1] : undefined,
  };
}

export function linkDownload(_key: string, options: unknown): string {
  seen.download = options;
  throw new HostError({ tag: "not-allowed", val: "links are not available on this node yet" });
}

export function linkUpload(_key: string, options: unknown): never {
  seen.upload = options;
  throw new HostError({ tag: "not-allowed", val: "links are not available on this node yet" });
}
