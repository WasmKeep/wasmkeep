// URL splitting for `http.fetch`, and its error. Pure: no host imports,
// so the unit tests run it under node.

/** Why a request failed (no response): DNS, refused, timeout, denied by
 * the host's egress rules, ... */
export class FetchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FetchError";
  }
}

/** `url` split into scheme, authority and path (with query). */
export function splitUrl(url: string): { scheme: "HTTP" | "HTTPS"; authority: string; path: string } {
  const m = /^([a-zA-Z][a-zA-Z0-9+.-]*):\/\/([^/?#]*)([^#]*)/.exec(url);
  if (!m) throw new FetchError(`${url}: not an absolute URL`);
  const scheme = m[1].toLowerCase();
  if (scheme !== "http" && scheme !== "https") throw new FetchError(`${url}: unsupported scheme ${m[1]}`);
  if (m[2] === "") throw new FetchError(`${url}: no host`);
  const rest = m[3] === "" ? "/" : m[3].startsWith("/") ? m[3] : `/${m[3]}`;
  return { scheme: scheme === "https" ? "HTTPS" : "HTTP", authority: m[2], path: rest };
}

