/// <reference path="./wasi-io-streams.d.ts" />
declare module 'wasmkeep:actor/blobs@0.2.0' {
  export function head(key: string): BlobInfo | undefined;
  /**
   * The whole file, up to 16 MiB (else `too-large`; use `open-read`).
   */
  export function get(key: string): [BlobInfo, Uint8Array] | undefined;
  /**
   * Writes the whole file, up to 16 MiB. Returns once it is durable.
   */
  export function put(key: string, body: Uint8Array, options: WriteOptions): BlobInfo;
  /**
   * Streams a file, or the bytes `[start, start + length)` of it.
   */
  export function openRead(key: string, range: [bigint, bigint] | undefined): [BlobInfo, InputStream] | undefined;
  /**
   * Starts a streamed write; nothing is visible until `finish`.
   */
  export function openWrite(key: string, options: WriteOptions): Upload;
  /**
   * Removes the file if it exists. Durable when it returns.
   */
  export { _delete as delete };
  function _delete(key: string): void;
  /**
   * Files whose key starts with `prefix` as a string (`up` finds
   * `uploads/a`; `uploads/` finds what is under it), in key order,
   * after the key `after`, at most `limit` (1 to 1,000). `prefix` is
   * empty or over the key alphabet and may end with `/`; `after` is a
   * key.
   */
  export function list(prefix: string, after: string | undefined, limit: number): Listing;
  /**
   * A link a browser downloads the file from, without this code.
   */
  export function linkDownload(key: string, options: DownloadOptions): string;
  /**
   * A link a browser uploads one new file to, without this code.
   */
  export function linkUpload(key: string, options: UploadOptions): UploadLink;
  export type InputStream = import('wasi:io/streams@0.2.12').InputStream;
  export type OutputStream = import('wasi:io/streams@0.2.12').OutputStream;
  export interface BlobInfo {
    key: string,
    size: bigint,
    /**
     * As given when written; `application/octet-stream` if none was.
     */
    contentType: string,
    /**
     * The bucket's ETag, opaque: compare, never parse.
     */
    etag: string,
    /**
     * Milliseconds since the epoch, by the bucket's clock.
     */
    modifiedMs: bigint,
  }
  export type Condition = ConditionAny | ConditionAbsent | ConditionEtag;
  /**
   * Write whatever is there.
   */
  export interface ConditionAny {
    tag: 'any',
  }
  /**
   * Write only if no file has this key.
   */
  export interface ConditionAbsent {
    tag: 'absent',
  }
  /**
   * Write only if the file's ETag is this one.
   */
  export interface ConditionEtag {
    tag: 'etag',
    val: string,
  }
  export interface WriteOptions {
    /**
     * `type/subtype` with optional parameters, at most 256 bytes.
     */
    contentType?: string,
    condition: Condition,
  }
  export type Disposition = DispositionAttachment | DispositionInline;
  /**
   * Downloaded under this name, or the key's last segment.
   */
  export interface DispositionAttachment {
    tag: 'attachment',
    val: string | undefined,
  }
  /**
   * Shown by the browser: only for images, audio, video and plain text.
   */
  export interface DispositionInline {
    tag: 'inline',
  }
  export interface DownloadOptions {
    /**
     * Default 300, at most 3,600.
     */
    expiresInSecs?: number,
    disposition: Disposition,
  }
  export interface UploadOptions {
    /**
     * The exact size the browser will send.
     */
    size: bigint,
    contentType: string,
    /**
     * Default 900, at most 3,600.
     */
    expiresInSecs?: number,
  }
  /**
   * What the browser must send: `method` to `url`, with exactly these
   * headers, and a body of exactly the size given.
   */
  export interface UploadLink {
    url: string,
    method: string,
    headers: Array<[string, string]>,
    expiresAtMs: bigint,
  }
  export interface Listing {
    entries: Array<BlobInfo>,
    /**
     * Pass as `after` for the next page; none at the end.
     */
    next?: string,
  }
  export type BlobError = BlobErrorInvalidKey | BlobErrorTooLarge | BlobErrorQuotaExceeded | BlobErrorConflict | BlobErrorNotAllowed | BlobErrorUnavailable;
  export interface BlobErrorInvalidKey {
    tag: 'invalid-key',
    val: string,
  }
  /**
   * Past a size limit.
   */
  export interface BlobErrorTooLarge {
    tag: 'too-large',
  }
  /**
   * The app is over its stored-bytes limit.
   */
  export interface BlobErrorQuotaExceeded {
    tag: 'quota-exceeded',
  }
  /**
   * The write's condition did not hold.
   */
  export interface BlobErrorConflict {
    tag: 'conflict',
  }
  /**
   * Refused as asked (an inline link for an unsafe type, a write
   * without a valid hold, a closed upload).
   */
  export interface BlobErrorNotAllowed {
    tag: 'not-allowed',
    val: string,
  }
  /**
   * The bucket or disk failed or did not answer in time: not known
   * to have happened. Safe to retry.
   */
  export interface BlobErrorUnavailable {
    tag: 'unavailable',
    val: string,
  }
  
  export class Upload implements Disposable {
    /**
     * This type does not have a public constructor.
     */
    private constructor();
    /**
    * The stream to write the body to; only once.
    */
    body(): OutputStream;
    /**
    * Ends the body and makes the file durable, with its condition.
    * Dropping an upload without `finish` abandons it.
    */
    static finish(this_: Upload): BlobInfo;
    [Symbol.dispose](): void;
  }
}
