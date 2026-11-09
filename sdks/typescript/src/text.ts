// UTF-8 `TextEncoder` and `TextDecoder` for engines that lack them.
//
// StarlingMonkey (`wasmkeep-ts build --engine starlingmonkey`) provides
// both; QuickJS (the default engine) provides neither. When they are
// missing, this module installs UTF-8-only versions on `globalThis`, so
// the SDK and actor code use the same names on either engine. Other
// encodings and `decode(..., { stream: true })` are not supported.

class Utf8Encoder {
  readonly encoding = "utf-8";

  encode(input = ""): Uint8Array {
    // At most 3 bytes per UTF-16 code unit (a surrogate pair is 2 units, 4 bytes).
    const out = new Uint8Array(input.length * 3);
    let n = 0;
    for (let i = 0; i < input.length; i++) {
      let c = input.charCodeAt(i);
      if (c >= 0xd800 && c <= 0xdfff) {
        const d = c <= 0xdbff && i + 1 < input.length ? input.charCodeAt(i + 1) : 0;
        if (d >= 0xdc00 && d <= 0xdfff) {
          c = 0x10000 + ((c - 0xd800) << 10) + (d - 0xdc00);
          i++;
        } else {
          c = 0xfffd; // a lone surrogate
        }
      }
      if (c < 0x80) {
        out[n++] = c;
      } else if (c < 0x800) {
        out[n++] = 0xc0 | (c >> 6);
        out[n++] = 0x80 | (c & 0x3f);
      } else if (c < 0x10000) {
        out[n++] = 0xe0 | (c >> 12);
        out[n++] = 0x80 | ((c >> 6) & 0x3f);
        out[n++] = 0x80 | (c & 0x3f);
      } else {
        out[n++] = 0xf0 | (c >> 18);
        out[n++] = 0x80 | ((c >> 12) & 0x3f);
        out[n++] = 0x80 | ((c >> 6) & 0x3f);
        out[n++] = 0x80 | (c & 0x3f);
      }
    }
    return out.slice(0, n);
  }
}

class Utf8Decoder {
  readonly encoding = "utf-8";
  readonly fatal: boolean;
  readonly ignoreBOM: boolean;

  constructor(label = "utf-8", options: { fatal?: boolean; ignoreBOM?: boolean } = {}) {
    const l = String(label).trim().toLowerCase();
    if (l !== "utf-8" && l !== "utf8" && l !== "unicode-1-1-utf-8") {
      throw new RangeError(`unsupported encoding ${label}: only utf-8 is available`);
    }
    this.fatal = options.fatal === true;
    this.ignoreBOM = options.ignoreBOM === true;
  }

  // The WHATWG UTF-8 decoder: each maximal invalid sequence becomes one
  // U+FFFD, or a TypeError when `fatal`.
  decode(input?: ArrayBuffer | ArrayBufferView): string {
    const b =
      input === undefined
        ? new Uint8Array()
        : input instanceof ArrayBuffer
          ? new Uint8Array(input)
          : new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
    let i = !this.ignoreBOM && b[0] === 0xef && b[1] === 0xbb && b[2] === 0xbf ? 3 : 0;
    let s = "";
    const codes: number[] = [];
    const flush = () => {
      s += String.fromCodePoint(...codes);
      codes.length = 0;
    };
    while (i < b.length) {
      const x = b[i];
      let cp = x;
      let need = 0;
      let lower = 0x80;
      let upper = 0xbf;
      if (x < 0x80) {
        i++;
      } else {
        if (x >= 0xc2 && x <= 0xdf) {
          need = 1;
          cp = x & 0x1f;
        } else if (x >= 0xe0 && x <= 0xef) {
          need = 2;
          cp = x & 0x0f;
          if (x === 0xe0) lower = 0xa0;
          if (x === 0xed) upper = 0x9f;
        } else if (x >= 0xf0 && x <= 0xf4) {
          need = 3;
          cp = x & 0x07;
          if (x === 0xf0) lower = 0x90;
          if (x === 0xf4) upper = 0x8f;
        }
        let j = 1;
        if (need > 0) {
          for (; j <= need; j++) {
            const y = b[i + j];
            if (y === undefined || y < lower || y > upper) break;
            lower = 0x80;
            upper = 0xbf;
            cp = (cp << 6) | (y & 0x3f);
          }
        }
        if (need === 0 || j <= need) {
          if (this.fatal) throw new TypeError("The encoded data was not valid utf-8");
          cp = 0xfffd;
          i += need === 0 ? 1 : j; // the bad byte starts the next sequence
        } else {
          i += need + 1;
        }
      }
      codes.push(cp);
      if (codes.length >= 4096) flush();
    }
    flush();
    return s;
  }
}

const g = globalThis as Record<string, unknown>;
if (typeof g.TextEncoder !== "function") g.TextEncoder = Utf8Encoder;
if (typeof g.TextDecoder !== "function") g.TextDecoder = Utf8Decoder;

export { Utf8Decoder, Utf8Encoder };
