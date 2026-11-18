// QuickJS's TextEncoder/TextDecoder (src/text.ts) against node's native
// ones, short inputs (the general path) and long ones (the fast paths).
import { test } from "node:test";
import assert from "node:assert/strict";
import { Utf8Decoder, Utf8Encoder } from "../src/text";
const long = (s: string) => (s ? s.repeat(Math.ceil(200 / s.length)) : s);
const texts = ["", "x", "é漢😀", "﻿bom", "a\ud800b", "\udc00", "\ud83d"].flatMap(s => [s, long(s), long("x") + s, s + long("é")]);
test("the polyfill encodes as TextEncoder does", () => {
    for (const s of texts)
        assert.deepEqual(new Utf8Encoder().encode(s), new TextEncoder().encode(s), JSON.stringify(s));
});
test("the polyfill decodes as TextDecoder does, fatal or not, BOM or not", () => {
    const bad = [[0xff], [0xc3, 0x28], [0xe2, 0x82], [0xed, 0xa0, 0x80], [0xc0, 0xaf], [0xf4, 0x90, 0x80, 0x80], [0xf0, 0x9f, 0x98]];
    const inputs = texts.map(s => new TextEncoder().encode(s)).concat(bad.flatMap(b => {
        const pad = new TextEncoder().encode(long("é"));
        return [Uint8Array.from(b), Uint8Array.from([...pad, ...b]), Uint8Array.from([...b, ...pad]), Uint8Array.from([0xef, 0xbb, 0xbf, ...pad, ...b, ...pad])];
    }));
    for (const b of inputs)
        for (const fatal of [false, true])
            for (const ignoreBOM of [false, true]) {
                let want: string | Error, got: string | Error;
                try { want = new TextDecoder("utf-8", { fatal, ignoreBOM }).decode(b); } catch (e) { want = e as Error; }
                try { got = new Utf8Decoder("utf-8", { fatal, ignoreBOM }).decode(b); } catch (e) { got = e as Error; }
                if (want instanceof Error)
                    assert(got instanceof TypeError, `${[...b]} fatal=${fatal}`);
                else
                    assert.equal(got, want, `${[...b]} fatal=${fatal} ignoreBOM=${ignoreBOM}`);
            }
});
