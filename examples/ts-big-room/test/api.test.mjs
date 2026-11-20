import { test } from 'node:test';
import assert from 'node:assert/strict';
import { build } from '../../../sdks/typescript/node_modules/esbuild/lib/main.js';
const bundle = await build({ entryPoints: ['src/api.ts'], bundle: true, write: false, format: 'esm', platform: 'node' });
const { api } = await import('data:text/javascript;base64,' + Buffer.from(bundle.outputFiles[0].text).toString('base64'));
const calls = [];
const handle = api({ join: (...a) => { calls.push(a); return {status: 101}; }, publish: m => { calls.push(m); return [{e: 1n,n: 1n}]; }, history: (p, limit) => { calls.push(p, limit); return new TextEncoder().encode('{"head":{"e":1,"n":1},"more":false,"messages":[]}'); }, stats: () => ({head: 1}) });
const req = (method, path, body = '') => ({method,path,text: limit => new TextEncoder().encode(body).length > limit ? undefined : body});
test('name and position validated before joining', () => {
  assert.equal(handle(req('GET','/join?name=alice&e=1&n=2')).status,101);
  assert.deepEqual(calls.pop(), ['alice',{e:1n,n:2n}]);
  for (const path of ['/join','/join?name=x&e=-1&n=0','/join?name=x&e=1','/join?name=x&e=1&n=9007199254740992']) assert.equal(handle(req('GET',path)).status,400);
});
test('publish bounds, identity and retry id', () => {
  assert.equal(handle(req('POST','/publish?name=alice',JSON.stringify({text:'hello',id:'retry-1'}))).status,200);
  assert.deepEqual(calls.pop(),[{body:'alice: hello',id:'alice:retry-1'}]);
  assert.equal(handle(req('POST','/publish?name=alice','{')).status,400);
  assert.equal(handle(req('POST','/publish?name=alice','x'.repeat(9000))).status,413);
  assert.equal(handle(req('GET','/publish?name=alice')).status,405);
});
test('history preserves epoch and checks malformed input', () => {
  const page = handle(req('GET','/history?name=alice&after=2.3'));
  assert.equal(page.status,200);
  assert.equal(new TextDecoder().decode(page.body),'{"head":{"e":1,"n":1},"more":false,"messages":[]}');
  assert.deepEqual(calls.splice(-2),[{e:2n,n:3n},256]);
  assert.equal(handle(req('GET','/history?name=alice&after=no')).status,400);
});
test('history limit: halved by the client, clamped to 1..256 by the front', () => {
  for (const [limit, want] of [['128',128],['1',1],['0',1],['9999',256]]) {
    assert.equal(handle(req('GET',`/history?name=alice&after=2.3&limit=${limit}`)).status,200);
    assert.equal(calls.splice(-2)[1],want);
  }
  for (const limit of ['-1','x','1.5','']) assert.equal(handle(req('GET',`/history?name=alice&limit=${limit}`)).status,400);
});
