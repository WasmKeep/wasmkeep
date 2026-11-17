import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
test('acknowledged final frame missing on a ready client fails bounded drain', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'room-tail-'));
  try {
    const out = join(dir,'result.json');
    const child = spawn(process.execPath,['--import','./bench/test/room-missing-tail.mjs','bench/room-group.mjs','--url','http://127.0.0.1:1','--clients','1','--duration','1','--rate','2','--smoke','--out',out],{stdio:'ignore'});
    const code = await new Promise(resolve=>child.on('exit',resolve));
    const result = JSON.parse(await readFile(out,'utf8'));
    assert.equal(code,1);
    assert.equal(result.incomplete_clients.length,1);
    assert.deepEqual(result.acknowledged_positions,[{e:1,n:1},{e:1,n:2}]);
    assert.deepEqual(result.final_cursors,[{e:1,n:1}]);
  } finally {await rm(dir,{recursive:true,force:true});}
});
