import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
test('a live frame held behind an overlapping history page is a discard, not a repeat', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'room-catchup-'));
  try {
    const out = join(dir,'result.json');
    const child = spawn(process.execPath,['--import','./bench/test/room-catchup-overlap.mjs','bench/room-group.mjs','--url','http://127.0.0.1:1','--clients','1','--duration','1','--rate','2','--smoke','--out',out],{stdio:'ignore'});
    const code = await new Promise(resolve=>child.on('exit',resolve));
    const result = JSON.parse(await readFile(out,'utf8'));
    assert.deepEqual(result.acknowledged_positions,[{e:1,n:1},{e:1,n:2}]);
    assert.deepEqual(result.final_cursors,[{e:1,n:2}]);
    assert.equal(result.counters.deliveries,2);
    assert.equal(result.counters.repeats,0);
    assert.equal(result.counters.discarded_catchup,1);
    assert.equal(result.counters.discarded_live,0);
    assert.equal(code,0);
  } finally {await rm(dir,{recursive:true,force:true});}
});
