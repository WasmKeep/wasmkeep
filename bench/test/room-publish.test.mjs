import {test} from 'node:test';
import assert from 'node:assert/strict';
import {publishWithRetry} from '../room-publish.mjs';
test('publish retries every HTTP failure with the identical body for 30 seconds', async()=>{
 let now=0;const bodies=[], failures=[];let retries=0;
 const result=await publishWithRetry({body:'{"id":"same"}', send:async(body)=>{bodies.push(body);return new Response('',{status:503});}, now:()=>now,sleep:async(ms)=>{now+=ms;}, failure:s=>failures.push(s),retry:()=>retries++});
 assert.equal(result,null);assert.equal(now,30000);assert(bodies.length>3);assert(bodies.every(b=>b===bodies[0]));assert.equal(failures.length,bodies.length);assert.equal(retries,bodies.length-1);
});
test('unknown outcomes and non-503 failures retry, then return acknowledgment',async()=>{
 let now=0,n=0;const failures=[];
 const result=await publishWithRetry({body:'same',send:async()=>{n++;if(n===1)throw Error('timeout');return n===2?new Response('',{status:500}):Response.json([{e:1,n:1}]);},now:()=>now,sleep:async(ms)=>{now+=ms;},failure:s=>failures.push(s)});
 assert.deepEqual(result,[{e:1,n:1}]);assert.deepEqual(failures,['network',500]);
});
