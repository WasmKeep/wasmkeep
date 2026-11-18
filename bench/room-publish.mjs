// Retry an unknown outcome under the same id/body, within one absolute deadline.
export async function publishWithRetry({body, send, budgetMs=30000, now=()=>performance.now(), sleep=ms=>new Promise(r=>setTimeout(r,ms)), failure=()=>{}, retry=()=>{}}) {
  const deadline=now()+budgetMs;
  let attempt=0;
  while(now()<deadline) {
    if(attempt++) retry();
    let delay=250;
    try {
      const response=await send(body, Math.max(1, Math.ceil(deadline-now())));
      if(response.ok) {
        const positions=await response.json();
        if(!Array.isArray(positions)||positions.length!==1||positions.some(p=>!Number.isSafeInteger(p.e)||p.e<1||!Number.isSafeInteger(p.n)||p.n<1)) {
          failure('invalid_ack');
        } else return positions;
      } else {
        failure(response.status);
        const after=response.headers.get('retry-after');
        const ms=after===null?0:Number.isFinite(Number(after))?Number(after)*1000:Date.parse(after)-Date.now();
        if(Number.isFinite(ms)) delay=Math.max(delay,ms);
        await response.arrayBuffer();
      }
    } catch { failure('network'); }
    await sleep(Math.min(delay,Math.max(0,deadline-now())));
  }
  return null;
}
