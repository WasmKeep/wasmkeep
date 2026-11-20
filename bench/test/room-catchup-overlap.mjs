// Deterministic transport: a stalled reader's catch-up. The first publish is
// announced with `behind`; while its history page is in flight the live frame
// for the same number arrives and is held, so the page and the held frame
// overlap. The SDK applies number 1 once and discards the held copy.
const sockets = [];
globalThis.WebSocket = class {
  static OPEN = 1;
  readyState = 1;
  constructor() { sockets.push(this); setTimeout(() => {this.onopen?.(); this.onmessage?.({data:JSON.stringify({t:'ready',e:1,n:0})});}, 0); }
  addEventListener() {}
  send() {}
  close() {}
};
const live = (n, d) => JSON.stringify({t:'m',e:1,b:0,n,m:[{a:0,d}]});
const texts = [];
let n = 0;
globalThis.fetch = async (url, init) => {
  if (init?.method === 'POST') {
    n++;
    texts.push(JSON.parse(init.body).text);
    if (n === 1) for (const s of sockets) s.onmessage?.({data:JSON.stringify({t:'behind',e:1,n:1})});
    else for (const s of sockets) s.onmessage?.({data:live(n, texts[n - 1])});
    return Response.json([{e:1,n}]);
  }
  if (new URL(url).pathname.endsWith('history')) {
    for (const s of sockets) s.onmessage?.({data:live(1, texts[0])});
    await new Promise(r => setTimeout(r, 20));
    return Response.json({head:{e:1,n:1},more:false,messages:[{position:{e:1,n:1},at:0,body:texts[0]}]});
  }
  return Response.json({edges:1,relays:0});
};
