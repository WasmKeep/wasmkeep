// Deterministic transport: acknowledge two publishes, deliver only the first.
const sockets = [];
globalThis.WebSocket = class {
  static OPEN = 1;
  readyState = 1;
  constructor() { sockets.push(this); setTimeout(() => {this.onopen?.(); this.onmessage?.({data:JSON.stringify({t:'ready',e:1,n:0})});}, 0); }
  addEventListener() {}
  send() {}
  close() {}
};
let n = 0;
globalThis.fetch = async (url, init) => {
  if (init?.method === 'POST') {
    n++;
    if (n === 1) for (const s of sockets) s.onmessage?.({data:JSON.stringify({t:'m',e:1,b:0,n,m:[{a:0,d:JSON.parse(init.body).text}]})});
    return Response.json([{e:1,n}]);
  }
  return Response.json({edges:1,relays:0});
};
