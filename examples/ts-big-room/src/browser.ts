import { RoomClient } from '@wasmkeep/sdk/room-client';
const el = (id: string) => document.getElementById(id)!;
const input = (id: string) => el(id) as HTMLInputElement;
let client: RoomClient | undefined, socket: WebSocket | undefined, name = '';
const counts = { received:0, repeatsDropped:0, reconnects:0, gapsFetched:0, void:0, resets:0 };
const render = () => { el('counters').textContent = Object.entries(counts).map(([k,v]) => `${k}: ${v}`).join(' · '); };
const url = (path: string) => new URL(`${path}?name=${encodeURIComponent(name)}`,location.href).href;
let attempts = 0;
(el('signin') as HTMLFormElement).onsubmit = event => {
  event.preventDefault(); client?.close(); name = input('name').value;
  el('messages').replaceChildren(); Object.keys(counts).forEach(k => counts[k] = 0); attempts = 0;
  const join = new URL(url('/join')); join.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  client = new RoomClient({ join:join.href, history:url('/history'),
    onState: state => { el('state').textContent = state; if (state === 'connecting' && attempts++ > 0) counts.reconnects++; render(); },
    onMessage: (p, body) => { counts.received++; const li = document.createElement('li'); li.dataset.n = String(p.n); li.textContent = `${p.e}.${p.n} ${body}`; el('messages').append(li); while(el('messages').children.length > 2000) el('messages').firstChild!.remove(); render(); },
    onRepeat: () => { counts.repeatsDropped++; render(); },
    onLost: (first,last) => { counts.void += last-first+1; for(const li of Array.from(el('messages').children)) if(Number((li as HTMLElement).dataset.n) >= first) li.remove(); render(); },
    onReset: () => { counts.resets++; el('messages').replaceChildren(); render(); },
    onError: error => { el('error').textContent = String(error); },
    fetch: async (url, options) => { counts.gapsFetched++; render(); return fetch(url,options); },
    socket: address => { socket = new WebSocket(address); return socket; },
  });
  client.connect(); input('text').disabled = false; input('submit').disabled = false; input('drop').disabled = false;
};
el('drop').onclick = () => socket?.close();
let pending: {text:string;id:string} | undefined;
(el('send') as HTMLFormElement).onsubmit = async event => {
  event.preventDefault();
  const text = input('text').value;
  if (!pending || pending.text !== text) pending = {text,id:crypto.randomUUID()};
  try {
    const response = await fetch(url('/publish'),{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(pending)});
    if (!response.ok) throw Error(`Send answered ${response.status}; retry to keep the same message id.`);
    pending = undefined; input('text').value = ''; el('error').textContent = '';
    const stats = await fetch(url('/stats')); if(stats.ok) el('stats').textContent = JSON.stringify(await stats.json());
  } catch(error) { el('error').textContent = String(error); }
};
render();
