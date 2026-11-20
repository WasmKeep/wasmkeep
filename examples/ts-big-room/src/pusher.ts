import { broadcast, storage } from '@wasmkeep/sdk';
// The room's delivery (ADR 0040, section 13): it pulls from room/lobby and
// pushes to the edges, through relays above 16 of them.
const pusher = broadcast.definePusher({scaling:{directory:'directory/bg:lobby',edgeClass:'edge',relayClass:'relay',capacity:150}});
export default { ...pusher, fetch(req) {
  // This example exposes persisted topology, not unavailable runtime counters.
  if (req.path === '/stats' && req.method === 'GET') {
    try {
      const rows = storage.query('SELECT data FROM wasmkeep_room_scale WHERE id=1');
      const state = JSON.parse(String(rows[0]?.data ?? '{}'));
      return {headers:{'content-type':'application/json'},body:JSON.stringify({edges:state.children?.length ?? 0,relays:state.relays?.length ?? 0})};
    } catch { return {body:'{"edges":0,"relays":0}'}; }
  }
  return pusher.fetch!(req);
} };
