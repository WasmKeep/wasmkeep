import { actors, broadcast, defineFront } from '@wasmkeep/sdk/front';
import { api } from './api';
import { page, browser } from './assets';
const room = new broadcast.Client('room/lobby');
const handle = api({
  join: (name, after) => broadcast.join('room/lobby', 'directory/bg:lobby', name, after),
  publish: messages => room.publish(messages),
  // 256 messages a page: a front on QuickJS converts each page in the guest
  // (no native UTF-8), so smaller pages keep it well inside its CPU budget;
  // the client asks again while `more`.
  history: after => broadcast.historyJSON(room.history(after, 256)),
  stats: () => ({ head: broadcast.historyJSON(room.history()).head,
    ...JSON.parse(actors.call('room/lobby', {path:'/stats'}).text()) }),
});
export default defineFront({ fetch(req) {
  if (req.method === 'GET' && req.path === '/') return {headers:{'content-type':'text/html; charset=utf-8'},body:page};
  if (req.method === 'GET' && req.path === '/client.js') return {headers:{'content-type':'text/javascript; charset=utf-8'},body:browser};
  return handle(req);
} });
