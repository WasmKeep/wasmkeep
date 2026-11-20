import { actors, broadcast, defineFront } from '@wasmkeep/sdk/front';
import { api } from './api';
import { page, browser } from './assets';
const room = new broadcast.Client('room/lobby');
const handle = api({
  join: (name, after) => broadcast.join('room/lobby', 'directory/bg:lobby', name, after),
  publish: messages => room.publish(messages),
  // The room renders the page's JSON (at most 256 messages and about
  // 64 KiB of bodies); the front forwards it unread, so its cost does not
  // grow with the page. The client asks again while `more`.
  history: (after, limit) => room.historyJSON(after, limit),
  stats: () => ({ head: broadcast.historyJSON(room.history()).head,
    ...JSON.parse(actors.call('pusher/lobby', {path:'/stats'}).text()) }),
});
export default defineFront({ fetch(req) {
  if (req.method === 'GET' && req.path === '/') return {headers:{'content-type':'text/html; charset=utf-8'},body:page};
  if (req.method === 'GET' && req.path === '/client.js') return {headers:{'content-type':'text/javascript; charset=utf-8'},body:browser};
  return handle(req);
} });
