// Pure routing/validation; the front supplies the SDK calls.
type Position = { e: bigint; n: bigint };
export function api(room: {
  join(name: string, after?: Position): any;
  publish(messages: { body: string; id: string }[]): unknown;
  history(after?: Position): unknown;
  stats(): unknown;
}) {
  const json = (value: unknown) => ({ status: 200, headers: { 'content-type': 'application/json' }, body: JSON.stringify(value, (_k, v) => typeof v === 'bigint' ? Number(v) : v) });
  const position = (e: string | null, n: string | null): Position | undefined => {
    if (e === null && n === null) return;
    if (![e,n].every(s => s !== null && /^\d+$/.test(s) && Number.isSafeInteger(Number(s)))) throw Error('invalid position');
    return {e: BigInt(e!), n: BigInt(n!)};
  };
  return (req: {method: string; path: string; text(limit: number): string | undefined}) => {
    // URL is not a guest global on every engine.
    const [path, query = ''] = req.path.split('?');
    const args = new Map(query.split('&').map(pair => {
      const [k, v = ''] = pair.split('=');
      try { return [k, decodeURIComponent(v)] as const; } catch { return [k, ''] as const; }
    }));
    if (!['/join','/publish','/history','/stats'].includes(path)) return {status:404};
    if (req.method !== (path === '/publish' ? 'POST' : 'GET')) return {status:405};
    const name = args.get('name') ?? '';
    if (!/^[a-zA-Z0-9_-]{1,24}$/.test(name)) return {status:400,body:'Choose a name: 1–24 letters, digits, _ or -.'};
    try {
      if (path === '/join') return room.join(name, position(args.get('e') ?? null,args.get('n') ?? null));
      if (path === '/history') {
        const after = args.get('after');
        const parts = after?.split('.');
        if (parts && parts.length !== 2) return {status:400};
        return json(room.history(position(parts?.[0] ?? null,parts?.[1] ?? null)));
      }
      if (path === '/stats') return json(room.stats());
      const body = req.text(8192);
      if (body === undefined) return {status:413};
      let m;
      try { m = JSON.parse(body); } catch { return {status:400}; }
      if (typeof m.text !== 'string' || !m.text.trim() || m.text.length > 2000 || typeof m.id !== 'string' || !/^[a-zA-Z0-9-]{1,36}$/.test(m.id)) return {status:400};
      return json(room.publish([{body: `${name}: ${m.text}`, id: `${name}:${m.id}`} ]));
    } catch (e) {
      if (e instanceof Error && e.message === 'invalid position') return {status:400};
      console.error(String(e));
      return {status:503,headers:{'retry-after':'1'},body:'Room unavailable; retry shortly.'};
    }
  };
}
