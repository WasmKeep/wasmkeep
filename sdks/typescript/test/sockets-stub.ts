export let result = "ok";
export function outcome(value: string) { result = value; }
export function send() { if (result !== "ok") throw { payload: { tag: result } }; }
export function close() {}
export function connections() { return new BigUint64Array(); }
