export let result = "ok";
export function outcome(value: string) { result = value; }
/** The host's open sockets, and the text frames sent to them, for tests. */
export const open: bigint[] = [];
export const sent: [bigint, string][] = [];
export function send(id: bigint, message: { tag: string; val: unknown }) {
    if (result !== "ok") throw { payload: { tag: result } };
    if (message.tag === "text") sent.push([id, message.val as string]);
}
export function close() {}
export function connections() { return BigUint64Array.from(open); }
