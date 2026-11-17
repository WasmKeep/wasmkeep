import * as host from "wasmkeep:actor/websockets@0.2.0";
export function trySend(conn: bigint, data: string | Uint8Array): "ok" | "gone" | "backpressure" {
  try { host.send(conn, typeof data === "string" ? { tag: "text", val: data } : { tag: "binary", val: data }); return "ok"; }
  catch (error) {
    const e = error as { payload?: { tag?: string }; tag?: string };
    const tag = e?.payload?.tag ?? e?.tag;
    if (tag === "backpressure") return "backpressure";
    if (tag === "unknown-connection") return "gone";
    throw error;
  }
}
