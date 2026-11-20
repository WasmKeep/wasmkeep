// Round tests run twice (ADR 0040, section 13): over a local Core, and over
// the room's pull through its handler's wire format and the pusher's Pulled.
import { test as nodeTest } from "node:test";
import { serve } from "../src/broadcast";
import { Pulled } from "../src/broadcast-pusher";
import { local, type Source } from "../src/broadcast-round";
import { errorMessage, type Core } from "../src/broadcast-core";
import { install as hostInstall, type Host } from "./host-stub";
export type Mode = "local" | "pulled";
export const modes: Mode[] = ["local", "pulled"];
const PUSHER = "tenant/pusher/r";
export function sources(mode: Mode) {
    let current: Core | undefined;
    const pulled = new Pulled("room/r");
    return {
        mode,
        test: (name: string, f: () => void | Promise<void>) => nodeTest(`${name} (${mode} source)`, f),
        /** The source a round reads `core` through. */
        of(core: Core): Source {
            current = core;
            return mode === "local" ? local(core) : pulled;
        },
        /** `host` answers every call but the room's pulls, which `of`'s core answers. */
        install(host: Host) {
            hostInstall((address, r) => {
                if (address !== "room/r" || !r.path.startsWith("/wasmkeep-room/v1/"))
                    return host(address, r);
                try {
                    return { status: 200, headers: [], body: serve(current!, r.path.slice("/wasmkeep-room/v1/".length), r.body, PUSHER, PUSHER) };
                }
                catch (e) {
                    return { status: errorMessage(e).includes("invalid") ? 400 : 503, headers: [], body: new TextEncoder().encode(errorMessage(e)) };
                }
            });
        },
    };
}
