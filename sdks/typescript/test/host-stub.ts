// Stands in for `wasmkeep:actor/actors` under node (scripts/test.mjs maps
// the import here): each test installs the actor that answers calls.

export interface Request {
  method: string;
  path: string;
  headers: Array<[string, string]>;
  body: Uint8Array;
}

export interface Response {
  status: number;
  headers: Array<[string, string]>;
  body: Uint8Array;
}

export type Host = (address: string, request: Request) => Response;

const state: { host: Host } = {
  host: () => {
    throw new Error("no host installed");
  },
};

/** Answers every `actors.call` with `host` until the next `install`. */
export function install(host: Host): void {
  state.host = host;
}

export function call(address: string, request: Request): Response {
  return state.host(address, request);
}
