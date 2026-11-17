// The typed client for a browser. `Client` in the SDK is synchronous, as an
// actor's calls are; a page has `fetch`, which is not. So the page sends the
// operation's request itself, then hands the answer to a `Client` whose
// transport returns it: the answer is checked and sorted into a value or a
// failure by the same code an actor uses.
import { CallError, Client, type ErrorMap, type InputArgs, type Operation, type Result, type WireRequest } from "@wasmkeep/sdk/contracts";
import { add, list, remove, toggle } from "../../src/contract";

/** The reply of the server, or why there is none. */
type Reply = { status: number; headers: Record<string, string>; body: Uint8Array } | CallError;

async function send(address: string, request: WireRequest): Promise<Reply> {
  try {
    const response = await fetch(`/actor/${address}${request.path}`, {
      method: request.method,
      headers: request.headers,
      body: request.body ? new TextDecoder().decode(request.body) : null,
    });
    const headers: Record<string, string> = {};
    response.headers.forEach((value, name) => (headers[name] = value));
    return { status: response.status, headers, body: new Uint8Array(await response.arrayBuffer()) };
  } catch (e) {
    return new CallError("failed", e instanceof Error ? e.message : "the request failed");
  }
}

/** Calls `operation` on the actor at `address` (`class/name`). Nothing the
 * server or the network does is thrown: the result says what happened. */
export async function call<I, O, E extends ErrorMap>(
  address: string,
  operation: Operation<I, O, E>,
  ...input: InputArgs<I>
): Promise<Result<O, E>> {
  let reply: Reply;
  try {
    reply = await send(address, operation.request(input[0] as I));
  } catch {
    // The input is not valid: the client reports it, and nothing is sent.
    reply = new CallError("failed", "not sent");
  }
  return new Client({
    call() {
      if (reply instanceof CallError) throw reply;
      return reply;
    },
    callMany() {
      throw new Error("batches are not used here");
    },
  }).call(address, operation, ...input);
}

/** The typed methods of one list. */
export function todoList(name: string) {
  const address = `todos/${name}`;
  return {
    list: () => call(address, list),
    add: (text: string) => call(address, add, { text }),
    toggle: (id: number) => call(address, toggle, { id }),
    remove: (id: number) => call(address, remove, { id }),
  };
}

/** A failure as one line for the person using the page. */
export function describe(failure: { kind: string; message?: string; status?: number }): string {
  if (failure.kind === "declared" && failure.message) return failure.message;
  if (failure.kind === "transport") return "The server cannot be reached. Try again.";
  if (failure.kind === "invalid-request") return "That cannot be sent.";
  return `The server answered something unexpected${failure.status ? ` (${failure.status})` : ""}.`;
}
