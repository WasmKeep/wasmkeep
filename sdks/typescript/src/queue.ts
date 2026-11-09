/**
 * Queues with at-least-once delivery, built from one actor (ADR 0015): the
 * producer and consumer side.
 *
 * A queue is an actor whose SQLite table holds its messages: the Rust SDK's
 * queue, deployed from `examples/jobs/queue` (its settings, such as the push
 * consumer, are code in that class). This module speaks its protocol
 * (`/wasmkeep-queue/v1`) over `actors.call`:
 *
 * - producers `send` (returns once the message is durable);
 * - pull consumers `lease` a batch, then `ack` or `retry` each message;
 * - push consumers answer deliveries with `handleDelivery`.
 *
 * A message may be delivered more than once (a lost ack, a lease that ran
 * out, a failover), with the same `id`: record the ids you handled when a
 * duplicate would matter.
 *
 * ```ts
 * import { defineActor, queue } from "@wasmkeep/sdk";
 *
 * const jobs = new queue.Client("queue/jobs");
 *
 * export default defineActor({
 *   fetch(req) {
 *     if (queue.isDelivery(req)) return queue.handleDelivery(req, (_queue, m) => "ack");
 *     jobs.send("resize 42"); // calls run in an event, never at module level
 *     return "ok\n";
 *   },
 * });
 * ```
 *
 * @module
 */

import { call } from "./call";
import {
  type DeadLetter,
  type Message,
  type Outcome,
  type Settlement,
  type Stats,
  decodeCounts,
  decodeDead,
  decodeMessages,
  encodeBodies,
  encodeSettlements,
  exactAll,
  header,
  lossy,
  toBytes,
  toU32,
  toU64,
} from "./wire";
import type { ActorRequest, ActorResponse } from "./index";

export type { DeadLetter, Message, Outcome, Settlement, Stats } from "./wire";

const PREFIX = "/wasmkeep-queue/v1";
/** The path a push delivery arrives on, at the consumer. */
export const DELIVER_PATH = "/wasmkeep-queue/v1/deliver";
const DELAY = "x-wasmkeep-queue-delay-ms";
/** Messages one send may carry. */
export const MAX_BATCH = 1000;

/** Why a queue operation failed, as `kind`:
 *
 * - `storage`: the queue's database refused the operation;
 * - `call`: the call to the queue failed. For a send, the message may or
 *   may not be queued;
 * - `refused`: the queue answered with an unexpected `status`. */
export class QueueError extends Error {
  readonly kind: "storage" | "call" | "refused";
  /** The queue's status, for `refused`. */
  readonly status?: number;
  /** The detail: the call's error, or the queue's answer. */
  readonly detail: string;

  constructor(kind: QueueError["kind"], detail: string, status?: number) {
    super(kind === "storage" ? `storage: ${detail}` : kind === "call" ? detail : `queue answered ${status}: ${detail}`);
    this.name = "QueueError";
    this.kind = kind;
    this.detail = detail;
    if (status !== undefined) this.status = status;
  }
}

function badReply(): QueueError {
  return new QueueError("refused", "malformed reply from queue", 200);
}

/** Reaches one queue actor. */
export class Client {
  /** The queue at `address` (`class/name`). */
  constructor(readonly address: string) {}

  private call(method: string, path: string, headers: [string, string][], body: Uint8Array): Uint8Array {
    const r = call(this.address, method, path, headers, body);
    if ("error" in r) throw new QueueError("call", r.error);
    if (r.reply.status !== 200) throw new QueueError("refused", lossy(r.reply.body), r.reply.status);
    return r.reply.body;
  }

  /** Queues `body`; returns its id once it is durable. */
  send(body: string | Uint8Array): number {
    return this.sendDelayed(body, 0);
  }

  /** Queues `body`, visible after `delayMs`. */
  sendDelayed(body: string | Uint8Array, delayMs: number): number {
    const [id] = this.sendBatchDelayed([body], delayMs);
    if (id === undefined) throw badReply();
    return id;
  }

  /** Queues several messages (at most `MAX_BATCH`) in one durable write;
   * returns their ids in order. All or none are queued. */
  sendBatch(bodies: (string | Uint8Array)[]): number[] {
    return this.sendBatchDelayed(bodies, 0);
  }

  private sendBatchDelayed(bodies: (string | Uint8Array)[], delayMs: number): number[] {
    const headers: [string, string][] = [[DELAY, toU64(delayMs).toString()]];
    const reply = this.call("POST", `${PREFIX}/send`, headers, encodeBodies(bodies.map(toBytes)));
    const counts = decodeCounts(reply);
    const ids = counts && exactAll(counts);
    if (ids === undefined || ids.length !== bodies.length) throw badReply();
    return ids;
  }

  /** Leases up to `max` visible messages for `visibilityMs`. */
  lease(max: number, visibilityMs: number): Message[] {
    const path = `${PREFIX}/lease?max=${toU32(max, "max")}&visibility_ms=${toU64(visibilityMs)}`;
    const messages = decodeMessages(this.call("POST", path, [], new Uint8Array()));
    if (messages === undefined) throw badReply();
    return messages;
  }

  /** Deletes a leased message. `false` if this lease is no longer current
   * (it ran out and the message was leased again, or was settled). */
  ack(message: Pick<Message, "id" | "attempt">): boolean {
    return this.settle([{ id: message.id, attempt: message.attempt, outcome: "ack" }])[0] === true;
  }

  /** Makes a leased message visible again after `delayMs` (`undefined`: the
   * queue's backoff). `false` if this lease is no longer current. */
  retry(message: Pick<Message, "id" | "attempt">, delayMs?: number): boolean {
    const outcome: Outcome = delayMs === undefined ? "retry" : { retryAfterMs: delayMs };
    return this.settle([{ id: message.id, attempt: message.attempt, outcome }])[0] === true;
  }

  /** Settles several leased messages in one call; one flag per item, `true`
   * where the lease was current. */
  settle(items: Settlement[]): boolean[] {
    const flags = decodeCounts(this.call("POST", `${PREFIX}/settle`, [], encodeSettlements(items)));
    if (flags === undefined) throw badReply();
    return flags.map((f) => f === 1n);
  }

  /** How many messages are ready, delayed, in flight and dead. */
  stats(): Stats {
    const counts = decodeCounts(this.call("GET", `${PREFIX}/stats`, [], new Uint8Array()));
    if (counts === undefined || counts.length !== 4) throw badReply();
    const [ready, delayed, inFlight, dead] = counts.map(Number);
    return { ready, delayed, inFlight, dead };
  }

  /** The oldest `limit` dead-lettered messages. */
  deadLetters(limit: number): DeadLetter[] {
    const reply = this.call("GET", `${PREFIX}/dead?limit=${toU32(limit, "limit")}`, [], new Uint8Array());
    const items = decodeDead(reply);
    if (items === undefined) throw badReply();
    return items;
  }
}

/** Whether `req` is a push delivery from a queue (answer it with
 * `handleDelivery`). */
export function isDelivery(req: ActorRequest): boolean {
  return req.method === "POST" && req.path === DELIVER_PATH;
}

/** Answers a push delivery: `handle` gets the sending queue's address
 * (`tenant/class/name`) and each message, and returns its outcome. Return
 * the result from `fetch`. Deliveries from outside clients (without
 * `x-wasmkeep-caller`) get `403`. If `handle` throws, the exception
 * propagates (`fetch` then answers 500) and the queue retries the whole
 * batch with its backoff. Record handled ids in this actor's database, in
 * the same event, when a duplicate delivery would matter. */
export function handleDelivery(
  req: ActorRequest,
  handle: (queue: string, message: Message) => Outcome,
): ActorResponse {
  const queue = header(req.headers, "x-wasmkeep-caller");
  if (queue === undefined) {
    return { status: 403, headers: { "content-type": "text/plain" }, body: "deliveries come from queue actors\n" };
  }
  const messages = decodeMessages(req.bytes());
  if (messages === undefined) {
    return { status: 400, headers: { "content-type": "text/plain" }, body: "malformed delivery\n" };
  }
  const outcomes: Settlement[] = messages.map((m) => ({ id: m.id, attempt: m.attempt, outcome: handle(queue, m) }));
  return {
    status: 200,
    headers: { "content-type": "application/octet-stream" },
    body: encodeSettlements(outcomes),
  };
}
