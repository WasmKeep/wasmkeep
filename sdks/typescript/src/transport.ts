// The typed client over the real actor transport (`actors.call` and
// `actors.callMany`). The only typed-client module that imports the host;
// contracts, codecs, errors and the client itself are in ./pure.ts.

import { actors } from "./actors";
import { Client, type Transport } from "./client";

/** `actors.call` and `actors.callMany` as a {@link Transport}: address
 * handling, the call chain and the body limits are the host's. Calls are
 * synchronous, as in `actors`. */
export const actorTransport: Transport = {
  call: (address, request) => actors.call(address, request),
  callMany: (calls) => actors.callMany(calls),
};

/** A typed client for the current app's actors, from an actor or a front. */
export function actorClient(): Client {
  return new Client(actorTransport);
}
