// What a contract file imports (`@wasmkeep/sdk/contracts`): the error
// envelope, codecs, operation contracts and the typed client. Nothing here
// imports a `wasmkeep:` or `wasi:` module, so a front, an actor, a test or
// a plain Node program can load it. The binding to `actors.call` is
// `actorClient()`, exported by `@wasmkeep/sdk` and `@wasmkeep/sdk/front`.

import "./text";

export * as codec from "./codec";
export { CodecError, type Codec, type Infer, type OptionalCodec } from "./codec";
export { HeaderList, type HeadersInit } from "./headers";
export { CALL_ERROR_KINDS, CallError, isCallErrorKind, type CallErrorKind } from "./errors";
export {
  MAX_PROBLEM_BYTES,
  PROBLEM_CONTENT_TYPE,
  PROBLEM_VERSION,
  classifyFailure,
  isProblemCode,
  problem,
  problemResponse,
  type FailureAnswer,
  type JsonValue,
  type Problem,
} from "./problem";
export {
  defineContract,
  defineOperation,
  urlPath,
  withQuery,
  type Contract,
  type ErrorMap,
  type InputArgs,
  type Operation,
  type OperationSpec,
  type WireRequest,
} from "./contracts";
export {
  Client,
  MAX_BATCH,
  type BatchEntry,
  type CheckedEntries,
  type BatchResults,
  type Bound,
  type Failure,
  type Result,
  type Transport,
} from "./client";
