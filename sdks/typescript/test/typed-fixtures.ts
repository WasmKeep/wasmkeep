// Compile-time fixtures: `tsc --noEmit` (npm run check) must accept the
// lines above each `@ts-expect-error` marker's absence and reject the ones
// marked. Not run; the runtime tests are test/*.test.ts. Each marker that
// stops erroring fails the check ("unused @ts-expect-error").

import * as codec from "../src/codec";
import { defineContract, defineOperation } from "../src/contracts";
import { Client, type Transport } from "../src/client";

const getCart = defineOperation({
  name: "cart.get",
  path: "/",
  output: codec.object({ count: codec.integer(), label: codec.optional(codec.string()), owner: codec.nullable(codec.string()) }),
  errors: { "cart.gone": codec.object({ since: codec.string() }) },
});
const add = defineOperation({
  name: "cart.add",
  method: "POST",
  path: "/",
  input: codec.object({ sku: codec.string(), qty: codec.integer() }),
  output: codec.object({ count: codec.integer() }),
});
const contract = defineContract({ getCart, add });

declare const transport: Transport;
const client = new Client(transport);

// Accepted.
const r1 = client.call("cart/a", add, { sku: "s", qty: 1 });
const r2 = client.call("cart/a", getCart);
if (r1.ok) {
  const n: number = r1.value.count;
  void n;
}
if (r2.ok) {
  const label: string | undefined = r2.value.label;
  const owner: string | null = r2.value.owner;
  void label;
  void owner;
} else if (r2.failure.kind === "declared") {
  const code: "cart.gone" = r2.failure.code;
  const since: string = r2.failure.details.since;
  void code;
  void since;
}
const batch = client.batch([
  ["cart/a", add, { sku: "s", qty: 1 }],
  ["cart/b", getCart],
] as const);
const b0 = batch[0];
const b1 = batch[1];
if (b0.ok) {
  const n: number = b0.value.count;
  void n;
}
if (b1.ok) {
  const owner: string | null = b1.value.owner;
  void owner;
}
const bound = client.bind("cart/a", contract);
bound.add({ sku: "s", qty: 1 });
bound.getCart();

// Rejected.
// @ts-expect-error wrong input shape
client.call("cart/a", add, { sku: 1, qty: 1 });
// @ts-expect-error missing input
client.call("cart/a", add);
// @ts-expect-error an operation that takes no input takes none
client.call("cart/a", getCart, { x: 1 });
// @ts-expect-error not an operation
client.call("cart/a", "cart.add", { sku: "s", qty: 1 });
// @ts-expect-error unknown method on a bound contract
bound.remove();
if (r1.ok) {
  // @ts-expect-error count is a number, not a string
  const s: string = r1.value.count;
  void s;
  // @ts-expect-error no such field
  r1.value.other;
}
if (r2.ok) {
  // @ts-expect-error an optional field may be undefined
  const label: string = r2.value.label;
  void label;
  // @ts-expect-error a nullable field may be null
  const owner: string = r2.value.owner;
  void owner;
}
if (!r2.ok && r2.failure.kind === "declared") {
  // @ts-expect-error not a declared code
  const code: "cart.other" = r2.failure.code;
  void code;
}
// @ts-expect-error batch entry with a wrong input
client.batch([["cart/a", add, { sku: 1, qty: 1 }]] as const);

// @ts-expect-error the second batch result is a getCart result, not an add result
const wrong: typeof b0 = batch[1];
void wrong;

// The path function's parameter is inferred from the input codec.
export const inferred = defineOperation({
  name: "x",
  method: "POST",
  path: (input) => `/items/${input.sku.toUpperCase()}`,
  input: codec.object({ sku: codec.string() }),
});
