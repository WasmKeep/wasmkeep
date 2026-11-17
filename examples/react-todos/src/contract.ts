// The contract of the `todos` class: what a call looks like, as values the
// actor and the browser both import. It imports only
// `@wasmkeep/sdk/contracts`, which loads no host module, so it works in the
// actor (src/todos.ts), in the page (web/src/api.ts) and in a test.
import { codec, defineContract, defineOperation, urlPath } from "@wasmkeep/sdk/contracts";

/** The longest item, in characters. */
export const MAX_TEXT = 200;

const todo = codec.object({ id: codec.integer(), text: codec.string(), done: codec.boolean() });
const notFound = { "todos.not_found": codec.none() };

export type Todo = ReturnType<typeof todo.parse>;

export const list = defineOperation({
  name: "todos.list",
  path: "/",
  output: codec.object({ todos: codec.array(todo) }),
});

export const add = defineOperation({
  name: "todos.add",
  method: "POST",
  path: "/",
  input: codec.object({ text: codec.string() }),
  output: todo,
  errors: { "todos.invalid_text": codec.object({ max: codec.integer() }) },
});

export const toggle = defineOperation({
  name: "todos.toggle",
  method: "POST",
  path: (input) => urlPath`/${input.id}/toggle`,
  input: codec.object({ id: codec.integer() }),
  output: todo,
  errors: notFound,
});

export const remove = defineOperation({
  name: "todos.remove",
  method: "DELETE",
  path: (input) => urlPath`/${input.id}`,
  input: codec.object({ id: codec.integer() }),
  errors: notFound,
});

export const todos = defineContract({ list, add, toggle, remove });
