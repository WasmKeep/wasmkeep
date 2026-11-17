import { useCallback, useEffect, useState, type FormEvent } from "react";
import { MAX_TEXT, type Todo } from "../../src/contract";
import { describe, todoList } from "./api";

/** The list is the last part of `/l/<name>`; the page is served at any such
 * path (`not-found = "spa"`), and `/` is the list `main`. */
function listName(path: string): string {
  const match = /^\/l\/([a-z0-9-]{1,32})\/?$/.exec(path);
  return match ? match[1] : "main";
}

export function App() {
  const name = listName(window.location.pathname);
  const [todos, setTodos] = useState<Todo[] | undefined>();
  const [text, setText] = useState("");
  const [error, setError] = useState<string | undefined>();
  const api = todoList(name);

  const load = useCallback(async () => {
    const r = await todoList(name).list();
    if (r.ok) {
      setTodos(r.value.todos);
      setError(undefined);
    } else setError(describe(r.failure));
  }, [name]);

  useEffect(() => {
    void load();
  }, [load]);

  async function run(done: () => Promise<{ ok: boolean; failure?: Parameters<typeof describe>[0] }>) {
    const r = await done();
    if (r.ok) setError(undefined);
    else if (r.failure) setError(describe(r.failure));
    await load();
    return r.ok;
  }

  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    if (await run(() => api.add(text))) setText("");
  }

  const left = todos?.filter((t) => !t.done).length ?? 0;
  return (
    <main>
      <header>
        <h1>
          <svg viewBox="0 0 24 24" width="28" height="28" aria-hidden="true" focusable="false">
            <path d="M4 12.5l5 5L20 6.5" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          Todos
        </h1>
        <p className="list-name">List: {name}</p>
      </header>
      <form onSubmit={onSubmit}>
        <label htmlFor="new-todo">New item</label>
        <input id="new-todo" value={text} maxLength={MAX_TEXT} onChange={(e) => setText(e.target.value)} autoFocus />
        <button type="submit">Add</button>
      </form>
      {error && <p role="alert" className="error">{error}</p>}
      {todos === undefined ? (
        !error && <p>Loading</p>
      ) : todos.length === 0 ? (
        <p>Nothing to do.</p>
      ) : (
        <ul>
          {todos.map((t) => (
            <li key={t.id} className={t.done ? "done" : undefined}>
              <label>
                <input type="checkbox" checked={t.done} onChange={() => void run(() => api.toggle(t.id))} />
                <span>{t.text}</span>
              </label>
              <button type="button" onClick={() => void run(() => api.remove(t.id))} aria-label={`Remove ${t.text}`}>
                Remove
              </button>
            </li>
          ))}
        </ul>
      )}
      {todos && todos.length > 0 && <p className="count">{left} left</p>}
    </main>
  );
}
