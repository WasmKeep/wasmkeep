// The page's script: the token stays in this tab (sessionStorage) and goes
// in the Authorization header, never in a cookie or a URL.
const $ = (id) => document.getElementById(id);
let token = sessionStorage.getItem("token") || "";

async function api(method, path, body) {
  const r = await fetch(`/api/todos${path}`, {
    method,
    headers: { authorization: `Bearer ${token}` },
    body,
  });
  if (r.status === 401) {
    sessionStorage.removeItem("token");
    token = "";
    $("add").hidden = true;
    $("status").textContent = "Sign in again.";
    throw new Error("signed out");
  }
  if (!r.ok) {
    $("status").textContent = `Error ${r.status}`;
    throw new Error(String(r.status));
  }
  return r.status === 204 ? null : r.json();
}

async function refresh() {
  const items = await api("GET", "");
  $("add").hidden = false;
  $("status").textContent = "";
  $("list").replaceChildren(
    ...items.map((item) => {
      const li = document.createElement("li");
      li.textContent = item.text;
      const done = document.createElement("button");
      done.textContent = "Done";
      done.addEventListener("click", () => api("DELETE", `/${item.id}`).then(refresh));
      li.append(" ", done);
      return li;
    }),
  );
}

$("signin").addEventListener("submit", (e) => {
  e.preventDefault();
  token = $("token").value.trim();
  sessionStorage.setItem("token", token);
  refresh().catch(() => {});
});

$("add").addEventListener("submit", (e) => {
  e.preventDefault();
  api("POST", "", $("text").value)
    .then(() => {
      $("text").value = "";
      return refresh();
    })
    .catch(() => {});
});

if (token) refresh().catch(() => {});
