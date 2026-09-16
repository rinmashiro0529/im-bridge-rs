const root = document.getElementById("app");
let csrfToken = null;

async function getCsrf() {
  if (csrfToken) return csrfToken;
  const response = await fetch("/api/v1/csrf", { credentials: "same-origin" });
  if (!response.ok) throw new Error("Unable to initialize the secure session.");
  const json = await response.json();
  csrfToken = json.token;
  return csrfToken;
}

async function api(method, path, body) {
  const init = { method, credentials: "same-origin", headers: { "content-type": "application/json" } };
  if (method !== "GET" && method !== "HEAD") init.headers["x-csrf-token"] = await getCsrf();
  if (body !== undefined) init.body = JSON.stringify(body);
  let response = await fetch(path, init);
  if (response.status === 403 && method !== "GET" && method !== "HEAD") {
    csrfToken = null;
    init.headers["x-csrf-token"] = await getCsrf();
    response = await fetch(path, init);
  }
  const json = await response.json().catch(() => ({ error: { message: response.statusText } }));
  if (!response.ok) {
    const error = new Error(json?.error?.message || response.statusText);
    error.code = json?.error?.code;
    throw error;
  }
  return json;
}

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key === "class") node.className = value;
    else if (key.startsWith("on") && typeof value === "function") node[key] = value;
    else if (value !== null && value !== undefined) node.setAttribute(key, value);
  }
  for (const child of children) {
    if (child == null) continue;
    node.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
  }
  return node;
}

function renderLogin() {
  const username = el("input", { type: "text", autocomplete: "username", placeholder: "Username" });
  const password = el("input", { type: "password", autocomplete: "current-password", placeholder: "Password" });
  const error = el("div", { class: "error", role: "alert" });
  const login = async () => {
    error.textContent = "";
    try {
      await api("POST", "/api/v1/auth/login", { username: username.value, password: password.value });
      password.value = "";
      await render();
    } catch (err) {
      password.value = "";
      error.textContent = err.message;
    }
  };
  root.replaceChildren(el("section", { class: "panel" },
    el("h2", {}, "Sign in / 登录"),
    el("div", { class: "row" }, el("label", {}, "Username / 用户名"), username),
    el("div", { class: "row" }, el("label", {}, "Password / 密码"), password),
    el("button", { class: "primary", onclick: login }, "Sign in / 登录"),
    error,
  ));
}

async function renderDashboard(me) {
  const ready = await api("GET", "/health/ready").catch((error) => ({ ok: false, error }));
  const bots = await api("GET", "/api/v1/telegram/bots");
  const list = el("div");
  for (const item of bots.items) {
    list.appendChild(el("div", { class: "row" },
      el("span", { class: `status ${item.status}` }, item.status),
      el("code", {}, item.bot.id),
      el("button", { onclick: async () => { await api("POST", `/api/v1/telegram/bots/${item.bot.id}/start`); await render(); } }, "Start / 启动"),
      el("button", { onclick: async () => { await api("POST", `/api/v1/telegram/bots/${item.bot.id}/stop`); await render(); } }, "Stop / 停止"),
      el("button", { onclick: async () => {
        const result = await api("POST", `/api/v1/telegram/bots/${item.bot.id}/bind-code`);
        window.alert(`Binding code / 绑定码: ${result.code}`);
      } }, "Bind / 绑定"),
    ));
  }
  const token = el("input", { type: "password", autocomplete: "off", placeholder: "Telegram Bot Token" });
  const panel = el("section", { class: "panel" },
    el("h2", {}, "IM Bridge"),
    el("p", {}, `Signed in / 已登录：${me.account.username}`),
    el("p", {}, `Service ready / 服务就绪：${ready.ok ? "yes / 是" : "no / 否"}`),
    el("p", { class: "muted" }, "Characters, chats, models, and generation are owned by SillyTavern. Native chat routes are intentionally retired."),
    el("h3", {}, "Telegram bots"),
    list,
    el("div", { class: "row" }, token, el("button", { class: "primary", onclick: async () => {
      await api("POST", "/api/v1/telegram/bots", { token: token.value, desired_enabled: false });
      token.value = "";
      await render();
    } }, "Save disabled bot / 保存为停用 Bot")),
    el("button", { onclick: async () => { await api("POST", "/api/v1/auth/logout"); await render(); } }, "Sign out / 登出"),
  );
  root.replaceChildren(panel);
}

async function render() {
  try {
    await renderDashboard(await api("GET", "/api/v1/auth/me"));
  } catch {
    renderLogin();
  }
}

render();
