(() => {
  "use strict";

  const app = document.querySelector("#app");
  const modalRoot = document.querySelector("#modal-root");
  const toastRegion = document.querySelector("#toast-region");
  const API = "/api";
  const runtime = window.PHENOGRAM_RUNTIME || {
    surface: "combined",
    landingBaseUrl: window.location.origin,
    appBaseUrl: window.location.origin,
    apiBaseUrl: window.location.origin,
  };
  const surface = ["landing", "app", "combined"].includes(runtime.surface) ? runtime.surface : "combined";
  const appHref = (path = "/") => `${String(runtime.appBaseUrl || window.location.origin).replace(/\/$/, "")}/#${path.startsWith("/") ? path : `/${path}`}`;
  const landingHref = (anchor = "") => `${String(runtime.landingBaseUrl || window.location.origin).replace(/\/$/, "")}/${anchor ? `#${anchor.replace(/^#/, "")}` : ""}`;
  const privacyHref = () => `${String(runtime.landingBaseUrl || window.location.origin).replace(/\/$/, "")}/privacy`;

  const state = {
    phase: "booting",
    user: null,
    membership: null,
    health: null,
    csrfToken: null,
    bots: [],
    botCoverage: null,
    selectedBotId: null,
    bot: null,
    activity: [],
    updates: [],
    conversations: [],
    selectedConversationId: null,
    route: { name: "landing", params: {} },
    loading: {},
    errors: {},
    filters: { type: "", query: "", limit: "50" },
    updatesPaused: false,
    updateTimer: null,
    updatesStream: null,
    updatesStreamStatus: "idle",
    updatesStreamRetryTimer: null,
    updatesStreamRetryAttempt: 0,
    updatesStreamGeneration: 0,
    updatesStreamCursors: {},
    updatesFilterRefreshTimer: null,
    updatesFilterRefreshToken: 0,
    updatesFilterRefreshInFlightToken: null,
    updatesFilterRefreshPending: false,
    updatesFilterRefreshRetryAttempt: 0,
    updatesRenderFrame: null,
    mobileMenu: false,
    modal: null,
    drawer: null,
    streamKey: null,
    streamKeyId: null,
    streamKeys: [],
    fileLink: null,
    botContextVersion: 0,
    sessionVersion: 0,
    requestSequence: 0,
    requestTickets: {},
    modalReturnFocus: null,
    authError: null,
  };

  const icon = (name, className = "") =>
    `<svg class="icon ${className}" aria-hidden="true"><use href="#i-${name}"></use></svg>`;

  const esc = (value) => String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");

  const initials = (value) => {
    const parts = String(value || "Bot").trim().split(/\s+/).filter(Boolean);
    return esc(parts.slice(0, 2).map((part) => part[0]).join("").toUpperCase() || "B");
  };

  const nonEmailIdentity = (value) => {
    const text = String(value || "").trim();
    return text && !/\S+@\S+\.\S+/.test(text) ? text : "";
  };

  const userProvider = () => String(state.user?.provider || "").trim().toLowerCase();
  const userProviderLabel = () => ({ github: "GitHub", google: "Google" })[userProvider()] || "Social account";
  const userDisplayName = () => [state.user?.display_name, state.user?.provider_login]
    .map(nonEmailIdentity)
    .find(Boolean) || `${userProviderLabel()} user`;
  const userProviderLogin = () => nonEmailIdentity(state.user?.provider_login);
  const userIdentityMeta = () => {
    const login = userProviderLogin().replace(/^@/, "");
    return login ? `@${login} on ${userProviderLabel()}` : `Signed in with ${userProviderLabel()}`;
  };

  const unwrap = (payload, key) => {
    if (payload == null) return null;
    if (key && payload[key] != null) return payload[key];
    if (payload.data != null) {
      if (key && payload.data[key] != null) return payload.data[key];
      return payload.data;
    }
    return payload;
  };

  const listFrom = (payload, key) => {
    const value = unwrap(payload, key);
    if (Array.isArray(value)) return value;
    if (value && Array.isArray(value.items)) return value.items;
    if (value && Array.isArray(value.results)) return value.results;
    return [];
  };

  const api = async (path, options = {}) => {
    const headers = { Accept: "application/json", ...(options.headers || {}) };
    const method = String(options.method || "GET").toUpperCase();
    if (!["GET", "HEAD", "OPTIONS"].includes(method) && state.csrfToken) {
      headers["X-Phenogram-CSRF"] = state.csrfToken;
    }
    let body = options.body;
    if (body != null && !(body instanceof FormData) && typeof body !== "string") {
      headers["Content-Type"] = "application/json";
      body = JSON.stringify(body);
    }

    let response;
    try {
      response = await fetch(`${API}${path}`, {
        ...options,
        body,
        headers,
        credentials: "same-origin",
      });
    } catch (cause) {
      const error = new Error("Phenogram could not reach the server. Check your connection and try again.");
      error.cause = cause;
      throw error;
    }

    const contentType = response.headers.get("content-type") || "";
    let payload = null;
    if (response.status !== 204) {
      try {
        payload = contentType.includes("json") ? await response.json() : await response.text();
      } catch (_) {
        payload = null;
      }
    }

    if (!response.ok) {
      const message = typeof payload === "string"
        ? payload
        : payload?.message || payload?.error?.message || payload?.error || payload?.detail;
      const error = new Error(message || `Request failed (${response.status})`);
      error.status = response.status;
      error.payload = payload;
      if (response.status === 401 && state.user) {
        window.queueMicrotask(() => handleExpiredSession());
      }
      throw error;
    }
    return payload;
  };

  const botId = (bot) => String(bot?.id ?? bot?.bot_id ?? bot?.uuid ?? "");
  const botName = (bot) => bot?.display_name || bot?.name || bot?.first_name || bot?.bot_name || bot?.username || "Telegram bot";
  const botUsername = (bot) => {
    const raw = bot?.username || bot?.bot_username || bot?.telegram_username || "";
    return raw ? `@${String(raw).replace(/^@/, "")}` : "Telegram bot";
  };
  const botStatus = (bot) => {
    if (bot?.token_valid === false) return "token_invalid";
    return String(bot?.status || bot?.health || "unknown").toLowerCase();
  };

  const botStatusView = (bot) => {
    const status = botStatus(bot);
    if (["active", "healthy", "ready", "ok"].includes(status)) return { tone: "success", label: "Healthy" };
    if (["invalid", "token_invalid", "error", "disabled", "failed"].includes(status)) return { tone: "danger", label: status === "token_invalid" ? "Token invalid" : "Needs attention" };
    if (["provisioning", "setup", "pending"].includes(status)) return { tone: "warning", label: "Provisioning" };
    if (["degraded", "warning"].includes(status)) return { tone: "warning", label: "Degraded" };
    return { tone: "info", label: "Status unknown" };
  };

  const renderBotStatusBadge = (bot) => {
    const view = botStatusView(bot);
    return `<span class="badge badge--${view.tone}">${esc(view.label)}</span>`;
  };

  const currentBot = () => {
    if (state.bot && botId(state.bot) === String(state.selectedBotId)) return state.bot;
    return state.bots.find((bot) => botId(bot) === String(state.selectedBotId)) || null;
  };

  const membershipPlan = () => {
    const value = state.membership?.plan_name || state.membership?.plan?.name || state.membership?.plan || state.membership?.tier || "Free";
    return String(value).replace(/_/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
  };

  const membershipLimit = () => {
    const explicit = state.membership?.bot_limit ?? state.membership?.limits?.bots ?? state.membership?.max_bots;
    if (Number.isFinite(Number(explicit))) return Number(explicit);
    const plan = membershipPlan().toLowerCase();
    if (plan.includes("scale")) return 25;
    if (plan.includes("pro")) return 5;
    return 1;
  };

  const retentionDays = () => {
    const explicit = state.membership?.retention_days ?? state.membership?.limits?.retention_days;
    if (Number.isFinite(Number(explicit))) return Number(explicit);
    const plan = membershipPlan().toLowerCase();
    if (plan.includes("scale")) return 365;
    if (plan.includes("pro")) return 90;
    return 30;
  };

  const isManagedBot = (bot) => bot?.is_managed === true || String(bot?.bot_kind || "").toLowerCase() === "managed";
  const connectedBots = () => state.bots.filter((bot) => !isManagedBot(bot));
  const botManagerId = (bot) => String(bot?.manager_bot_id || "");
  const botRetentionDays = (bot) => {
    const effective = Number(bot?.effective_retention_days);
    return Number.isFinite(effective) && effective > 0 ? effective : retentionDays();
  };
  const retentionLabel = (bot) => botRetentionDays(bot) === 1 ? "24-hour history" : `${formatNumber(botRetentionDays(bot))}-day history`;
  const retentionValue = (bot) => botRetentionDays(bot) === 1 ? "24 hours" : `${formatNumber(botRetentionDays(bot))} days`;
  const botRetentionWarning = (bot) => {
    const warning = String(bot?.retention_warning || "").toLowerCase();
    return ["manager_missing", "free_plan", "plan_limit"].includes(warning) ? warning : null;
  };
  const botNeedsRetentionWarning = (bot) => isManagedBot(bot)
    && (botRetentionWarning(bot) != null || bot?.plan_covered === false || botRetentionDays(bot) <= 1);
  const telegramBotId = (bot) => String(bot?.telegram_bot_id || bot?.telegram_id || "");
  const findManagerBot = (bot) => {
    const managerId = botManagerId(bot);
    if (managerId) {
      const manager = state.bots.find((candidate) => botId(candidate) === managerId && botId(candidate) !== botId(bot));
      if (manager) return manager;
    }
    const managerTelegramId = String(bot?.manager_telegram_bot_id || "");
    return managerTelegramId
      ? state.bots.find((candidate) => telegramBotId(candidate) === managerTelegramId && botId(candidate) !== botId(bot)) || null
      : null;
  };
  const managerLabel = (bot) => {
    const manager = findManagerBot(bot);
    if (manager) return botUsername(manager) !== "Telegram bot" ? botUsername(manager) : botName(manager);
    const username = String(bot?.manager_username || "").replace(/^@/, "");
    const managerTelegramId = String(bot?.manager_telegram_bot_id || "");
    return username ? `@${username}` : bot?.manager_display_name || (managerTelegramId ? `bot ${managerTelegramId}` : "manager bot");
  };
  const botAncestorChain = (bot) => {
    if (!bot) return [];
    const ancestors = [];
    const seen = new Set([botId(bot)]);
    let current = bot;
    while (isManagedBot(current)) {
      const manager = findManagerBot(current);
      const id = botId(manager);
      if (!manager || !id || seen.has(id)) break;
      ancestors.unshift(manager);
      seen.add(id);
      current = manager;
    }
    return [...ancestors, bot];
  };
  const managedDescendantCount = (bot) => state.bots.filter((candidate) => isManagedBot(candidate)
    && botAncestorChain(candidate).slice(0, -1).some((ancestor) => botId(ancestor) === botId(bot))).length;
  const sortBots = (bots) => [...bots].sort((left, right) => botName(left).localeCompare(botName(right), undefined, { sensitivity: "base" }));
  const botHierarchy = () => {
    const childrenByManager = new Map();
    state.bots.filter(isManagedBot).forEach((bot) => {
      const manager = findManagerBot(bot);
      if (!manager) return;
      const managerId = botId(manager);
      const children = childrenByManager.get(managerId) || [];
      children.push(bot);
      childrenByManager.set(managerId, children);
    });
    childrenByManager.forEach((children, id) => childrenByManager.set(id, sortBots(children)));

    const included = new Set();
    const buildNode = (bot, ancestors = new Set()) => {
      const id = botId(bot);
      included.add(id);
      const path = new Set(ancestors);
      path.add(id);
      const children = (childrenByManager.get(id) || [])
        .filter((child) => !path.has(botId(child)) && !included.has(botId(child)))
        .map((child) => buildNode(child, path));
      return { bot, children };
    };

    const roots = sortBots(connectedBots()).map((bot) => buildNode(bot));
    const remaining = () => sortBots(state.bots.filter((bot) => isManagedBot(bot) && !included.has(botId(bot))));
    const orphans = [];
    remaining().filter((bot) => !findManagerBot(bot)).forEach((bot) => {
      if (!included.has(botId(bot))) orphans.push(buildNode(bot));
    });
    while (remaining().length) orphans.push(buildNode(remaining()[0]));
    return { roots, orphans };
  };
  const coverageStats = () => {
    const coverage = state.botCoverage || {};
    const fallbackCovered = state.bots.filter((bot) => bot?.plan_covered !== false).length;
    const fallbackUncovered = state.bots.filter((bot) => bot?.plan_covered === false).length;
    const coveredValue = Number(coverage.covered_bot_count);
    const uncoveredValue = Number(coverage.uncovered_bot_count);
    const limitValue = Number(coverage.bot_limit);
    const covered = Number.isFinite(coveredValue) ? coveredValue : fallbackCovered;
    const uncovered = Number.isFinite(uncoveredValue) ? uncoveredValue : fallbackUncovered;
    return {
      covered,
      uncovered,
      total: covered + uncovered,
      limit: Number.isFinite(limitValue) ? limitValue : membershipLimit(),
    };
  };

  const formatNumber = (value) => {
    const number = Number(value);
    return Number.isFinite(number) ? new Intl.NumberFormat(undefined, { notation: number > 9999 ? "compact" : "standard", maximumFractionDigits: 1 }).format(number) : "—";
  };

  const asDate = (value) => {
    if (value == null || value === "") return null;
    const normalized = typeof value === "number" && value < 1e12 ? value * 1000 : value;
    const date = new Date(normalized);
    return Number.isNaN(date.getTime()) ? null : date;
  };

  const formatDate = (value, mode = "short") => {
    const date = asDate(value);
    if (!date) return "—";
    if (mode === "time") return new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit" }).format(date);
    if (mode === "full") return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(date);
    return new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }).format(date);
  };

  const relativeTime = (value) => {
    const date = asDate(value);
    if (!date) return "No activity yet";
    const seconds = Math.round((date.getTime() - Date.now()) / 1000);
    const absolute = Math.abs(seconds);
    const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
    if (absolute < 60) return rtf.format(seconds, "second");
    if (absolute < 3600) return rtf.format(Math.round(seconds / 60), "minute");
    if (absolute < 86400) return rtf.format(Math.round(seconds / 3600), "hour");
    return rtf.format(Math.round(seconds / 86400), "day");
  };

  const errorMessage = (error) => error?.message || "Something went wrong. Please try again.";
  const botPath = (id, section = "overview") => `#/bots/${encodeURIComponent(id)}/${section}`;

  const parseRoute = () => {
    if (window.location.pathname.replace(/\/+$/, "") === "/privacy") return { name: "privacy", params: {} };
    const raw = (window.location.hash.replace(/^#/, "") || "/").split("?", 1)[0];
    if (!raw.startsWith("/")) return { name: "landing", params: { anchor: raw } };
    const parts = raw.split("/").filter(Boolean).map((part) => {
      try { return decodeURIComponent(part); } catch (_) { return ""; }
    });
    if (!parts.length) return { name: "landing", params: {} };
    if (parts[0] === "login") return { name: "auth", params: {} };
    if (parts[0] === "privacy") return { name: "privacy", params: {} };
    if (parts[0] === "overview") return { name: "overview", params: {} };
    if (parts[0] === "bots" && !parts[1]) return { name: "bots", params: {} };
    if (parts[0] === "bots" && parts[1]) {
      const section = ["overview", "updates", "view", "integration", "settings"].includes(parts[2]) ? parts[2] : "overview";
      return { name: `bot-${section}`, params: { botId: parts[1] } };
    }
    if (parts[0] === "billing") return { name: "billing", params: {} };
    if (parts[0] === "settings") return { name: "settings", params: {} };
    return { name: state.user ? "overview" : "landing", params: {} };
  };

  const consumeOAuthError = () => {
    const url = new URL(window.location.href);
    const hashParts = url.hash.replace(/^#/, "").split("?");
    const hashPath = hashParts.shift() || "";
    const hashParams = new URLSearchParams(hashParts.join("?"));
    const keys = ["oauth_error", "error", "error_code", "error_description", "provider"];
    const read = (key) => hashParams.get(key) || url.searchParams.get(key) || "";
    const code = String(read("oauth_error") || read("error_code") || read("error")).trim().toLowerCase();
    if (!code) return null;

    const message = ["access_denied", "cancelled", "canceled", "user_cancelled"].includes(code)
      ? "Sign-in was cancelled. Choose a provider when you’re ready."
      : ["state_mismatch", "invalid_state", "expired_state", "expired"].includes(code)
        ? "That sign-in attempt expired. Please try again."
        : ["provider_not_configured", "oauth_not_configured", "temporarily_unavailable"].includes(code)
          ? "That sign-in provider is temporarily unavailable. Try the other provider or try again later."
          : "We couldn’t complete social sign-in. Please try again.";

    keys.forEach((key) => {
      hashParams.delete(key);
      url.searchParams.delete(key);
    });
    const remainingHashQuery = hashParams.toString();
    url.hash = hashPath ? `#${hashPath}${remainingHashQuery ? `?${remainingHashQuery}` : ""}` : "";
    window.history.replaceState(null, "", `${url.pathname}${url.search}${url.hash}`);
    return message;
  };

  const navigate = (path) => {
    const target = path.startsWith("#") ? path : `#${path}`;
    if (window.location.hash === target) routeChanged();
    else window.location.hash = target;
  };

  const setLoading = (key, value) => { state.loading[key] = value; };
  const setError = (key, value) => { state.errors[key] = value; };

  const startRequest = (key) => {
    const ticket = ++state.requestSequence;
    state.requestTickets[key] = ticket;
    return ticket;
  };

  const requestIsCurrent = (key, ticket) => state.requestTickets[key] === ticket;
  const botRequestIsCurrent = (key, ticket, id, contextVersion) => requestIsCurrent(key, ticket)
    && state.botContextVersion === contextVersion
    && String(state.selectedBotId) === String(id);

  function clearBotState({ clearSelection = false } = {}) {
    stopUpdatesStream({ status: "idle" });
    state.botContextVersion += 1;
    state.requestTickets = {};
    state.bot = null;
    state.activity = [];
    state.updates = [];
    state.conversations = [];
    state.selectedConversationId = null;
    state.streamKey = null;
    state.streamKeyId = null;
    state.streamKeys = [];
    state.fileLink = null;
    state.drawer = null;
    state.updatesPaused = false;
    state.loading = {};
    state.errors = {};
    if (clearSelection) state.selectedBotId = null;
  }

  function selectBot(id) {
    const nextId = id == null ? null : String(id);
    if (String(state.selectedBotId || "") === String(nextId || "")) return;
    clearSensitiveState({ scope: "bot" });
    state.selectedBotId = nextId;
  }

  function clearSensitiveState({ scope = "session" } = {}) {
    clearBotState({ clearSelection: true });
    if (scope === "bot") return;
    state.sessionVersion += 1;
    state.updatesStreamCursors = {};
    state.user = null;
    state.membership = null;
    state.csrfToken = null;
    state.bots = [];
    state.botCoverage = null;
    state.mobileMenu = false;
    state.phase = "guest";
    closeModal({ restoreFocus: false });
  }

  const toast = (message, type = "success") => {
    const element = document.createElement("div");
    element.className = `toast ${type === "error" ? "toast--error" : type === "warning" ? "toast--warning" : ""}`;
    element.innerHTML = `${icon(type === "error" || type === "warning" ? "alert" : "check")}<div>${esc(message)}</div>`;
    toastRegion.append(element);
    window.setTimeout(() => element.remove(), 4200);
  };

  const setModal = (name, data = {}) => {
    if (name && !state.modal) state.modalReturnFocus = document.activeElement;
    state.modal = name ? { name, data } : null;
    renderModal(true);
    if (name) window.setTimeout(() => modalRoot.querySelector("input, button, textarea, select")?.focus(), 20);
  };

  const closeModal = ({ restoreFocus = true } = {}) => {
    const returnFocus = state.modalReturnFocus;
    state.modal = null;
    state.modalReturnFocus = null;
    renderModal(true);
    if (restoreFocus && returnFocus instanceof HTMLElement && returnFocus.isConnected) {
      window.setTimeout(() => returnFocus.focus(), 0);
    }
  };

  async function bootstrap() {
    state.authError = consumeOAuthError();
    state.route = parseRoute();
    if (surface === "landing") {
      state.phase = "guest";
      render();
      if (state.route.params.anchor) window.setTimeout(() => document.getElementById(state.route.params.anchor)?.scrollIntoView(), 10);
      return;
    }
    const healthPromise = api("/health", { auth: false }).then((value) => { state.health = value || { status: "ok" }; }).catch((error) => { state.health = { status: "down", message: errorMessage(error) }; });
    try {
      const payload = await api("/me");
      state.user = unwrap(payload, "user") || payload?.user || null;
      state.membership = unwrap(payload, "membership") || payload?.membership || null;
      state.csrfToken = payload?.csrf_token || null;
      state.phase = state.user ? "ready" : "guest";
    } catch (error) {
      if (error.status !== 401) state.errors.session = errorMessage(error);
      state.phase = "guest";
    }
    await healthPromise;

    if (state.user) {
      await loadBots({ silent: true });
      if (["landing", "auth"].includes(state.route.name)) {
        navigate("/overview");
        return;
      }
    } else if ((surface === "app" && state.route.name === "landing") || !['landing', 'auth'].includes(state.route.name)) {
      navigate("/login");
      return;
    }
    await routeChanged();
  }

  async function loadBots({ silent = false } = {}) {
    const sessionVersion = state.sessionVersion;
    const ticket = startRequest("bots");
    if (!silent) { setLoading("bots", true); setError("bots", null); render(); }
    try {
      const payload = await api("/bots");
      if (state.sessionVersion !== sessionVersion || !requestIsCurrent("bots", ticket)) return;
      const bots = listFrom(payload, "bots");
      state.bots = bots;
      state.botCoverage = payload?.coverage || payload?.data?.coverage || null;
      if (!state.selectedBotId || !bots.some((bot) => botId(bot) === String(state.selectedBotId))) {
        const preferred = bots.find((bot) => !isManagedBot(bot)) || bots[0];
        selectBot(preferred ? botId(preferred) : null);
      }
    } catch (error) {
      if (state.sessionVersion !== sessionVersion || !requestIsCurrent("bots", ticket)) return;
      setError("bots", errorMessage(error));
    } finally {
      if (state.sessionVersion === sessionVersion && requestIsCurrent("bots", ticket)) setLoading("bots", false);
    }
  }

  async function loadBot(id, { silent = false } = {}) {
    if (!id) return;
    const contextVersion = state.botContextVersion;
    const ticket = startRequest("bot");
    if (!silent) setLoading("bot", true);
    setError("bot", null);
    try {
      const payload = await api(`/bots/${encodeURIComponent(id)}`);
      if (!botRequestIsCurrent("bot", ticket, id, contextVersion)) return;
      const core = unwrap(payload, "bot") || payload;
      const existing = state.bots.find((candidate) => botId(candidate) === String(id));
      state.bot = { ...(existing || {}), ...(core || {}), ...(payload?.stats || {}), integration: payload?.integration || core?.integration };
      if (payload?.membership) state.membership = payload.membership;
      const index = state.bots.findIndex((candidate) => botId(candidate) === String(id));
      if (state.bot && index >= 0) state.bots[index] = { ...state.bots[index], ...state.bot };
    } catch (error) {
      if (botRequestIsCurrent("bot", ticket, id, contextVersion)) setError("bot", errorMessage(error));
    } finally {
      if (botRequestIsCurrent("bot", ticket, id, contextVersion)) setLoading("bot", false);
    }
  }

  async function loadActivity({ silent = false } = {}) {
    const id = state.selectedBotId;
    if (!id) return;
    const contextVersion = state.botContextVersion;
    const ticket = startRequest("activity");
    if (!silent) setLoading("activity", true);
    setError("activity", null);
    try {
      const payload = await api(`/bots/${encodeURIComponent(id)}/activity`);
      if (!botRequestIsCurrent("activity", ticket, id, contextVersion)) return;
      state.activity = listFrom(payload, "activity");
    } catch (error) {
      if (botRequestIsCurrent("activity", ticket, id, contextVersion)) setError("activity", errorMessage(error));
    } finally {
      if (botRequestIsCurrent("activity", ticket, id, contextVersion)) setLoading("activity", false);
    }
  }

  async function loadStreamKeys({ silent = false } = {}) {
    const id = state.selectedBotId;
    if (!id) return;
    const contextVersion = state.botContextVersion;
    const ticket = startRequest("streamKeys");
    if (!silent) { setLoading("streamKeys", true); setError("streamKeys", null); render(); }
    try {
      const payload = await api(`/bots/${encodeURIComponent(id)}/stream-keys`);
      if (!botRequestIsCurrent("streamKeys", ticket, id, contextVersion)) return;
      state.streamKeys = listFrom(payload, "stream_keys");
      setError("streamKeys", null);
    } catch (error) {
      if (botRequestIsCurrent("streamKeys", ticket, id, contextVersion)) setError("streamKeys", errorMessage(error));
    } finally {
      if (botRequestIsCurrent("streamKeys", ticket, id, contextVersion)) {
        setLoading("streamKeys", false);
        render();
      }
    }
  }

  async function loadUpdates({ silent = false } = {}) {
    const id = state.selectedBotId;
    if (!id || (silent && state.updatesPaused)) return null;
    const contextVersion = state.botContextVersion;
    const ticket = startRequest("updates");
    if (!silent) { setLoading("updates", true); setError("updates", null); render(); }
    try {
      const params = new URLSearchParams();
      params.set("limit", state.filters.limit || "50");
      if (state.filters.type) params.set("type", state.filters.type);
      if (state.filters.query) params.set("query", state.filters.query);
      const payload = await api(`/bots/${encodeURIComponent(id)}/updates?${params}`);
      if (!botRequestIsCurrent("updates", ticket, id, contextVersion)) return null;
      const snapshot = listFrom(payload, "updates");
      const snapshotCursor = normalizeJournalId(payload?.stream_cursor ?? payload?.data?.stream_cursor)
        || newestUpdateJournalId(snapshot);
      const streamedAfterSnapshot = snapshotCursor
        ? state.updates.filter((item) => compareJournalIds(updateJournalId(item), snapshotCursor) > 0)
        : [];
      state.updates = mergeStoredUpdates(snapshot, streamedAfterSnapshot);
      advanceUpdatesStreamCursor(id, snapshotCursor);
      refreshDrawerUpdateReference();
      state.updatesFilterRefreshRetryAttempt = 0;
      setError("updates", null);
      return true;
    } catch (error) {
      if (!botRequestIsCurrent("updates", ticket, id, contextVersion)) return null;
      setError("updates", errorMessage(error));
      if (!silent) toast(errorMessage(error), "error");
      return false;
    } finally {
      if (botRequestIsCurrent("updates", ticket, id, contextVersion)) {
        setLoading("updates", false);
        renderUpdatesPanel();
      }
    }
  }

  async function loadConversations({ silent = false } = {}) {
    const id = state.selectedBotId;
    if (!id) return;
    const contextVersion = state.botContextVersion;
    const ticket = startRequest("conversations");
    if (!silent) { setLoading("conversations", true); setError("conversations", null); render(); }
    try {
      const payload = await api(`/bots/${encodeURIComponent(id)}/conversations`);
      if (!botRequestIsCurrent("conversations", ticket, id, contextVersion)) return;
      state.conversations = listFrom(payload, "conversations");
      if (!state.selectedConversationId || !state.conversations.some((item) => conversationId(item) === String(state.selectedConversationId))) {
        state.selectedConversationId = state.conversations[0] ? conversationId(state.conversations[0]) : null;
      }
      if (state.selectedConversationId) await loadConversationMessages(state.selectedConversationId);
    } catch (error) {
      if (botRequestIsCurrent("conversations", ticket, id, contextVersion)) setError("conversations", errorMessage(error));
    } finally {
      if (botRequestIsCurrent("conversations", ticket, id, contextVersion)) {
        setLoading("conversations", false);
        render();
      }
    }
  }

  async function loadConversationMessages(chatId) {
    const botIdAtRequest = state.selectedBotId;
    const contextVersion = state.botContextVersion;
    const requestKey = `messages:${chatId}`;
    const ticket = startRequest(requestKey);
    const conversation = state.conversations.find((item) => conversationId(item) === String(chatId));
    if (!conversation || !botIdAtRequest) return;
    try {
      const payload = await api(`/bots/${encodeURIComponent(botIdAtRequest)}/conversations/${encodeURIComponent(chatId)}/messages`);
      if (!botRequestIsCurrent(requestKey, ticket, botIdAtRequest, contextVersion)) return;
      conversation.messages = listFrom(payload, "messages");
    } catch (_) {
      // Conversation summaries remain useful even when historical payload loading fails.
    }
  }

  async function handleExpiredSession() {
    if (!state.user) return;
    clearSensitiveState();
    navigate("/login");
    toast("Your session expired. Sign in again.", "error");
  }

  const UPDATES_STREAM_RETRY_DELAYS = [1000, 2000, 4000, 8000, 15000, 30000];
  const FILTERED_UPDATES_RETRY_DELAYS = [1000, 2000, 4000, 8000, 15000, 30000];

  function normalizeJournalId(value) {
    const id = String(value ?? "").trim();
    return /^\d+$/.test(id) ? id.replace(/^0+(?=\d)/, "") : "";
  }

  function updateJournalId(item) {
    return normalizeJournalId(item?.id ?? item?.row_id);
  }

  function compareJournalIds(left, right) {
    const leftId = normalizeJournalId(left);
    const rightId = normalizeJournalId(right);
    if (leftId === rightId) return 0;
    if (!leftId) return -1;
    if (!rightId) return 1;
    try {
      return BigInt(leftId) > BigInt(rightId) ? 1 : -1;
    } catch (_) {
      return leftId.length === rightId.length
        ? (leftId > rightId ? 1 : -1)
        : (leftId.length > rightId.length ? 1 : -1);
    }
  }

  function newestUpdateJournalId(items = state.updates) {
    return items.reduce((newest, item) => {
      const candidate = updateJournalId(item);
      return compareJournalIds(candidate, newest) > 0 ? candidate : newest;
    }, "");
  }

  function updateListLimit() {
    const limit = Number.parseInt(state.filters.limit, 10);
    return Number.isFinite(limit) ? Math.min(200, Math.max(1, limit)) : 50;
  }

  function mergeStoredUpdates(base, incoming = []) {
    const byId = new Map();
    [...base, ...incoming].forEach((item) => {
      if (!item || typeof item !== "object") return;
      const id = updateJournalId(item);
      const fallback = `${updateId(item)}:${String(updateTime(item) || "")}`;
      const key = id ? `journal:${id}` : `fallback:${fallback}`;
      const existing = byId.get(key);
      byId.set(key, existing ? { ...existing, ...item } : item);
    });
    return [...byId.values()]
      .sort((left, right) => {
        const byJournal = compareJournalIds(updateJournalId(right), updateJournalId(left));
        if (byJournal) return byJournal;
        return String(updateTime(right) || "").localeCompare(String(updateTime(left) || ""));
      })
      .slice(0, updateListLimit());
  }

  function updatesStreamCursor(botIdValue = state.selectedBotId) {
    return normalizeJournalId(state.updatesStreamCursors[String(botIdValue || "")]);
  }

  function advanceUpdatesStreamCursor(botIdValue, candidate) {
    const botKey = String(botIdValue || "");
    const next = normalizeJournalId(candidate);
    if (!botKey || !next) return;
    const current = updatesStreamCursor(botKey);
    if (compareJournalIds(next, current) > 0) state.updatesStreamCursors[botKey] = next;
  }

  function updatesViewIsCurrent(botIdValue, sessionVersion, contextVersion) {
    return state.route.name === "bot-updates"
      && Boolean(state.user)
      && state.sessionVersion === sessionVersion
      && state.botContextVersion === contextVersion
      && String(state.selectedBotId) === String(botIdValue);
  }

  function updatesStreamContextIsCurrent(context, source = null) {
    return updatesViewIsCurrent(context.botId, context.sessionVersion, context.contextVersion)
      && state.updatesStreamGeneration === context.generation
      && (!source || state.updatesStream === source);
  }

  function stopUpdatesPolling() {
    if (state.updateTimer) window.clearInterval(state.updateTimer);
    state.updateTimer = null;
  }

  function startUpdatesPolling({ reportFallback = false } = {}) {
    stopUpdatesPolling();
    if (state.route.name !== "bot-updates" || state.updatesPaused) return;
    if (reportFallback) {
      state.updatesStreamStatus = "fallback";
      renderUpdatesLiveStatus();
    }
    state.updateTimer = window.setInterval(() => {
      if (document.visibilityState === "visible") loadUpdates({ silent: true });
    }, 8000);
  }

  function resetFilteredUpdatesReload() {
    if (state.updatesFilterRefreshTimer) window.clearTimeout(state.updatesFilterRefreshTimer);
    state.updatesFilterRefreshTimer = null;
    state.updatesFilterRefreshToken += 1;
    state.updatesFilterRefreshInFlightToken = null;
    state.updatesFilterRefreshPending = false;
    state.updatesFilterRefreshRetryAttempt = 0;
  }

  function stopUpdatesStream({ status = "idle" } = {}) {
    state.updatesStreamGeneration += 1;
    if (state.updatesStream) state.updatesStream.close();
    state.updatesStream = null;
    if (state.updatesStreamRetryTimer) window.clearTimeout(state.updatesStreamRetryTimer);
    state.updatesStreamRetryTimer = null;
    resetFilteredUpdatesReload();
    if (state.updatesRenderFrame) window.cancelAnimationFrame(state.updatesRenderFrame);
    state.updatesRenderFrame = null;
    stopUpdatesPolling();
    state.updatesStreamStatus = status;
    if (status !== "reconnecting") state.updatesStreamRetryAttempt = 0;
    renderUpdatesLiveStatus();
  }

  function renderUpdatesPanel() {
    if (state.route.name !== "bot-updates") return;
    const panel = document.querySelector("#updates-panel");
    if (panel) panel.innerHTML = renderUpdatesTable();
    renderUpdatesLiveStatus();
  }

  function scheduleUpdatesPanelRender() {
    if (state.updatesRenderFrame) return;
    state.updatesRenderFrame = window.requestAnimationFrame(() => {
      state.updatesRenderFrame = null;
      renderUpdatesPanel();
    });
  }

  function updatesLiveView() {
    if (state.updatesPaused) return { label: "Paused", className: "is-paused" };
    if (state.updatesStreamStatus === "live") return { label: "Live", className: "is-live" };
    if (state.updatesStreamStatus === "fallback") return { label: "Auto refresh", className: "is-fallback" };
    if (state.updatesStreamStatus === "reconnecting") return { label: "Reconnecting", className: "is-reconnecting" };
    return { label: "Connecting", className: "is-connecting" };
  }

  function renderUpdatesLiveStatus() {
    if (state.route.name !== "bot-updates") return;
    const view = updatesLiveView();
    const status = document.querySelector("#updates-live-state");
    if (status) {
      status.className = `live-state ${view.className}`;
      status.textContent = view.label;
    }
    const toggle = document.querySelector('[data-action="toggle-updates"]');
    if (toggle) {
      toggle.setAttribute("aria-label", state.updatesPaused ? "Resume live updates" : "Pause live updates");
      toggle.innerHTML = icon(state.updatesPaused ? "play" : "pause");
    }
  }

  function refreshDrawerUpdateReference() {
    if (!state.drawer || state.drawer.type !== "update") return;
    const itemId = normalizeJournalId(state.drawer.itemId || updateJournalId(state.drawer.item));
    if (!itemId) return;
    const current = state.updates.find((item) => updateJournalId(item) === itemId);
    if (current) state.drawer = { ...state.drawer, itemId, item: current };
  }

  function scheduleFilteredUpdatesReload(context, { retry = false } = {}) {
    state.updatesFilterRefreshPending = true;
    if (state.updatesFilterRefreshTimer || state.updatesFilterRefreshInFlightToken != null) return;
    const delay = retry
      ? FILTERED_UPDATES_RETRY_DELAYS[Math.min(state.updatesFilterRefreshRetryAttempt, FILTERED_UPDATES_RETRY_DELAYS.length - 1)]
      : 350;
    state.updatesFilterRefreshTimer = window.setTimeout(async () => {
      state.updatesFilterRefreshTimer = null;
      if (!updatesStreamContextIsCurrent(context) || state.updatesPaused) {
        state.updatesFilterRefreshPending = false;
        return;
      }
      state.updatesFilterRefreshPending = false;
      const refreshToken = ++state.updatesFilterRefreshToken;
      state.updatesFilterRefreshInFlightToken = refreshToken;
      const loaded = await loadUpdates({ silent: true });
      if (state.updatesFilterRefreshInFlightToken !== refreshToken) return;
      state.updatesFilterRefreshInFlightToken = null;
      if (!updatesStreamContextIsCurrent(context)) return;
      if (loaded === true) {
        state.updatesFilterRefreshRetryAttempt = 0;
        if (state.updatesFilterRefreshPending) scheduleFilteredUpdatesReload(context);
        return;
      }
      state.updatesFilterRefreshPending = true;
      const retryAttempt = state.updatesFilterRefreshRetryAttempt;
      scheduleFilteredUpdatesReload(context, { retry: true });
      state.updatesFilterRefreshRetryAttempt = Math.min(
        retryAttempt + 1,
        FILTERED_UPDATES_RETRY_DELAYS.length - 1,
      );
    }, delay);
  }

  function mergeStreamedUpdate(context, event) {
    if (!updatesStreamContextIsCurrent(context, event.currentTarget)) return;
    let update;
    try {
      update = JSON.parse(event.data);
    } catch (_) {
      scheduleUpdatesStreamReconnect(context, { probeSession: false });
      return;
    }
    if (!update || typeof update !== "object" || Array.isArray(update)) return;
    const eventId = normalizeJournalId(event.lastEventId || update.id || update.row_id);
    if (!eventId) return;
    advanceUpdatesStreamCursor(context.botId, eventId);
    update = { ...update, id: eventId };
    if (state.filters.type || state.filters.query) {
      scheduleFilteredUpdatesReload(context);
      return;
    }
    state.updates = mergeStoredUpdates(state.updates, [update]);
    refreshDrawerUpdateReference();
    setError("updates", null);
    scheduleUpdatesPanelRender();
  }

  async function resyncUpdatesStream(context, event) {
    if (!updatesStreamContextIsCurrent(context, event.currentTarget)) return;
    advanceUpdatesStreamCursor(context.botId, event.lastEventId);
    const { botId, sessionVersion, contextVersion } = context;
    stopUpdatesStream({ status: "reconnecting" });
    await loadUpdates({ silent: true });
    if (updatesViewIsCurrent(botId, sessionVersion, contextVersion) && !state.updatesPaused) {
      startUpdatesStream({ reconnecting: true });
    }
  }

  async function handleUpdatesStreamRevoked(context, event) {
    if (!updatesStreamContextIsCurrent(context, event.currentTarget)) return;
    const { botId, sessionVersion, contextVersion } = context;
    stopUpdatesStream({ status: "reconnecting" });
    try {
      await api("/me");
      if (!updatesViewIsCurrent(botId, sessionVersion, contextVersion)) return;
      setError("updates", "Live update access ended. Refresh the bot list or choose another bot.");
      renderUpdatesPanel();
    } catch (error) {
      if (error.status !== 401 && updatesViewIsCurrent(botId, sessionVersion, contextVersion)) {
        scheduleUpdatesStreamReconnect({
          botId,
          sessionVersion,
          contextVersion,
          generation: state.updatesStreamGeneration,
        }, { probeSession: false });
      }
    }
  }

  function scheduleUpdatesStreamReconnect(context, { probeSession = true } = {}) {
    if (!updatesStreamContextIsCurrent(context, context.source || null)) return;
    if (state.updatesStream) state.updatesStream.close();
    state.updatesStream = null;
    state.updatesStreamGeneration += 1;
    const retryContext = {
      botId: context.botId,
      sessionVersion: context.sessionVersion,
      contextVersion: context.contextVersion,
      generation: state.updatesStreamGeneration,
    };
    state.updatesStreamStatus = state.updateTimer ? "fallback" : "reconnecting";
    renderUpdatesLiveStatus();
    const attempt = state.updatesStreamRetryAttempt;
    state.updatesStreamRetryAttempt = Math.min(attempt + 1, UPDATES_STREAM_RETRY_DELAYS.length - 1);
    if (attempt >= 3 && !state.updateTimer) startUpdatesPolling({ reportFallback: true });
    const delay = UPDATES_STREAM_RETRY_DELAYS[Math.min(attempt, UPDATES_STREAM_RETRY_DELAYS.length - 1)];

    const reconnect = () => {
      if (!updatesStreamContextIsCurrent(retryContext) || state.updatesPaused) return;
      state.updatesStreamRetryTimer = window.setTimeout(() => {
        state.updatesStreamRetryTimer = null;
        if (updatesStreamContextIsCurrent(retryContext) && !state.updatesPaused) {
          startUpdatesStream({ reconnecting: true });
        }
      }, delay);
    };

    if (!probeSession) {
      reconnect();
      return;
    }
    api("/me").then(reconnect).catch((error) => {
      if (error.status !== 401) reconnect();
    });
  }

  function startUpdatesStream({ reconnecting = false } = {}) {
    const botIdValue = state.selectedBotId;
    if (!botIdValue || state.route.name !== "bot-updates" || state.updatesPaused || !state.user) return;
    if (typeof window.EventSource !== "function") {
      startUpdatesPolling({ reportFallback: true });
      return;
    }
    if (state.updatesStream) state.updatesStream.close();
    state.updatesStream = null;
    if (state.updatesStreamRetryTimer) window.clearTimeout(state.updatesStreamRetryTimer);
    state.updatesStreamRetryTimer = null;
    if (!reconnecting) stopUpdatesPolling();
    state.updatesStreamGeneration += 1;
    state.updatesStreamStatus = reconnecting && state.updateTimer ? "fallback" : reconnecting ? "reconnecting" : "connecting";
    renderUpdatesLiveStatus();

    const cursor = updatesStreamCursor(botIdValue);
    const suffix = cursor ? `?after=${encodeURIComponent(cursor)}` : "";
    let source;
    try {
      source = new window.EventSource(`${API}/bots/${encodeURIComponent(botIdValue)}/updates/stream${suffix}`);
    } catch (_) {
      scheduleUpdatesStreamReconnect({
        botId: String(botIdValue),
        sessionVersion: state.sessionVersion,
        contextVersion: state.botContextVersion,
        generation: state.updatesStreamGeneration,
      }, { probeSession: false });
      return;
    }
    const context = {
      botId: String(botIdValue),
      sessionVersion: state.sessionVersion,
      contextVersion: state.botContextVersion,
      generation: state.updatesStreamGeneration,
      source,
    };
    state.updatesStream = source;

    source.addEventListener("open", () => {
      if (!updatesStreamContextIsCurrent(context, source)) return;
      state.updatesStreamStatus = "live";
      state.updatesStreamRetryAttempt = 0;
      stopUpdatesPolling();
      renderUpdatesLiveStatus();
      if (reconnecting && (state.filters.type || state.filters.query)) {
        resetFilteredUpdatesReload();
        scheduleFilteredUpdatesReload(context);
      }
    });
    source.addEventListener("update", (event) => mergeStreamedUpdate(context, event));
    source.addEventListener("resync", (event) => resyncUpdatesStream(context, event));
    source.addEventListener("revoked", (event) => handleUpdatesStreamRevoked(context, event));
    source.addEventListener("error", (event) => {
      if (!updatesStreamContextIsCurrent(context, source)) return;
      scheduleUpdatesStreamReconnect(context, { probeSession: !("data" in event && event.data) });
    });
  }

  async function toggleUpdatesStream() {
    if (state.updatesPaused) {
      state.updatesPaused = false;
      state.updatesStreamStatus = "connecting";
      renderUpdatesLiveStatus();
      const botIdValue = state.selectedBotId;
      const sessionVersion = state.sessionVersion;
      const contextVersion = state.botContextVersion;
      await loadUpdates({ silent: true });
      if (updatesViewIsCurrent(botIdValue, sessionVersion, contextVersion) && !state.updatesPaused) {
        startUpdatesStream();
      }
      return;
    }
    state.updatesPaused = true;
    stopUpdatesStream({ status: "paused" });
  }

  function setMobileMenu(open, { restoreFocus = false } = {}) {
    state.mobileMenu = Boolean(open);
    render();
    window.setTimeout(() => {
      if (state.mobileMenu) document.querySelector("#app-sidebar a, #app-sidebar button")?.focus();
      else if (restoreFocus) document.querySelector(".mobile-menu-btn")?.focus();
    }, 0);
  }

  async function routeChanged() {
    stopUpdatesStream({ status: "idle" });
    state.route = parseRoute();
    state.mobileMenu = false;
    state.drawer = null;
    if (state.route.name !== "bot-integration") {
      state.streamKey = null;
      state.streamKeyId = null;
      state.fileLink = null;
    }

    if (!state.user) {
      if (surface === "landing") {
        if (state.route.name !== "privacy") state.route = { name: "landing", params: state.route.params };
      } else if (!(surface === "combined" && state.route.name === "privacy") && (surface === "app" || !["landing", "auth"].includes(state.route.name))) {
        state.route = { name: "auth", params: {} };
      }
      render();
      if (state.route.params.anchor) window.setTimeout(() => document.getElementById(state.route.params.anchor)?.scrollIntoView(), 10);
      return;
    }

    const routedBot = state.route.params.botId;
    if (routedBot && String(routedBot) !== String(state.selectedBotId)) {
      selectBot(routedBot);
    } else if (routedBot) {
      state.selectedBotId = routedBot;
    }
    if (state.route.name === "bot-updates") {
      state.updatesStreamStatus = state.updatesPaused ? "paused" : "connecting";
    }
    render();

    const tasks = [];
    if (state.selectedBotId && ["overview", "bot-overview", "bot-updates", "bot-view", "bot-integration", "bot-settings"].includes(state.route.name)) {
      tasks.push(loadBot(state.selectedBotId, { silent: true }));
    }
    if (["overview", "bot-overview", "bot-integration"].includes(state.route.name) && state.selectedBotId) tasks.push(loadActivity({ silent: true }));
    if (state.route.name === "bot-updates") tasks.push(loadUpdates({ silent: true }));
    if (state.route.name === "bot-view") tasks.push(loadConversations({ silent: true }));
    if (state.route.name === "bot-integration") tasks.push(loadStreamKeys({ silent: true }));
    await Promise.allSettled(tasks);
    render();
    if (state.route.name === "bot-updates" && !state.updatesPaused) startUpdatesStream();
    document.querySelector("#main-content")?.focus({ preventScroll: true });
  }

  function render() {
    if (state.phase === "booting") return;
    if (!state.user) {
      const publicSurface = surface === "landing" || (surface === "combined" && state.route.name !== "auth");
      app.innerHTML = state.route.name === "privacy" && publicSurface ? renderPrivacy() : publicSurface ? renderLanding() : renderAuth();
      document.body.classList.remove("console-open");
    } else {
      app.innerHTML = renderApp();
      document.body.classList.add("console-open");
    }
    renderModal();
  }

  function renderLanding() {
    return `
      <main class="landing" id="main-content" tabindex="-1">
        <header class="landing__header">
          <a class="brand" href="${esc(landingHref())}" aria-label="Phenogram home"><span class="brand-mark" aria-hidden="true"><span></span><span></span><span></span></span><span class="brand__word">Phenogram</span></a>
          <nav class="landing__nav" aria-label="Main navigation">
            <a href="#platform">Platform</a><a href="#workflow">How it works</a><a href="#pricing">Pricing</a><a href="#security">Security</a>
          </nav>
          <div class="landing__actions">
            <a class="btn btn--dark-ghost" href="${esc(appHref("/login"))}">Sign in</a>
            <a class="btn btn--white" href="${esc(appHref("/login"))}">Start free ${icon("arrow")}</a>
          </div>
        </header>

        <section class="hero">
          <div class="hero__copy">
            <div class="hero__badge"><span class="hero__badge-dot"></span>Bot API observability platform</div>
            <h1>Telegram bots,<br><em>finally observable.</em></h1>
            <p class="hero__lead">Keep the Bot API your code already knows. Add durable updates, real-time debugging, safer file access, and an operator view—with one endpoint change.</p>
            <div class="hero__actions">
              <a class="btn btn--white btn--lg" href="${esc(appHref("/login"))}">Connect your first bot ${icon("arrow")}</a>
              <a class="btn btn--dark-ghost btn--lg" href="#workflow">See the 2-minute setup</a>
            </div>
            <p class="hero__note">Free forever for one bot · 30-day update history · No card required</p>
          </div>
          <div class="console-preview" aria-label="Phenogram dashboard preview">
            <div class="console-preview__window">
              <div class="console-preview__bar"><span class="console-preview__dot"></span><span class="console-preview__dot"></span><span class="console-preview__dot"></span><span class="console-preview__url">app.phenogram.io / weather-assistant</span></div>
              <div class="console-preview__body">
                <aside class="console-preview__side"><div class="preview-brand"><span class="brand-mark"><span></span><span></span><span></span></span>Phenogram</div><div class="preview-nav-line active"></div><div class="preview-nav-line"></div><div class="preview-nav-line"></div><div class="preview-nav-line"></div></aside>
                <div class="console-preview__main">
                  <div class="preview-head"><div class="preview-head__identity"><span class="preview-avatar">${icon("bot")}</span><strong>Weather Assistant</strong></div><span class="preview-status">Healthy</span></div>
                  <div class="preview-metrics"><div class="preview-metric"><span>Updates today</span><strong>8,421</strong></div><div class="preview-metric"><span>Delivered</span><strong>99.98%</strong></div><div class="preview-metric"><span>p95 latency</span><strong>42 ms</strong></div></div>
                  <div class="preview-stream"><div class="preview-stream__title"><span>Live updates</span><span class="preview-live">● Listening</span></div><div class="preview-event"><span></span><b>message</b><i></i><span>200 OK</span></div><div class="preview-event"><span></span><b>callback</b><i></i><span>200 OK</span></div><div class="preview-event"><span></span><b>message</b><i></i><span>200 OK</span></div><div class="preview-event"><span></span><b>chat_member</b><i></i><span>200 OK</span></div></div>
                </div>
              </div>
            </div>
            <div class="preview-code"><div class="preview-code__top"><i></i><i></i><i></i></div><span class="blue">const</span> baseURL =<br><span class="green">"https://api.phenogram.io"</span>;<br><br><span class="blue">// Everything else stays the same.</span></div>
          </div>
        </section>

        <div class="trust-strip"><div class="trust-item"><strong>Drop-in compatible</strong> with the Telegram Bot API</div><div class="trust-item"><strong>Encrypted credentials</strong> never shown after setup</div><div class="trust-item"><strong>Built in Rust</strong> for predictable performance</div></div>

        <section class="landing-section landing-section--light" id="platform"><div class="landing-section__inner">
          <div class="section-heading"><p class="eyebrow">One control plane</p><h2>See every update. Understand every failure.</h2><p>Phenogram sits between your bot and Telegram, preserving the API contract while making the invisible parts of production visible.</p></div>
          <div class="feature-grid">
            <article class="feature-card feature-card--wide"><div class="feature-card__icon">${icon("pulse")}</div><h3>Durable update history</h3><p>Search the exact payload your bot received, inspect delivery attempts, and trace failures without reconstructing production from scattered logs.</p><div class="feature-card__visual"><div class="mini-event"><span class="mini-event__dot"></span><strong>message</strong><code>update_914022</code><time>12 ms</time></div><div class="mini-event"><span class="mini-event__dot mini-event__dot--violet"></span><strong>callback_query</strong><code>update_914021</code><time>18 ms</time></div></div></article>
            <article class="feature-card"><div class="feature-card__icon feature-card__icon--mint">${icon("message")}</div><h3>Bot View</h3><p>Experience conversations as your bot does. Inspect context and safely reply as the bot from one operator console.</p></article>
            <article class="feature-card"><div class="feature-card__icon feature-card__icon--violet">${icon("link")}</div><h3>Flexible delivery</h3><p>Start with a reliable live stream and add new delivery models as your architecture grows.</p></article>
            <article class="feature-card feature-card--wide"><div class="feature-card__icon">${icon("shield")}</div><h3>Share without leaking tokens</h3><p>Use scoped, expiring public references for downloads and event access. Bot credentials stay encrypted and out of URLs, logs, and browser history.</p><div class="feature-card__visual feature-card__visual--code mono">/public/<span class="text-primary">phg_a8c2…</span>/files/report.pdf?expires=…&amp;sig=…</div></article>
          </div>
        </div></section>

        <section class="landing-section landing-section--dark" id="workflow"><div class="landing-section__inner">
          <div class="section-heading"><p class="eyebrow">Two-minute migration</p><h2>Your bot code stays yours.</h2><p>Prove ownership with the BotFather token, point your client at Phenogram, and watch the first update arrive.</p></div>
          <div class="steps"><article class="step"><h3>Connect securely</h3><p>We verify the token with Telegram, show you the bot identity, then encrypt the credential. It cannot be viewed again.</p></article><article class="step"><h3>Change one host</h3><p>Replace api.telegram.org with api.phenogram.io. Methods, payloads, and responses remain familiar.</p></article><article class="step"><h3>Ship with context</h3><p>Use the dashboard to follow API calls, updates, conversations, and delivery health in real time.</p></article></div>
          <div class="code-slab"><span class="prompt">$</span> export TELEGRAM_API_BASE=<span class="host">https://api.phenogram.io</span><br><span class="code-slab__comment"># Keep your bot token in the server environment—never in frontend code.</span></div>
        </div></section>

        <section class="landing-section landing-section--white" id="pricing"><div class="landing-section__inner">
          <div class="section-heading"><p class="eyebrow">Simple plans</p><h2>Start free. Keep more history as you grow.</h2><p>Every plan includes the compatible API gateway, durable updates, Bot View, and safe public references.</p></div>
          <div class="pricing-grid">
            ${landingPrice("Free", "$0", "For personal bots and evaluation.", ["1 connected bot", "30-day log retention", "Core developer console"], false)}
            ${landingPrice("Pro", "$29", "For production developers and growing bots.", ["5 connected bots", "90-day log retention", "Local Bot API routing"], true)}
            ${landingPrice("Scale", "$99", "For teams operating a bot portfolio.", ["25 connected bots", "365-day log retention", "Local Bot API routing"], false)}
          </div>
        </div></section>

        <section class="landing-section landing-section--dark" id="security"><div class="landing-section__inner security-band">
          <div class="security-orbit">${icon("lock")}</div>
          <div class="security-copy"><p class="eyebrow">Designed for sensitive credentials</p><h2>Your bot token stays private.</h2><p>Phenogram verifies a bot server-side, encrypts the token at rest, and redacts credentials from request history. Public bot keys identify; they never authorize API calls.</p><div class="security-list"><div>${icon("check")}Encrypted token storage</div><div>${icon("check")}Expiring, scoped file links</div><div>${icon("check")}Audited operator actions</div><div>${icon("check")}Explicit bot deletion controls</div></div></div>
        </div></section>

        <section class="final-cta"><h2>Make your next bot easier to operate.</h2><p>Connect one bot for free and see what it sees—without rewriting the integration you already trust.</p><a class="btn btn--primary btn--lg" href="${esc(appHref("/login"))}">Start with one free bot ${icon("arrow")}</a></section>
        <footer class="landing-footer"><div class="landing-footer__inner"><a class="brand" href="${esc(landingHref())}"><span class="brand-mark"><span></span><span></span><span></span></span>Phenogram</a><div class="landing-footer__links"><a href="#platform">Platform</a><a href="#pricing">Pricing</a><a href="#security">Security</a><a href="${esc(privacyHref())}">Privacy</a><a href="https://github.com/phenogram/platform" target="_blank" rel="noreferrer">Source</a></div><div class="landing-footer__note">Independent software. Not affiliated with Telegram.</div></div></footer>
      </main>`;
  }

  function renderPrivacy() {
    return `<main class="privacy-page" id="main-content" tabindex="-1">
      <header class="privacy-header"><a class="brand" href="${esc(landingHref())}" aria-label="Phenogram home"><span class="brand-mark" aria-hidden="true"><span></span><span></span><span></span></span><span class="brand__word">Phenogram</span></a><a class="btn btn--dark-ghost" href="${esc(landingHref())}">${icon("arrow")} Back to Phenogram</a></header>
      <article class="privacy-document">
        <p class="eyebrow">Privacy</p><h1>Minimal identity, by design.</h1><p class="privacy-lead">Phenogram uses Google or GitHub only to recognize your account. We do not request, receive, or store your email address.</p>
        <section><h2>Identity data we use</h2><p>When you choose social sign-in, we store the provider name, its stable account identifier, and the public profile fields needed to show your identity in the console: display name, username, and avatar URL. Your provider password is never shared with Phenogram.</p></section>
        <section><h2>Why we use it</h2><p>The stable provider identifier lets us return you to the same Phenogram workspace. Public profile fields make that account recognizable to you. We do not use this data for advertising or sell it.</p></section>
        <section><h2>Operational data</h2><p>Phenogram stores the bot configuration, updates, API activity, and operator actions needed to provide the platform. Bot tokens are encrypted at rest. Retention depends on the selected membership plan.</p></section>
        <section><h2>Deletion</h2><p>You can delete individual bots and their Phenogram data from the console. Account deletion requests are handled through the public project.</p></section>
        <section><h2>Open development</h2><p>Phenogram is developed publicly. Privacy questions and account-deletion requests can be submitted through the <a href="https://github.com/phenogram/platform/issues" target="_blank" rel="noreferrer">Phenogram platform issue tracker</a>. Do not include bot tokens or other secrets in a public issue.</p></section>
        <p class="privacy-updated">Effective August 13, 2026</p>
      </article>
    </main>`;
  }

  function landingPrice(name, price, copy, features, featured) {
    return `<article class="price-card ${featured ? "price-card--featured" : ""}">${featured ? '<span class="price-card__tag">Most popular</span>' : ""}<h3>${esc(name)}</h3><div class="price-card__price">${esc(price)}${price !== "$0" ? "<span> / month</span>" : ""}</div><p>${esc(copy)}</p><ul class="price-list">${features.map((feature) => `<li>${icon("check")}${esc(feature)}</li>`).join("")}</ul><a class="btn ${featured ? "btn--primary" : "btn--secondary"} btn--block" href="${esc(appHref("/login"))}">Start free</a></article>`;
  }

  function renderAuth() {
    return `<main class="auth-screen" id="main-content" tabindex="-1">
      <section class="auth-panel">
        <a class="brand" href="${esc(landingHref())}"><span class="brand-mark brand-mark--primary"><span></span><span></span><span></span></span><span class="brand__word">Phenogram</span></a>
        <div class="auth-panel__body">
          <p class="eyebrow">Phenogram console</p>
          <h1>Manage your Telegram bots.</h1>
          <p class="auth-panel__lead">Inspect updates, trace deliveries, and reply to users from Bot View.</p>
          ${state.authError || state.errors.session ? `<div class="auth-error" role="alert">${icon("alert")}<span>${esc(state.authError || state.errors.session)}</span></div>` : ""}
          <div class="oauth-stack" role="group" aria-label="Social sign-in options">
            <a class="oauth-button oauth-button--google" href="${API}/auth/oauth/google/start"><svg class="social-icon" aria-hidden="true"><use href="#i-google"></use></svg><span>Continue with Google</span></a>
            <a class="oauth-button oauth-button--github" href="${API}/auth/oauth/github/start"><svg class="social-icon" aria-hidden="true"><use href="#i-github"></use></svg><span>Continue with GitHub</span></a>
          </div>
          <p class="auth-fineprint">New to Phenogram? Your free workspace is created automatically.</p>
          <p class="auth-policy"><a href="${esc(privacyHref())}">Privacy policy</a></p>
        </div>
      </section>
      <aside class="auth-side" aria-hidden="true"><div class="auth-side__content"><p class="eyebrow">Bot operations, made legible</p><h2>Every update has a story.</h2><p>Phenogram keeps the payload, delivery path, API activity, and conversation context together—so production debugging starts with evidence.</p><div class="auth-quote"><span class="auth-quote__event">update.message</span> received<br><span class="auth-quote__delivery">delivery.webhook</span> 200 OK · 36 ms<br><span class="auth-quote__trace">trace</span> phg_01J8F9…</div></div></aside>
    </main>`;
  }

  const descendantCount = (node) => node.children.reduce((count, child) => count + 1 + descendantCount(child), 0);

  function renderSidebarBotNode(node) {
    const bot = node.bot;
    const id = botId(bot);
    const selected = id === String(state.selectedBotId);
    const current = selected && state.route.name.startsWith("bot-");
    const managed = isManagedBot(bot);
    const warning = botNeedsRetentionWarning(bot);
    const descendants = descendantCount(node);
    const meta = managed
      ? warning ? "24-hour history" : retentionLabel(bot)
      : descendants ? `${descendants} managed bot${descendants === 1 ? "" : "s"}` : "Connected bot";
    return `<li class="sidebar-bot-node ${managed ? "is-managed" : "is-connected"}"><a class="sidebar-bot ${selected ? "active" : ""} ${warning ? "has-warning" : ""}" href="${botPath(id, "overview")}"${current ? ' aria-current="page"' : ""}><span class="sidebar-bot__marker">${icon("bot")}</span><span class="sidebar-bot__copy"><strong>${esc(botName(bot))}</strong><span>${esc(meta)}</span></span>${warning ? '<span class="sidebar-bot__warning" aria-label="24-hour history">24h</span>' : ""}</a>${node.children.length ? `<ul>${node.children.map(renderSidebarBotNode).join("")}</ul>` : ""}</li>`;
  }

  function renderSidebarBotTree() {
    const { roots, orphans } = botHierarchy();
    if (!roots.length && !orphans.length) return "";
    return `<nav class="sidebar-bot-scroll" aria-label="Bot hierarchy">${roots.length ? `<ul class="sidebar-bot-tree">${roots.map(renderSidebarBotNode).join("")}</ul>` : ""}${orphans.length ? `<div class="sidebar-orphans"><p>${icon("alert")}Manager not connected</p><ul class="sidebar-bot-tree">${orphans.map(renderSidebarBotNode).join("")}</ul></div>` : ""}</nav>`;
  }

  function renderPickerBotNode(node) {
    const bot = node.bot;
    const id = botId(bot);
    const selected = id === String(state.selectedBotId);
    const managed = isManagedBot(bot);
    const warning = botNeedsRetentionWarning(bot);
    const descendants = descendantCount(node);
    const meta = managed
      ? `Managed by ${managerLabel(bot)} · ${retentionLabel(bot)}`
      : `${botUsername(bot)}${descendants ? ` · ${descendants} managed bot${descendants === 1 ? "" : "s"}` : ""}`;
    return `<li><button class="bot-picker-item ${managed ? "is-managed" : ""} ${selected ? "active" : ""} ${warning ? "has-warning" : ""}" type="button" data-action="pick-bot" data-bot-id="${esc(id)}" aria-pressed="${selected ? "true" : "false"}"><span class="bot-avatar">${initials(botName(bot))}</span><span class="bot-picker-item__copy"><span><strong>${esc(botName(bot))}</strong>${managed ? '<em>Managed</em>' : ""}</span><small>${esc(meta)}</small></span>${warning ? '<span class="badge badge--warning">24-hour history</span>' : selected ? icon("check") : icon("chevron")}</button>${node.children.length ? `<ul>${node.children.map(renderPickerBotNode).join("")}</ul>` : ""}</li>`;
  }

  function renderBotPickerTree() {
    const { roots, orphans } = botHierarchy();
    if (!roots.length && !orphans.length) return '<div class="empty-state empty-state--modal"><p>No bots connected yet.</p></div>';
    return `${roots.length ? `<section class="bot-picker-group" aria-labelledby="connected-bots-label"><h3 id="connected-bots-label">Connected managers and their bots</h3><ul class="bot-picker-tree">${roots.map(renderPickerBotNode).join("")}</ul></section>` : ""}${orphans.length ? `<section class="bot-picker-group bot-picker-group--warning" aria-labelledby="orphan-bots-label"><div class="bot-picker-group__warning"><span>${icon("alert")}</span><div><h3 id="orphan-bots-label">Manager not connected</h3><p>These bot families have no connected root manager. History coverage is shown for each bot.</p></div></div><ul class="bot-picker-tree">${orphans.map(renderPickerBotNode).join("")}</ul></section>` : ""}`;
  }

  function renderRetentionWarning(bot, { compact = false } = {}) {
    if (!botNeedsRetentionWarning(bot)) return "";
    const warning = botRetentionWarning(bot);
    const coverage = coverageStats();
    const title = warning === "manager_missing" ? "Manager not connected · 24-hour history"
      : warning === "plan_limit" ? "Outside full-history coverage"
        : "24-hour history for this managed bot";
    const copy = warning === "manager_missing"
      ? "This managed bot does not have a connected manager in this workspace, so only the last 24 hours are kept."
      : warning === "plan_limit"
        ? `Your plan covers ${coverage.covered} of ${coverage.total} bots with full history. Only the last 24 hours are kept for this bot.`
        : warning === "free_plan"
          ? "The Free plan keeps 24 hours of history for managed bots. Connected bots keep the plan’s full history."
          : "Only the last 24 hours of updates and conversations are kept for this managed bot.";
    return `<div class="status-banner retention-warning ${compact ? "retention-warning--compact" : ""}" role="status" aria-label="Managed bot retention warning">${icon("alert")}<div class="status-banner__copy"><strong>${esc(title)}</strong>${esc(copy)}</div></div>`;
  }

  function renderCoverageSummary({ compact = false } = {}) {
    const coverage = coverageStats();
    const percentage = coverage.total ? Math.round((coverage.covered / coverage.total) * 100) : 0;
    const uncoveredCopy = coverage.uncovered
      ? `${coverage.uncovered} managed bot${coverage.uncovered === 1 ? " keeps" : "s keep"} only 24 hours of history.`
      : "Every bot keeps its full plan history.";
    return `<section class="coverage-summary ${compact ? "coverage-summary--compact" : ""} ${coverage.uncovered ? "has-warning" : ""}" aria-label="Bot history coverage"><div class="coverage-summary__copy"><span class="stat-label">Full-history coverage</span><strong>${coverage.covered} of ${coverage.total} bots</strong><p>${esc(uncoveredCopy)} ${esc(membershipPlan())} includes full history for up to ${coverage.limit} bot${coverage.limit === 1 ? "" : "s"}.</p><progress class="progress" value="${percentage}" max="100" aria-label="${coverage.covered} of ${coverage.total} bots have full history">${percentage}%</progress></div><dl class="coverage-summary__stats"><div><dt>Covered</dt><dd>${coverage.covered}</dd></div><div class="${coverage.uncovered ? "is-warning" : ""}"><dt>24-hour</dt><dd>${coverage.uncovered}</dd></div><div><dt>Total</dt><dd>${coverage.total}</dd></div></dl></section>`;
  }

  function renderPortfolioBotNode(node) {
    const bot = node.bot;
    const managed = isManagedBot(bot);
    const warning = botNeedsRetentionWarning(bot);
    return `<li><a class="portfolio-bot ${managed ? "is-managed" : ""} ${warning ? "has-warning" : ""}" href="${botPath(botId(bot), "overview")}"><span class="portfolio-bot__marker">${icon("bot")}</span><span class="portfolio-bot__copy"><strong>${esc(botName(bot))}</strong><span>${esc(managed ? `Managed by ${managerLabel(bot)}` : botUsername(bot))}</span></span><span class="portfolio-bot__retention ${warning ? "is-warning" : ""}">${esc(retentionLabel(bot))}</span></a>${node.children.length ? `<ul>${node.children.map(renderPortfolioBotNode).join("")}</ul>` : ""}</li>`;
  }

  function renderPortfolioPanel() {
    const { roots, orphans } = botHierarchy();
    return `<section class="panel portfolio-panel"><div class="panel__head"><div><h2>Bot portfolio</h2><p>Managed bots appear automatically beneath their manager.</p></div><a class="btn btn--ghost btn--sm" href="#/bots">View all ${icon("chevron")}</a></div><div class="panel__body">${renderCoverageSummary({ compact: true })}<div class="portfolio-tree">${roots.length ? `<ul>${roots.map(renderPortfolioBotNode).join("")}</ul>` : ""}${orphans.length ? `<div class="portfolio-orphans"><p>${icon("alert")}Manager not connected</p><ul>${orphans.map(renderPortfolioBotNode).join("")}</ul></div>` : ""}</div></div></section>`;
  }

  function renderManagedBotNode(node) {
    const bot = node.bot;
    const warning = botNeedsRetentionWarning(bot);
    return `<li><a class="managed-bot-row ${warning ? "has-warning" : ""}" href="${botPath(botId(bot), "overview")}"><span class="bot-avatar">${initials(botName(bot))}</span><span class="managed-bot-row__copy"><span><strong>${esc(botName(bot))}</strong><em>Managed</em></span><small>${esc(botUsername(bot))} · Managed by ${esc(managerLabel(bot))}</small></span><span class="managed-bot-row__state">${warning ? '<span class="badge badge--warning">24-hour history</span>' : `<span>${esc(retentionLabel(bot))}</span>`}${renderBotStatusBadge(bot)}</span><span class="managed-bot-row__arrow">${icon("chevron")}</span></a>${node.children.length ? `<ul>${node.children.map(renderManagedBotNode).join("")}</ul>` : ""}</li>`;
  }

  function renderBotFamily(node) {
    const bot = node.bot;
    const descendants = descendantCount(node);
    return `<article class="bot-family"><div class="bot-family__eyebrow">${descendants ? "Connected manager" : "Connected bot"}</div>${renderBotCard(bot, descendants)}${node.children.length ? `<div class="bot-family__children"><div class="bot-family__children-head"><strong>Managed bots</strong><span>${descendants} total</span></div><ul>${node.children.map(renderManagedBotNode).join("")}</ul></div>` : '<div class="bot-family__empty">Managed bots will appear here automatically.</div>'}</article>`;
  }

  function renderBotFamilies() {
    const { roots, orphans } = botHierarchy();
    return `<div class="bot-families">${roots.map(renderBotFamily).join("")}${orphans.length ? `<section class="orphan-bot-family" aria-labelledby="orphan-family-title"><header><span class="orphan-bot-family__icon">${icon("alert")}</span><div><h2 id="orphan-family-title">Manager not connected</h2><p>These bot families have no connected root manager. Each bot below shows its own history coverage.</p></div><span class="badge badge--warning">${orphans.reduce((count, node) => count + 1 + descendantCount(node), 0)} bots</span></header><ul>${orphans.map(renderManagedBotNode).join("")}</ul></section>` : ""}</div>`;
  }

  function renderApp() {
    const bot = currentBot();
    const routeName = state.route.name;
    const botCrumb = bot ? botAncestorChain(bot).map((item) => botUsername(item) === "Telegram bot" ? botName(item) : botUsername(item)).join(" › ") : "";
    const titleMap = {
      overview: "Overview",
      bots: "Bots",
      "bot-overview": botName(bot),
      "bot-updates": "Update log",
      "bot-view": "Bot View",
      "bot-integration": "Delivery & API",
      "bot-settings": "Bot settings",
      billing: "Usage & billing",
      settings: "Account settings",
    };
    const health = String(state.health?.status || "").toLowerCase();
    const healthClass = ["ok", "healthy", "ready"].includes(health) ? "is-healthy" : health === "down" ? "is-down" : "";
    return `<div class="app-shell ${state.mobileMenu ? "is-menu-open" : ""}">
      ${renderSidebar(bot)}
      <button class="mobile-overlay" type="button" data-action="close-menu" aria-label="Close navigation" aria-hidden="${state.mobileMenu ? "false" : "true"}" tabindex="${state.mobileMenu ? "0" : "-1"}"></button>
      <div class="app-main">
        <header class="topbar">
          <div class="topbar__left"><button class="btn btn--ghost btn--icon mobile-menu-btn" type="button" data-action="toggle-menu" aria-label="${state.mobileMenu ? "Close" : "Open"} navigation" aria-controls="app-sidebar" aria-expanded="${state.mobileMenu ? "true" : "false"}">${icon(state.mobileMenu ? "close" : "menu")}</button><span class="topbar__title">${esc(titleMap[routeName] || "Phenogram")}</span>${bot && routeName.startsWith("bot-") ? `<span class="topbar__crumb">${esc(botCrumb)}</span>` : ""}</div>
          <div class="topbar__actions"><span class="health-pill ${healthClass}">${healthClass === "is-down" ? "API issue" : "Platform online"}</span><button class="btn btn--secondary btn--sm" type="button" data-action="open-connect">${icon("plus")}<span>Connect bot</span></button></div>
        </header>
        <main id="main-content" tabindex="-1">${renderMain()}</main>
      </div>
      ${renderMobileNav()}
      ${renderDrawer()}
    </div>`;
  }

  function renderSidebar(bot) {
    const route = state.route.name;
    const routeActive = (...names) => names.includes(route) ? "active" : "";
    const identity = userDisplayName();
    const switcherMeta = bot
      ? isManagedBot(bot) ? `Managed by ${managerLabel(bot)}${botNeedsRetentionWarning(bot) ? " · 24-hour history" : ""}` : botUsername(bot)
      : "Connect your first bot";
    return `<aside class="sidebar" id="app-sidebar" aria-label="Application navigation">
      <a class="brand sidebar__brand" href="#/overview"><span class="brand-mark"><span></span><span></span><span></span></span><span class="brand__word">Phenogram</span></a>
      <button class="bot-switcher ${bot && botNeedsRetentionWarning(bot) ? "has-warning" : ""}" type="button" data-action="open-bot-picker" aria-label="Switch bot${bot ? `, current bot ${esc(botName(bot))}` : ""}">
        <span class="bot-avatar">${bot ? initials(botName(bot)) : icon("bot")}</span>
        <span class="bot-switcher__text"><strong>${esc(bot ? botName(bot) : "No bot selected")}</strong><span>${esc(switcherMeta)}</span></span>${icon("chevron")}
      </button>
      <p class="sidebar__section-label">Workspace</p>
      <nav class="side-nav"><a class="${routeActive("overview")}" href="#/overview">${icon("grid")}Overview</a><a class="${routeActive("bots")}" href="#/bots">${icon("bot")}Bots</a></nav>
      ${state.bots.length ? `<p class="sidebar__section-label sidebar__section-label--bots">Bot hierarchy</p>${renderSidebarBotTree()}` : ""}
      ${bot ? `<p class="sidebar__section-label">Selected bot</p><nav class="side-nav"><a class="${routeActive("bot-overview")}" href="${botPath(botId(bot), "overview")}">${icon("pulse")}Health & activity</a><a class="${routeActive("bot-view")}" href="${botPath(botId(bot), "view")}">${icon("message")}Bot View</a><a class="${routeActive("bot-updates")}" href="${botPath(botId(bot), "updates")}">${icon("terminal")}Update log</a><a class="${routeActive("bot-integration")}" href="${botPath(botId(bot), "integration")}">${icon("link")}Delivery & API</a></nav>` : ""}
      <p class="sidebar__section-label">Manage</p>
      <nav class="side-nav"><a class="${routeActive("billing")}" href="#/billing">${icon("card")}Usage & billing</a><a class="${routeActive("settings", "bot-settings")}" href="#/settings">${icon("settings")}Settings</a></nav>
      <div class="sidebar__footer"><button class="account-chip" type="button" data-action="logout"><span class="account-chip__avatar">${initials(identity)}</span><span class="account-chip__copy"><strong>${esc(identity)}</strong><span>${esc(userProviderLabel())} · ${esc(membershipPlan())} plan</span></span>${icon("logout")}</button></div>
    </aside>`;
  }

  function renderMobileNav() {
    const bot = currentBot();
    const route = state.route.name;
    const item = (href, name, label, active) => `<a href="${href}" class="${active ? "active" : ""}"${active ? ' aria-current="page"' : ""}>${icon(name)}<span>${label}</span></a>`;
    return `<nav class="mobile-nav" aria-label="Mobile navigation">
      ${item("#/overview", "grid", "Overview", ["overview", "bot-overview"].includes(route))}
      ${item("#/bots", "bot", "Bots", route === "bots")}
      ${item(bot ? botPath(botId(bot), "updates") : "#/bots", "pulse", "Updates", route === "bot-updates")}
      ${item(bot ? botPath(botId(bot), "view") : "#/bots", "message", "Bot View", route === "bot-view")}
      ${item("#/settings", "more", "More", ["settings", "billing", "bot-settings", "bot-integration"].includes(route))}
    </nav>`;
  }

  function renderMain() {
    let content;
    switch (state.route.name) {
      case "overview": content = renderOverview(); break;
      case "bots": content = renderBots(); break;
      case "bot-overview": content = renderBotOverview(); break;
      case "bot-updates": content = renderUpdates(); break;
      case "bot-view": content = renderBotView(); break;
      case "bot-integration": content = renderIntegration(); break;
      case "bot-settings": content = renderBotSettings(); break;
      case "billing": content = renderBilling(); break;
      case "settings": content = renderSettings(); break;
      default: content = renderOverview();
    }
    const bot = currentBot();
    return state.route.name.startsWith("bot-") && botNeedsRetentionWarning(bot)
      ? `<div class="bot-route-alert">${renderRetentionWarning(bot)}</div>${content}`
      : content;
  }

  function pageHeader(title, copy, actions = "") {
    return `<header class="page-header"><div class="page-header__copy"><h1>${esc(title)}</h1>${copy ? `<p>${esc(copy)}</p>` : ""}</div>${actions ? `<div class="page-header__actions">${actions}</div>` : ""}</header>`;
  }

  function renderRouteError(message, action = "retry-route") {
    return `<div class="panel"><div class="empty-state"><span class="empty-state__icon empty-state__icon--danger">${icon("alert")}</span><h2>We couldn't load this view</h2><p>${esc(message)}</p><button class="btn btn--secondary" type="button" data-action="${action}">${icon("refresh")}Try again</button></div></div>`;
  }

  function renderNoBots(copy = "Connect a Telegram bot to start receiving updates and API activity.") {
    return `<div class="panel"><div class="empty-state"><span class="empty-state__icon">${icon("bot")}</span><h2>Connect your first bot</h2><p>${esc(copy)}</p><button class="btn btn--primary" type="button" data-action="open-connect">${icon("plus")}Connect bot</button></div></div>`;
  }

  function renderOverview() {
    const bot = currentBot();
    if (state.errors.bots && !state.bots.length) return `<div class="page">${pageHeader("Overview", "Health, activity, and usage across your bot workspace.")}${renderRouteError(state.errors.bots)}</div>`;
    if (!bot) return `<div class="page">${pageHeader("Welcome to Phenogram", "Your bot operations workspace is ready.")}${renderNoBots()}</div>`;
    return `<div class="page">
      ${pageHeader("Good to see you.", `Here’s what ${botName(bot)} has seen in the last 24 hours.`, `<a class="btn btn--secondary" href="${botPath(botId(bot), "updates")}">${icon("pulse")}Live updates</a><a class="btn btn--primary" href="${botPath(botId(bot), "view")}">${icon("message")}Open Bot View</a>`)}
      ${renderRetentionWarning(bot)}
      ${renderHealthHero(bot)}
      ${renderMetrics(bot)}
      ${renderPortfolioPanel()}
      <div class="dashboard-grid">
        <section class="panel"><div class="panel__head"><div><h2>Recent activity</h2><p>API requests and update events for ${esc(botName(bot))}</p></div><a class="btn btn--ghost btn--sm" href="${botPath(botId(bot), "integration")}">API details ${icon("chevron")}</a></div>${renderActivityList()}</section>
        <div class="panel-stack"><section class="panel"><div class="panel__head"><div><h2>Setup</h2><p>Your production readiness checklist</p></div></div><div class="panel__body">${renderChecklist(bot)}</div></section><section class="panel"><div class="panel__head"><div><h2>Plan usage</h2><p>${esc(membershipPlan())} · ${esc(retentionLabel(bot))} for this bot</p></div><a class="btn btn--ghost btn--sm" href="#/billing">Manage</a></div><div class="panel__body">${renderUsage(bot)}</div></section></div>
      </div>
    </div>`;
  }

  function renderHealthHero(bot) {
    const status = botStatus(bot);
    const bad = ["invalid", "token_invalid", "error", "disabled", "failed"].includes(status);
    const provisioning = ["provisioning", "setup", "pending"].includes(status);
    const degraded = ["degraded", "warning"].includes(status);
    const unknown = !bad && !provisioning && !degraded && !["active", "healthy", "ready", "ok"].includes(status);
    const warning = provisioning || degraded || unknown;
    const title = bad ? "This bot needs attention" : degraded ? "This bot is degraded" : provisioning ? "Webhook provisioning is in progress" : unknown ? "Bot status is unavailable" : "Bot is healthy";
    const copy = bad ? "Verify the bot token and review recent API activity." : degraded ? "The bot is connected, but an upstream setup step failed. Review its recent activity." : provisioning ? "Phenogram is registering its upstream webhook. Activity will appear when Telegram starts delivering updates." : unknown ? "Refresh this workspace before assuming the bot is ready." : "Phenogram is receiving and processing activity normally.";
    const lastUpdate = bot.last_update_at || bot.last_update || bot.latest_update_at;
    const lastApi = bot.last_api_call_at || bot.last_api_request_at || bot.last_request_at || bot.latest_activity_at;
    return `<section class="health-hero ${bad ? "is-error" : warning ? "is-warning" : ""}"><span class="health-hero__icon">${icon(bad ? "alert" : warning ? "clock" : "check")}</span><div class="health-hero__copy"><h2>${title}</h2><p>${copy}</p></div><div class="health-hero__meta"><div><span>Last update</span><strong>${esc(relativeTime(lastUpdate))}</strong></div><div><span>Last API call</span><strong>${esc(relativeTime(lastApi))}</strong></div><div><span>Retention</span><strong>${esc(retentionValue(bot))}</strong></div></div></section>`;
  }

  function renderMetrics(bot) {
    const cards = [
      ["pulse", "Updates", bot.updates_24h ?? bot.updates_count_24h ?? bot.metrics?.updates_24h, "last 24 hours"],
      ["terminal", "API requests", bot.api_calls_24h ?? bot.api_requests_24h ?? bot.requests_24h ?? bot.metrics?.api_requests_24h, "last 24 hours"],
      ["check", "Delivery success", percent(bot.delivery_success_rate ?? bot.metrics?.delivery_success_rate), "successful deliveries"],
      ["zap", "Avg. API latency", milliseconds(bot.average_api_latency_ms ?? bot.p95_latency_ms ?? bot.metrics?.p95_latency_ms), "last 24 hours"],
    ];
    return `<section class="metrics-grid">${cards.map(([symbol, label, value, detail]) => `<article class="metric-card"><div class="metric-card__top"><span class="stat-label">${label}</span><span class="metric-card__icon">${icon(symbol)}</span></div><div class="metric-card__value">${esc(value ?? "—")}</div><div class="metric-card__detail">${detail}</div></article>`).join("")}</section>`;
  }

  const percent = (value) => {
    if (value == null || value === "") return "—";
    const number = Number(value);
    if (!Number.isFinite(number)) return String(value);
    return `${number <= 1 ? (number * 100).toFixed(2) : number.toFixed(2)}%`;
  };
  const milliseconds = (value) => value == null || value === "" ? "—" : `${formatNumber(value)} ms`;

  function renderActivityList(limit = 7) {
    if (state.loading.activity) return `<div class="panel__body skeleton-stack"><div class="skeleton skeleton--activity"></div><div class="skeleton skeleton--activity"></div><div class="skeleton skeleton--activity"></div></div>`;
    if (state.errors.activity) return `<div class="empty-state empty-state--compact"><span class="empty-state__icon empty-state__icon--danger">${icon("alert")}</span><h3>Activity unavailable</h3><p>${esc(state.errors.activity)}</p></div>`;
    if (!state.activity.length) return `<div class="empty-state empty-state--waiting"><span class="empty-state__icon">${icon("pulse")}</span><h3>Waiting for the first request</h3><p>Point your bot client at Phenogram. API requests and update events will appear here.</p></div>`;
    return `<div class="activity-list">${state.activity.slice(0, limit).map(renderActivityRow).join("")}</div>`;
  }

  function renderActivityRow(item) {
    const kind = item.type || item.kind || item.event || (item.method ? "api_request" : "update");
    const method = item.method || item.name || item.update_type || kind;
    const status = item.http_status || item.status_code || item.status || item.response_status;
    const identifier = item.trace_id || item.request_id || item.update_id || item.id || "";
    const timestamp = item.created_at || item.timestamp || item.received_at;
    return `<div class="activity-row"><span class="activity-row__icon">${icon(String(kind).includes("update") ? "pulse" : "terminal")}</span><div class="activity-row__copy"><strong>${esc(method)}</strong><span>${esc(identifier ? String(identifier) : status ? `status ${status}` : kind)}</span></div><time datetime="${esc(asDate(timestamp)?.toISOString() || "")}">${esc(relativeTime(timestamp))}</time></div>`;
  }

  function renderChecklist(bot) {
    const tokenOkay = bot.token_valid !== false && !["token_invalid", "invalid"].includes(botStatus(bot));
    const sawRequest = Boolean(bot.last_api_call_at || bot.last_api_request_at || bot.last_request_at || bot.api_calls_24h || bot.api_requests_24h || state.activity.length);
    const sawUpdate = Boolean(bot.last_update_at || bot.last_update || bot.updates_24h || bot.updates_count_24h);
    const items = [[tokenOkay, "Bot ownership verified"], [sawRequest, "First API request received"], [sawUpdate, "First Telegram update received"]];
    return `<div class="checklist">${items.map(([done, label]) => `<div class="check-item ${done ? "is-complete" : ""}"><span class="check-item__mark">${done ? icon("check") : ""}</span>${esc(label)}</div>`).join("")}</div>`;
  }

  function renderUsage(bot) {
    const coverage = coverageStats();
    const coveragePercentage = coverage.total ? Math.round((coverage.covered / coverage.total) * 100) : 0;
    const retentionPercentage = Math.min(100, botRetentionDays(bot) / 3.65);
    return `<div class="usage-card ${coverage.uncovered ? "has-warning" : ""}"><div class="usage-card__row"><span>Full-history coverage</span><strong>${coverage.covered} of ${coverage.total}</strong></div><progress class="progress" value="${coveragePercentage}" max="100" aria-label="${coverage.covered} of ${coverage.total} bots have full history">${coveragePercentage}%</progress></div><div class="usage-card ${botNeedsRetentionWarning(bot) ? "has-warning" : ""}"><div class="usage-card__row"><span>This bot’s history</span><strong>${esc(retentionValue(bot))}</strong></div><progress class="progress" value="${retentionPercentage}" max="100" aria-label="History retained for ${esc(retentionValue(bot))}">${Math.round(retentionPercentage)}%</progress></div>`;
  }

  function renderBots() {
    const coverage = coverageStats();
    const actions = `<span class="muted page-header__usage">${coverage.covered} full history · ${coverage.total} total</span><button class="btn btn--primary" type="button" data-action="open-connect">${icon("plus")}Connect bot</button>`;
    if (state.loading.bots) return `<div class="page">${pageHeader("Bots", "Connected managers and every bot they manage, in one place.", actions)}<div class="bots-grid"><div class="skeleton-card"></div><div class="skeleton-card"></div><div class="skeleton-card"></div></div></div>`;
    if (state.errors.bots && !state.bots.length) return `<div class="page">${pageHeader("Bots", "Connected managers and every bot they manage, in one place.", actions)}${renderRouteError(state.errors.bots)}</div>`;
    return `<div class="page">${pageHeader("Bots", "Managed bots appear beneath their manager automatically. Open any bot to inspect it or reply from Bot View.", actions)}${state.bots.length ? `${renderCoverageSummary()}${renderBotFamilies()}` : renderNoBots()}</div>`;
  }

  function renderBotCard(bot, managedCount = 0) {
    const id = botId(bot);
    return `<a class="bot-card bot-family__root" href="${botPath(id, "overview")}"><div class="bot-card__top"><span class="bot-avatar bot-avatar--lg">${initials(botName(bot))}</span><span class="bot-card__copy"><strong>${esc(botName(bot))}</strong><span>${esc(botUsername(bot))}</span></span>${renderBotStatusBadge(bot)}</div><div class="bot-card__meta"><div><span class="stat-label">Last update</span><strong>${esc(relativeTime(bot.last_update_at || bot.last_update))}</strong></div><div><span class="stat-label">Managed bots</span><strong>${managedCount}</strong></div><div><span class="stat-label">Retention</span><strong>${esc(retentionValue(bot))}</strong></div></div><div class="bot-card__foot"><span>Connected bot</span><span>Open workspace ${icon("arrow")}</span></div></a>`;
  }

  function renderBotOverview() {
    const bot = currentBot();
    if (state.loading.bot && !bot) return `<div class="page"><div class="skeleton skeleton--page-title"></div><div class="skeleton-card"></div></div>`;
    if (state.errors.bot) return `<div class="page">${pageHeader("Bot workspace", "")}${renderRouteError(state.errors.bot)}</div>`;
    if (!bot) return `<div class="page">${renderNoBots("This bot could not be found. Choose another bot or connect a new one.")}</div>`;
    const relationship = isManagedBot(bot) ? `${botUsername(bot)} · Managed by ${managerLabel(bot)}` : `${botUsername(bot)} · Connected bot`;
    return `<div class="page">${pageHeader(botName(bot), `${relationship} · Bot health, API activity, and update delivery.`, `<a class="btn btn--secondary" href="${botPath(botId(bot), "settings")}">${icon("settings")}Settings</a><a class="btn btn--primary" href="${botPath(botId(bot), "view")}">${icon("message")}Open Bot View</a>`)}${renderHealthHero(bot)}${renderMetrics(bot)}<section class="panel"><div class="panel__head"><div><h2>API activity</h2><p>Most recent proxied calls and received updates</p></div><a class="btn btn--ghost btn--sm" href="${botPath(botId(bot), "updates")}">View update log ${icon("chevron")}</a></div>${renderActivityList(12)}</section></div>`;
  }

  const updatePayload = (item) => item?.payload ?? item?.update ?? item?.raw ?? item?.data ?? item ?? {};
  const telegramEnvelope = (item) => {
    const payload = updatePayload(item);
    const candidates = ["message", "edited_message", "channel_post", "edited_channel_post", "inline_query", "chosen_inline_result", "callback_query", "shipping_query", "pre_checkout_query", "poll", "poll_answer", "my_chat_member", "chat_member", "chat_join_request"];
    const kind = item?.type || item?.update_type || item?.event_type || candidates.find((key) => payload?.[key] != null) || "update";
    const envelope = payload?.[kind] || payload?.message || payload?.callback_query?.message || payload;
    return { payload, kind, envelope };
  };

  const updateId = (item) => String(item?.update_id ?? updatePayload(item)?.update_id ?? item?.id ?? "");
  const updateTime = (item) => item?.received_at || item?.created_at || item?.timestamp || telegramEnvelope(item).envelope?.date;
  const updateStatus = (item) => String(item?.delivery_status || item?.status || (item?.delivered === true ? "delivered" : item?.delivered === false ? "failed" : "stored")).toLowerCase();
  const updateChat = (item) => {
    const { payload, envelope } = telegramEnvelope(item);
    const chat = envelope?.chat || envelope?.message?.chat || payload?.callback_query?.message?.chat || {};
    const user = envelope?.from || payload?.callback_query?.from || {};
    return chat.title || [chat.first_name, chat.last_name].filter(Boolean).join(" ") || user.username && `@${user.username}` || user.first_name || (chat.id != null ? `Chat ${chat.id}` : "—");
  };

  const statusBadge = (status) => {
    if (["delivered", "success", "ok", "processed"].includes(status)) return `<span class="badge badge--success">${esc(status)}</span>`;
    if (["failed", "error", "rejected"].includes(status)) return `<span class="badge badge--danger">${esc(status)}</span>`;
    if (["pending", "retrying", "queued"].includes(status)) return `<span class="badge badge--warning">${esc(status)}</span>`;
    return `<span class="badge badge--info">${esc(status)}</span>`;
  };

  function renderUpdates() {
    const bot = currentBot();
    if (!bot) return `<div class="page">${renderNoBots()}</div>`;
    const days = botRetentionDays(bot);
    const cutoff = new Date(Date.now() - days * 86400000);
    const live = updatesLiveView();
    return `<div class="page">
      ${pageHeader("Update log", `Search the exact updates received for ${botName(bot)} and inspect their stored payloads.`)}
      <div class="status-banner status-banner--info">${icon("clock")}<div class="status-banner__copy"><strong>${esc(retentionLabel(bot))}</strong>Updates received before ${esc(new Intl.DateTimeFormat(undefined, { dateStyle: "medium" }).format(cutoff))} are removed automatically for this bot.</div></div>
      <form class="toolbar" id="update-filter-form">
        <div class="toolbar__search">${icon("search")}<label class="visually-hidden" for="update-query">Search updates</label><input class="search-input" id="update-query" name="query" type="search" value="${esc(state.filters.query)}" placeholder="Update ID, chat, or payload"></div>
        <label class="visually-hidden" for="update-type">Update type</label><select id="update-type" name="type"><option value="">All event types</option>${["message", "edited_message", "callback_query", "inline_query", "chat_member", "poll"].map((type) => `<option value="${type}" ${state.filters.type === type ? "selected" : ""}>${type}</option>`).join("")}</select>
        <button class="btn btn--secondary" type="submit">${icon("filter")}Filter</button>
        <span class="toolbar__spacer"></span><span class="live-state ${live.className}" id="updates-live-state" role="status" aria-live="polite">${live.label}</span><button class="btn btn--ghost btn--icon" type="button" data-action="toggle-updates" aria-label="${state.updatesPaused ? "Resume live updates" : "Pause live updates"}">${icon(state.updatesPaused ? "play" : "pause")}</button>
      </form>
      <section class="panel" id="updates-panel">${renderUpdatesTable()}</section>
    </div>`;
  }

  function renderUpdatesTable() {
    if (state.loading.updates && !state.updates.length) return `<div class="panel__body skeleton-stack skeleton-stack--updates"><div class="skeleton"></div><div class="skeleton"></div><div class="skeleton"></div></div>`;
    if (state.errors.updates && !state.updates.length) return renderRouteError(state.errors.updates, "refresh-updates");
    if (!state.updates.length) return `<div class="empty-state"><span class="empty-state__icon">${icon("pulse")}</span><h2>${state.filters.query || state.filters.type ? "No matching updates" : "Listening for updates"}</h2><p>${state.filters.query || state.filters.type ? "Try a broader query or remove the event-type filter." : "New Telegram updates will appear here automatically once this bot starts receiving them."}</p>${state.filters.query || state.filters.type ? '<button class="btn btn--secondary" type="button" data-action="clear-update-filters">Clear filters</button>' : ""}</div>`;
    return `<div class="table-wrap"><table class="data-table"><thead><tr><th>Received</th><th>Update</th><th>Type</th><th>Chat / user</th><th>Attempts</th><th>Status</th></tr></thead><tbody>${state.updates.map((item, index) => {
      const envelope = telegramEnvelope(item);
      const status = updateStatus(item);
      return `<tr tabindex="0" role="button" data-action="view-update" data-update-id="${esc(updateJournalId(item))}" data-update-index="${index}" aria-label="Inspect update ${esc(updateId(item))}"><td>${esc(formatDate(updateTime(item)))}</td><td><strong class="mono">${esc(updateId(item) || "—")}</strong></td><td><span class="tag">${esc(envelope.kind)}</span></td><td>${esc(updateChat(item))}</td><td>${esc(item.attempts ?? item.delivery_attempts ?? "—")}</td><td>${statusBadge(status)}</td></tr>`;
    }).join("")}</tbody></table></div>`;
  }

  function conversationId(item) {
    return String(item?.chat_id ?? item?.id ?? item?.chat?.id ?? item?.conversation_id ?? "");
  }

  const conversationTitle = (item) => item?.title || item?.display_name || item?.name || item?.chat?.title || [item?.first_name || item?.chat?.first_name, item?.last_name || item?.chat?.last_name].filter(Boolean).join(" ") || item?.username && `@${item.username}` || `Chat ${conversationId(item)}`;
  const conversationMessages = (item) => Array.isArray(item?.messages) ? item.messages : Array.isArray(item?.items) ? item.items : [];
  const messageText = (item) => item?.text || item?.message?.text || item?.caption || item?.message?.caption || item?.content?.text || "";
  const messageTime = (item) => item?.sent_at || item?.created_at || item?.timestamp || item?.date;
  const isOutgoing = (item) => item?.outgoing === true || item?.is_outgoing === true || ["out", "outgoing", "bot", "sent"].includes(String(item?.direction || item?.sender_type || "").toLowerCase());

  function renderBotView() {
    const bot = currentBot();
    if (!bot) return `<div class="page">${renderNoBots()}</div>`;
    return `<div class="page page--wide">
      ${pageHeader("Bot View", `See conversations exactly as ${botName(bot)} does, then reply with clear operator intent.`, `<span class="badge badge--success">Access audited</span>`)}
      ${state.errors.conversations ? `<div class="status-banner status-banner--danger">${icon("alert")}<div class="status-banner__copy"><strong>Conversations unavailable</strong>${esc(state.errors.conversations)}</div><button class="btn btn--sm btn--secondary" data-action="retry-conversations">Retry</button></div>` : ""}
      <section class="bot-view ${state.selectedConversationId ? "has-chat" : ""}">${renderConversationList()}${renderChatPane(bot)}</section>
    </div>`;
  }

  function renderConversationList() {
    const items = state.loading.conversations && !state.conversations.length
      ? `<div class="panel__body skeleton-stack skeleton-stack--conversations"><div class="skeleton"></div><div class="skeleton"></div><div class="skeleton"></div></div>`
      : state.conversations.length
        ? state.conversations.map((item) => {
          const id = conversationId(item);
          const messages = conversationMessages(item);
          const last = item.last_message || messages[messages.length - 1] || {};
          return `<button class="conversation ${String(state.selectedConversationId) === id ? "active" : ""}" type="button" data-action="select-conversation" data-conversation-id="${esc(id)}"><span class="chat-avatar">${initials(conversationTitle(item))}</span><span class="conversation__copy"><span class="conversation__line"><strong>${esc(conversationTitle(item))}</strong><time>${esc(formatDate(item.last_update_at || item.updated_at || messageTime(last), "time"))}</time></span><span class="conversation__preview">${esc(messageText(last) || item.last_message_preview || item.last_message_text || "No messages yet")}</span></span></button>`;
        }).join("")
        : `<div class="empty-state"><span class="empty-state__icon">${icon("message")}</span><h3>No conversations yet</h3><p>Chats appear after this bot receives message updates.</p></div>`;
    return `<aside class="conversation-list"><div class="conversation-list__head"><h2>Conversations</h2><div class="toolbar__search">${icon("search")}<input class="search-input" id="conversation-search" type="search" placeholder="Search name or chat ID" aria-label="Search conversations"></div></div><div class="conversation-list__items" id="conversation-items">${items}</div></aside>`;
  }

  function renderChatPane(bot) {
    const conversation = state.conversations.find((item) => conversationId(item) === String(state.selectedConversationId));
    if (!conversation) return `<section class="chat-pane"><div class="empty-state empty-state--fill"><span class="empty-state__icon">${icon("message")}</span><h2>Select a conversation</h2><p>Choose a chat to inspect the timeline and reply as ${esc(botUsername(bot))}.</p></div></section>`;
    const messages = conversationMessages(conversation);
    return `<section class="chat-pane"><header class="chat-pane__head"><button class="btn btn--ghost btn--icon chat-back" type="button" data-action="chat-back" aria-label="Back to conversations">${icon("arrow", "" )}</button><span class="chat-avatar">${initials(conversationTitle(conversation))}</span><span class="chat-pane__head-copy"><strong>${esc(conversationTitle(conversation))}</strong><span>chat_id: ${esc(conversationId(conversation))}</span></span><span class="badge badge--success">Bot can reply</span></header>
      <div class="timeline" id="chat-timeline">${messages.length ? `<div class="timeline-day"><span>Conversation history</span></div>${messages.map(renderMessage).join("")}` : `<div class="empty-state"><span class="empty-state__icon">${icon("message")}</span><h3>No message history</h3><p>This conversation exists, but no message payloads were returned.</p></div>`}</div>
      <footer class="composer"><div class="composer__label"><span>Reply as <strong>${esc(botUsername(bot))}</strong></span><span>Operator sends are audited</span></div><form class="composer__form" id="message-form"><textarea name="text" rows="1" maxlength="4096" placeholder="Write a reply…" aria-label="Reply text" required></textarea><button class="btn btn--primary btn--icon" type="submit" aria-label="Send reply">${icon("send")}</button></form><div data-form-error aria-live="polite"></div></footer>
    </section>`;
  }

  function renderMessage(item) {
    const text = messageText(item);
    const type = item?.type || item?.event_type || "message";
    if (!text) return `<div class="message-event">${icon("pulse")}<span>${esc(type)}</span><time>${esc(formatDate(messageTime(item), "time"))}</time></div>`;
    const outgoing = isOutgoing(item);
    const status = item.status || item.delivery_status || (outgoing ? "sent" : "received");
    return `<article class="message ${outgoing ? "message--out" : ""}"><div class="message__bubble"><div class="message__text">${esc(text)}</div><div class="message__meta"><span>${esc(formatDate(messageTime(item), "time"))}</span>${outgoing ? `<span>${esc(status)}</span>` : ""}</div></div></article>`;
  }

  function renderStreamKeyList() {
    if (state.loading.streamKeys) return '<div class="credential-list"><div class="skeleton skeleton--credential"></div></div>';
    if (state.errors.streamKeys) return `<div class="inline-error">${icon("alert")}<span>${esc(state.errors.streamKeys)}</span><button class="btn btn--ghost btn--sm" type="button" data-action="retry-stream-keys">Retry</button></div>`;
    if (!state.streamKeys.length) return '<p class="empty-copy">No stream credentials yet.</p>';
    return `<div class="credential-list">${state.streamKeys.map((key) => {
      const revoked = Boolean(key.revoked_at);
      return `<div class="credential-row"><div class="credential-row__copy"><strong>${esc(key.name || "Stream consumer")}</strong><span>Created ${esc(formatDate(key.created_at, "full"))}${key.last_used_at ? ` · used ${esc(relativeTime(key.last_used_at))}` : " · never used"}</span></div>${revoked ? '<span class="badge badge--info">Revoked</span>' : `<button class="btn btn--ghost btn--sm text-danger" type="button" data-action="revoke-stream-key" data-key-id="${esc(key.id)}">Revoke</button>`}</div>`;
    }).join("")}</div>`;
  }

  function renderIntegration() {
    const bot = currentBot();
    if (!bot) return `<div class="page">${renderNoBots()}</div>`;
    const apiBase = bot.integration?.api_base || `${window.location.origin}/bot${"${BOT_TOKEN}"}`;
    const endpoint = `${String(apiBase).replace(/\/$/, "")}/${"${METHOD}"}`;
    return `<div class="page">
      ${pageHeader("Delivery & API", `Connect ${botName(bot)} to the compatible API gateway and create scoped stream credentials.`)}
      <div class="integration-grid"><div class="panel-stack">
        <section class="panel"><div class="panel__head"><div><h2>API gateway</h2><p>Change the host. Keep Telegram methods and payloads.</p></div><span class="badge badge--success">Compatible</span></div><div class="panel__body"><div class="endpoint-block"><code>${esc(endpoint)}</code><button class="btn btn--ghost btn--icon btn--sm copy-btn" type="button" data-action="copy-value" data-copy-value="${esc(endpoint)}" aria-label="Copy API endpoint">${icon("copy")}</button></div><div class="integration-steps"><div class="integration-step" data-step="1"><strong>Keep the token server-side</strong><p>Load it from your deployment secret or environment. Never put a bot token in frontend code.</p></div><div class="integration-step" data-step="2"><strong>Set the API base URL</strong><p>Use the exact deployment-specific base URL shown above in your Telegram client library.</p></div><div class="integration-step" data-step="3"><strong>Make one test call</strong><p>Call getMe and confirm the request appears in API activity below.</p></div></div></div></section>
        <section class="panel"><div class="panel__head"><div><h2>Recent API activity</h2><p>Credentials and authorization headers are always redacted.</p></div></div>${renderActivityList(6)}</section>
        <section class="panel"><div class="panel__head"><div><h2>Signed file link</h2><p>Create a public download URL without exposing the bot token.</p></div></div><div class="panel__body"><form id="file-link-form" class="compact-form"><div class="field"><label for="file-path">Telegram file path</label><input id="file-path" name="file_path" type="text" autocomplete="off" spellcheck="false" placeholder="documents/report.pdf" required></div><div class="field compact-form__small"><label for="file-ttl">Expires in</label><select id="file-ttl" name="expires_in_seconds"><option value="300">5 minutes</option><option value="3600" selected>1 hour</option><option value="86400">24 hours</option><option value="604800">7 days</option></select></div><button class="btn btn--primary" type="submit">${icon("link")}Create link</button></form><p class="field__hint">Use the relative <span class="mono">file_path</span> returned by Telegram getFile.</p><div data-form-error aria-live="polite"></div>${state.fileLink ? `<div class="stream-key-box"><div class="stream-key-box__label">Public until ${esc(formatDate(state.fileLink.expires_at, "full"))}</div><code>${esc(state.fileLink.url)}</code><div class="result-actions"><button class="btn btn--secondary btn--sm" type="button" data-action="copy-value" data-copy-value="${esc(state.fileLink.url)}">${icon("copy")}Copy link</button><a class="btn btn--ghost btn--sm" href="${esc(state.fileLink.url)}" target="_blank" rel="noopener noreferrer">Open ${icon("external")}</a></div></div>` : ""}</div></section>
      </div><aside class="panel-stack">
        <section class="panel"><div class="panel__head"><div><h2>Event stream access</h2><p>Create and revoke scoped SSE consumers.</p></div></div><div class="panel__body"><div class="form-note">${icon("shield")}Stream secrets are separate from the Telegram bot token. The full URL is shown once.</div><form id="stream-key-form" class="compact-form compact-form--stack"><div class="field"><label for="stream-key-name">Consumer name</label><input id="stream-key-name" name="name" type="text" maxlength="80" value="Default stream" required></div><button class="btn btn--primary btn--block" type="submit">${icon("plus")}Create stream URL</button></form>${state.streamKey ? `<div class="stream-key-box"><div class="stream-key-box__label">Shown once — copy the full URL now</div><code>${esc(state.streamKey)}</code><div class="result-actions"><button class="btn btn--secondary btn--sm" type="button" data-action="copy-value" data-copy-value="${esc(state.streamKey)}">${icon("copy")}Copy stream URL</button><button class="btn btn--ghost btn--sm" type="button" data-action="dismiss-stream-secret">Dismiss</button></div></div>` : ""}<div class="credential-section"><div class="credential-section__head"><strong>Credentials</strong><button class="btn btn--ghost btn--sm" type="button" data-action="retry-stream-keys">${icon("refresh")}Refresh</button></div>${renderStreamKeyList()}</div></div></section>
        <section class="panel"><div class="panel__head"><div><h2>Delivery adapters</h2><p>A focused MVP with room to grow.</p></div></div><div class="panel__body"><div class="coming-list"><div class="coming-item">${icon("zap")}Server-sent events <span>Available</span></div><div class="coming-item">${icon("link")}Telegram webhooks <span>API</span></div><div class="coming-item">${icon("pulse")}WebSocket & Kafka <span>Planned</span></div></div></div></section>
      </aside></div>
    </div>`;
  }

  function renderBilling() {
    const plan = membershipPlan();
    const coverage = coverageStats();
    const renewal = state.membership?.current_period_ends_at || state.membership?.renews_at || state.membership?.current_period_end || state.membership?.expires_at;
    return `<div class="page page--narrow">
      ${pageHeader("Usage & billing", "Your plan controls full-history coverage, update retention, and access to local Telegram Bot API routing.")}
      <section class="panel"><div class="plan-summary"><span class="plan-summary__icon">${icon("card")}</span><div class="plan-summary__copy"><span>Current plan</span><h2>${esc(plan)}</h2><p>${renewal ? `Renews ${formatDate(renewal, "full")}` : "No payment method required for the Free plan"}</p></div>${plan.toLowerCase() === "free" ? '<button class="btn btn--white" type="button" data-action="request-plan" data-plan="Pro">Upgrade plan</button>' : '<span class="badge badge--success">Active</span>'}</div><div class="plan-features"><div class="plan-feature"><span class="stat-label">Bots in workspace</span><strong>${coverage.total}</strong></div><div class="plan-feature"><span class="stat-label">Full-history coverage</span><strong>${coverage.covered} / ${coverage.total}</strong></div><div class="plan-feature"><span class="stat-label">Plan retention</span><strong>${retentionDays()} days</strong></div></div></section>
      ${coverage.uncovered ? renderCoverageSummary({ compact: true }) : ""}
      <div class="billing-plans">${billingPlan("Free", "$0", 1, 30, false)}${billingPlan("Pro", "$29", 5, 90, true)}${billingPlan("Scale", "$99", 25, 365, true)}</div>
      <div class="status-banner section-gap">${icon("info")}<div class="status-banner__copy"><strong>Coverage adjusts automatically</strong>If your workspace exceeds full-history coverage, affected managed bots keep 24 hours of history and remain available throughout the console.</div></div>
    </div>`;
  }

  function billingPlan(name, price, bots, retention, localApi) {
    const current = membershipPlan().toLowerCase() === name.toLowerCase();
    return `<article class="billing-plan ${current ? "is-current" : ""}"><div class="billing-plan__head"><h3>${esc(name)}</h3>${current ? '<span class="badge badge--success">Current</span>' : ""}</div><div class="billing-plan__price">${price}<span> / month</span></div><ul><li>${icon("check")}Full history for ${bots} bot${bots === 1 ? "" : "s"}</li><li>${icon("check")}${retention}-day update history</li><li>${icon("check")}${localApi ? "Local Bot API routing" : "Compatible API gateway"}</li></ul>${current ? '<button class="btn btn--secondary btn--block" type="button" disabled>Current plan</button>' : `<button class="btn btn--secondary btn--block" type="button" data-action="request-plan" data-plan="${esc(name)}">Choose ${esc(name)}</button>`}</article>`;
  }

  function renderSettings() {
    const identity = userDisplayName();
    const bot = currentBot();
    return `<div class="page page--narrow">
      ${pageHeader("Settings", "Manage your account and the currently selected bot.")}
      <section class="panel"><div class="panel__head"><div><h2>Account</h2><p>Your Phenogram workspace identity</p></div></div><div class="settings-grid"><div class="settings-row"><div class="settings-row__intro"><h3>Sign-in method</h3><p>The provider used to access this workspace.</p></div><div class="identity-card"><span class="account-chip__avatar">${initials(identity)}</span><div><strong>${esc(identity)}</strong><span>${esc(userIdentityMeta())} · ${esc(membershipPlan())} membership</span></div></div></div><div class="settings-row"><div class="settings-row__intro"><h3>Session</h3><p>End the current browser session securely.</p></div><div><button class="btn btn--secondary" type="button" data-action="logout">${icon("logout")}Sign out</button></div></div></div></section>
      ${bot ? `<section class="panel panel--spaced"><div class="panel__head"><div><h2>Selected bot</h2><p>Ownership and data controls for ${esc(botName(bot))}</p></div><a class="btn btn--secondary btn--sm" href="${botPath(botId(bot), "settings")}">Manage bot ${icon("chevron")}</a></div><div class="panel__body"><div class="identity-card"><span class="bot-avatar bot-avatar--lg">${initials(botName(bot))}</span><div><strong>${esc(botName(bot))}</strong><span>${esc(botUsername(bot))}</span></div></div></div></section>` : ""}
    </div>`;
  }

  function renderRoutingSettings(bot) {
    const currentMode = String(bot.routing_mode || "cloud").toLowerCase();
    const localEligible = Boolean(state.membership?.local_bot_api);
    const modeLabel = currentMode === "local" ? "Local Bot API" : "Telegram cloud";
    const targetMode = currentMode === "local" ? "cloud" : "local";
    const targetLabel = targetMode === "local" ? "Local Bot API" : "Telegram cloud";
    const canChange = targetMode === "cloud" || localEligible;
    return `<section class="panel panel--spaced"><div class="panel__head"><div><h2>Telegram API routing</h2><p>Premium workspaces can route a bot through the separately operated local Bot API service.</p></div><span class="badge badge--info">${esc(modeLabel)}</span></div><div class="settings-grid"><div class="settings-row"><div class="settings-row__intro"><h3>Current backend</h3><p>All proxied Bot API calls and file requests use this route.</p></div><div class="routing-choice"><strong>${esc(modeLabel)}</strong><span>${currentMode === "local" ? "Extended local-server file limits are available when the deployment is configured." : "Managed by Telegram's official cloud Bot API."}</span></div></div><div class="settings-row"><div class="settings-row__intro"><h3>Switch to ${esc(targetLabel)}</h3><p>Routing migration logs the bot out of its current Bot API server and may briefly interrupt traffic.</p></div><div>${canChange ? `<button class="btn btn--secondary" type="button" data-action="confirm-routing" data-mode="${targetMode}">${icon("refresh")}Migrate routing</button>` : `<div class="form-note">${icon("lock")}Local Bot API routing requires a Pro or Scale membership.</div><a class="btn btn--ghost btn--sm btn--top-gap" href="#/billing">Compare plans ${icon("chevron")}</a>`}</div></div></div></section>`;
  }

  function renderBotSettings() {
    const bot = currentBot();
    if (!bot) return `<div class="page">${renderNoBots()}</div>`;
    const fingerprint = bot.public_id || bot.public_key || "Assigned securely by Phenogram";
    const verified = !["token_invalid", "invalid"].includes(botStatus(bot));
    const managed = isManagedBot(bot);
    const connectedManager = managed && Boolean(botManagerId(bot));
    const descendants = managedDescendantCount(bot);
    const credentialRow = managed
      ? `<div class="settings-row"><div class="settings-row__intro"><h3>Bot credential</h3><p>Kept current through ${esc(managerLabel(bot))}.</p></div><div class="form-note">${icon("lock")}The credential stays encrypted. Phenogram refreshes it automatically from the manager after Telegram reports a token change.</div></div>`
      : `<div class="settings-row"><div class="settings-row__intro"><h3>Bot token</h3><p>Phenogram does not reveal stored credentials.</p></div><div class="form-note">${icon("lock")}The bot token is encrypted and cannot be viewed after connection. If it may be exposed, revoke it through BotFather and reconnect the bot.</div></div>`;
    const removalPanel = connectedManager
      ? `<section class="panel panel--spaced"><div class="panel__head"><div><h2>Managed relationship</h2><p>This bot is maintained through ${esc(managerLabel(bot))}</p></div></div><div class="settings-row"><div class="settings-row__intro"><h3>Automatic availability</h3><p>Managed bots stay in the workspace while their manager relationship is active.</p></div><div class="form-note">${icon("info")}This bot cannot be removed separately while ${esc(managerLabel(bot))} manages it.</div></div></section>`
      : `<section class="panel panel--spaced danger-zone"><div class="panel__head"><div><h2>Danger zone</h2><p>Permanent workspace actions</p></div></div><div class="settings-row"><div class="settings-row__intro"><h3>Delete this bot</h3><p>${managed ? "Remove this managerless managed bot and its stored Phenogram data." : `Disconnect the token and remove its stored data.${descendants ? ` ${descendants} managed bot${descendants === 1 ? "" : "s"} beneath it will remain in Phenogram; direct children become managerless.` : ""}`}</p></div><div><button class="btn btn--danger" type="button" data-action="confirm-delete-bot">${icon("trash")}Delete ${esc(botName(bot))}</button></div></div></section>`;
    return `<div class="page page--narrow">
      ${pageHeader("Bot settings", `Ownership, credentials, and stored data for ${botName(bot)}.`)}
      <section class="panel"><div class="panel__head"><div><h2>Telegram identity</h2><p>Verified server-side using Telegram getMe</p></div>${verified ? renderBotStatusBadge(bot) : '<span class="badge badge--danger">Token invalid</span>'}</div><div class="settings-grid"><div class="settings-row"><div class="settings-row__intro"><h3>Bot</h3><p>The Telegram identity associated with this workspace bot.</p></div><div class="identity-card"><span class="bot-avatar bot-avatar--lg">${initials(botName(bot))}</span><div><strong>${esc(botName(bot))}</strong><span>${esc(botUsername(bot))}${managed ? ` · Managed by ${esc(managerLabel(bot))}` : ""}</span></div></div></div><div class="settings-row"><div class="settings-row__intro"><h3>Platform status</h3><p>Current provisioning and delivery state reported by Phenogram.</p></div><div>${renderBotStatusBadge(bot)}</div></div><div class="settings-row"><div class="settings-row__intro"><h3>Public identifier</h3><p>Identifies this bot without authorizing Telegram API calls.</p></div><div><div class="fingerprint">${esc(fingerprint)}</div><p class="field__hint field__hint--spaced">This value is safe to reference publicly. Signed file links still expire separately.</p></div></div>${credentialRow}<div class="settings-row"><div class="settings-row__intro"><h3>Data retention</h3><p>Updates outside this window are removed automatically.</p></div><div><strong class="settings-value">${esc(retentionValue(bot))}</strong><p class="field__hint field__hint--compact-spaced">${botNeedsRetentionWarning(bot) ? "This managed bot is outside full-history coverage." : `Covered by your ${esc(membershipPlan())} plan.`}</p></div></div></div></section>
      ${renderRoutingSettings(bot)}
      ${removalPanel}
    </div>`;
  }

  function renderDrawer() {
    if (!state.drawer || state.drawer.type !== "update") return "";
    const itemId = normalizeJournalId(state.drawer.itemId || updateJournalId(state.drawer.item));
    const item = state.updates.find((candidate) => updateJournalId(candidate) === itemId) || state.drawer.item;
    if (!item) return "";
    const envelope = telegramEnvelope(item);
    const payload = updatePayload(item);
    return `<aside class="detail-drawer" role="dialog" aria-modal="true" aria-labelledby="update-detail-title"><header class="detail-drawer__head"><div class="detail-drawer__head-copy"><h2 id="update-detail-title">${esc(envelope.kind)}</h2><p>update_id: ${esc(updateId(item) || "unknown")}</p></div><button class="btn btn--ghost btn--icon" type="button" data-action="close-drawer" aria-label="Close update details">${icon("close")}</button></header><div class="detail-drawer__body"><div class="detail-grid"><div class="detail-stat"><span class="stat-label">Received</span><strong>${esc(formatDate(updateTime(item), "full"))}</strong></div><div class="detail-stat"><span class="stat-label">Delivery</span><strong>${esc(updateStatus(item))}</strong></div><div class="detail-stat"><span class="stat-label">Chat</span><strong>${esc(updateChat(item))}</strong></div><div class="detail-stat"><span class="stat-label">Attempts</span><strong>${esc(item.attempts ?? item.delivery_attempts ?? "—")}</strong></div></div><div class="panel__head panel__head--flush"><div><h3>Stored payload</h3><p>Sensitive request headers and credentials are not included.</p></div><button class="btn btn--ghost btn--sm" type="button" data-action="copy-json">${icon("copy")}Copy JSON</button></div><pre class="json-view" id="update-json">${esc(JSON.stringify(payload, null, 2))}</pre></div></aside>`;
  }

  function renderModal(force = false) {
    if (!state.modal) { modalRoot.innerHTML = ""; delete modalRoot.dataset.modalName; return; }
    const { name } = state.modal;
    if (!force && modalRoot.dataset.modalName === name && modalRoot.firstElementChild) return;
    modalRoot.dataset.modalName = name;
    if (name === "connect") {
      const atLimit = connectedBots().length >= membershipLimit();
      modalRoot.innerHTML = `<div class="modal-backdrop" data-action="close-modal"><section class="modal" role="dialog" aria-modal="true" aria-labelledby="connect-title" data-modal-panel><header class="modal__head"><div><h2 id="connect-title">${atLimit ? "Your bot limit is full" : "Connect a Telegram bot"}</h2><p>${atLimit ? `The ${membershipPlan()} plan includes ${membershipLimit()} bot${membershipLimit() === 1 ? "" : "s"}.` : "Paste the token from @BotFather."}</p></div><button class="btn btn--ghost btn--icon" type="button" data-action="close-modal" aria-label="Close">${icon("close")}</button></header>${atLimit ? `<div class="modal__body"><div class="form-note">${icon("info")}Your existing bots stay active. Upgrade the workspace before connecting another bot.</div></div><footer class="modal__actions"><button class="btn btn--secondary" type="button" data-action="close-modal">Not now</button><button class="btn btn--primary" type="button" data-action="go-billing">See plans</button></footer>` : `<form id="connect-bot-form" autocomplete="off"><div class="modal__body"><div class="form-stack"><div class="field"><div class="field__row"><label for="bot-token">Telegram bot token</label><span class="field__hint">From @BotFather</span></div><div class="input-wrap">${icon("lock")}<input id="bot-token" name="token" type="password" inputmode="text" autocomplete="new-password" spellcheck="false" placeholder="123456789:AA…" required></div><p class="field__hint">Your token is encrypted and will not be shown again.</p></div><div class="form-note">${icon("info")}Connecting transfers this bot’s webhook to Phenogram. If a webhook is already set, Phenogram will keep delivering updates to the same destination.</div><div data-form-error aria-live="polite"></div></div></div><footer class="modal__actions"><button class="btn btn--secondary" type="button" data-action="close-modal">Cancel</button><button class="btn btn--primary" type="submit">Verify and connect ${icon("arrow")}</button></footer></form>`}</section></div>`;
      return;
    }
    if (name === "bot-picker") {
      modalRoot.innerHTML = `<div class="modal-backdrop" data-action="close-modal"><section class="modal" role="dialog" aria-modal="true" aria-labelledby="picker-title" data-modal-panel><header class="modal__head"><div><h2 id="picker-title">Switch bot</h2><p>Choose a connected manager or any bot beneath it.</p></div><button class="btn btn--ghost btn--icon" type="button" data-action="close-modal" aria-label="Close">${icon("close")}</button></header><div class="modal__body"><div class="bot-picker-list">${renderBotPickerTree()}</div></div><footer class="modal__actions"><button class="btn btn--secondary" type="button" data-action="open-connect">${icon("plus")}Connect another bot</button></footer></section></div>`;
      return;
    }
    if (name === "delete-bot") {
      const bot = currentBot();
      const managed = isManagedBot(bot);
      const descendants = managedDescendantCount(bot);
      const impact = managed
        ? "The Telegram bot remains in Telegram, but its Phenogram history and console data will be deleted."
        : `The Telegram bot remains in Telegram, but its Phenogram history and console data will be deleted.${descendants ? ` ${descendants} managed bot${descendants === 1 ? "" : "s"} beneath it will remain in Phenogram; direct children become managerless, and each bot keeps the retention shown in its own status.` : ""}`;
      modalRoot.innerHTML = `<div class="modal-backdrop"><section class="modal" role="alertdialog" aria-modal="true" aria-labelledby="delete-title" data-modal-panel><header class="modal__head"><div><h2 id="delete-title">Delete ${esc(botName(bot))}?</h2><p>${managed ? "This removes the managerless bot and its stored Phenogram data." : "This disconnects the bot and removes its stored Phenogram data."}</p></div><button class="btn btn--ghost btn--icon" type="button" data-action="close-modal" aria-label="Close">${icon("close")}</button></header><form id="delete-bot-form"><div class="modal__body"><div class="status-banner status-banner--danger">${icon("alert")}<div class="status-banner__copy"><strong>This action cannot be undone</strong>${esc(impact)}</div></div><div class="field"><label for="delete-confirmation">Type <strong>${esc(botUsername(bot))}</strong> to confirm</label><input id="delete-confirmation" name="confirmation" type="text" autocomplete="off" data-expected="${esc(botUsername(bot))}" required></div><div data-form-error aria-live="polite"></div></div><footer class="modal__actions"><button class="btn btn--secondary" type="button" data-action="close-modal">Cancel</button><button class="btn btn--danger" type="submit">${icon("trash")}Delete bot</button></footer></form></section></div>`;
      return;
    }
    if (name === "routing") {
      const bot = currentBot();
      const mode = state.modal.data.mode === "local" ? "local" : "cloud";
      const label = mode === "local" ? "Local Bot API" : "Telegram cloud";
      modalRoot.innerHTML = `<div class="modal-backdrop" data-action="close-modal"><section class="modal" role="alertdialog" aria-modal="true" aria-labelledby="routing-title" data-modal-panel><header class="modal__head"><div><h2 id="routing-title">Migrate ${esc(botName(bot))} to ${esc(label)}?</h2><p>This changes the upstream Telegram API server for this bot.</p></div><button class="btn btn--ghost btn--icon" type="button" data-action="close-modal" aria-label="Close">${icon("close")}</button></header><form id="routing-form" data-mode="${mode}"><div class="modal__body"><div class="status-banner status-banner--danger">${icon("alert")}<div class="status-banner__copy"><strong>Expect a short interruption</strong>Telegram logout and login rules can delay the new route. Confirm webhook delivery after the migration.</div></div><label class="webhook-consent"><input name="confirm_migration" type="checkbox" required><span><strong>I understand this changes live bot traffic</strong>Proceed with the routing migration and record it in the audit log.</span></label><div data-form-error aria-live="polite"></div></div><footer class="modal__actions"><button class="btn btn--secondary" type="button" data-action="close-modal">Cancel</button><button class="btn btn--primary" type="submit">Migrate to ${esc(label)}</button></footer></form></section></div>`;
    }
  }

  function formError(form, message) {
    const target = form.querySelector("[data-form-error]");
    if (target) target.innerHTML = message ? `<p class="form-error">${esc(message)}</p>` : "";
  }

  function setSubmitting(form, submitting, label) {
    const button = form.querySelector('button[type="submit"]');
    if (!button) return;
    if (!button.dataset.originalLabel) button.dataset.originalLabel = button.innerHTML;
    button.disabled = submitting;
    button.innerHTML = submitting ? `${icon("refresh")} ${esc(label || "Working…")}` : button.dataset.originalLabel;
  }

  function surfaceWarnings(payload) {
    const warnings = Array.isArray(payload?.warnings) ? payload.warnings : [];
    warnings.forEach((warning) => toast(String(warning), "warning"));
  }

  async function submitConnectBot(form) {
    formError(form, "");
    const data = new FormData(form);
    const sessionVersion = state.sessionVersion;
    let token = String(data.get("token") || "").trim();
    if (!token) { formError(form, "Paste the token provided by BotFather."); return; }
    setSubmitting(form, true, "Verifying with Telegram…");
    try {
      const payload = await api("/bots", { method: "POST", body: { token } });
      if (state.sessionVersion !== sessionVersion || !state.user) return;
      token = "";
      form.reset();
      const created = unwrap(payload, "bot") || payload;
      await loadBots({ silent: true });
      if (state.sessionVersion !== sessionVersion || !state.user) return;
      if (created && botId(created)) selectBot(botId(created));
      closeModal();
      toast(`Connected ${created && typeof created === "object" ? botName(created) : "Telegram bot"}.`);
      surfaceWarnings(payload);
      navigate(state.selectedBotId ? `/bots/${encodeURIComponent(state.selectedBotId)}/overview` : "/bots");
    } catch (error) {
      if (state.sessionVersion !== sessionVersion || !state.user || !form.isConnected) return;
      formError(form, errorMessage(error));
      setSubmitting(form, false);
      form.elements.token.focus();
    }
  }

  async function submitMessage(form) {
    formError(form, "");
    const conversation = state.conversations.find((item) => conversationId(item) === String(state.selectedConversationId));
    const chatId = conversationId(conversation);
    const id = state.selectedBotId;
    const contextVersion = state.botContextVersion;
    const ticket = startRequest("sendMessage");
    const text = String(new FormData(form).get("text") || "").trim();
    if (!chatId || !text) return;
    setSubmitting(form, true, "Sending…");
    try {
      const payload = await api(`/bots/${encodeURIComponent(id)}/messages`, { method: "POST", body: { chat_id: Number(chatId), text } });
      if (!botRequestIsCurrent("sendMessage", ticket, id, contextVersion) || String(state.selectedConversationId) !== String(chatId)) return;
      const result = payload?.result || unwrap(payload, "message") || {};
      const sent = { ...result, text: result.text || text, direction: "outgoing", status: "sent", created_at: new Date().toISOString() };
      if (!Array.isArray(conversation.messages)) conversation.messages = [];
      conversation.messages.push(sent);
      conversation.last_message = sent;
      form.reset();
      render();
      window.setTimeout(() => { const timeline = document.querySelector("#chat-timeline"); if (timeline) timeline.scrollTop = timeline.scrollHeight; }, 10);
      toast("Reply sent as the bot.");
    } catch (error) {
      if (!botRequestIsCurrent("sendMessage", ticket, id, contextVersion)) return;
      formError(form, errorMessage(error));
      setSubmitting(form, false);
    }
  }

  async function submitDeleteBot(form) {
    const expected = form.elements.confirmation.dataset.expected;
    const actual = String(form.elements.confirmation.value || "").trim();
    if (actual !== expected) { formError(form, `Type ${expected} exactly to confirm.`); return; }
    setSubmitting(form, true, "Deleting…");
    const id = state.selectedBotId;
    const contextVersion = state.botContextVersion;
    try {
      await api(`/bots/${encodeURIComponent(id)}`, { method: "DELETE" });
      if (state.botContextVersion !== contextVersion || String(state.selectedBotId) !== String(id)) {
        await loadBots({ silent: true });
        return;
      }
      closeModal();
      clearSensitiveState({ scope: "bot" });
      await loadBots({ silent: true });
      toast("Bot and its Phenogram data were deleted.");
      navigate("/bots");
    } catch (error) {
      formError(form, errorMessage(error));
      setSubmitting(form, false);
    }
  }

  async function logout() {
    if (state.loading.logout) return;
    setLoading("logout", true);
    try {
      await api("/auth/logout", { method: "POST" });
      clearSensitiveState();
      navigate("/");
      toast("Signed out.");
    } catch (error) {
      if (error.status === 401) {
        clearSensitiveState();
        navigate("/");
        toast("Signed out.");
      } else {
        toast(`Could not sign out: ${errorMessage(error)}`, "error");
      }
    } finally {
      setLoading("logout", false);
    }
  }

  async function submitStreamKey(form) {
    formError(form, "");
    const id = state.selectedBotId;
    const contextVersion = state.botContextVersion;
    const name = String(new FormData(form).get("name") || "").trim();
    if (!id || !name) return;
    setSubmitting(form, true, "Creating…");
    try {
      const payload = await api(`/bots/${encodeURIComponent(id)}/stream-keys`, { method: "POST", body: { name } });
      if (state.botContextVersion !== contextVersion || String(state.selectedBotId) !== String(id)) return;
      const value = payload?.url || payload?.key || payload?.stream_key || payload?.token || unwrap(payload, "stream_key");
      state.streamKey = typeof value === "object" ? value?.key || value?.token : value;
      state.streamKeyId = payload?.id || null;
      if (!state.streamKey) throw new Error("The server created a key but did not return its one-time value.");
      await loadStreamKeys({ silent: true });
      render();
      toast("Stream key created. Copy it before leaving this page.");
    } catch (error) {
      if (state.botContextVersion === contextVersion && String(state.selectedBotId) === String(id)) {
        formError(form, errorMessage(error));
        setSubmitting(form, false);
      }
    }
  }

  async function revokeStreamKey(button) {
    const id = state.selectedBotId;
    const keyId = button.dataset.keyId;
    const contextVersion = state.botContextVersion;
    if (!id || !keyId) return;
    button.disabled = true;
    try {
      await api(`/bots/${encodeURIComponent(id)}/stream-keys/${encodeURIComponent(keyId)}`, { method: "DELETE" });
      if (state.botContextVersion !== contextVersion || String(state.selectedBotId) !== String(id)) return;
      if (String(state.streamKeyId || "") === String(keyId)) {
        state.streamKey = null;
        state.streamKeyId = null;
      }
      await loadStreamKeys({ silent: true });
      toast("Stream credential revoked.");
    } catch (error) {
      if (state.botContextVersion === contextVersion && String(state.selectedBotId) === String(id)) {
        button.disabled = false;
        toast(errorMessage(error), "error");
      }
    }
  }

  async function submitFileLink(form) {
    formError(form, "");
    const id = state.selectedBotId;
    const contextVersion = state.botContextVersion;
    const data = new FormData(form);
    const filePath = String(data.get("file_path") || "").trim();
    const ttl = Number(data.get("expires_in_seconds") || 3600);
    if (!id || !filePath) return;
    setSubmitting(form, true, "Creating…");
    try {
      const payload = await api(`/bots/${encodeURIComponent(id)}/file-links`, { method: "POST", body: { file_path: filePath, expires_in_seconds: ttl } });
      if (state.botContextVersion !== contextVersion || String(state.selectedBotId) !== String(id)) return;
      state.fileLink = { url: payload?.url, expires_at: payload?.expires_at };
      if (!state.fileLink.url) throw new Error("The server did not return a signed file URL.");
      render();
      toast("Signed file link created.");
    } catch (error) {
      if (state.botContextVersion === contextVersion && String(state.selectedBotId) === String(id)) {
        formError(form, errorMessage(error));
        setSubmitting(form, false);
      }
    }
  }

  async function submitRouting(form) {
    formError(form, "");
    if (!form.elements.confirm_migration.checked) {
      formError(form, "Confirm that you understand this changes live bot traffic.");
      return;
    }
    const id = state.selectedBotId;
    const contextVersion = state.botContextVersion;
    const mode = form.dataset.mode === "local" ? "local" : "cloud";
    setSubmitting(form, true, "Migrating…");
    try {
      const payload = await api(`/bots/${encodeURIComponent(id)}/routing`, { method: "POST", body: { mode, confirm_migration: true } });
      if (state.botContextVersion !== contextVersion || String(state.selectedBotId) !== String(id)) return;
      const updated = unwrap(payload, "bot") || payload?.bot;
      if (updated) {
        state.bot = { ...(state.bot || {}), ...updated };
        const index = state.bots.findIndex((candidate) => botId(candidate) === String(id));
        if (index >= 0) state.bots[index] = { ...state.bots[index], ...updated };
      }
      closeModal();
      render();
      toast(`Routing migration to ${mode === "local" ? "Local Bot API" : "Telegram cloud"} started.`);
      surfaceWarnings(payload);
    } catch (error) {
      if (state.botContextVersion === contextVersion && String(state.selectedBotId) === String(id)) {
        formError(form, errorMessage(error));
        setSubmitting(form, false);
      }
    }
  }

  async function copyText(value) {
    try {
      if (navigator.clipboard?.writeText) await navigator.clipboard.writeText(value);
      else {
        const area = document.createElement("textarea");
        area.value = value;
        area.className = "clipboard-helper";
        document.body.append(area);
        area.select();
        document.execCommand("copy");
        area.remove();
      }
      toast("Copied to clipboard.");
    } catch (_) {
      toast("Could not copy automatically. Select the value and copy it manually.", "error");
    }
  }

  document.addEventListener("submit", (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) return;
    if (form.id === "connect-bot-form") { event.preventDefault(); submitConnectBot(form); }
    if (form.id === "message-form") { event.preventDefault(); submitMessage(form); }
    if (form.id === "delete-bot-form") { event.preventDefault(); submitDeleteBot(form); }
    if (form.id === "stream-key-form") { event.preventDefault(); submitStreamKey(form); }
    if (form.id === "file-link-form") { event.preventDefault(); submitFileLink(form); }
    if (form.id === "routing-form") { event.preventDefault(); submitRouting(form); }
    if (form.id === "update-filter-form") {
      event.preventDefault();
      const data = new FormData(form);
      resetFilteredUpdatesReload();
      state.filters.query = String(data.get("query") || "").trim();
      state.filters.type = String(data.get("type") || "");
      loadUpdates();
    }
  });

  document.addEventListener("click", (event) => {
    const trigger = event.target.closest("[data-action]");
    if (!trigger) return;
    const action = trigger.dataset.action;
    if (action === "close-modal" && trigger.classList.contains("modal-backdrop") && event.target !== trigger) return;
    if (trigger.tagName === "A") return;
    event.preventDefault();

    if (action === "open-connect") setModal("connect");
    else if (action === "close-modal") closeModal();
    else if (action === "open-bot-picker") state.bots.length ? setModal("bot-picker") : setModal("connect");
    else if (action === "pick-bot") { const id = trigger.dataset.botId; selectBot(id); closeModal(); navigate(`/bots/${encodeURIComponent(id)}/overview`); }
    else if (action === "toggle-menu") setMobileMenu(!state.mobileMenu, { restoreFocus: state.mobileMenu });
    else if (action === "close-menu") setMobileMenu(false, { restoreFocus: true });
    else if (action === "logout") logout();
    else if (action === "go-billing") { closeModal(); navigate("/billing"); }
    else if (action === "retry-route") routeChanged();
    else if (action === "refresh-updates") loadUpdates();
    else if (action === "retry-conversations") loadConversations();
    else if (action === "clear-update-filters") { resetFilteredUpdatesReload(); state.filters = { ...state.filters, type: "", query: "" }; loadUpdates(); }
    else if (action === "toggle-updates") toggleUpdatesStream();
    else if (action === "view-update") { const itemId = normalizeJournalId(trigger.dataset.updateId); const item = state.updates.find((candidate) => updateJournalId(candidate) === itemId) || state.updates[Number(trigger.dataset.updateIndex)]; if (item) { state.drawer = { type: "update", itemId: updateJournalId(item), item }; render(); } }
    else if (action === "close-drawer") { state.drawer = null; render(); }
    else if (action === "copy-json") { const itemId = normalizeJournalId(state.drawer?.itemId || updateJournalId(state.drawer?.item)); const item = state.updates.find((candidate) => updateJournalId(candidate) === itemId) || state.drawer?.item; if (item) copyText(JSON.stringify(updatePayload(item), null, 2)); }
    else if (action === "copy-value") copyText(trigger.dataset.copyValue || "");
    else if (action === "select-conversation") {
      state.selectedConversationId = trigger.dataset.conversationId;
      render();
      loadConversationMessages(state.selectedConversationId).finally(() => {
        render();
        window.setTimeout(() => { const timeline = document.querySelector("#chat-timeline"); if (timeline) timeline.scrollTop = timeline.scrollHeight; }, 10);
      });
    }
    else if (action === "chat-back") { state.selectedConversationId = null; render(); }
    else if (action === "retry-stream-keys") loadStreamKeys();
    else if (action === "revoke-stream-key") revokeStreamKey(trigger);
    else if (action === "dismiss-stream-secret") { state.streamKey = null; state.streamKeyId = null; render(); }
    else if (action === "confirm-routing") setModal("routing", { mode: trigger.dataset.mode });
    else if (action === "confirm-delete-bot") setModal("delete-bot");
    else if (action === "request-plan") toast(`${trigger.dataset.plan || "That"} plan checkout is not enabled in this MVP yet. Your current plan is unchanged.`);
  });

  document.addEventListener("input", (event) => {
    if (event.target.matches("#conversation-search")) {
      const query = event.target.value.trim().toLowerCase();
      document.querySelectorAll("#conversation-items .conversation").forEach((item) => {
        item.hidden = query && !item.textContent.toLowerCase().includes(query);
      });
    }
    if (event.target.matches("input[aria-invalid='true']")) event.target.removeAttribute("aria-invalid");
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      if (state.modal) closeModal();
      else if (state.drawer) { state.drawer = null; render(); }
      else if (state.mobileMenu) setMobileMenu(false, { restoreFocus: true });
    }
    if (event.key === "Tab" && state.modal) {
      const panel = modalRoot.querySelector("[data-modal-panel]");
      const focusable = panel ? [...panel.querySelectorAll('a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])')].filter((element) => !element.hidden && element.getClientRects().length) : [];
      if (focusable.length) {
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (!panel.contains(document.activeElement)) { event.preventDefault(); (event.shiftKey ? last : first).focus(); }
        else if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
      }
    }
    if ((event.key === "Enter" || event.key === " ") && event.target.matches('tr[data-action="view-update"]')) event.target.click();
  });

  window.addEventListener("hashchange", routeChanged);
  window.addEventListener("beforeunload", () => stopUpdatesStream({ status: "idle" }));
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible" && state.route.name === "bot-updates" && !state.updatesPaused && !state.updatesStream && !state.updatesStreamRetryTimer) {
      startUpdatesStream({ reconnecting: state.updatesStreamStatus === "reconnecting" });
    }
  });

  bootstrap();
})();
