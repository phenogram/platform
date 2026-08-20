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
    recentlyConnectedBotId: null,
    botSetupRefreshTimer: null,
    botSetupRefreshAttempt: 0,
    botViewRefreshTimer: null,
    botViewRefreshPromise: null,
    botViewRefreshGeneration: 0,
    botViewSendsInFlight: new Map(),
    botViewConversationListPinned: false,
    botViewDrafts: new Map(),
    botViewOptimisticMessages: new Map(),
    botViewScrollState: new Map(),
    botViewOpenPanel: null,
    botViewUploadProgress: null,
    botViewRecorder: null,
    botViewMessagesStream: null,
    botViewMessagesStreamRetryTimer: null,
    botViewMessagesStreamRetryAttempt: 0,
    botViewMessagesStreamGeneration: 0,
    botViewMessageCursors: new Map(),
    botViewMessageNextBefore: new Map(),
    botViewLoadingOlder: false,
    botViewTimelineResizeObserver: null,
    botViewBulkModeKey: null,
    botViewBulkSelection: new Map(),
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

  const isPlatformUnauthorizedPayload = (payload) => Boolean(
    payload
      && typeof payload === "object"
      && payload.error
      && typeof payload.error === "object"
      && payload.error.code === "unauthorized",
  );

  const isTelegramFailurePayload = (payload) => Boolean(
    payload
      && typeof payload === "object"
      && payload.ok === false,
  );

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

    if (!response.ok || isTelegramFailurePayload(payload)) {
      const message = typeof payload === "string"
        ? payload
        : payload?.description || payload?.message || payload?.error?.message || payload?.error || payload?.detail;
      const error = new Error(message || `Request failed (${response.status})`);
      error.status = isTelegramFailurePayload(payload) ? Number(payload?.error_code || response.status) : response.status;
      error.httpStatus = response.status;
      error.telegramRejected = isTelegramFailurePayload(payload);
      error.payload = payload;
      if (response.status === 401 && state.user && isPlatformUnauthorizedPayload(payload)) {
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
  const reportedBotStatus = (bot) => {
    if (bot?.token_valid === false) return "token_invalid";
    return String(bot?.status || bot?.health || "unknown").toLowerCase();
  };

  const botStatus = (bot) => {
    const status = reportedBotStatus(bot);
    const justConnected = state.recentlyConnectedBotId
      && botId(bot) === String(state.recentlyConnectedBotId);
    return justConnected && ["degraded", "warning"].includes(status) ? "provisioning" : status;
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
  const botUsesTestEnvironment = (bot) => bot?.telegram_test_dc === true;
  const botEnvironmentLabel = (bot) => botUsesTestEnvironment(bot) ? "Telegram Test" : "Telegram Production";
  const renderBotEnvironmentBadge = (bot) => botUsesTestEnvironment(bot)
    ? '<span class="badge badge--info">Test</span>'
    : "";
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
    stopBotViewRefresh();
    stopBotViewMessageStream();
    stopBotSetupRefresh();
    state.botContextVersion += 1;
    state.requestTickets = {};
    state.bot = null;
    state.activity = [];
    state.updates = [];
    state.conversations.forEach((conversation) => conversationMessages(conversation).forEach(revokeTimelineLocalPreviews));
    state.conversations = [];
    state.selectedConversationId = null;
    state.botViewConversationListPinned = false;
    state.botViewSendsInFlight.clear();
    state.botViewDrafts.forEach((draft) => revokeDraftFiles(draft));
    state.botViewOptimisticMessages.forEach((messages) => messages.forEach((message) => revokeDraftFiles(message?._draft)));
    state.botViewDrafts.clear();
    state.botViewOptimisticMessages.clear();
    state.botViewScrollState.clear();
    state.botViewMessageCursors.clear();
    state.botViewMessageNextBefore.clear();
    state.botViewBulkModeKey = null;
    state.botViewBulkSelection.clear();
    state.botViewLoadingOlder = false;
    state.botViewTimelineResizeObserver?.disconnect?.();
    state.botViewTimelineResizeObserver = null;
    state.botViewOpenPanel = null;
    state.botViewUploadProgress = null;
    stopVoiceRecording({ cancel: true, renderResult: false });
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

  const BOT_SETUP_REFRESH_DELAYS = [1000, 1500, 2500, 4000, 6000, 8000, 10000, 12000];

  function botSetupIsPending(bot) {
    return ["provisioning", "setup", "pending", "degraded", "warning"].includes(reportedBotStatus(bot));
  }

  function connectResponseWillRetry(payload, bot) {
    if (["provisioning", "setup", "pending"].includes(reportedBotStatus(bot))) return true;
    if (!["degraded", "warning"].includes(reportedBotStatus(bot))) return false;
    const warnings = Array.isArray(payload?.warnings) ? payload.warnings : [];
    return warnings.some((warning) => /still running|finish it automatically|not complete yet|retry automatically/i.test(String(warning)));
  }

  function stopBotSetupRefresh() {
    if (state.botSetupRefreshTimer) window.clearTimeout(state.botSetupRefreshTimer);
    state.botSetupRefreshTimer = null;
    state.botSetupRefreshAttempt = 0;
    state.recentlyConnectedBotId = null;
  }

  function scheduleBotSetupRefresh() {
    const id = String(state.recentlyConnectedBotId || "");
    if (!id || state.botSetupRefreshTimer) return;
    if (state.botSetupRefreshAttempt >= BOT_SETUP_REFRESH_DELAYS.length) {
      stopBotSetupRefresh();
      render();
      return;
    }
    const sessionVersion = state.sessionVersion;
    const contextVersion = state.botContextVersion;
    const delay = BOT_SETUP_REFRESH_DELAYS[state.botSetupRefreshAttempt];
    state.botSetupRefreshTimer = window.setTimeout(async () => {
      state.botSetupRefreshTimer = null;
      if (state.sessionVersion !== sessionVersion
        || state.botContextVersion !== contextVersion
        || String(state.selectedBotId || "") !== id) {
        stopBotSetupRefresh();
        return;
      }
      await loadBot(id, { silent: true });
      if (state.sessionVersion !== sessionVersion
        || state.botContextVersion !== contextVersion
        || String(state.selectedBotId || "") !== id) return;
      if (!botSetupIsPending(currentBot())) {
        stopBotSetupRefresh();
        render();
        return;
      }
      state.botSetupRefreshAttempt += 1;
      render();
      scheduleBotSetupRefresh();
    }, delay);
  }

  function trackConnectedBotSetup(id) {
    stopBotSetupRefresh();
    state.recentlyConnectedBotId = String(id);
    scheduleBotSetupRefresh();
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

  const MOBILE_BOT_VIEW_MEDIA = "(max-width: 640px)";

  function mobileBotViewIsSinglePane() {
    return typeof window.matchMedia === "function" && window.matchMedia(MOBILE_BOT_VIEW_MEDIA).matches;
  }

  async function loadConversations({ silent = false, renderResult = true, refreshContext = null } = {}) {
    const id = state.selectedBotId;
    if (!id) return false;
    const contextVersion = state.botContextVersion;
    const ticket = startRequest("conversations");
    if (!silent) { setLoading("conversations", true); setError("conversations", null); render(); }
    try {
      const payload = await api(`/bots/${encodeURIComponent(id)}/conversations`);
      if (!botRequestIsCurrent("conversations", ticket, id, contextVersion)
        || (refreshContext && !botViewRefreshContextIsCurrent(refreshContext))) return false;
      const previousConversations = new Map(state.conversations.map((item) => [conversationId(item), item]));
      state.conversations = listFrom(payload, "conversations").map((item) => {
        const previous = previousConversations.get(conversationId(item));
        return previous?.messages ? { ...item, messages: previous.messages } : item;
      });
      const selectedConversationExists = state.selectedConversationId
        && state.conversations.some((item) => conversationId(item) === String(state.selectedConversationId));
      if (state.selectedConversationId && !selectedConversationExists) state.selectedConversationId = null;
      if (!state.selectedConversationId
        && (!state.botViewConversationListPinned || !mobileBotViewIsSinglePane())) {
        state.selectedConversationId = state.conversations[0] ? conversationId(state.conversations[0]) : null;
        state.botViewConversationListPinned = false;
      }
      if (state.selectedConversationId && (!refreshContext || !state.botViewMessagesStream)) await loadConversationMessages(state.selectedConversationId);
      if (!botRequestIsCurrent("conversations", ticket, id, contextVersion)) return false;
      setError("conversations", null);
      return true;
    } catch (error) {
      if (botRequestIsCurrent("conversations", ticket, id, contextVersion)) setError("conversations", errorMessage(error));
      return false;
    } finally {
      if (botRequestIsCurrent("conversations", ticket, id, contextVersion)) {
        setLoading("conversations", false);
        if (renderResult) render();
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
      const payload = await api(`/bots/${encodeURIComponent(botIdAtRequest)}/conversations/${encodeURIComponent(chatId)}/messages?limit=100`);
      if (!botRequestIsCurrent(requestKey, ticket, botIdAtRequest, contextVersion)) return;
      const currentConversation = state.conversations.find((item) => conversationId(item) === String(chatId));
      if (!currentConversation) return;
      const key = botViewKey(botIdAtRequest, chatId);
      const latestCursor = String(payload?.latest_cursor || payload?.data?.latest_cursor || "");
      currentConversation.messages = mergeConversationMessageSnapshot(
        listFrom(payload, "messages"),
        conversationMessages(currentConversation),
        latestCursor,
      );
      advanceBotViewMessageCursor(key, latestCursor);
      state.botViewMessageNextBefore.set(key, payload?.next_before ?? payload?.data?.next_before ?? null);
    } catch (_) {
      // Conversation summaries remain useful even when historical payload loading fails.
    }
  }

  const BOT_VIEW_REFRESH_INTERVAL = 2500;

  function botViewRefreshContextIsCurrent(context) {
    return state.route.name === "bot-view"
      && Boolean(state.user)
      && document.visibilityState === "visible"
      && state.sessionVersion === context.sessionVersion
      && state.botContextVersion === context.contextVersion
      && state.botViewRefreshGeneration === context.generation
      && String(state.selectedBotId || "") === context.botId;
  }

  function stopBotViewRefresh() {
    state.botViewRefreshGeneration += 1;
    if (state.botViewRefreshTimer) window.clearTimeout(state.botViewRefreshTimer);
    state.botViewRefreshTimer = null;
  }

  function scheduleBotViewRefresh(context) {
    if (!botViewRefreshContextIsCurrent(context) || state.botViewRefreshTimer) return;
    state.botViewRefreshTimer = window.setTimeout(async () => {
      state.botViewRefreshTimer = null;
      if (!botViewRefreshContextIsCurrent(context)) return;
      if (state.botViewRefreshPromise) {
        scheduleBotViewRefresh(context);
        return;
      }
      const refresh = loadConversations({ silent: true, renderResult: false, refreshContext: context });
      state.botViewRefreshPromise = refresh;
      try {
        await refresh;
        if (botViewRefreshContextIsCurrent(context)) renderConversationListLive();
      } finally {
        if (state.botViewRefreshPromise === refresh) state.botViewRefreshPromise = null;
        if (botViewRefreshContextIsCurrent(context)) scheduleBotViewRefresh(context);
      }
    }, BOT_VIEW_REFRESH_INTERVAL);
  }

  function startBotViewRefresh() {
    if (state.route.name !== "bot-view" || !state.user || !state.selectedBotId || document.visibilityState !== "visible") return;
    stopBotViewRefresh();
    const context = {
      botId: String(state.selectedBotId),
      sessionVersion: state.sessionVersion,
      contextVersion: state.botContextVersion,
      generation: state.botViewRefreshGeneration,
    };
    scheduleBotViewRefresh(context);
    startBotViewMessageStream();
  }

  function renderConversationListLive() {
    if (state.route.name !== "bot-view") return;
    const current = document.querySelector(".conversation-list");
    if (!current) return;
    const search = document.querySelector("#conversation-search");
    const searchValue = search?.value || "";
    const focused = document.activeElement === search;
    current.outerHTML = renderConversationList();
    const next = document.querySelector("#conversation-search");
    if (next) {
      next.value = searchValue;
      applyConversationFilter(searchValue);
      if (focused) next.focus({ preventScroll: true });
    }
  }

  const BOT_VIEW_STREAM_RETRY_DELAYS = [1000, 2000, 4000, 8000, 15000, 30000];

  function timelineItemCursor(item) {
    return normalizeJournalId(item?._observed_cursor ?? item?.cursor ?? item?.event_id ?? item?.id);
  }

  function advanceBotViewMessageCursor(key, candidate) {
    const next = normalizeJournalId(candidate);
    const current = normalizeJournalId(state.botViewMessageCursors.get(key));
    if (next && compareJournalIds(next, current) > 0) state.botViewMessageCursors.set(key, next);
    return state.botViewMessageCursors.get(key) || "";
  }

  function mergeConversationMessageSnapshot(snapshot, observed, snapshotCursor) {
    const merged = [...(snapshot || [])];
    const positions = new Map(merged.map((item, index) => [messageStableId(item, index), index]));
    const semanticPositions = new Map();
    merged.forEach((item, index) => {
      const semantic = timelineSemanticIdentity(item);
      if (semantic) semanticPositions.set(semantic, index);
    });
    (observed || []).forEach((item, index) => {
      const stableId = messageStableId(item, index);
      const semantic = timelineSemanticIdentity(item);
      const position = positions.get(stableId) ?? (semantic ? semanticPositions.get(semantic) : undefined);
      const newerThanSnapshot = compareJournalIds(timelineItemCursor(item), snapshotCursor) > 0;
      if (position != null) {
        const snapshotItem = merged[position];
        const pendingBaseline = normalizeJournalId(item?._response_baseline_cursor ?? timelineItemCursor(item));
        const snapshotIsNotNewerThanPending = item?._response_pending
          && compareJournalIds(timelineItemCursor(snapshotItem), pendingBaseline) <= 0;
        if (newerThanSnapshot || snapshotIsNotNewerThanPending) {
          merged[position] = {
            ...item,
            id: snapshotItem?.id ?? item?.id,
            cursor: item?.cursor ?? snapshotItem?.cursor,
          };
        } else {
          revokeTimelineLocalPreviews(item);
        }
        return;
      }
      if (newerThanSnapshot || item?._locally_observed) {
        positions.set(stableId, merged.length);
        if (semantic) semanticPositions.set(semantic, merged.length);
        merged.push(item);
      }
    });
    return merged.sort((left, right) => {
      const cursorOrder = compareJournalIds(timelineItemCursor(left), timelineItemCursor(right));
      if (cursorOrder) return cursorOrder;
      return messageTimeMs(left) - messageTimeMs(right);
    });
  }

  function stopBotViewMessageStream() {
    state.botViewMessagesStreamGeneration += 1;
    if (state.botViewMessagesStreamRetryTimer) window.clearTimeout(state.botViewMessagesStreamRetryTimer);
    state.botViewMessagesStreamRetryTimer = null;
    state.botViewMessagesStream?.close?.();
    state.botViewMessagesStream = null;
  }

  function botViewMessageStreamContextIsCurrent(context, source = null) {
    return state.route.name === "bot-view"
      && Boolean(state.user)
      && document.visibilityState === "visible"
      && state.sessionVersion === context.sessionVersion
      && state.botContextVersion === context.contextVersion
      && state.botViewMessagesStreamGeneration === context.generation
      && String(state.selectedBotId || "") === context.botId
      && String(state.selectedConversationId || "") === context.conversationId
      && (!source || state.botViewMessagesStream === source);
  }

  function scheduleBotViewMessageStreamRetry(context) {
    if (!botViewMessageStreamContextIsCurrent(context) || state.botViewMessagesStreamRetryTimer) return;
    const delay = BOT_VIEW_STREAM_RETRY_DELAYS[Math.min(state.botViewMessagesStreamRetryAttempt, BOT_VIEW_STREAM_RETRY_DELAYS.length - 1)];
    state.botViewMessagesStreamRetryAttempt += 1;
    state.botViewMessagesStreamRetryTimer = window.setTimeout(() => {
      state.botViewMessagesStreamRetryTimer = null;
      if (botViewMessageStreamContextIsCurrent(context)) startBotViewMessageStream();
    }, delay);
  }

  function startBotViewMessageStream() {
    if (state.route.name !== "bot-view" || !state.user || !state.selectedBotId || !state.selectedConversationId || document.visibilityState !== "visible" || typeof EventSource !== "function") return;
    stopBotViewMessageStream();
    const generation = state.botViewMessagesStreamGeneration;
    const botIdValue = String(state.selectedBotId);
    const conversationIdValue = String(state.selectedConversationId);
    const key = botViewKey(botIdValue, conversationIdValue);
    const cursor = state.botViewMessageCursors.get(key) || "0";
    const url = `${API}/bots/${encodeURIComponent(botIdValue)}/conversations/${encodeURIComponent(conversationIdValue)}/messages/stream?after=${encodeURIComponent(cursor)}`;
    const source = new EventSource(url, { withCredentials: true });
    const context = { botId: botIdValue, conversationId: conversationIdValue, sessionVersion: state.sessionVersion, contextVersion: state.botContextVersion, generation };
    state.botViewMessagesStream = source;
    source.addEventListener("open", () => {
      if (!botViewMessageStreamContextIsCurrent(context, source)) return;
      state.botViewMessagesStreamRetryAttempt = 0;
    });
    source.addEventListener("message", (event) => {
      if (!botViewMessageStreamContextIsCurrent(context, source)) return;
      try {
        let message = JSON.parse(event.data);
        const eventId = String(event.lastEventId || message?.cursor || message?.event_id || "");
        if (eventId) {
          advanceBotViewMessageCursor(key, eventId);
          message = { ...message, cursor: message?.cursor || eventId, _locally_observed: true };
        }
        mergeBotViewStreamMessage(context, message);
      } catch (_) {
        // Ignore malformed frames; the next reconnect snapshot is authoritative.
      }
    });
    source.addEventListener("resync", async () => {
      if (!botViewMessageStreamContextIsCurrent(context, source)) return;
      source.close();
      state.botViewMessagesStream = null;
      await loadConversationMessages(conversationIdValue);
      if (!botViewMessageStreamContextIsCurrent(context)) return;
      renderBotViewLive();
      startBotViewMessageStream();
    });
    source.addEventListener("error", () => {
      if (!botViewMessageStreamContextIsCurrent(context, source)) return;
      source.close();
      state.botViewMessagesStream = null;
      scheduleBotViewMessageStreamRetry(context);
    });
  }

  function mergeBotViewStreamMessage(context, streamedItem) {
    if (!botViewMessageStreamContextIsCurrent(context)) return;
    const conversation = state.conversations.find((candidate) => conversationId(candidate) === context.conversationId);
    if (!conversation) return;
    const messages = conversationMessages(conversation);
    let item = streamedItem;
    const eventType = String(item?.event_type || item?.type || "");
    const callback = callbackEventFromTimelineItem(item);
    if (eventType === "callback_query" || callback) {
      const targetId = callback?.message?.message_id ?? callback?.message_id;
      const targetIndex = findLastMessageIndexByTelegramId(messages, targetId);
      if (targetIndex >= 0) {
        item = { ...withCallbackEvent(messages[targetIndex], callback, item), _observed_cursor: timelineItemCursor(streamedItem), _locally_observed: true };
      } else {
        item = { ...item, _timeline_callback_event: callback };
      }
    } else if (["poll", "poll_answer"].includes(eventType)) {
      const poll = item?.payload?.poll || item?.poll;
      const pollId = poll?.id || item?.payload?.poll_answer?.poll_id || item?.poll_answer?.poll_id;
      const target = [...messages].reverse().find((candidate) => String(telegramMessage(candidate)?.poll?.id || "") === String(pollId || ""));
      if (!target || !poll) return;
      const targetMessage = telegramMessage(target);
      item = {
        ...target,
        payload: target?.payload === targetMessage ? { ...targetMessage, poll } : target?.payload,
        ...(target?.payload !== targetMessage ? { poll } : {}),
        _observed_cursor: timelineItemCursor(streamedItem),
        _locally_observed: true,
      };
    }
    const deletion = item?.status === "deleted" || ["deleteMessage", "deleteBusinessMessages", "deleteEphemeralMessage", "deleted_business_messages"].includes(String(item?.event_type || item?.type || ""));
    if (deletion) {
      const telegramId = telegramMessageId(item);
      const ephemeralId = item?.ephemeral_message_id ?? telegramMessage(item)?.ephemeral_message_id;
      const receiverId = item?.receiver_user_id ?? telegramMessage(item)?.receiver_user_id;
      const target = [...messages].reverse().find((candidate) => {
        if (candidate?.direction === "action") return false;
        if (telegramId !== "" && telegramId != null && Number(telegramId) !== 0) return String(telegramMessageId(candidate)) === String(telegramId);
        return ephemeralId !== "" && ephemeralId != null && receiverId !== "" && receiverId != null
          && String(candidate?.ephemeral_message_id ?? telegramMessage(candidate)?.ephemeral_message_id ?? "") === String(ephemeralId)
          && String(candidate?.receiver_user_id ?? telegramMessage(candidate)?.receiver_user_id ?? "") === String(receiverId);
      });
      if (target) item = { ...target, status: "deleted", event_type: item.event_type || "deleted", cursor: item.cursor, created_at: item.created_at || target.created_at };
    }
    const streamedMessage = telegramMessage(item);
    const streamedEphemeral = item?.ephemeral_message_id ?? streamedMessage?.ephemeral_message_id;
    const streamedReceiver = item?.receiver_user_id ?? streamedMessage?.receiver_user_id ?? conversation?.receiver_user_id;
    const staleEphemeralRows = [];
    if (streamedEphemeral !== "" && streamedEphemeral != null && streamedReceiver !== "" && streamedReceiver != null) {
      messages.forEach((candidate) => {
        const candidateMessage = telegramMessage(candidate);
        const candidateEphemeral = candidate?.ephemeral_message_id ?? candidateMessage?.ephemeral_message_id;
        const candidateReceiver = candidate?.receiver_user_id ?? candidateMessage?.receiver_user_id ?? conversation?.receiver_user_id;
        if (String(candidateEphemeral ?? "") === String(streamedEphemeral)
          && String(candidateReceiver ?? "") === String(streamedReceiver)
          && messageStableId(candidate) !== messageStableId(item)) {
          candidate.actionable = false;
          candidate._observed_cursor = timelineItemCursor(streamedItem);
          candidate._locally_observed = true;
          staleEphemeralRows.push(candidate);
        }
      });
    }
    const id = messageStableId(item);
    const existingIndex = messages.findIndex((candidate) => messageStableId(candidate) === id);
    if (existingIndex >= 0) messages[existingIndex] = item;
    else messages.push(item);
    conversation.messages = messages;
    conversation.last_update_at = messageTime(item) || conversation.last_update_at;
    conversation.last_message_preview = `${isOutgoing(item) ? "You: " : ""}${messagePreview(item)}`;
    renderConversationListLive();
    const timeline = document.querySelector("#chat-timeline");
    if (!timeline) return;
    const key = botViewKey(context.botId, context.conversationId);
    const scrollState = state.botViewScrollState.get(key) || {};
    const wasNearBottom = botViewNearBottom(timeline);
    const selectorId = typeof CSS !== "undefined" && CSS.escape ? CSS.escape(id) : id.replace(/[^a-zA-Z0-9_-]/g, "");
    const existing = timeline.querySelector(`[data-message-id="${selectorId}"]`);
    if (existing) existing.outerHTML = renderMessage(item, existingIndex);
    else {
      timeline.querySelector(".empty-state")?.remove();
      if (!timeline.querySelector(".timeline-day")) timeline.insertAdjacentHTML("afterbegin", '<div class="timeline-day"><span>Conversation history</span></div>');
      timeline.insertAdjacentHTML("beforeend", renderMessage(item, messages.length - 1));
    }
    staleEphemeralRows.forEach((candidate) => {
      const stableId = messageStableId(candidate);
      const escapedId = typeof CSS !== "undefined" && CSS.escape ? CSS.escape(stableId) : stableId.replace(/[^a-zA-Z0-9_-]/g, "");
      const row = timeline.querySelector(`[data-message-id="${escapedId}"]`);
      if (row) row.outerHTML = renderMessage(candidate, messages.indexOf(candidate));
    });
    if (wasNearBottom) timeline.scrollTop = timeline.scrollHeight;
    else scrollState.unread = botViewUnreadAfterInsert(scrollState.unread, existing ? 0 : 1, false);
    scrollState.top = timeline.scrollTop;
    scrollState.nearBottom = wasNearBottom;
    scrollState.messageCount = messages.length;
    state.botViewScrollState.set(key, scrollState);
    updateScrollLatestControl();
    observeBotViewMedia();
  }

  async function loadOlderBotViewMessages() {
    if (state.botViewLoadingOlder || !state.selectedBotId || !state.selectedConversationId) return;
    const botIdValue = String(state.selectedBotId);
    const conversationIdValue = String(state.selectedConversationId);
    const key = botViewKey(botIdValue, conversationIdValue);
    const before = state.botViewMessageNextBefore.get(key);
    if (!before) return;
    const sessionVersion = state.sessionVersion;
    const contextVersion = state.botContextVersion;
    const timeline = document.querySelector("#chat-timeline");
    const oldHeight = timeline?.scrollHeight || 0;
    const oldTop = timeline?.scrollTop || 0;
    state.botViewLoadingOlder = true;
    timeline?.querySelector('[data-action="load-older-messages"]')?.setAttribute("disabled", "");
    try {
      const payload = await api(`/bots/${encodeURIComponent(botIdValue)}/conversations/${encodeURIComponent(conversationIdValue)}/messages?limit=100&before=${encodeURIComponent(before)}`);
      if (state.sessionVersion !== sessionVersion || state.botContextVersion !== contextVersion || String(state.selectedConversationId) !== conversationIdValue) return;
      const older = listFrom(payload, "messages");
      const conversation = state.conversations.find((candidate) => conversationId(candidate) === conversationIdValue);
      if (!conversation) return;
      const existingIds = new Set(conversationMessages(conversation).map((item) => messageStableId(item)));
      const uniqueOlder = older.filter((item) => !existingIds.has(messageStableId(item)));
      conversation.messages = [...uniqueOlder, ...conversationMessages(conversation)];
      state.botViewMessageNextBefore.set(key, payload?.next_before ?? payload?.data?.next_before ?? null);
      const currentTimeline = document.querySelector("#chat-timeline");
      if (currentTimeline && uniqueOlder.length) {
        const day = currentTimeline.querySelector(".timeline-day");
        day?.insertAdjacentHTML("afterend", uniqueOlder.map(renderMessage).join(""));
        currentTimeline.scrollTop = botViewPrependScrollTop(oldTop, oldHeight, currentTimeline.scrollHeight);
        observeBotViewMedia();
      }
      const loadContainer = currentTimeline?.querySelector(".load-older");
      const nextBefore = state.botViewMessageNextBefore.get(key);
      if (loadContainer) loadContainer.outerHTML = nextBefore ? '<div class="load-older"><button class="btn btn--secondary btn--sm" type="button" data-action="load-older-messages">Load earlier messages</button></div>' : "";
      const scrollState = state.botViewScrollState.get(key) || {};
      scrollState.top = currentTimeline?.scrollTop || 0;
      scrollState.messageCount = conversation.messages.length;
      state.botViewScrollState.set(key, scrollState);
    } catch (error) {
      toast(botViewErrorMessage(error), "error");
    } finally {
      state.botViewLoadingOlder = false;
      document.querySelector('[data-action="load-older-messages"]')?.removeAttribute("disabled");
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
    stopBotViewRefresh();
    stopBotViewMessageStream();
    const previousRouteName = state.route.name;
    state.route = parseRoute();
    if (previousRouteName !== "bot-view" || state.route.name !== "bot-view") {
      state.botViewConversationListPinned = false;
    }
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
    if (state.route.name === "bot-view") tasks.push(loadConversations({ silent: true, renderResult: false }));
    if (state.route.name === "bot-integration") tasks.push(loadStreamKeys({ silent: true }));
    await Promise.allSettled(tasks);
    render();
    if (state.route.name === "bot-updates" && !state.updatesPaused) startUpdatesStream();
    if (state.route.name === "bot-view") startBotViewRefresh();
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
    if (state.route.name === "bot-view") window.requestAnimationFrame(initializeBotViewDom);
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

        <div class="trust-strip"><div class="trust-item"><strong>Drop-in compatible</strong> with the Telegram Bot API</div><div class="trust-item"><strong>Private credentials</strong> never shown after setup</div><div class="trust-item"><strong>Built in Rust</strong> for predictable performance</div></div>

        <section class="landing-section landing-section--light" id="platform"><div class="landing-section__inner">
          <div class="section-heading"><p class="eyebrow">One control plane</p><h2>See every update. Understand every failure.</h2><p>Phenogram sits between your bot and Telegram, preserving the API contract while making the invisible parts of production visible.</p></div>
          <div class="feature-grid">
            <article class="feature-card feature-card--wide"><div class="feature-card__icon">${icon("pulse")}</div><h3>Durable update history</h3><p>Search the exact payload your bot received, inspect delivery attempts, and trace failures without reconstructing production from scattered logs.</p><div class="feature-card__visual"><div class="mini-event"><span class="mini-event__dot"></span><strong>message</strong><code>update_914022</code><time>12 ms</time></div><div class="mini-event"><span class="mini-event__dot mini-event__dot--violet"></span><strong>callback_query</strong><code>update_914021</code><time>18 ms</time></div></div></article>
            <article class="feature-card"><div class="feature-card__icon feature-card__icon--mint">${icon("message")}</div><h3>Bot View</h3><p>Experience conversations as your bot does. Inspect context and safely reply as the bot from one operator console.</p></article>
            <article class="feature-card"><div class="feature-card__icon feature-card__icon--violet">${icon("link")}</div><h3>Flexible delivery</h3><p>Start with a reliable live stream and add new delivery models as your architecture grows.</p></article>
            <article class="feature-card feature-card--wide"><div class="feature-card__icon">${icon("shield")}</div><h3>Share without leaking tokens</h3><p>Public share links use scoped, expiring references instead of bot tokens.</p><div class="feature-card__visual feature-card__visual--code mono">/public/<span class="text-primary">phg_a8c2…</span>/files/report.pdf?expires=…&amp;sig=…</div></article>
          </div>
        </div></section>

        <section class="landing-section landing-section--dark" id="workflow"><div class="landing-section__inner">
          <div class="section-heading"><p class="eyebrow">Two-minute migration</p><h2>Your bot code stays yours.</h2><p>Prove ownership with the BotFather token, point your client at Phenogram, and watch the first update arrive.</p></div>
          <div class="steps"><article class="step"><h3>Connect securely</h3><p>We verify the token with Telegram, store the application credential securely, and never display it again.</p></article><article class="step"><h3>Change one host</h3><p>Replace api.telegram.org with api.phenogram.io. Methods, payloads, and responses remain familiar.</p></article><article class="step"><h3>Ship with context</h3><p>Use the dashboard to follow API calls, updates, conversations, and delivery health in real time.</p></article></div>
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
          <div class="security-copy"><p class="eyebrow">Designed for sensitive credentials</p><h2>Your token is never shown again.</h2><p>Phenogram encrypts the credential in its application database and never displays it again. The official Bot API server uses its native storage format. Request history is credential-redacted, and public bot keys identify rather than authorize.</p><div class="security-list"><div>${icon("check")}Encrypted application credentials</div><div>${icon("check")}Expiring, scoped file links</div><div>${icon("check")}Audited operator actions</div><div>${icon("check")}Explicit bot deletion controls</div></div></div>
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
        <section><h2>Operational data</h2><p>Phenogram stores the bot configuration, updates, API activity, and operator actions needed to provide the platform. Credentials are encrypted in Phenogram's application database and never displayed again. The official Bot API server uses its native storage format. Retention depends on the selected membership plan.</p></section>
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
    return `<li><button class="bot-picker-item ${managed ? "is-managed" : ""} ${selected ? "active" : ""} ${warning ? "has-warning" : ""}" type="button" data-action="pick-bot" data-bot-id="${esc(id)}" aria-pressed="${selected ? "true" : "false"}"><span class="bot-avatar">${initials(botName(bot))}</span><span class="bot-picker-item__copy"><span><strong>${esc(botName(bot))}</strong>${managed ? '<em>Managed</em>' : ""}${renderBotEnvironmentBadge(bot)}</span><small>${esc(meta)}</small></span>${warning ? '<span class="badge badge--warning">24-hour history</span>' : selected ? icon("check") : icon("chevron")}</button>${node.children.length ? `<ul>${node.children.map(renderPickerBotNode).join("")}</ul>` : ""}</li>`;
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
      <div class="app-main${routeName === "bot-view" ? " app-main--bot-view" : ""}">
        <header class="topbar">
          <div class="topbar__left"><button class="btn btn--ghost btn--icon mobile-menu-btn" type="button" data-action="toggle-menu" aria-label="${state.mobileMenu ? "Close" : "Open"} navigation" aria-controls="app-sidebar" aria-expanded="${state.mobileMenu ? "true" : "false"}">${icon(state.mobileMenu ? "close" : "menu")}</button><span class="topbar__title">${esc(titleMap[routeName] || "Phenogram")}</span>${bot && routeName.startsWith("bot-") ? `<span class="topbar__crumb">${esc(botCrumb)}</span>` : ""}</div>
          <div class="topbar__actions"><span class="health-pill ${healthClass}">${healthClass === "is-down" ? "API issue" : "Platform online"}</span><button class="btn btn--secondary btn--sm" type="button" data-action="open-connect">${icon("plus")}<span>Connect bot</span></button></div>
        </header>
        <main id="main-content"${routeName === "bot-view" ? ' class="main--bot-view"' : ""} tabindex="-1">${renderMain()}</main>
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
    const webhookRecoveryRequired = bot?.webhook_secret_required === true;
    const finishingConnection = state.recentlyConnectedBotId
      && botId(bot) === String(state.recentlyConnectedBotId)
      && botSetupIsPending(bot);
    const status = botStatus(bot);
    const bad = ["invalid", "token_invalid", "error", "disabled", "failed"].includes(status);
    const provisioning = ["provisioning", "setup", "pending"].includes(status);
    const degraded = ["degraded", "warning"].includes(status);
    const unknown = !bad && !provisioning && !degraded && !["active", "healthy", "ready", "ok"].includes(status);
    const warning = provisioning || degraded || unknown;
    const title = webhookRecoveryRequired ? "Webhook details are needed to continue this managed bot" : bad ? "This bot needs attention" : finishingConnection ? "Bot setup is finishing" : degraded ? "This bot is degraded" : provisioning ? "Webhook provisioning is in progress" : unknown ? "Bot status is unavailable" : "Bot is healthy";
    const copy = webhookRecoveryRequired ? "The native webhook remains active and unchanged. Phenogram API routing is paused until you confirm its authentication and IP behavior." : bad ? "Verify the bot token and review recent API activity." : finishingConnection ? "Phenogram is completing Telegram setup. This page will update automatically." : degraded ? "The bot is connected, but an upstream setup step failed. Review its recent activity." : provisioning ? "Phenogram is registering its upstream webhook. Activity will appear when Telegram starts delivering updates." : unknown ? "Refresh this workspace before assuming the bot is ready." : "Phenogram is receiving and processing activity normally.";
    const lastUpdate = bot.last_update_at || bot.last_update || bot.latest_update_at;
    const lastApi = bot.last_api_call_at || bot.last_api_request_at || bot.last_request_at || bot.latest_activity_at;
    return `<section class="health-hero ${bad ? "is-error" : warning || webhookRecoveryRequired ? "is-warning" : ""}"><span class="health-hero__icon">${icon(bad || webhookRecoveryRequired ? "alert" : warning ? "clock" : "check")}</span><div class="health-hero__copy"><h2>${title}</h2><p>${copy}</p>${webhookRecoveryRequired ? `<button class="btn btn--secondary btn--sm btn--top-gap" type="button" data-action="open-managed-webhook-recovery">Resolve webhook transfer</button>` : ""}</div><div class="health-hero__meta"><div><span>Last update</span><strong>${esc(relativeTime(lastUpdate))}</strong></div><div><span>Last API call</span><strong>${esc(relativeTime(lastApi))}</strong></div><div><span>Retention</span><strong>${esc(retentionValue(bot))}</strong></div></div></section>`;
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
    return `<a class="bot-card bot-family__root" href="${botPath(id, "overview")}"><div class="bot-card__top"><span class="bot-avatar bot-avatar--lg">${initials(botName(bot))}</span><span class="bot-card__copy"><strong>${esc(botName(bot))}</strong><span>${esc(botUsername(bot))}</span></span>${renderBotEnvironmentBadge(bot)}${renderBotStatusBadge(bot)}</div><div class="bot-card__meta"><div><span class="stat-label">Last update</span><strong>${esc(relativeTime(bot.last_update_at || bot.last_update))}</strong></div><div><span class="stat-label">Managed bots</span><strong>${managedCount}</strong></div><div><span class="stat-label">Retention</span><strong>${esc(retentionValue(bot))}</strong></div></div><div class="bot-card__foot"><span>Connected bot</span><span>Open workspace ${icon("arrow")}</span></div></a>`;
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
    return String(item?.conversation_id ?? item?.id ?? item?.chat_id ?? item?.chat?.id ?? "");
  }

  const conversationChatId = (item) => String(item?.chat_id ?? item?.chat?.id ?? "");

  const conversationTitle = (item) => item?.title || item?.display_name || item?.name || item?.chat?.title || [item?.first_name || item?.chat?.first_name, item?.last_name || item?.chat?.last_name].filter(Boolean).join(" ") || item?.username && `@${item.username}` || (conversationChatId(item) ? `Chat ${conversationChatId(item)}` : "Conversation");
  const conversationContextLabel = (item) => {
    const scopes = [];
    if (item?.business_connection_id) scopes.push("Business");
    if (item?.guest_query_id) scopes.push("Guest query");
    if (item?.message_thread_id != null) scopes.push(item?.topic_name ? `Topic: ${item.topic_name}` : `Topic #${item.message_thread_id}`);
    if (item?.direct_messages_topic_id != null) scopes.push(`Direct topic #${item.direct_messages_topic_id}`);
    if (item?.receiver_user_id != null) scopes.push(`Ephemeral recipient ${item.receiver_user_id}`);
    return scopes.join(" · ");
  };
  const conversationMessages = (item) => Array.isArray(item?.messages) ? item.messages : Array.isArray(item?.items) ? item.items : [];
  const BOT_VIEW_MESSAGE_ENVELOPES = ["message", "edited_message", "channel_post", "edited_channel_post", "business_message", "edited_business_message", "guest_message"];
  const BOT_VIEW_EMOJI = ["😀", "😂", "😍", "🥰", "😎", "🤔", "👍", "👎", "❤️", "🔥", "🎉", "🙏", "👀", "🚀", "✅", "✨"];
  const BOT_VIEW_MAX_FILES = 10;
  const BOT_VIEW_CLOUD_MAX_FILE_BYTES = 50_000_000;
  const BOT_VIEW_CLOUD_MAX_TOTAL_BYTES = 500_000_000;
  const BOT_VIEW_LOCAL_MAX_FILE_BYTES = 2_000_000_000;
  const BOT_VIEW_LOCAL_MAX_TOTAL_BYTES = 20_000_000_000;
  const BOT_VIEW_PHOTO_MAX_FILE_BYTES = 10_000_000;

  function telegramMessage(item) {
    const candidates = [item?.message, item?.payload, item?.content, item];
    for (const candidate of candidates) {
      if (!candidate || typeof candidate !== "object") continue;
      for (const key of BOT_VIEW_MESSAGE_ENVELOPES) {
        if (candidate[key] && typeof candidate[key] === "object") return candidate[key];
      }
      if (candidate?.result && typeof candidate.result === "object") return candidate.result;
      if (candidate?.message_id != null || candidate?.text != null || candidate?.caption != null) return candidate;
    }
    return item || {};
  }

  function replaceTelegramMessageValue(item, nextMessage) {
    const current = telegramMessage(item);
    if (current === item) return { ...item, ...nextMessage };
    if (item?.message === current) return { ...item, message: nextMessage };
    for (const containerKey of ["payload", "content"]) {
      const container = item?.[containerKey];
      if (container === current) return { ...item, [containerKey]: nextMessage };
      if (!container || typeof container !== "object") continue;
      for (const envelope of BOT_VIEW_MESSAGE_ENVELOPES) {
        if (container[envelope] === current) return { ...item, [containerKey]: { ...container, [envelope]: nextMessage } };
      }
    }
    return { ...item, payload: nextMessage };
  }

  const messageText = (item) => {
    const message = telegramMessage(item);
    return item?.text ?? message?.text ?? item?.caption ?? message?.caption ?? item?.content?.text ?? "";
  };
  const messageTime = (item) => item?.sent_at || item?.created_at || item?.timestamp || item?.date || telegramMessage(item)?.date;
  const isOutgoing = (item) => item?.outgoing === true || item?.is_outgoing === true || ["out", "outgoing", "bot", "sent"].includes(String(item?.direction || item?.sender_type || "").toLowerCase());

  function botViewKey(botIdValue = state.selectedBotId, chatId = state.selectedConversationId) {
    return `${String(botIdValue || "")}:${String(chatId || "")}`;
  }

  const botViewNearBottom = ({ scrollHeight = 0, scrollTop = 0, clientHeight = 0 }, threshold = 72) => scrollHeight - scrollTop - clientHeight <= threshold;
  const botViewPrependScrollTop = (oldTop, oldHeight, newHeight) => Math.max(0, Number(oldTop || 0) + Math.max(0, Number(newHeight || 0) - Number(oldHeight || 0)));
  const botViewUnreadAfterInsert = (previous, added, wasNearBottom) => wasNearBottom ? 0 : Math.max(0, Number(previous || 0)) + Math.max(0, Number(added || 0));

  function emptyBotViewDraft() {
    return { text: "", files: [], reply: null, edit: null, sendMode: "media", parseMode: "", replyMarkup: null, retryClientId: null, deliveryUnknown: false, suppressEphemeralReply: false };
  }

  function messageTimeMs(item) {
    const value = messageTime(item);
    if (typeof value === "number" && Number.isFinite(value)) return value < 1_000_000_000_000 ? value * 1000 : value;
    const parsed = Date.parse(String(value || ""));
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function recentEphemeralReply(conversation, now = Date.now()) {
    if (conversation?.receiver_user_id == null) return null;
    const incoming = [...conversationMessages(conversation)].reverse().find((item) => {
      const ephemeralId = item?.ephemeral_message_id ?? telegramMessage(item)?.ephemeral_message_id;
      const sentAt = messageTimeMs(item);
      return !isOutgoing(item) && ephemeralId !== "" && ephemeralId != null && sentAt > 0 && now - sentAt >= 0 && now - sentAt <= 15_000;
    });
    if (!incoming) return null;
    return {
      ephemeral_message_id: incoming?.ephemeral_message_id ?? telegramMessage(incoming)?.ephemeral_message_id,
      action_generation: incoming?.action_generation,
      preview: messagePreview(incoming),
    };
  }

  function ephemeralMessageIsActionable(item, conversation) {
    const message = telegramMessage(item);
    const ephemeralId = item?.ephemeral_message_id ?? message?.ephemeral_message_id;
    const receiverId = item?.receiver_user_id ?? message?.receiver_user_id ?? conversation?.receiver_user_id;
    if (ephemeralId === "" || ephemeralId == null || receiverId === "" || receiverId == null) return false;
    if (typeof item?.actionable === "boolean") return item.actionable;
    const newest = [...conversationMessages(conversation)].reverse().find((candidate) => {
      const candidateMessage = telegramMessage(candidate);
      const candidateEphemeral = candidate?.ephemeral_message_id ?? candidateMessage?.ephemeral_message_id;
      const candidateReceiver = candidate?.receiver_user_id ?? candidateMessage?.receiver_user_id ?? conversation?.receiver_user_id;
      return String(candidateEphemeral ?? "") === String(ephemeralId) && String(candidateReceiver ?? "") === String(receiverId);
    });
    return Boolean(newest) && messageStableId(newest) === messageStableId(item)
      && newest?.status !== "deleted" && newest?.deleted !== true;
  }

  function effectiveReplyForConversation(draft, conversation) {
    return draft?.reply || (!draft?.suppressEphemeralReply ? recentEphemeralReply(conversation) : null);
  }

  function botViewDraft({ create = true } = {}) {
    const key = botViewKey();
    if (!state.botViewDrafts.has(key) && create) state.botViewDrafts.set(key, emptyBotViewDraft());
    return state.botViewDrafts.get(key) || emptyBotViewDraft();
  }

  function revokeDraftFiles(draft) {
    (draft?.files || []).forEach((attachment) => {
      if (attachment?.url?.startsWith?.("blob:")) URL.revokeObjectURL(attachment.url);
    });
  }

  function revokeTimelineLocalPreviews(item) {
    (item?._local_preview_urls || []).forEach((url) => {
      if (String(url || "").startsWith("blob:")) URL.revokeObjectURL(url);
    });
  }

  function telegramMessageId(item) {
    const message = telegramMessage(item);
    return item?.telegram_message_id ?? item?.message_id ?? message?.message_id ?? "";
  }

  function messageStableId(item, index = 0) {
    if (item?.id || item?.client_id) return String(item.id || item.client_id);
    if (item?.cursor !== "" && item?.cursor != null) return `cursor-${item.cursor}`;
    const ephemeral = item?.ephemeral_message_id ?? telegramMessage(item)?.ephemeral_message_id;
    const receiver = item?.receiver_user_id ?? telegramMessage(item)?.receiver_user_id;
    if (ephemeral !== "" && ephemeral != null && receiver !== "" && receiver != null) return `ephemeral-${receiver}-${ephemeral}`;
    return String(telegramMessageId(item) || `${messageTime(item) || "message"}-${index}`);
  }

  function timelineSemanticIdentity(item) {
    if (!item || typeof item !== "object") return "";
    const message = telegramMessage(item);
    const eventType = String(item?.event_type || item?.type || item?.payload?.action || "");
    const actionPayload = item?.payload && typeof item.payload === "object" ? item.payload : {};
    const request = actionPayload?.request && typeof actionPayload.request === "object" ? actionPayload.request : {};
    if (eventType === "answerGuestQuery") {
      const resultId = request?.result?.id;
      if (resultId != null) return `guest:${resultId}`;
    }
    const telegramId = telegramMessageId(item);
    if (telegramId !== "" && telegramId != null && Number(telegramId) !== 0) {
      if (String(item?.direction || "").toLowerCase() === "action") return `action:${eventType}:${telegramId}`;
      return `message:${isOutgoing(item) ? "out" : "in"}:${telegramId}`;
    }
    const ephemeralId = item?.ephemeral_message_id ?? message?.ephemeral_message_id ?? request?.ephemeral_message_id;
    const receiverId = item?.receiver_user_id ?? message?.receiver_user_id ?? message?.receiver_user?.id;
    if (ephemeralId !== "" && ephemeralId != null && receiverId !== "" && receiverId != null) {
      const date = message?.date ?? "";
      return `${String(item?.direction || "").toLowerCase() === "action" ? `action:${eventType}` : `ephemeral:${isOutgoing(item) ? "out" : "in"}`}:${receiverId}:${ephemeralId}:${date}`;
    }
    return "";
  }

  function safeMediaUrl(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    try {
      const parsed = new URL(raw, window.location.origin);
      if (parsed.protocol === "blob:") return parsed.origin === window.location.origin ? parsed.href : "";
      return ["http:", "https:"].includes(parsed.protocol) && parsed.origin === window.location.origin ? parsed.href : "";
    } catch (_) {
      return "";
    }
  }

  function safeExternalLink(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    try {
      const parsed = new URL(raw, window.location.origin);
      return ["http:", "https:", "tg:"].includes(parsed.protocol) ? parsed.href : "";
    } catch (_) {
      return "";
    }
  }

  function formatBytes(value) {
    const bytes = Number(value);
    if (!Number.isFinite(bytes) || bytes < 1) return "";
    const units = ["B", "KB", "MB", "GB"];
    const position = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    const size = bytes / (1024 ** position);
    return `${size >= 10 || position === 0 ? Math.round(size) : size.toFixed(1)} ${units[position]}`;
  }

  function normalizedAttachments(item) {
    const message = telegramMessage(item);
    const normalized = Array.isArray(item?.media) ? item.media
      : Array.isArray(item?.attachments) ? item.attachments
        : Array.isArray(item?.content?.media) ? item.content.media
        : Array.isArray(message?.media) ? message.media
          : [];
    if (normalized.length) return normalized.map((attachment) => ({ ...attachment, kind: attachment.kind || attachment.type || "document" }));
    const attachments = [];
    const photos = Array.isArray(message?.photo) ? message.photo : [];
    if (photos.length) {
      const photo = [...photos].sort((left, right) => ((left.width || 0) * (left.height || 0)) - ((right.width || 0) * (right.height || 0))).pop();
      attachments.push({ ...photo, kind: "photo" });
    }
    ["animation", "video", "video_note", "audio", "voice", "document", "sticker", "story"].forEach((kind) => {
      if (message?.[kind]) attachments.push({ ...message[kind], kind });
    });
    if (message?.live_photo) {
      const livePhoto = message.live_photo;
      const photoPart = Array.isArray(livePhoto.photo) ? livePhoto.photo[livePhoto.photo.length - 1] : livePhoto.photo || livePhoto;
      if (photoPart?.file_id) attachments.push({ ...photoPart, kind: "photo", label: "Live photo" });
      if (livePhoto.file_id) attachments.push({ ...livePhoto, kind: "live_photo", label: "Live photo" });
    }
    const paidMedia = message?.paid_media?.paid_media || message?.paid_media?.media;
    if (Array.isArray(paidMedia)) paidMedia.forEach((entry) => {
      if (Array.isArray(entry?.photo) && entry.photo.length) attachments.push({ ...entry.photo[entry.photo.length - 1], kind: "paid_photo", label: "Paid photo" });
      else if (entry?.video) attachments.push({ ...entry.video, kind: "paid_video", label: "Paid video" });
      else if (entry?.live_photo) {
        const livePhoto = entry.live_photo;
        const photo = Array.isArray(livePhoto.photo) ? livePhoto.photo[livePhoto.photo.length - 1] : null;
        if (photo?.file_id) attachments.push({ ...photo, kind: "paid_photo", label: "Paid live photo" });
        if (livePhoto.file_id) attachments.push({ ...livePhoto, kind: "paid_live_photo", label: "Paid live photo" });
      } else if (entry?.preview) attachments.push({ ...entry.preview, kind: "paid_media", label: "Paid media preview" });
    });
    if (item?._optimistic && Array.isArray(item.media)) return item.media;
    return attachments;
  }

  function attachmentUrl(attachment) {
    const supplied = safeMediaUrl(attachment?.url || attachment?.file_url || attachment?.download_url || attachment?.src || attachment?.thumbnail?.url || attachment?.thumb?.url);
    if (supplied) return supplied;
    const fileId = String(attachment?.file_id || "");
    return state.selectedBotId && /^[A-Za-z0-9_-]{1,512}$/.test(fileId) ? `${API}/bots/${encodeURIComponent(state.selectedBotId)}/media/${encodeURIComponent(fileId)}` : "";
  }

  function renderAttachment(attachment, index) {
    const kind = String(attachment?.kind || attachment?.type || "document").toLowerCase();
    const visualKind = kind === "live_photo" || kind.endsWith("_live_photo") ? "video" : kind.startsWith("paid_") ? kind.slice(5) : kind;
    const url = attachmentUrl(attachment);
    const name = attachment?.label || attachment?.file_name || attachment?.name || ({ photo: "Photo", video: "Video", animation: "Animation", video_note: "Video message", audio: "Audio", voice: "Voice message", sticker: attachment?.emoji ? `Sticker ${attachment.emoji}` : "Sticker", live_photo: "Live photo", story: "Story", media: "Paid media" }[visualKind] || "Document");
    const detail = [attachment?.mime_type, formatBytes(attachment?.file_size || attachment?.size), attachment?.duration ? `${attachment.duration}s` : ""].filter(Boolean).join(" · ");
    let content;
    if (["photo", "sticker"].includes(visualKind) && url) {
      content = `<img src="${esc(url)}" alt="${esc(name)}" loading="lazy">`;
    } else if (["video", "animation", "video_note"].includes(visualKind) && url) {
      content = `<video src="${esc(url)}" controls playsinline preload="metadata" aria-label="${esc(name)}"></video>`;
    } else if (["audio", "voice"].includes(visualKind) && url) {
      content = `<div class="message-media__audio"><span aria-hidden="true">${visualKind === "voice" ? "🎙" : "♫"}</span><audio src="${esc(url)}" controls preload="metadata"></audio></div>`;
    } else {
      const card = `<span class="message-file__icon" aria-hidden="true">${kind === "story" ? "◉" : kind.startsWith("paid_") ? "★" : "↧"}</span><span class="message-file__copy"><strong>${esc(name)}</strong>${detail ? `<small>${esc(detail)}</small>` : `<small>${url ? "Open attachment" : "Telegram file"}</small>`}</span>`;
      content = url ? `<a class="message-file" href="${esc(url)}" target="_blank" rel="noopener noreferrer" download>${card}</a>` : `<div class="message-file">${card}</div>`;
    }
    const media = `<div class="message-media message-media--${esc(kind)}" data-media-index="${index}">${content}</div>`;
    return attachment?.has_media_spoiler ? `<details class="message-spoiler"><summary>Reveal spoiler</summary>${media}</details>` : media;
  }

  function renderReplySnippet(reply) {
    if (!reply) return "";
    const author = reply.from?.first_name || reply.sender_name || reply.author || (reply.outgoing ? "Bot" : "Message");
    const text = reply.text || reply.caption || reply.preview || normalizedAttachments(reply)[0]?.kind || "Message";
    return `<div class="message-reply"><strong>${esc(author)}</strong><span>${esc(String(text).slice(0, 180))}</span></div>`;
  }

  function renderPoll(poll) {
    if (!poll) return "";
    const total = Number(poll.total_voter_count || 0);
    return `<section class="message-poll"><strong>${esc(poll.question?.text || poll.question || "Poll")}</strong>${Array.isArray(poll.options) ? poll.options.map((option) => {
      const votes = Number(option.voter_count || 0);
      const percent = total ? Math.round((votes / total) * 100) : 0;
      return `<div class="message-poll__option"><span><b>${esc(option.text || option.option || "Option")}</b><em>${percent}%</em></span><i style="--poll-result:${percent}%"></i></div>`;
    }).join("") : ""}<small>${total} ${total === 1 ? "vote" : "votes"}${poll.is_closed ? " · closed" : ""}</small></section>`;
  }

  function renderStructuredMessage(item) {
    const message = telegramMessage(item);
    const chunks = [];
    const attachments = message?.rich_message ? [] : normalizedAttachments(item);
    if (attachments.length) chunks.push(`<div class="message-media-grid${attachments.length > 1 ? " is-album" : ""}">${attachments.map(renderAttachment).join("")}</div>`);
    if (message?.contact) chunks.push(`<div class="message-card"><span aria-hidden="true">👤</span><div><strong>${esc([message.contact.first_name, message.contact.last_name].filter(Boolean).join(" ") || "Contact")}</strong><a href="tel:${esc(message.contact.phone_number || "")}">${esc(message.contact.phone_number || "No phone number")}</a></div></div>`);
    const location = message?.venue?.location || message?.location;
    if (location) {
      const latitude = Number(location.latitude);
      const longitude = Number(location.longitude);
      const mapUrl = Number.isFinite(latitude) && Number.isFinite(longitude) ? `https://www.openstreetmap.org/?mlat=${encodeURIComponent(latitude)}&mlon=${encodeURIComponent(longitude)}#map=16/${encodeURIComponent(latitude)}/${encodeURIComponent(longitude)}` : "";
      const label = message?.venue?.title || "Location";
      chunks.push(`<div class="message-card message-card--location"><span aria-hidden="true">📍</span><div><strong>${esc(label)}</strong><span>${esc(message?.venue?.address || [latitude, longitude].filter(Number.isFinite).join(", "))}</span>${mapUrl ? `<a href="${esc(mapUrl)}" target="_blank" rel="noopener noreferrer">Open map</a>` : ""}</div></div>`);
    }
    if (message?.poll) chunks.push(renderPoll(message.poll));
    if (message?.dice) chunks.push(`<div class="message-dice" aria-label="Dice result ${esc(message.dice.value)}"><span>${esc(message.dice.emoji || "🎲")}</span><strong>${esc(message.dice.value ?? "")}</strong></div>`);
    if (message?.game) chunks.push(`<div class="message-card"><span aria-hidden="true">🎮</span><div><strong>${esc(message.game.title || "Game")}</strong><span>${esc(message.game.description || "")}</span></div></div>`);
    if (message?.rich_message) chunks.push(renderRichMessage(message.rich_message));
    if (message?.checklist) chunks.push(renderChecklist(message.checklist));
    if (message?.new_chat_members) chunks.push(`<div class="message-service">${esc(message.new_chat_members.map((member) => member.first_name || member.username || "Member").join(", "))} joined the chat</div>`);
    if (message?.left_chat_member) chunks.push(`<div class="message-service">${esc(message.left_chat_member.first_name || message.left_chat_member.username || "Member")} left the chat</div>`);
    if (message?.pinned_message) chunks.push(`<div class="message-service">Pinned: ${esc(messageText(message.pinned_message) || "message")}</div>`);
    if (message?.forward_origin) chunks.unshift(`<div class="message-service">Forwarded from ${esc(message.forward_origin.sender_user?.first_name || message.forward_origin.sender_user_name || message.forward_origin.chat?.title || message.forward_origin.type || "another chat")}</div>`);
    if (message?.checklist_tasks_done) chunks.push(`<div class="message-service">Checklist updated · ${esc((message.checklist_tasks_done.checklist_task_ids || []).length)} completed</div>`);
    if (message?.checklist_tasks_added) chunks.push(`<div class="message-service">Checklist updated · ${esc((message.checklist_tasks_added.tasks || []).length)} added</div>`);
    if (message?.invoice) chunks.push(`<div class="message-card"><span aria-hidden="true">🧾</span><div><strong>${esc(message.invoice.title || "Invoice")}</strong><span>${esc(message.invoice.description || "")}</span><b>${esc(message.invoice.total_amount ?? "")} ${esc(message.invoice.currency || "")}</b></div></div>`);
    if (message?.successful_payment) chunks.push(`<div class="message-card"><span aria-hidden="true">✓</span><div><strong>Payment received</strong><span>${esc(message.successful_payment.total_amount ?? "")} ${esc(message.successful_payment.currency || "")}</span></div></div>`);
    if (message?.refunded_payment) chunks.push(`<div class="message-card"><span aria-hidden="true">↩</span><div><strong>Payment refunded</strong><span>${esc(message.refunded_payment.total_amount ?? "")} ${esc(message.refunded_payment.currency || "")}</span></div></div>`);
    const serviceLabels = {
      forum_topic_created: "Forum topic created", forum_topic_closed: "Forum topic closed", forum_topic_reopened: "Forum topic reopened", forum_topic_edited: "Forum topic edited",
      video_chat_scheduled: "Video chat scheduled", video_chat_started: "Video chat started", video_chat_ended: "Video chat ended", video_chat_participants_invited: "Participants invited",
      suggested_post_approved: "Suggested post approved", suggested_post_declined: "Suggested post declined", suggested_post_paid: "Suggested post paid", suggested_post_refunded: "Suggested post refunded",
      giveaway_created: "Giveaway created", giveaway_completed: "Giveaway completed", giveaway_winners: "Giveaway winners announced", boost_added: "Chat boost added",
    };
    Object.entries(serviceLabels).forEach(([field, label]) => { if (message?.[field]) chunks.push(`<div class="message-service">${esc(label)}</div>`); });
    if (Array.isArray(message?.reactions) && message.reactions.length) chunks.push(`<div class="message-reactions">${message.reactions.map((reaction) => `<span>${esc(reaction.emoji || reaction.type || "Reaction")}${reaction.count ? ` ${esc(reaction.count)}` : ""}</span>`).join("")}</div>`);
    if (Array.isArray(item?._callback_events)) item._callback_events.forEach((callback) => {
      chunks.push(renderCallbackEvent(callback));
    });
    return chunks.join("");
  }

  function renderRichMessage(richMessage) {
    const blocks = Array.isArray(richMessage) ? richMessage : Array.isArray(richMessage?.blocks) ? richMessage.blocks : [richMessage];
    return `<section class="message-rich"><span class="message-rich__label">Rich message</span>${renderRichBlocks(blocks)}</section>`;
  }

  function richTextPlain(value) {
    if (typeof value === "string" || typeof value === "number") return String(value);
    if (Array.isArray(value)) return value.map(richTextPlain).join("");
    if (!value || typeof value !== "object") return "";
    return richTextPlain(value.text ?? value.children ?? value.content ?? value.value ?? "");
  }

  function renderRichText(value, depth = 0) {
    if (depth > 20) return esc(richTextPlain(value));
    if (typeof value === "string" || typeof value === "number") return esc(String(value));
    if (Array.isArray(value)) return value.map((part) => renderRichText(part, depth + 1)).join("");
    if (!value || typeof value !== "object") return "";
    const type = String(value.type || "plain").toLowerCase();
    const inner = renderRichText(value.text ?? value.children ?? value.content ?? value.value ?? "", depth + 1);
    const tag = {
      bold: "strong", italic: "em", underline: "u", strikethrough: "s", code: "code",
      monospace: "code", subscript: "sub", superscript: "sup", marked: "mark",
    }[type];
    if (tag) return `<${tag}>${inner}</${tag}>`;
    if (["spoiler", "hidden"].includes(type)) return `<span class="text-spoiler" tabindex="0">${inner}</span>`;
    if (["url", "text_url", "link"].includes(type)) {
      const href = safeExternalLink(value.url || value.href || richTextPlain(value.text));
      return href ? `<a href="${esc(href)}" target="_blank" rel="noopener noreferrer">${inner || esc(href)}</a>` : inner;
    }
    if (type === "email") {
      const address = String(value.email || richTextPlain(value.text) || "").trim();
      return /^[^\s@]+@[^\s@]+$/.test(address) ? `<a href="mailto:${esc(address)}">${inner || esc(address)}</a>` : inner;
    }
    if (["phone", "phone_number"].includes(type)) {
      const number = String(value.phone_number || value.phone || richTextPlain(value.text) || "").trim();
      return number && /^[+0-9().\-\s]+$/.test(number) ? `<a href="tel:${esc(number)}">${inner || esc(number)}</a>` : inner;
    }
    return inner || esc(richTextPlain(value));
  }

  function renderRichBlocks(blocks) {
    return (Array.isArray(blocks) ? blocks : [blocks]).map((block) => {
      if (typeof block === "string") return `<p>${esc(block)}</p>`;
      if (!block || typeof block !== "object") return "";
      const type = String(block.type || "paragraph");
      const sourceText = block.text ?? block.expression ?? "";
      const text = richTextPlain(sourceText);
      const richText = renderRichText(sourceText);
      if (type === "divider") return "<hr>";
      if (["heading", "section_heading"].includes(type)) return `<h4>${richText}</h4>`;
      if (["pre", "preformatted"].includes(type)) return `<pre><code>${esc(text)}</code></pre>`;
      if (["paragraph", "footer", "mathematical_expression", "anchor", "thinking"].includes(type)) return `<p class="rich-${esc(type)}">${richText || esc(block.name || (type === "thinking" ? "Thinking…" : ""))}</p>`;
      if (["blockquote", "pullquote", "block_quotation", "pull_quotation"].includes(type)) return `<blockquote>${block.blocks ? renderRichBlocks(block.blocks) : richText}${block.credit ? `<cite>${renderRichText(block.credit)}</cite>` : ""}</blockquote>`;
      if (type === "details") return `<details${block.is_open ? " open" : ""}><summary>${renderRichText(block.summary) || "Details"}</summary>${renderRichBlocks(block.blocks || [])}</details>`;
      if (type === "list") return `<ul>${(block.items || []).map((item) => `<li>${item.has_checkbox ? `<input type="checkbox" disabled${item.is_checked ? " checked" : ""}>` : item.label ? `<span>${renderRichText(item.label)}</span>` : ""}${renderRichBlocks(item.blocks || [])}</li>`).join("")}</ul>`;
      if (type === "table") return `<div class="message-rich__table"><table>${(block.cells || []).map((row) => `<tr>${(row || []).map((cell) => {
        const tag = cell?.is_header ? "th" : "td";
        const colspan = Math.max(1, Math.min(20, Number(cell?.colspan) || 1));
        const rowspan = Math.max(1, Math.min(100, Number(cell?.rowspan) || 1));
        const content = cell?.blocks ? renderRichBlocks(cell.blocks) : renderRichText(cell?.text ?? cell ?? "");
        return `<${tag}${colspan > 1 ? ` colspan="${colspan}"` : ""}${rowspan > 1 ? ` rowspan="${rowspan}"` : ""}>${content}</${tag}>`;
      }).join("")}</tr>`).join("")}</table></div>`;
      if (["collage", "slideshow"].includes(type)) return `<div class="message-rich__gallery">${renderRichBlocks(block.blocks || [])}</div>${block.caption ? `<p>${renderRichText(block.caption)}</p>` : ""}`;
      if (type === "map" && block.location) return renderStructuredMessage({ location: block.location, venue: block.caption ? { location: block.location, title: richTextPlain(block.caption) } : null });
      const media = block.photo || block.video || block.animation || block.audio || block.voice_note;
      if (media) return `${renderAttachment({ ...(Array.isArray(media) ? media[media.length - 1] : media), kind: type === "voice_note" ? "voice" : type, has_media_spoiler: block.has_spoiler }, 0)}${block.caption ? `<p>${renderRichText(block.caption)}</p>` : ""}`;
      return `<div class="message-rich__unsupported"><strong>${esc(type.replaceAll("_", " "))}</strong>${text ? `<p>${richText}</p>` : ""}</div>`;
    }).join("");
  }

  function renderChecklist(checklist) {
    const tasks = Array.isArray(checklist?.tasks) ? checklist.tasks : [];
    return `<section class="message-checklist"><strong>${esc(richTextPlain(checklist?.title) || "Checklist")}</strong>${tasks.map((task) => `<label><input type="checkbox" disabled${task.completed_by_user || task.completed_by_chat || task.completion_date ? " checked" : ""}><span>${esc(richTextPlain(task.text) || "Task")}</span></label>`).join("")}</section>`;
  }

  function renderMessageText(text, entities = []) {
    const source = String(text || "");
    if (!Array.isArray(entities) || !entities.length) return esc(source);
    const safeEntities = [...entities].filter((entity) => Number.isInteger(entity?.offset) && Number.isInteger(entity?.length) && entity.length > 0 && entity.offset >= 0 && entity.offset < source.length).map((entity) => ({ ...entity, end: Math.min(source.length, entity.offset + entity.length) })).sort((left, right) => left.offset - right.offset || right.end - left.end);
    const wrap = (entity, inner) => {
      const segment = source.slice(entity.offset, entity.end);
      const type = String(entity.type || "");
      const href = type === "text_link" ? safeExternalLink(entity.url) : type === "url" ? safeExternalLink(segment) : "";
      if (href) return `<a href="${esc(href)}" target="_blank" rel="noopener noreferrer">${inner}</a>`;
      const tag = { bold: "strong", italic: "em", underline: "u", strikethrough: "s", code: "code", pre: "code", spoiler: "span" }[type];
      return tag ? `<${tag}${type === "spoiler" ? ' class="text-spoiler" tabindex="0"' : ""}>${inner}</${tag}>` : inner;
    };
    const renderRange = (start, end, candidates) => {
      let cursor = start;
      const chunks = [];
      candidates.forEach((entity) => {
        if (entity.offset < cursor || entity.offset >= end || entity.end > end) return;
        chunks.push(esc(source.slice(cursor, entity.offset)));
        const children = candidates.filter((candidate) => candidate !== entity && candidate.offset >= entity.offset && candidate.end <= entity.end);
        chunks.push(wrap(entity, renderRange(entity.offset, entity.end, children)));
        cursor = entity.end;
      });
      chunks.push(esc(source.slice(cursor, end)));
      return chunks.join("");
    };
    return renderRange(0, source.length, safeEntities);
  }

  function renderReplyMarkup(message) {
    const rows = message?.reply_markup?.inline_keyboard;
    if (!Array.isArray(rows)) return "";
    return `<div class="message-keyboard">${rows.map((row) => `<div>${(Array.isArray(row) ? row : []).map((button) => {
      const label = esc(button?.text || "Button");
      const url = safeExternalLink(button?.url || button?.web_app?.url);
      return url ? `<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${label}</a>` : `<span>${label}</span>`;
    }).join("")}</div>`).join("")}</div>`;
  }

  function messagePreview(item) {
    const text = messageText(item);
    if (text) return text;
    const message = telegramMessage(item);
    const attachment = normalizedAttachments(item)[0];
    if (attachment) return `${attachment.kind === "photo" ? "📷" : "📎"} ${attachment.file_name || attachment.kind}`;
    if (message?.poll) return `📊 ${message.poll.question?.text || message.poll.question || "Poll"}`;
    if (message?.location || message?.venue) return "📍 Location";
    if (message?.contact) return "👤 Contact";
    if (message?.dice) return `${message.dice.emoji || "🎲"} ${message.dice.value || ""}`.trim();
    return item?.event_type || item?.type || "Message";
  }

  function callbackEventStableId(callback, item) {
    return String(callback?._event_id || callback?.id || item?._event_id || item?.id || item?.cursor || `${callback?.from?.id || "actor"}:${callback?.message?.message_id || callback?.message_id || "message"}:${callback?.data || callback?.game_short_name || "button"}`);
  }

  function callbackEventFromTimelineItem(item) {
    const raw = item?.payload?.callback_query || item?.callback_query;
    if (raw) return {
      ...raw,
      _event_id: item?.id || item?.cursor || raw?.id,
      _actionable: item?.actionable,
      _action_generation: item?.action_generation,
    };
    const content = item?.content;
    if (content?.kind !== "callback_query") return null;
    return {
      _event_id: item?.id || item?.cursor,
      from: content.actor,
      data: content.data,
      game_short_name: content.game_short_name,
      message_id: content.target_message_id,
      _actionable: item?.actionable,
      _action_generation: item?.action_generation,
    };
  }

  function renderCallbackEvent(callback) {
    const actor = callback?.from?.first_name || callback?.from?.username || callback?.from?.title || "A user";
    const label = callback?.data || callback?.game_short_name || "an inline button";
    const canAnswer = callback?._actionable !== false && callback?._action_generation != null && callback?._action_generation !== "";
    return `<div class="message-service message-callback"><span>${esc(actor)} pressed <strong>${esc(label)}</strong></span>${canAnswer ? `<button type="button" data-action="open-callback-answer" data-action-generation="${esc(callback._action_generation)}" aria-label="Answer callback query">Answer</button>` : '<small>Answered</small>'}</div>`;
  }

  function withCallbackEvent(target, callback, item) {
    const event = {
      ...(callback || item?.payload || item),
      _actionable: callback?._actionable ?? item?.actionable,
      _action_generation: callback?._action_generation ?? item?.action_generation,
    };
    const eventId = callbackEventStableId(callback, item);
    const events = [...(target?._callback_events || [])];
    if (!events.some((candidate) => callbackEventStableId(candidate, candidate) === eventId)) events.push({ ...event, _event_id: eventId });
    return { ...target, _callback_events: events };
  }

  function findLastMessageIndexByTelegramId(messages, targetId) {
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      if (String(telegramMessageId(messages[index])) === String(targetId)) return index;
    }
    return -1;
  }

  function collapseMediaGroups(messages) {
    const result = [];
    const groups = new Map();
    messages.forEach((item) => {
      const eventType = String(item?.event_type || item?.type || "");
      const callback = callbackEventFromTimelineItem(item);
      if (eventType === "callback_query" || callback) {
        const targetId = callback?.message?.message_id ?? callback?.message_id;
        const targetIndex = findLastMessageIndexByTelegramId(result, targetId);
        if (targetIndex >= 0) result[targetIndex] = withCallbackEvent(result[targetIndex], callback, item);
        else result.push({ ...item, _timeline_callback_event: callback });
        return;
      }
      if (["poll", "poll_answer"].includes(eventType)) {
        const poll = item?.payload?.poll || item?.poll;
        const pollId = poll?.id || item?.payload?.poll_answer?.poll_id || item?.poll_answer?.poll_id;
        const target = [...result].reverse().find((candidate) => String(telegramMessage(candidate)?.poll?.id || "") === String(pollId || ""));
        if (target && poll) telegramMessage(target).poll = poll;
        return;
      }
      const message = telegramMessage(item);
      const groupId = item?.media_group_id || message?.media_group_id;
      if (!groupId) { result.push(item); return; }
      const groupKey = `${isOutgoing(item) ? "out" : "in"}:${groupId}`;
      const existing = groups.get(groupKey);
      if (!existing) {
        const grouped = { ...item, media: [...normalizedAttachments(item)], _media_group: groupId };
        groups.set(groupKey, grouped);
        result.push(grouped);
      } else {
        existing.media.push(...normalizedAttachments(item));
        if (!messageText(existing) && messageText(item)) existing.caption = messageText(item);
      }
    });
    return result;
  }

  function renderBotView() {
    const bot = currentBot();
    if (!bot) return `<div class="page">${renderNoBots()}</div>`;
    return `<div class="page page--wide page--bot-view">
      ${pageHeader("Bot View", `See conversations exactly as ${botName(bot)} does, then reply with clear operator intent.`, `<a class="btn btn--secondary btn--sm" href="${botPath(botId(bot), "updates")}">${icon("pulse")}Raw updates</a><span class="badge badge--success">Access audited</span>`)}
      <div id="bot-view-error" aria-live="polite">${renderBotViewError()}</div>
      <section class="bot-view ${state.selectedConversationId ? "has-chat" : ""}">${renderConversationList()}${renderChatPane(bot)}</section>
    </div>`;
  }

  function renderBotViewError() {
    return state.errors.conversations ? `<div class="status-banner status-banner--danger">${icon("alert")}<div class="status-banner__copy"><strong>Conversations unavailable</strong>${esc(state.errors.conversations)}</div><button class="btn btn--sm btn--secondary" data-action="retry-conversations">Retry</button></div>` : "";
  }

  function applyConversationFilter(value) {
    const query = String(value || "").trim().toLowerCase();
    document.querySelectorAll("#conversation-items .conversation").forEach((item) => {
      item.hidden = Boolean(query && !item.textContent.toLowerCase().includes(query));
    });
  }

  function captureBotViewUiState() {
    saveBotViewDraftFromDom();
    const search = document.querySelector("#conversation-search");
    const composer = document.querySelector("#message-form textarea");
    const timeline = document.querySelector("#chat-timeline");
    return {
      conversationId: String(state.selectedConversationId || ""),
      searchValue: search?.value || "",
      searchFocused: document.activeElement === search,
      composerValue: composer?.value || "",
      composerFocused: document.activeElement === composer,
      composerSelectionStart: composer?.selectionStart,
      composerSelectionEnd: composer?.selectionEnd,
      timelineScrollTop: timeline?.scrollTop || 0,
      timelineWasNearBottom: timeline ? botViewNearBottom(timeline) : true,
      renderedMessageCount: timeline?.querySelectorAll(".message, .message-event").length || 0,
    };
  }

  function restoreBotViewUiState(snapshot) {
    const search = document.querySelector("#conversation-search");
    if (search) {
      search.value = snapshot.searchValue;
      applyConversationFilter(snapshot.searchValue);
      if (snapshot.searchFocused) search.focus({ preventScroll: true });
    }
    if (snapshot.conversationId !== String(state.selectedConversationId || "")) return;
    const composer = document.querySelector("#message-form textarea");
    if (composer) {
      composer.value = botViewDraft().text || snapshot.composerValue;
      resizeBotViewComposer(composer);
      if (snapshot.composerFocused) {
        composer.focus({ preventScroll: true });
        if (Number.isInteger(snapshot.composerSelectionStart) && Number.isInteger(snapshot.composerSelectionEnd)) {
          composer.setSelectionRange(snapshot.composerSelectionStart, snapshot.composerSelectionEnd);
        }
      }
    }
    const timeline = document.querySelector("#chat-timeline");
    if (timeline) {
      const key = botViewKey();
      const nextCount = timeline.querySelectorAll(".message, .message-event").length;
      const prior = state.botViewScrollState.get(key) || {};
      const added = Math.max(0, nextCount - snapshot.renderedMessageCount);
      timeline.scrollTop = snapshot.timelineWasNearBottom
        ? timeline.scrollHeight
        : Math.min(snapshot.timelineScrollTop, Math.max(0, timeline.scrollHeight - timeline.clientHeight));
      state.botViewScrollState.set(key, {
        ...prior,
        initialized: true,
        top: timeline.scrollTop,
        nearBottom: snapshot.timelineWasNearBottom,
        messageCount: nextCount,
        unread: botViewUnreadAfterInsert(prior.unread, added, snapshot.timelineWasNearBottom),
      });
      updateScrollLatestControl();
      observeBotViewMedia();
    }
  }

  function renderBotViewLive() {
    if (state.route.name !== "bot-view") return;
    const bot = currentBot();
    const view = document.querySelector(".bot-view");
    if (!bot || !view) return;
    const snapshot = captureBotViewUiState();
    const error = document.querySelector("#bot-view-error");
    if (error) error.innerHTML = renderBotViewError();
    view.className = `bot-view ${state.selectedConversationId ? "has-chat" : ""}`;
    view.innerHTML = `${renderConversationList()}${renderChatPane(bot)}`;
    restoreBotViewUiState(snapshot);
  }

  function saveBotViewDraftFromDom() {
    if (!state.selectedConversationId) return;
    const composer = document.querySelector("#message-form textarea");
    if (!composer) return;
    const draft = botViewDraft();
    draft.text = composer.value;
  }

  function resizeBotViewComposer(textarea) {
    if (!textarea) return;
    textarea.style.height = "0px";
    textarea.style.height = `${Math.min(144, Math.max(42, textarea.scrollHeight))}px`;
  }

  function updateBotViewPrimaryAction() {
    const textarea = document.querySelector("#message-form textarea");
    const button = document.querySelector("#message-form .composer-send, #message-form .composer-record");
    if (!textarea || !button) return;
    const draft = botViewDraft();
    const conversation = state.conversations.find((item) => conversationId(item) === String(state.selectedConversationId));
    const guest = Boolean(conversation?.guest_query_id);
    const canSend = Boolean(textarea.value.trim() || draft.files.length || draft.edit);
    button.type = canSend || guest ? "submit" : "button";
    button.classList.toggle("composer-send", canSend || guest);
    button.classList.toggle("composer-record", !canSend && !guest);
    button.disabled = guest && !canSend;
    if (canSend || guest) button.removeAttribute("data-action");
    else button.dataset.action = "start-voice-recording";
    button.setAttribute("aria-label", canSend ? (draft.edit ? "Save edited message" : "Send message") : guest ? "Write a text reply" : "Record voice message");
    button.innerHTML = canSend || guest ? icon(draft.edit ? "check" : "send") : "●";
  }

  function initializeBotViewDom() {
    if (state.route.name !== "bot-view" || !state.selectedConversationId) return;
    const timeline = document.querySelector("#chat-timeline");
    if (!timeline) return;
    const key = botViewKey();
    const existing = state.botViewScrollState.get(key);
    if (!existing?.initialized || existing.nearBottom !== false) timeline.scrollTop = timeline.scrollHeight;
    else timeline.scrollTop = Math.min(existing.top || 0, Math.max(0, timeline.scrollHeight - timeline.clientHeight));
    const nearBottom = botViewNearBottom(timeline);
    state.botViewScrollState.set(key, {
      ...existing,
      initialized: true,
      top: timeline.scrollTop,
      nearBottom,
      unread: nearBottom ? 0 : existing?.unread || 0,
      messageCount: timeline.querySelectorAll(".message, .message-event").length,
    });
    resizeBotViewComposer(document.querySelector("#message-form textarea"));
    updateScrollLatestControl();
    observeBotViewMedia();
  }

  function observeBotViewMedia() {
    state.botViewTimelineResizeObserver?.disconnect?.();
    state.botViewTimelineResizeObserver = null;
    const timeline = document.querySelector("#chat-timeline");
    if (!timeline) return;
    const key = botViewKey();
    const repin = () => {
      if (key !== botViewKey() || !timeline.isConnected) return;
      const scrollState = state.botViewScrollState.get(key);
      if (scrollState?.nearBottom) {
        timeline.scrollTop = timeline.scrollHeight;
        scrollState.top = timeline.scrollTop;
      }
    };
    timeline.querySelectorAll("img, video, audio").forEach((media) => {
      media.addEventListener("load", repin, { once: true });
      media.addEventListener("loadedmetadata", repin, { once: true });
    });
    if (typeof ResizeObserver === "function") {
      let previousHeight = timeline.scrollHeight;
      const observer = new ResizeObserver(() => {
        if (timeline.scrollHeight !== previousHeight) {
          previousHeight = timeline.scrollHeight;
          repin();
        }
      });
      timeline.querySelectorAll(".message, .message-event").forEach((message) => observer.observe(message));
      state.botViewTimelineResizeObserver = observer;
    }
  }

  function updateScrollLatestControl() {
    const key = botViewKey();
    const timeline = document.querySelector("#chat-timeline");
    const button = document.querySelector(".scroll-latest");
    if (!timeline || !button) return;
    const current = state.botViewScrollState.get(key) || {};
    const nearBottom = botViewNearBottom(timeline);
    current.top = timeline.scrollTop;
    current.nearBottom = nearBottom;
    if (nearBottom) current.unread = 0;
    state.botViewScrollState.set(key, current);
    button.classList.toggle("is-visible", !nearBottom);
    button.innerHTML = `↓${current.unread ? `<span>${esc(current.unread > 99 ? "99+" : current.unread)}</span>` : ""}`;
  }

  function renderConversationList() {
    const items = state.loading.conversations && !state.conversations.length
      ? `<div class="panel__body skeleton-stack skeleton-stack--conversations"><div class="skeleton"></div><div class="skeleton"></div><div class="skeleton"></div></div>`
      : state.conversations.length
        ? state.conversations.map((item) => {
          const id = conversationId(item);
          const messages = conversationMessages(item);
          const last = item.last_message || messages[messages.length - 1] || {};
          const contextLabel = conversationContextLabel(item);
          return `<button class="conversation ${String(state.selectedConversationId) === id ? "active" : ""}" type="button" data-action="select-conversation" data-conversation-id="${esc(id)}"><span class="chat-avatar">${initials(conversationTitle(item))}</span><span class="conversation__copy"><span class="conversation__line"><strong>${esc(conversationTitle(item))}</strong><time>${esc(formatDate(item.last_update_at || item.updated_at || messageTime(last), "time"))}</time></span>${contextLabel ? `<small class="conversation__context">${esc(contextLabel)}</small>` : ""}<span class="conversation__preview">${esc(item.last_message_preview || item.last_message_text || messagePreview(last) || "No messages yet")}</span></span></button>`;
        }).join("")
        : `<div class="empty-state"><span class="empty-state__icon">${icon("message")}</span><h3>No conversations yet</h3><p>Chats appear after this bot receives message updates.</p></div>`;
    return `<aside class="conversation-list"><div class="conversation-list__head"><h2>Conversations</h2><div class="toolbar__search">${icon("search")}<input class="search-input" id="conversation-search" type="search" placeholder="Search name or chat ID" aria-label="Search conversations"></div></div><div class="conversation-list__items" id="conversation-items">${items}</div></aside>`;
  }

  function renderChatPane(bot) {
    const conversation = state.conversations.find((item) => conversationId(item) === String(state.selectedConversationId));
    if (!conversation) return `<section class="chat-pane"><div class="empty-state empty-state--fill"><span class="empty-state__icon">${icon("message")}</span><h2>Select a conversation</h2><p>Choose a chat to inspect the timeline and reply as ${esc(botUsername(bot))}.</p></div></section>`;
    const key = botViewKey(botId(bot), conversationId(conversation));
    const optimistic = state.botViewOptimisticMessages.get(key) || [];
    const messages = collapseMediaGroups([...conversationMessages(conversation), ...optimistic]);
    const sending = botViewSendIsInFlight(botId(bot), conversationId(conversation));
    const draft = botViewDraft();
    const scrollState = state.botViewScrollState.get(key) || {};
    const nextBefore = state.botViewMessageNextBefore.get(key);
    const older = nextBefore ? `<div class="load-older"><button class="btn btn--secondary btn--sm" type="button" data-action="load-older-messages"${state.botViewLoadingOlder ? " disabled" : ""}>${state.botViewLoadingOlder ? `${icon("refresh")} Loading…` : "Load earlier messages"}</button></div>` : "";
    const contextLabel = conversationContextLabel(conversation);
    const selecting = state.botViewBulkModeKey === key;
    const selected = state.botViewBulkSelection.get(key) || new Set();
    const bulkCandidates = messages.filter((item) => {
      const messageIdValue = telegramMessageId(item);
      const ephemeralId = item?.ephemeral_message_id ?? telegramMessage(item)?.ephemeral_message_id;
      return messageIdValue !== "" && messageIdValue != null && Number(messageIdValue) !== 0 && (ephemeralId === "" || ephemeralId == null) && item?.status !== "deleted";
    });
    const bulkBar = selecting ? `<div class="chat-bulk-bar"><strong>${selected.size ? `${esc(selected.size)} selected` : "Select messages"}</strong><details><summary>Choose from this page</summary><div class="chat-bulk-picker">${bulkCandidates.map((item) => { const messageIdValue = String(telegramMessageId(item)); return `<label><input type="checkbox" data-action="toggle-selected-message" data-telegram-message-id="${esc(messageIdValue)}"${selected.has(messageIdValue) ? " checked" : ""}><span>${esc(messagePreview(item).slice(0, 80))}</span><small>#${esc(messageIdValue)}</small></label>`; }).join("") || "<span>No deletable messages on this page.</span>"}</div></details><button class="btn btn--ghost btn--sm" type="button" data-action="cancel-bulk-select">Cancel</button><button class="btn btn--danger btn--sm" type="button" data-action="delete-selected-messages"${selected.size ? "" : " disabled"}>Delete selected</button></div>` : "";
    return `<section class="chat-pane${selecting ? " is-selecting" : ""}" data-chat-key="${esc(key)}"><header class="chat-pane__head"><button class="btn btn--ghost btn--icon chat-back" type="button" data-action="chat-back" aria-label="Back to conversations">${icon("arrow", "" )}</button><span class="chat-avatar">${initials(conversationTitle(conversation))}</span><span class="chat-pane__head-copy"><strong>${esc(conversationTitle(conversation))}</strong><span>${esc(conversation.username ? `@${String(conversation.username).replace(/^@/, "")} · ` : "")}chat_id: ${esc(conversationChatId(conversation))}${contextLabel ? ` · ${esc(contextLabel)}` : ""}</span></span><button class="btn btn--ghost btn--sm" type="button" data-action="toggle-bulk-select" aria-pressed="${selecting ? "true" : "false"}">${selecting ? "Selecting" : "Select"}</button><span class="badge badge--success">Bot can reply</span></header>${bulkBar}
      <div class="timeline-wrap"><div class="timeline" id="chat-timeline" tabindex="0" role="log" aria-live="polite" aria-relevant="additions">${messages.length ? `${older}<div class="timeline-day"><span>Conversation history</span></div>${messages.map(renderMessage).join("")}` : `<div class="empty-state"><span class="empty-state__icon">${icon("message")}</span><h3>No message history</h3><p>This conversation exists, but no message payloads were returned.</p></div>`}</div><button class="scroll-latest ${scrollState.nearBottom === false ? "is-visible" : ""}" type="button" data-action="scroll-latest" aria-label="Scroll to latest message">↓${scrollState.unread ? `<span>${esc(scrollState.unread > 99 ? "99+" : scrollState.unread)}</span>` : ""}</button><div class="drop-target" aria-hidden="true"><span>＋</span><strong>Drop files to send</strong><small>Up to ${BOT_VIEW_MAX_FILES} files</small></div></div>
      ${renderComposer(bot, conversation, draft, sending)}
    </section>`;
  }

  function renderComposer(bot, conversation, draft, sending) {
    const key = botViewKey(botId(bot), conversationId(conversation));
    const panel = state.botViewOpenPanel?.key === key ? state.botViewOpenPanel.name : null;
    const files = draft.files || [];
    const recorder = state.botViewRecorder?.key === key ? state.botViewRecorder : null;
    const guest = Boolean(conversation.guest_query_id);
    const ephemeral = conversation.receiver_user_id != null;
    const ephemeralEdit = draft.edit?.ephemeral_message_id !== "" && draft.edit?.ephemeral_message_id != null;
    const recentEphemeral = draft.suppressEphemeralReply ? null : recentEphemeralReply(conversation);
    const placeholder = draft.edit ? "Edit message…" : files.length ? "Add a caption…" : "Write a message…";
    const context = draft.edit || draft.reply || (!draft.edit ? recentEphemeral : null);
    const contextLabel = draft.edit ? "Editing message" : draft.reply ? "Replying to" : "Ephemeral reply window";
    const progress = state.botViewUploadProgress?.key === key ? state.botViewUploadProgress : null;
    return `<footer class="composer${files.length ? " has-files" : ""}${recorder ? " is-recording" : ""}">
      ${context ? `<div class="composer-context"><span class="composer-context__bar"></span><div><strong>${esc(contextLabel)}</strong><span>${esc(context.preview || "Message")}</span></div><button type="button" data-action="cancel-message-context"${!draft.edit && !draft.reply ? ' data-auto-ephemeral="true"' : ""} aria-label="Cancel ${draft.edit ? "editing" : "reply"}">${icon("close")}</button></div>` : ""}
      ${files.length ? `<div class="attachment-strip" aria-label="Selected attachments">${files.map((attachment) => `<figure class="attachment-preview">${attachment.type.startsWith("image/") ? `<img src="${esc(attachment.url)}" alt="">` : attachment.type.startsWith("video/") ? `<video src="${esc(attachment.url)}" muted></video>` : attachment.type.startsWith("audio/") ? `<audio src="${esc(attachment.url)}" controls preload="metadata"></audio>` : `<span aria-hidden="true">↧</span>`}<figcaption><strong>${esc(attachment.name)}</strong><small>${esc(formatBytes(attachment.size))}</small></figcaption><button type="button" data-action="remove-attachment" data-attachment-id="${esc(attachment.id)}" aria-label="Remove ${esc(attachment.name)}">${icon("close")}</button></figure>`).join("")}</div><div class="attachment-mode" role="radiogroup" aria-label="Attachment delivery"><button type="button" role="radio" aria-checked="${draft.sendMode !== "document"}" class="${draft.sendMode !== "document" ? "active" : ""}" data-action="set-attachment-mode" data-mode="media">Media / album</button><button type="button" role="radio" aria-checked="${draft.sendMode === "document"}" class="${draft.sendMode === "document" ? "active" : ""}" data-action="set-attachment-mode" data-mode="document">As files</button></div>` : ""}
      ${progress ? `<div class="upload-progress" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${esc(progress.percent || 0)}"><i style="--upload-progress:${esc(progress.percent || 0)}%"></i><span>${progress.percent ? `Uploading ${progress.percent}%` : "Preparing upload…"}</span></div>` : ""}
      ${draft.parseMode || draft.replyMarkup ? `<div class="composer-options">${draft.parseMode ? `<span>${esc(draft.parseMode)}</span>` : ""}${draft.replyMarkup ? `<span>${esc(draft.replyMarkup.inline_keyboard?.length || 0)} button row(s)</span>` : ""}<button type="button" data-action="clear-composer-options">Clear</button></div>` : ""}
      ${recorder ? `<div class="voice-recorder" role="status"><span class="voice-recorder__pulse" aria-hidden="true"></span><strong>Recording voice message</strong><time>00:00</time><button type="button" data-action="cancel-voice-recording">Cancel</button><button class="btn btn--primary btn--sm" type="button" data-action="stop-voice-recording">Stop</button></div>` : `<form class="composer__form" id="message-form"${sending ? ' aria-busy="true"' : ""}>
        <input class="visually-hidden" id="message-attachments" name="attachments" type="file" multiple accept="image/*,video/*,audio/*,.pdf,.zip,.txt,.csv,.doc,.docx,.xls,.xlsx,.ppt,.pptx" tabindex="-1">
        <button class="composer-tool" type="button" data-action="toggle-composer-panel" data-panel="actions" aria-label="Attach or send another message type" aria-expanded="${panel === "actions"}"${guest || ephemeralEdit ? ` disabled title="${guest ? "Guest replies support text only" : "Telegram does not accept new uploads when editing ephemeral media"}"` : ""}><span aria-hidden="true">＋</span></button>
        <div class="composer-input"><textarea name="text" rows="1" maxlength="${files.length ? 1024 : 4096}" placeholder="${esc(placeholder)}" aria-label="${esc(placeholder)}"${sending ? " disabled" : ""}>${esc(draft.text || "")}</textarea><button class="composer-emoji" type="button" data-action="toggle-composer-panel" data-panel="emoji" aria-label="Choose emoji" aria-expanded="${panel === "emoji"}">☺</button></div>
        ${sending ? `<button class="btn btn--primary btn--icon composer-send" type="button" aria-label="Sending message" disabled>${icon("refresh")}</button>` : draft.text.trim() || files.length || draft.edit ? `<button class="btn btn--primary btn--icon composer-send" type="submit" aria-label="${draft.edit ? "Save edited message" : "Send message"}">${icon(draft.edit ? "check" : "send")}</button>` : guest ? `<button class="btn btn--primary btn--icon composer-send" type="submit" aria-label="Write a text reply" disabled>${icon("send")}</button>` : `<button class="btn btn--primary btn--icon composer-record" type="button" data-action="start-voice-recording" aria-label="Record voice message">●</button>`}
      </form>`}
      ${renderComposerPanel(panel, conversation)}
      <div class="composer__meta"><span>${guest ? "Guest query · text reply as" : ephemeral ? recentEphemeral ? "Ephemeral · recent reply window · as" : "Ephemeral · outside reply window; bot must be an admin · as" : "Reply as"} <strong>${esc(botUsername(bot))}</strong></span><span><kbd>Enter</kbd> send · <kbd>Shift</kbd>+<kbd>Enter</kbd> new line</span></div><div data-form-error aria-live="assertive"></div>
    </footer>`;
  }

  function renderComposerPanel(panel, conversation) {
    if (panel === "emoji") return `<div class="composer-panel composer-panel--emoji" role="group" aria-label="Emoji">${BOT_VIEW_EMOJI.map((emoji) => `<button type="button" data-action="insert-emoji" data-emoji="${esc(emoji)}" aria-label="Insert ${esc(emoji)}">${esc(emoji)}</button>`).join("")}</div>`;
    if (panel === "reaction") {
      const data = state.botViewOpenPanel?.data || {};
      return `<div class="composer-panel composer-panel--reaction" role="group" aria-label="Choose reaction"><strong>React</strong>${["👍", "❤️", "🔥", "🎉", "😁", "😢", "👎", "🤯"].map((emoji) => `<button type="button" data-action="set-message-reaction" data-telegram-message-id="${esc(data.messageId || "")}" data-reaction="${esc(emoji)}" aria-label="React ${esc(emoji)}">${esc(emoji)}</button>`).join("")}<button class="composer-panel__clear" type="button" data-action="remove-message-reaction" data-telegram-message-id="${esc(data.messageId || "")}">Remove</button></div>`;
    }
    if (panel === "callback-answer") {
      const data = state.botViewOpenPanel?.data || {};
      return `<form class="composer-panel composer-special-form" id="callback-answer-form"><input type="hidden" name="action_generation" value="${esc(data.actionGeneration || "")}"><div class="composer-panel__head"><strong>Answer button press</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><label>Notification text <span>(optional)</span><textarea name="text" rows="3" maxlength="200" placeholder="Your request was received"></textarea></label><div class="composer-special-form__checks"><label><input type="checkbox" name="show_alert"> Show as an alert</label></div><button class="btn btn--primary btn--sm" type="submit">Answer callback</button><div data-form-error aria-live="assertive"></div></form>`;
    }
    if (panel === "suggested-post-approve") {
      const data = state.botViewOpenPanel?.data || {};
      return `<form class="composer-panel composer-special-form" id="suggested-post-approve-form"><input type="hidden" name="message_id" value="${esc(data.messageId || "")}"><div class="composer-panel__head"><strong>Approve suggested post</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><p class="composer-panel__hint">Publish it now, or choose a future time.</p><label>Publish time <span>(optional)</span><input name="send_date" type="datetime-local"></label><button class="btn btn--primary btn--sm" type="submit">Approve post</button><div data-form-error aria-live="assertive"></div></form>`;
    }
    if (panel === "suggested-post-decline") {
      const data = state.botViewOpenPanel?.data || {};
      return `<form class="composer-panel composer-special-form" id="suggested-post-decline-form"><input type="hidden" name="message_id" value="${esc(data.messageId || "")}"><div class="composer-panel__head"><strong>Decline suggested post</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><label>Comment <span>(optional)</span><textarea name="comment" rows="3" maxlength="128" placeholder="Tell the sender what should change"></textarea></label><button class="btn btn--danger btn--sm" type="submit">Decline post</button><div data-form-error aria-live="assertive"></div></form>`;
    }
    if (panel === "poll") return `<form class="composer-panel composer-special-form" id="poll-form"><div class="composer-panel__head"><strong>Create poll</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><label>Question<input name="question" maxlength="300" required placeholder="Ask a question"></label><label>Options<textarea name="options" rows="3" required placeholder="One option per line"></textarea></label><div class="composer-special-form__checks"><label><input type="checkbox" name="is_anonymous" checked> Anonymous</label><label><input type="checkbox" name="allows_multiple_answers"> Multiple answers</label></div><button class="btn btn--primary btn--sm" type="submit">Send poll</button><div data-form-error aria-live="assertive"></div></form>`;
    if (panel === "location") return `<form class="composer-panel composer-special-form" id="location-form"><div class="composer-panel__head"><strong>Send location</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><div class="composer-special-form__grid"><label>Latitude<input name="latitude" inputmode="decimal" required placeholder="51.5074"></label><label>Longitude<input name="longitude" inputmode="decimal" required placeholder="-0.1278"></label></div><div class="composer-special-form__actions"><button class="btn btn--secondary btn--sm" type="button" data-action="use-current-location">Use current location</button><button class="btn btn--primary btn--sm" type="submit">Send location</button></div><div data-form-error aria-live="assertive"></div></form>`;
    if (panel === "contact") return `<form class="composer-panel composer-special-form" id="contact-form"><div class="composer-panel__head"><strong>Send contact</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><div class="composer-special-form__grid"><label>First name<input name="first_name" maxlength="64" required></label><label>Phone number<input name="phone_number" type="tel" required placeholder="+1 555 0100"></label></div><button class="btn btn--primary btn--sm" type="submit">Send contact</button><div data-form-error aria-live="assertive"></div></form>`;
    if (panel === "venue") return `<form class="composer-panel composer-special-form" id="venue-form"><div class="composer-panel__head"><strong>Send venue</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><div class="composer-special-form__grid"><label>Latitude<input name="latitude" inputmode="decimal" required></label><label>Longitude<input name="longitude" inputmode="decimal" required></label></div><label>Place name<input name="title" maxlength="256" required></label><label>Address<input name="address" maxlength="256" required></label><button class="btn btn--primary btn--sm" type="submit">Send venue</button><div data-form-error aria-live="assertive"></div></form>`;
    if (panel === "live-photo") return `<form class="composer-panel composer-special-form" id="live-photo-form"><div class="composer-panel__head"><strong>Send live photo</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><label>Short video<input name="live_photo" type="file" accept="video/mp4,video/quicktime" required></label><label>Static photo<input name="photo" type="file" accept="image/jpeg,image/heic,image/heif" required></label><label>Caption<textarea name="caption" rows="2" maxlength="1024" placeholder="Optional caption"></textarea></label><p class="composer-panel__hint">Telegram accepts a video up to 10 seconds / 10 MB and a matching static photo up to 10 MB.</p><button class="btn btn--primary btn--sm" type="submit">Send live photo</button><div data-form-error aria-live="assertive"></div></form>`;
    if (panel === "rich") return `<form class="composer-panel composer-special-form" id="rich-message-form"><div class="composer-panel__head"><strong>Rich message</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><label>Markdown<textarea name="markdown" rows="6" maxlength="32768" required placeholder="# Heading&#10;&#10;A formatted message with **bold text**."></textarea></label><p class="composer-panel__hint">Telegram renders this as a native rich message.</p><button class="btn btn--primary btn--sm" type="submit">Send rich message</button><div data-form-error aria-live="assertive"></div></form>`;
    if (panel === "format") return `<form class="composer-panel composer-special-form" id="message-format-form"><div class="composer-panel__head"><strong>Text formatting</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><label>Format<select name="parse_mode"><option value="">Plain text</option><option value="MarkdownV2">MarkdownV2</option><option value="HTML">HTML</option></select></label><button class="btn btn--primary btn--sm" type="submit">Apply</button></form>`;
    if (panel === "buttons") return `<form class="composer-panel composer-special-form" id="message-buttons-form"><div class="composer-panel__head"><strong>Inline buttons</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><label>Buttons<textarea name="buttons" rows="4" maxlength="2048" required placeholder="Open website | https://example.com&#10;Help | https://example.com/help"></textarea></label><p class="composer-panel__hint">One button per row. Only HTTPS links are opened by users.</p><button class="btn btn--primary btn--sm" type="submit">Apply buttons</button><div data-form-error aria-live="assertive"></div></form>`;
    if (panel === "checklist" && conversation?.business_connection_id) {
      const data = state.botViewOpenPanel?.data || {};
      const checklist = data.checklist || {};
      const tasks = Array.isArray(checklist.tasks) ? checklist.tasks.map((task) => richTextPlain(task.text)).join("\n") : "";
      return `<form class="composer-panel composer-special-form" id="checklist-form"><input type="hidden" name="message_id" value="${esc(data.messageId || "")}"><div class="composer-panel__head"><strong>${data.messageId ? "Edit" : "Send"} checklist</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><label>Title<input name="title" maxlength="255" required value="${esc(richTextPlain(checklist.title))}"></label><label>Tasks<textarea name="tasks" rows="5" maxlength="3029" required placeholder="One task per line">${esc(tasks)}</textarea></label><div class="composer-special-form__checks"><label><input type="checkbox" name="others_can_add_tasks"${checklist.others_can_add_tasks ? " checked" : ""}> Others can add</label><label><input type="checkbox" name="others_can_mark_tasks_as_done"${checklist.others_can_mark_tasks_as_done ? " checked" : ""}> Others can complete</label></div><button class="btn btn--primary btn--sm" type="submit">${data.messageId ? "Save checklist" : "Send checklist"}</button><div data-form-error aria-live="assertive"></div></form>`;
    }
    if (panel === "forward") {
      const data = state.botViewOpenPanel?.data || {};
      const choices = state.conversations.filter((item) => !item.guest_query_id && item.receiver_user_id == null);
      return `<form class="composer-panel composer-special-form" id="forward-message-form"><input type="hidden" name="message_id" value="${esc(data.messageId || "")}"><input type="hidden" name="from_chat_id" value="${esc(data.fromChatId || "")}"><div class="composer-panel__head"><strong>Forward or copy</strong><button type="button" data-action="close-composer-panel" aria-label="Close">${icon("close")}</button></div><label>Destination<select name="conversation_id" required>${choices.map((item) => `<option value="${esc(conversationId(item))}">${esc(conversationTitle(item))}${conversationContextLabel(item) ? ` · ${esc(conversationContextLabel(item))}` : ""}</option>`).join("")}</select></label><div class="composer-special-form__checks"><label><input type="radio" name="mode" value="forward" checked> Forward with attribution</label><label><input type="radio" name="mode" value="copy"> Copy without attribution</label></div><button class="btn btn--primary btn--sm" type="submit">Send</button><div data-form-error aria-live="assertive"></div></form>`;
    }
    if (panel === "dice") return `<div class="composer-panel composer-panel--dice" role="group" aria-label="Send dice"><strong>Choose dice</strong>${["🎲", "🎯", "🏀", "⚽", "🎳", "🎰"].map((emoji) => `<button type="button" data-action="send-dice" data-emoji="${esc(emoji)}" aria-label="Send ${esc(emoji)}">${esc(emoji)}</button>`).join("")}</div>`;
    if (panel !== "actions") return "";
    const ephemeral = conversation?.receiver_user_id != null;
    const direct = conversation?.direct_messages_topic_id != null;
    return `<div class="composer-panel composer-panel--actions" role="menu"><button type="button" data-action="pick-attachments" data-accept="image/*,video/*" data-mode="media" role="menuitem"><span aria-hidden="true">▧</span><strong>Photo or video</strong></button><button type="button" data-action="pick-attachments" data-accept="*/*" data-mode="document" role="menuitem"><span aria-hidden="true">↧</span><strong>File</strong></button><button type="button" data-action="pick-attachments" data-accept="image/webp,.tgs,video/webm" data-mode="media" data-method="sendSticker" role="menuitem"><span aria-hidden="true">◇</span><strong>Sticker</strong></button><button type="button" data-action="pick-attachments" data-accept="video/mp4" data-mode="media" data-method="sendVideoNote" role="menuitem"><span aria-hidden="true">◉</span><strong>Video note</strong></button><button type="button" data-action="open-special-panel" data-panel="live-photo" role="menuitem"><span aria-hidden="true">◐</span><strong>Live photo</strong></button>${!ephemeral && !direct ? `<button type="button" data-action="open-special-panel" data-panel="poll" role="menuitem"><span aria-hidden="true">▥</span><strong>Poll</strong></button>` : ""}<button type="button" data-action="open-special-panel" data-panel="location" role="menuitem"><span aria-hidden="true">⌖</span><strong>Location</strong></button><button type="button" data-action="open-special-panel" data-panel="venue" role="menuitem"><span aria-hidden="true">⌂</span><strong>Venue</strong></button><button type="button" data-action="open-special-panel" data-panel="contact" role="menuitem"><span aria-hidden="true">♙</span><strong>Contact</strong></button>${!ephemeral ? `<button type="button" data-action="open-special-panel" data-panel="dice" role="menuitem"><span aria-hidden="true">⚄</span><strong>Dice</strong></button><button type="button" data-action="open-special-panel" data-panel="rich" role="menuitem"><span aria-hidden="true">¶</span><strong>Rich message</strong></button><button type="button" data-action="open-special-panel" data-panel="format" role="menuitem"><span aria-hidden="true">Aa</span><strong>Formatting</strong></button><button type="button" data-action="open-special-panel" data-panel="buttons" role="menuitem"><span aria-hidden="true">▤</span><strong>Buttons</strong></button>` : ""}${conversation?.business_connection_id ? `<button type="button" data-action="open-special-panel" data-panel="checklist" role="menuitem"><span aria-hidden="true">☑</span><strong>Checklist</strong></button>` : ""}</div>`;
  }

  function renderMessage(item, index) {
    const text = messageText(item);
    const type = item?.type || item?.event_type || "message";
    const outgoing = isOutgoing(item);
    const status = item.status || item.delivery_status || (outgoing ? "sent" : "received");
    const message = telegramMessage(item);
    const structured = renderStructuredMessage(item);
    const reply = message?.reply_to_message || message?.external_reply || message?.quote || item?.reply_to;
    const id = messageStableId(item, index);
    const telegramId = telegramMessageId(item);
    const ephemeralId = item?.ephemeral_message_id ?? message?.ephemeral_message_id ?? "";
    const usableMessageId = telegramId !== "" && telegramId != null && Number(telegramId) !== 0;
    const conversation = state.conversations.find((candidate) => conversationId(candidate) === String(state.selectedConversationId));
    const actionableEphemeral = ephemeralId !== "" && ephemeralId != null && ephemeralMessageIsActionable(item, conversation);
    const hasMessageIdentity = item?.actionable === false ? false : usableMessageId || actionableEphemeral;
    const deleted = status === "deleted" || type === "deleted_business_messages" || item?.deleted === true;
    if (deleted) return `<div class="message-event message-event--deleted">${icon("trash")}<span>Message deleted</span><time>${esc(formatDate(messageTime(item), "time"))}</time></div>`;
    if (item?._timeline_callback_event) return `<div class="message-event message-event--callback">${renderCallbackEvent(item._timeline_callback_event)}<time>${esc(formatDate(messageTime(item), "time"))}</time></div>`;
    if (item?._timeline_event_label) return `<div class="message-event message-event--action">${icon("pulse")}<span>${esc(item._timeline_event_label)}</span><time>${esc(formatDate(messageTime(item), "time"))}</time></div>`;
    if (String(item?.direction || "").toLowerCase() === "action") {
      const actionLabel = String(type || "Bot action").replace(/^send|^edit/, "").replaceAll("_", " ").replace(/([a-z])([A-Z])/g, "$1 $2").trim();
      return `<div class="message-event message-event--action">${icon("check")}<span>${esc(actionLabel || "Bot action")} ${status === "failed" ? "failed" : "completed"}</span><time>${esc(formatDate(messageTime(item), "time"))}</time></div>`;
    }
    if (!text && !structured) {
      const ignored = new Set(["message_id", "date", "chat", "from", "sender_chat", "business_connection_id", "message_thread_id", "direct_messages_topic", "reply_to_message", "external_reply", "quote", "forward_origin", "edit_date", "author_signature", "has_protected_content"]);
      const field = Object.keys(message || {}).find((key) => !ignored.has(key));
      const label = field || type;
      return `<details class="message-event message-event--raw"><summary>${icon("pulse")}<span>${esc(String(label).replaceAll("_", " "))}</span><time>${esc(formatDate(messageTime(item), "time"))}</time></summary><pre>${esc(JSON.stringify(field ? message[field] : item?.content || {}, null, 2).slice(0, 4000))}</pre></details>`;
    }
    const business = Boolean(conversation?.business_connection_id);
    const guest = Boolean(conversation?.guest_query_id);
    const suggestedPost = !outgoing && conversation?.direct_messages_topic_id != null && usableMessageId && Boolean(message?.suggested_post_info);
    const attachments = normalizedAttachments(item);
    const editableMedia = attachments.some((attachment) => ["animation", "audio", "document", "live_photo", "photo", "video"].includes(String(attachment.kind || attachment.type || "").replace(/^paid_/, "")));
    const editable = outgoing && hasMessageIdentity && (message?.text != null || message?.caption != null || editableMedia);
    const statusLabel = { pending: "Sending…", uploading: "Uploading…", sending: "Sending…", sent: "Sent", failed: "Not sent", delivery_unknown: "Delivery unknown" }[status] || status;
    const replyData = esc(JSON.stringify({ message_id: telegramId, ephemeral_message_id: ephemeralId, action_generation: item?.action_generation, preview: messagePreview(item), text, has_media: attachments.length > 0, has_caption: message?.caption != null, reply_markup: message?.reply_markup || null }));
    const entities = message?.caption != null && text === message.caption ? message.caption_entities : message?.entities;
    const retryAction = item?._action ? "retry-special-action" : "retry-message";
    const failureAction = ["failed", "delivery_unknown"].includes(status) ? `<div class="message-failure">${item.error ? `<span>${esc(item.error)}</span>` : ""}<button class="message-retry" type="button" data-action="${retryAction}" data-client-id="${esc(item.client_id || "")}">${status === "delivery_unknown" ? "Review and retry" : "Try again"}</button></div>` : "";
    const canReply = hasMessageIdentity && !guest;
    const canOperate = hasMessageIdentity && !guest;
    const checklistData = message?.checklist ? esc(JSON.stringify(message.checklist)) : "";
    return `<article class="message ${outgoing ? "message--out" : "message--in"} ${["failed", "delivery_unknown"].includes(status) ? "message--failed" : ""}" data-message-id="${esc(id)}"><div class="message__actions">${canReply ? `<button type="button" data-action="reply-message" data-message="${replyData}" aria-label="Reply to message">↩</button>` : ""}${text ? `<button type="button" data-action="copy-message-text" data-text="${esc(text)}" aria-label="Copy message">${icon("copy")}</button>` : ""}${usableMessageId && !guest ? `<button type="button" data-action="forward-message" data-telegram-message-id="${esc(telegramId)}" data-from-chat-id="${esc(conversationChatId(conversation))}" aria-label="Forward or copy message">↗</button>` : ""}${usableMessageId && !business && !guest ? `<button type="button" data-action="open-reaction-panel" data-telegram-message-id="${esc(telegramId)}" aria-label="Choose reaction">☺</button>` : ""}${suggestedPost ? `<button class="message__action-label" type="button" data-action="review-suggested-post" data-decision="approve" data-telegram-message-id="${esc(telegramId)}" aria-label="Approve suggested post">Approve</button><button class="message__action-label text-danger" type="button" data-action="review-suggested-post" data-decision="decline" data-telegram-message-id="${esc(telegramId)}" aria-label="Decline suggested post">Decline</button>` : ""}${outgoing && usableMessageId && message?.poll && !message.poll.is_closed ? `<button type="button" data-action="stop-poll" data-telegram-message-id="${esc(telegramId)}" aria-label="Stop poll">■</button>` : ""}${outgoing && usableMessageId && message?.location?.live_period ? `<button type="button" data-action="stop-live-location" data-telegram-message-id="${esc(telegramId)}" aria-label="Stop live location">⌖</button>` : ""}${business && outgoing && usableMessageId && message?.checklist ? `<button type="button" data-action="edit-checklist" data-telegram-message-id="${esc(telegramId)}" data-checklist="${checklistData}" aria-label="Edit checklist">☑</button>` : ""}${business && !outgoing && usableMessageId ? `<button type="button" data-action="mark-business-message-read" data-telegram-message-id="${esc(telegramId)}" aria-label="Mark message read">✓</button>` : ""}${editable && !guest ? `<button type="button" data-action="edit-message" data-message="${replyData}" aria-label="${editableMedia ? "Edit caption, media, or buttons" : "Edit message"}">✎</button>` : ""}${canOperate ? `<button type="button" data-action="delete-message" data-telegram-message-id="${esc(telegramId)}" data-ephemeral-message-id="${esc(ephemeralId)}" data-action-generation="${esc(item?.action_generation ?? "")}" aria-label="Delete message">${icon("trash")}</button>` : ""}</div><div class="message__bubble">${renderReplySnippet(reply)}${structured}${text ? `<div class="message__text">${renderMessageText(text, entities)}</div>` : ""}${renderReplyMarkup(message)}<div class="message__meta"><span>${esc(formatDate(messageTime(item), "time"))}</span>${message?.edit_date || type.startsWith("edit") ? "<span>edited</span>" : ""}${outgoing ? `<span class="message-status message-status--${esc(status)}">${status === "sent" ? "✓✓" : ["failed", "delivery_unknown"].includes(status) ? "!" : "◷"} ${esc(statusLabel)}</span>` : ""}</div>${failureAction}</div></article>`;
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
    const apiBase = bot.integration?.api_base || `${window.location.origin}/bot${"${BOT_TOKEN}"}${botUsesTestEnvironment(bot) ? "/test" : ""}`;
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
    const currentPool = ["standard", "local"].includes(String(bot.data_plane_pool || "").toLowerCase())
      ? String(bot.data_plane_pool).toLowerCase()
      : "";
    const localEligible = Boolean(state.membership?.local_bot_api);
    if (currentPool) {
      const poolLabel = currentPool === "local" ? "Phenogram Local" : "Phenogram Standard";
      const poolDescription = currentPool === "local"
        ? "Uses Phenogram's official local Bot API pool with extended file support."
        : "Uses Phenogram's official Bot API pool with Telegram-compatible request and delivery behavior.";
      return `<section class="panel panel--spaced"><div class="panel__head"><div><h2>Telegram API routing</h2><p>Bot API calls, polling, webhooks, and files use the official Telegram server operated by Phenogram.</p></div><span class="badge badge--info">${esc(poolLabel)}</span></div><div class="settings-grid"><div class="settings-row"><div class="settings-row__intro"><h3>Current backend</h3><p>All Bot API calls and file requests use this route.</p></div><div class="routing-choice"><strong>${esc(poolLabel)}</strong><span>${esc(poolDescription)}</span></div></div></div></section>`;
    }
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
      ? `<div class="settings-row"><div class="settings-row__intro"><h3>Bot credential</h3><p>Kept current through ${esc(managerLabel(bot))}.</p></div><div class="form-note">${icon("lock")}Phenogram encrypts the credential in its application database and refreshes it automatically after Telegram reports a token change. The official Bot API server uses its native storage format.</div></div>`
      : `<div class="settings-row"><div class="settings-row__intro"><h3>Bot token</h3><p>Phenogram does not reveal stored credentials.</p></div><div class="form-note">${icon("lock")}Phenogram encrypts the bot token in its application database and never displays it again. The official Bot API server uses its native storage format. If the token may be exposed, revoke it through BotFather.</div></div>`;
    const webhookRecoveryPanel = bot.webhook_secret_required === true
      ? `<section class="panel panel--spaced"><div class="panel__head"><div><h2>Managed bot setup paused</h2><p>The existing Telegram webhook is still active and unchanged; Phenogram API routing is paused.</p></div><span class="badge badge--warning">Action needed</span></div><div class="settings-row"><div class="settings-row__intro"><h3>Preserve webhook authentication</h3><p>Telegram does not reveal the receiver’s current secret header. If the webhook uses a custom certificate, replace it with a publicly trusted certificate before continuing.</p></div><div><button class="btn btn--primary" type="button" data-action="open-managed-webhook-recovery">${icon("refresh")}Continue setup</button></div></div></section>`
      : "";
    const removalPanel = connectedManager
      ? `<section class="panel panel--spaced"><div class="panel__head"><div><h2>Managed relationship</h2><p>This bot is maintained through ${esc(managerLabel(bot))}</p></div></div><div class="settings-row"><div class="settings-row__intro"><h3>Automatic availability</h3><p>Managed bots stay in the workspace while their manager relationship is active.</p></div><div class="form-note">${icon("info")}This bot cannot be removed separately while ${esc(managerLabel(bot))} manages it.</div></div></section>`
      : `<section class="panel panel--spaced danger-zone"><div class="panel__head"><div><h2>Danger zone</h2><p>Permanent workspace actions</p></div></div><div class="settings-row"><div class="settings-row__intro"><h3>Delete this bot</h3><p>${managed ? "Remove this managerless managed bot and its stored Phenogram data." : `Disconnect the token and remove its stored data.${descendants ? ` ${descendants} managed bot${descendants === 1 ? "" : "s"} beneath it will remain in Phenogram; direct children become managerless.` : ""}`}</p></div><div><button class="btn btn--danger" type="button" data-action="confirm-delete-bot">${icon("trash")}Delete ${esc(botName(bot))}</button></div></div></section>`;
    return `<div class="page page--narrow">
      ${pageHeader("Bot settings", `Ownership, credentials, and stored data for ${botName(bot)}.`)}
      <section class="panel"><div class="panel__head"><div><h2>Telegram identity</h2><p>Verified server-side using Telegram getMe</p></div>${verified ? renderBotStatusBadge(bot) : '<span class="badge badge--danger">Token invalid</span>'}</div><div class="settings-grid"><div class="settings-row"><div class="settings-row__intro"><h3>Bot</h3><p>The Telegram identity associated with this workspace bot.</p></div><div class="identity-card"><span class="bot-avatar bot-avatar--lg">${initials(botName(bot))}</span><div><strong>${esc(botName(bot))}</strong><span>${esc(botUsername(bot))}${managed ? ` · Managed by ${esc(managerLabel(bot))}` : ""}</span></div></div></div><div class="settings-row"><div class="settings-row__intro"><h3>Telegram environment</h3><p>Production and Test are separate Telegram Bot API environments.</p></div><div><strong class="settings-value">${esc(botEnvironmentLabel(bot))}</strong>${renderBotEnvironmentBadge(bot)}</div></div><div class="settings-row"><div class="settings-row__intro"><h3>Platform status</h3><p>Current provisioning and delivery state reported by Phenogram.</p></div><div>${renderBotStatusBadge(bot)}</div></div><div class="settings-row"><div class="settings-row__intro"><h3>Public identifier</h3><p>Identifies this bot without authorizing Telegram API calls.</p></div><div><div class="fingerprint">${esc(fingerprint)}</div><p class="field__hint field__hint--spaced">This value is safe to reference publicly. Signed file links still expire separately.</p></div></div>${credentialRow}<div class="settings-row"><div class="settings-row__intro"><h3>Data retention</h3><p>Updates outside this window are removed automatically.</p></div><div><strong class="settings-value">${esc(retentionValue(bot))}</strong><p class="field__hint field__hint--compact-spaced">${botNeedsRetentionWarning(bot) ? "This managed bot is outside full-history coverage." : `Covered by your ${esc(membershipPlan())} plan.`}</p></div></div></div></section>
      ${webhookRecoveryPanel}
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
      const poolField = state.membership?.local_bot_api === true
        ? `<div class="field"><label for="bot-pool">Telegram API backend</label><select id="bot-pool" name="pool"><option value="standard" selected>Phenogram Standard</option><option value="local">Phenogram Local — extended file support</option></select><p class="field__hint">This is the bot’s initial placement. Moving between pools is not available yet.</p></div>`
        : "";
      const environmentField = `<details class="advanced-options"><summary>Advanced options</summary><label class="advanced-option" for="bot-test-environment"><input id="bot-test-environment" name="test_dc" type="checkbox" value="true"><span><strong>Use Telegram Test environment</strong><small>Only for a bot created in Telegram’s separate test environment.</small></span></label></details>`;
      modalRoot.innerHTML = `<div class="modal-backdrop" data-action="close-modal"><section class="modal" role="dialog" aria-modal="true" aria-labelledby="connect-title" data-modal-panel><header class="modal__head"><div><h2 id="connect-title">${atLimit ? "Your bot limit is full" : "Connect a Telegram bot"}</h2><p>${atLimit ? `The ${membershipPlan()} plan includes ${membershipLimit()} bot${membershipLimit() === 1 ? "" : "s"}.` : "Paste the token from @BotFather."}</p></div><button class="btn btn--ghost btn--icon" type="button" data-action="close-modal" aria-label="Close">${icon("close")}</button></header>${atLimit ? `<div class="modal__body"><div class="form-note">${icon("info")}Your existing bots stay active. Upgrade the workspace before connecting another bot.</div></div><footer class="modal__actions"><button class="btn btn--secondary" type="button" data-action="close-modal">Not now</button><button class="btn btn--primary" type="button" data-action="go-billing">See plans</button></footer>` : `<form id="connect-bot-form" autocomplete="off"><div class="modal__body"><div class="form-stack"><div class="field"><div class="field__row"><label for="bot-token">Telegram bot token</label><span class="field__hint">From @BotFather</span></div><div class="input-wrap">${icon("lock")}<input id="bot-token" name="token" type="password" inputmode="text" autocomplete="new-password" spellcheck="false" placeholder="123456789:AA…" required></div><p class="field__hint">Phenogram encrypts this credential in its application database and will not display it again.</p></div>${environmentField}${poolField}<div class="form-note">${icon("info")}Connecting transfers this bot’s webhook to Phenogram. If a webhook is already set, Phenogram will keep delivering updates to the same destination.</div><div data-webhook-secret-resolution></div><div data-form-error aria-live="polite"></div></div></div><footer class="modal__actions"><button class="btn btn--secondary" type="button" data-action="close-modal">Cancel</button><button class="btn btn--primary" type="submit">Verify and connect ${icon("arrow")}</button></footer></form>`}</section></div>`;
      return;
    }
    if (name === "managed-webhook-recovery") {
      const bot = currentBot();
      modalRoot.innerHTML = `<div class="modal-backdrop" data-action="close-modal"><section class="modal" role="dialog" aria-modal="true" aria-labelledby="managed-webhook-recovery-title" data-modal-panel><header class="modal__head"><div><h2 id="managed-webhook-recovery-title">Continue ${esc(botName(bot))} setup</h2><p>Preserve the existing receiver before onboarding or changing the managed credential.</p></div><button class="btn btn--ghost btn--icon" type="button" data-action="close-modal" aria-label="Close">${icon("close")}</button></header><form id="managed-webhook-recovery-form" autocomplete="off"><div class="modal__body"><div class="form-stack"><div class="status-banner status-banner--warning">${icon("alert")}<div class="status-banner__copy"><strong>The existing webhook is active; Phenogram API routing is paused</strong>Telegram does not reveal the webhook secret or custom certificate. If a custom certificate is configured, replace it with a publicly trusted certificate before retrying.</div></div><div class="field"><label for="managed-webhook-secret-mode">Existing webhook authentication</label><select id="managed-webhook-secret-mode" name="existing_webhook_secret_mode" required><option value="" selected disabled>Choose one</option><option value="secret">It uses a secret token</option><option value="none">It does not use a secret token</option></select></div><div class="field"><label for="managed-existing-webhook-secret">Current secret token</label><input id="managed-existing-webhook-secret" name="existing_webhook_secret" type="password" autocomplete="off" spellcheck="false" placeholder="Required only when the receiver uses one"><p class="field__hint">This value is used only to create the encrypted, crash-resumable lifecycle operation and is never written to logs or job status.</p></div><div data-form-error aria-live="polite"></div></div></div><footer class="modal__actions"><button class="btn btn--secondary" type="button" data-action="close-modal">Cancel</button><button class="btn btn--primary" type="submit">Preserve webhook and continue ${icon("arrow")}</button></footer></form></section></div>`;
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
      modalRoot.innerHTML = `<div class="modal-backdrop" data-action="close-modal"><section class="modal" role="alertdialog" aria-modal="true" aria-labelledby="routing-title" data-modal-panel><header class="modal__head"><div><h2 id="routing-title">Migrate ${esc(botName(bot))} to ${esc(label)}?</h2><p>This changes the upstream Telegram API server for this bot.</p></div><button class="btn btn--ghost btn--icon" type="button" data-action="close-modal" aria-label="Close">${icon("close")}</button></header><form id="routing-form" data-mode="${mode}"><div class="modal__body"><div class="status-banner status-banner--danger">${icon("alert")}<div class="status-banner__copy"><strong>Pool changes are not available yet</strong>Phenogram keeps the bot on its current official Bot API pool to avoid an unsafe cross-server move.</div></div><div data-form-error aria-live="polite"></div></div><footer class="modal__actions"><button class="btn btn--secondary" type="button" data-action="close-modal">Cancel</button><button class="btn btn--primary" type="submit">Keep current pool</button></footer></form></section></div>`;
    }
  }

  function formError(form, message) {
    const target = form.querySelector("[data-form-error]");
    if (target) target.innerHTML = message ? `<p class="form-error">${esc(message)}</p>` : "";
  }

  function requestExistingWebhookSecret(form, detail) {
    const target = form.querySelector("[data-webhook-secret-resolution]");
    if (!target) return false;
    const destination = detail?.destination_host || "the current receiver";
    target.innerHTML = `<div class="status-banner status-banner--warning" role="status">${icon("alert")}<div class="status-banner__copy"><strong>Existing webhook detected at ${esc(destination)}</strong>Telegram does not reveal its current secret header. Tell Phenogram how this receiver is authenticated before the transfer continues.</div></div><div class="field"><label for="webhook-secret-mode">Existing webhook authentication</label><select id="webhook-secret-mode" name="existing_webhook_secret_mode" required><option value="" selected disabled>Choose one</option><option value="secret">It uses a secret token</option><option value="none">It does not use a secret token</option></select></div><div class="field"><label for="existing-webhook-secret">Current secret token</label><input id="existing-webhook-secret" name="existing_webhook_secret" type="password" autocomplete="off" spellcheck="false" placeholder="Required only when the receiver uses one"><p class="field__hint">Phenogram uses this value to preserve the receiver’s Telegram secret header.</p></div>`;
    target.querySelector("select")?.focus();
    return true;
  }

  function requestExistingWebhookIpAddress(form, detail) {
    if (form.querySelector('[name="existing_webhook_ip_address_mode"]')) return true;
    const reported = String(detail?.reported_ip_address || "");
    if (!reported) return false;
    const fields = `<div data-webhook-ip-resolution><div class="status-banner status-banner--warning" role="status">${icon("alert")}<div class="status-banner__copy"><strong>Choose fixed IP or DNS resolution</strong>Telegram currently reports ${esc(reported)}, but does not reveal whether that address was explicitly pinned. Phenogram will not guess.</div></div><div class="field"><label for="webhook-ip-address-mode">Existing webhook network routing</label><select id="webhook-ip-address-mode" name="existing_webhook_ip_address_mode" required><option value="" selected disabled>Choose one</option><option value="fixed">Keep ${esc(reported)} as a fixed IPv4 address</option><option value="dns">Resolve the webhook hostname through DNS</option></select><input type="hidden" name="existing_webhook_reported_ip_address" value="${esc(reported)}"><p class="field__hint">Fixed IP preserves this exact address. DNS follows future address changes for the hostname.</p></div></div>`;
    const secretTarget = form.querySelector("[data-webhook-secret-resolution]");
    if (secretTarget) secretTarget.insertAdjacentHTML("beforeend", fields);
    else form.querySelector("[data-form-error]")?.insertAdjacentHTML("beforebegin", fields);
    form.querySelector('[name="existing_webhook_ip_address_mode"]')?.focus();
    return true;
  }

  function setSubmitting(form, submitting, label) {
    const button = form.querySelector('button[type="submit"]');
    if (!button) return;
    if (!button.dataset.originalLabel) button.dataset.originalLabel = button.innerHTML;
    button.disabled = submitting;
    button.innerHTML = submitting ? `${icon("refresh")} ${esc(label || "Working…")}` : button.dataset.originalLabel;
  }

  function botViewSendKey(botIdValue, chatId) {
    return `${String(botIdValue || "")}:${String(chatId || "")}`;
  }

  function botViewSendIsInFlight(botIdValue, chatId) {
    return state.botViewSendsInFlight.has(botViewSendKey(botIdValue, chatId));
  }

  function reserveBotViewSend(key, context) {
    if (state.botViewSendsInFlight.has(key)) return false;
    state.botViewSendsInFlight.set(key, context);
    return true;
  }

  function finishBotViewSend(context) {
    if (state.botViewSendsInFlight.get(context.key) === context) {
      state.botViewSendsInFlight.delete(context.key);
    }
  }

  function setBotViewComposerSubmitting(form, submitting) {
    setSubmitting(form, submitting, "Sending…");
    const composer = form?.elements?.text;
    if (composer) composer.disabled = submitting;
    if (submitting) form?.setAttribute?.("aria-busy", "true");
    else form?.removeAttribute?.("aria-busy");
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
    const pool = String(data.get("pool") || "standard");
    const testDc = String(data.get("test_dc") || "false") === "true";
    const webhookSecretMode = String(data.get("existing_webhook_secret_mode") || "");
    const existingWebhookSecret = String(data.get("existing_webhook_secret") || "").trim();
    const webhookIpMode = String(data.get("existing_webhook_ip_address_mode") || "");
    const reportedWebhookIp = String(data.get("existing_webhook_reported_ip_address") || "");
    if (!token) { formError(form, "Paste the token provided by BotFather."); return; }
    if (webhookSecretMode === "secret" && !existingWebhookSecret) {
      formError(form, "Enter the current secret token used by the existing webhook.");
      form.elements.existing_webhook_secret?.focus();
      return;
    }
    if (form.elements.existing_webhook_ip_address_mode && !webhookIpMode) {
      formError(form, "Choose whether to keep the reported IPv4 address fixed or use DNS resolution.");
      form.elements.existing_webhook_ip_address_mode.focus();
      return;
    }
    setSubmitting(form, true, "Verifying with Telegram…");
    try {
      const body = { token, pool, test_dc: testDc };
      if (webhookSecretMode === "secret") body.existing_webhook_secret = existingWebhookSecret;
      if (webhookSecretMode === "none") body.existing_webhook_has_no_secret = true;
      if (webhookIpMode === "fixed") body.existing_webhook_ip_address = reportedWebhookIp;
      if (webhookIpMode === "dns") body.existing_webhook_has_no_ip_address = true;
      const payload = await api("/bots", { method: "POST", body });
      if (state.sessionVersion !== sessionVersion || !state.user) return;
      token = "";
      form.reset();
      const created = unwrap(payload, "bot") || payload;
      await loadBots({ silent: true });
      if (state.sessionVersion !== sessionVersion || !state.user) return;
      const connectedId = created && botId(created);
      if (connectedId) selectBot(connectedId);
      const connectedBot = state.bots.find((bot) => botId(bot) === String(connectedId)) || created;
      if (connectedId && connectResponseWillRetry(payload, created) && botSetupIsPending(connectedBot)) {
        trackConnectedBotSetup(connectedId);
      }
      closeModal();
      toast(`Connected ${created && typeof created === "object" ? botName(created) : "Telegram bot"}.`);
      surfaceWarnings(payload);
      navigate(state.selectedBotId ? `/bots/${encodeURIComponent(state.selectedBotId)}/overview` : "/bots");
    } catch (error) {
      if (state.sessionVersion !== sessionVersion || !state.user || !form.isConnected) return;
      const detail = error?.payload?.error;
      if (detail?.code === "webhook_secret_required" && requestExistingWebhookSecret(form, detail)) {
        formError(form, "");
        setSubmitting(form, false);
        return;
      }
      if (detail?.code === "webhook_ip_address_resolution_required" && requestExistingWebhookIpAddress(form, detail)) {
        formError(form, "");
        setSubmitting(form, false);
        return;
      }
      formError(form, errorMessage(error));
      setSubmitting(form, false);
      form.elements.token.focus();
    }
  }

  async function submitManagedWebhookRecovery(form) {
    formError(form, "");
    const id = state.selectedBotId;
    const contextVersion = state.botContextVersion;
    const data = new FormData(form);
    const mode = String(data.get("existing_webhook_secret_mode") || "");
    let secret = String(data.get("existing_webhook_secret") || "").trim();
    const webhookIpMode = String(data.get("existing_webhook_ip_address_mode") || "");
    const reportedWebhookIp = String(data.get("existing_webhook_reported_ip_address") || "");
    if (!id || !mode) { formError(form, "Choose how the existing webhook is authenticated."); return; }
    if (mode === "secret" && !secret) {
      formError(form, "Enter the current secret token used by the existing webhook.");
      form.elements.existing_webhook_secret?.focus();
      return;
    }
    if (form.elements.existing_webhook_ip_address_mode && !webhookIpMode) {
      formError(form, "Choose whether to keep the reported IPv4 address fixed or use DNS resolution.");
      form.elements.existing_webhook_ip_address_mode.focus();
      return;
    }
    setSubmitting(form, true, "Continuing safely…");
    try {
      const body = {};
      if (mode === "secret") body.existing_webhook_secret = secret;
      if (mode === "none") body.existing_webhook_has_no_secret = true;
      if (webhookIpMode === "fixed") body.existing_webhook_ip_address = reportedWebhookIp;
      if (webhookIpMode === "dns") body.existing_webhook_has_no_ip_address = true;
      const payload = await api(`/bots/${encodeURIComponent(id)}/managed-webhook-recovery`, { method: "POST", body });
      secret = "";
      form.reset();
      if (state.botContextVersion !== contextVersion || String(state.selectedBotId) !== String(id)) return;
      await loadBots({ silent: true });
      closeModal();
      render();
      toast("Managed bot setup continued and the existing webhook was preserved.");
      surfaceWarnings(payload);
    } catch (error) {
      const detail = error?.payload?.error;
      if (detail?.code === "webhook_ip_address_resolution_required" && requestExistingWebhookIpAddress(form, detail)) {
        formError(form, "");
        setSubmitting(form, false);
        return;
      }
      secret = "";
      if (state.botContextVersion === contextVersion && String(state.selectedBotId) === String(id)) {
        form.elements.existing_webhook_secret.value = "";
        formError(form, errorMessage(error));
        setSubmitting(form, false);
      }
    }
  }

  function botViewActionPath(botIdValue, conversationIdValue, method) {
    return `/bots/${encodeURIComponent(botIdValue)}/conversations/${encodeURIComponent(conversationIdValue)}/actions/${encodeURIComponent(method)}`;
  }

  function botViewErrorMessage(error) {
    const payload = error?.payload?.telegram || error?.payload || {};
    const detail = payload?.description || payload?.error?.description || payload?.message || errorMessage(error);
    const parameters = payload?.parameters || payload?.error?.parameters || {};
    const notes = [];
    if (parameters.retry_after) notes.push(`Try again in ${parameters.retry_after} seconds.`);
    if (parameters.migrate_to_chat_id) notes.push(`Telegram moved this chat to ${parameters.migrate_to_chat_id}.`);
    return [detail, ...notes].filter(Boolean).join(" ");
  }

  function botViewTimelineMessagesFromResponse(payload, previewFiles = []) {
    const messages = payload?._phenogram?.timeline_messages;
    if (!Array.isArray(messages) || !previewFiles.length) return Array.isArray(messages) ? messages : [];
    const urls = previewFiles.map((file) => file?.url).filter((url) => String(url || "").startsWith("blob:"));
    let urlIndex = 0;
    return messages.map((item) => {
      const media = Array.isArray(item?.content?.media) ? item.content.media : [];
      if (!media.length || !urls.length) return item;
      const claimedUrls = [];
      const previewMedia = media.map((entry) => {
        const url = urls[urlIndex];
        if (!url) return entry;
        urlIndex += 1;
        claimedUrls.push(url);
        return { ...entry, url };
      });
      if (!claimedUrls.length) return item;
      return {
        ...item,
        content: { ...item.content, media: previewMedia },
        _local_preview_urls: claimedUrls,
      };
    });
  }

  function mergeConversationTimelineItems(base, incoming) {
    const reconciled = reconcileBotViewActionPreviews(base || [], incoming || []);
    const merged = [...reconciled.messages];
    const positions = new Map(merged.map((item, index) => [messageStableId(item, index), index]));
    const semanticPositions = new Map();
    merged.forEach((item, index) => {
      const semantic = timelineSemanticIdentity(item);
      if (semantic) semanticPositions.set(semantic, index);
    });
    reconciled.remaining.forEach((item, index) => {
      if (!item || typeof item !== "object") return;
      const durableIncoming = Boolean(item?.id || item?.cursor || item?.event_id);
      const observed = durableIncoming
        ? { ...item, _locally_observed: true }
        : { ...item, _locally_observed: true, _response_pending: true, _response_baseline_cursor: timelineItemCursor(item) };
      const stableId = messageStableId(observed, index);
      const semantic = timelineSemanticIdentity(observed);
      const position = positions.get(stableId) ?? (semantic ? semanticPositions.get(semantic) : undefined);
      if (position == null) {
        positions.set(stableId, merged.length);
        if (semantic) semanticPositions.set(semantic, merged.length);
        merged.push(observed);
      } else {
        const durable = merged[position];
        const next = {
          ...durable,
          ...observed,
          id: durable?.id ?? observed?.id,
          cursor: durable?.cursor ?? observed?.cursor,
        };
        if (durableIncoming) {
          if (durable?._local_preview_urls?.length) revokeTimelineLocalPreviews(durable);
          delete next._local_preview_urls;
          delete next._response_pending;
          delete next._response_baseline_cursor;
        } else if (!next._response_baseline_cursor) {
          next._response_baseline_cursor = timelineItemCursor(durable);
        }
        merged[position] = next;
      }
    });
    return merged.sort((left, right) => {
      const cursorOrder = compareJournalIds(timelineItemCursor(left), timelineItemCursor(right));
      if (cursorOrder) return cursorOrder;
      return messageTimeMs(left) - messageTimeMs(right);
    });
  }

  function reconcileBotViewActionPreviews(base, incoming) {
    const messages = [...base];
    const remaining = [];
    const markPending = (item) => ({
      ...item,
      _locally_observed: true,
      _response_pending: true,
      _response_baseline_cursor: item?._response_baseline_cursor || timelineItemCursor(item),
    });
    incoming.forEach((item) => {
      const method = String(item?.event_type || item?.payload?.action || "");
      const payload = item?.payload && typeof item.payload === "object" ? item.payload : {};
      const request = payload?.request && typeof payload.request === "object" ? payload.request : {};
      const result = payload?.telegram_result;
      const isActionPreview = String(item?.direction || "").toLowerCase() === "action" || payload?.action;
      if (!isActionPreview) { remaining.push(item); return; }

      if (["deleteMessage", "deleteMessages", "deleteBusinessMessages", "deleteEphemeralMessage"].includes(method)) {
        const ids = Array.isArray(request.message_ids)
          ? request.message_ids.map(String)
          : request.message_id != null ? [String(request.message_id)] : [];
        const ephemeralId = request.ephemeral_message_id ?? item?.ephemeral_message_id;
        let matched = false;
        messages.forEach((candidate, index) => {
          const candidateMessage = telegramMessage(candidate);
          const standardMatch = ids.length && ids.includes(String(telegramMessageId(candidate)));
          const candidateEphemeral = candidate?.ephemeral_message_id ?? candidateMessage?.ephemeral_message_id;
          const ephemeralMatch = ephemeralId !== "" && ephemeralId != null && String(candidateEphemeral ?? "") === String(ephemeralId);
          if (!standardMatch && !ephemeralMatch) return;
          messages[index] = markPending({ ...candidate, status: "deleted", event_type: method });
          matched = true;
        });
        if (!matched) remaining.push(item);
        return;
      }

      if (["editEphemeralMessageText", "editEphemeralMessageCaption", "editEphemeralMessageMedia", "editEphemeralMessageReplyMarkup"].includes(method)) {
        const ephemeralId = request.ephemeral_message_id ?? item?.ephemeral_message_id;
        const receiverId = item?.receiver_user_id;
        let targetIndex = -1;
        for (let index = messages.length - 1; index >= 0; index -= 1) {
          const candidate = messages[index];
          const candidateMessage = telegramMessage(candidate);
          const candidateEphemeral = candidate?.ephemeral_message_id ?? candidateMessage?.ephemeral_message_id;
          const candidateReceiver = candidate?.receiver_user_id ?? candidateMessage?.receiver_user_id ?? candidateMessage?.receiver_user?.id;
          if (String(candidateEphemeral ?? "") === String(ephemeralId ?? "")
            && (receiverId == null || String(candidateReceiver ?? "") === String(receiverId))) {
            targetIndex = index;
            break;
          }
        }
        if (targetIndex < 0) { remaining.push(item); return; }
        const target = messages[targetIndex];
        const currentMessage = telegramMessage(target);
        const nextMessage = { ...currentMessage };
        if (method === "editEphemeralMessageText") nextMessage.text = String(request.text ?? "");
        if (method === "editEphemeralMessageCaption") nextMessage.caption = String(request.caption ?? "");
        if (method === "editEphemeralMessageMedia" && request.media?.caption != null) nextMessage.caption = String(request.media.caption);
        if (method === "editEphemeralMessageReplyMarkup" || request.reply_markup != null) nextMessage.reply_markup = request.reply_markup || undefined;
        messages[targetIndex] = markPending({
          ...replaceTelegramMessageValue(target, nextMessage),
          text: nextMessage.text ?? nextMessage.caption ?? target?.text,
        });
        return;
      }

      if (method === "stopPoll" && result && typeof result === "object") {
        const targetIndex = findLastMessageIndexByTelegramId(messages, request.message_id ?? item?.telegram_message_id);
        if (targetIndex < 0) { remaining.push(item); return; }
        const target = messages[targetIndex];
        const currentMessage = telegramMessage(target);
        messages[targetIndex] = markPending(replaceTelegramMessageValue(target, { ...currentMessage, poll: result }));
        return;
      }

      remaining.push(item);
    });
    return { messages, remaining };
  }

  function mergeBotViewActionResponse(conversationIdValue, payload, previewFiles = []) {
    const incoming = botViewTimelineMessagesFromResponse(payload, previewFiles);
    if (!incoming.length) return 0;
    const conversation = state.conversations.find((item) => conversationId(item) === String(conversationIdValue));
    if (!conversation) return 0;
    conversation.messages = mergeConversationTimelineItems(conversationMessages(conversation), incoming);
    const key = botViewKey(state.selectedBotId, conversationIdValue);
    incoming.forEach((item) => advanceBotViewMessageCursor(key, timelineItemCursor(item)));
    return incoming.length;
  }

  function replyParametersFromDraft(draft) {
    if (!draft?.reply) return null;
    if (draft.reply.ephemeral_message_id !== "" && draft.reply.ephemeral_message_id != null) return { ephemeral_message_id: draft.reply.ephemeral_message_id };
    return draft.reply.message_id !== "" && draft.reply.message_id != null && Number(draft.reply.message_id) !== 0 ? { message_id: Number(draft.reply.message_id) || draft.reply.message_id } : null;
  }

  function botViewActionGenerationHeaders(source) {
    const generation = source && typeof source === "object" ? source.action_generation : source;
    if (generation === "" || generation == null) return {};
    const value = String(generation).trim();
    return value && value.length <= 256 && !/[\r\n]/.test(value) ? { "x-phenogram-action-generation": value } : {};
  }

  function botViewUsesLocalPool(bot = currentBot()) {
    const local = [bot?.pool, bot?.routing_mode, bot?.delivery_mode, bot?.telegram_environment].some((value) => String(value || "").toLowerCase().includes("local"));
    return local;
  }

  function botViewUploadLimit(bot = currentBot()) {
    return botViewUsesLocalPool(bot) ? BOT_VIEW_LOCAL_MAX_FILE_BYTES : BOT_VIEW_CLOUD_MAX_FILE_BYTES;
  }

  function botViewAggregateUploadLimit(bot = currentBot()) {
    return botViewUsesLocalPool(bot) ? BOT_VIEW_LOCAL_MAX_TOTAL_BYTES : BOT_VIEW_CLOUD_MAX_TOTAL_BYTES;
  }

  function botViewDefinitivelyRejected(error) {
    const status = Number(error?.status || 0);
    return !error?.deliveryUnknown && (error?.telegramRejected === true || (status >= 400 && status < 500));
  }

  function methodForAttachment(attachment, sendMode) {
    if (attachment.explicitMethod) return attachment.explicitMethod;
    if (sendMode === "document" || attachment.forceDocument) return "sendDocument";
    if (attachment.isVoice && ["audio/ogg", "audio/mp4", "audio/mpeg", "audio/x-m4a"].some((type) => attachment.type.startsWith(type))) return "sendVoice";
    if (attachment.type === "image/gif") return "sendAnimation";
    if (attachment.type.startsWith("image/")) return "sendPhoto";
    if (attachment.type.startsWith("video/")) return "sendVideo";
    if (attachment.type.startsWith("audio/")) return "sendAudio";
    return "sendDocument";
  }

  function uploadFieldForMethod(method) {
    return { sendPhoto: "photo", sendAnimation: "animation", sendVideo: "video", sendVideoNote: "video_note", sendAudio: "audio", sendVoice: "voice", sendSticker: "sticker", sendDocument: "document" }[method] || "document";
  }

  function uploadApi(path, body, onProgress, headers = {}) {
    return new Promise((resolve, reject) => {
      const request = new XMLHttpRequest();
      request.open("POST", `${API}${path}`);
      request.withCredentials = true;
      request.setRequestHeader("Accept", "application/json");
      if (state.csrfToken) request.setRequestHeader("X-Phenogram-CSRF", state.csrfToken);
      Object.entries(headers || {}).forEach(([name, value]) => request.setRequestHeader(name, value));
      request.upload.addEventListener("progress", (event) => {
        if (event.lengthComputable) onProgress?.(Math.max(1, Math.round((event.loaded / event.total) * 100)));
      });
      request.addEventListener("load", () => {
        let payload = null;
        try { payload = request.responseText ? JSON.parse(request.responseText) : null; } catch (_) { payload = request.responseText; }
        if (request.status >= 200 && request.status < 300 && !isTelegramFailurePayload(payload)) { resolve(payload); return; }
        const message = typeof payload === "string" ? payload : payload?.description || payload?.message || payload?.error?.message;
        const error = new Error(message || `Request failed (${request.status})`);
        error.status = isTelegramFailurePayload(payload) ? Number(payload?.error_code || request.status) : request.status;
        error.httpStatus = request.status;
        error.telegramRejected = isTelegramFailurePayload(payload);
        error.payload = payload;
        if (request.status === 401 && state.user && isPlatformUnauthorizedPayload(payload)) window.queueMicrotask(() => handleExpiredSession());
        reject(error);
      });
      request.addEventListener("error", () => {
        const error = new Error("The connection ended before Phenogram could confirm delivery.");
        error.deliveryUnknown = true;
        reject(error);
      });
      request.addEventListener("abort", () => reject(new Error("Upload cancelled.")));
      request.send(body);
    });
  }

  function buildAttachmentUpload(draft) {
    const files = draft.files || [];
    const form = new FormData();
    const replyParameters = replyParametersFromDraft(draft);
    if (replyParameters) form.append("reply_parameters", JSON.stringify(replyParameters));
    if (files.length === 1) {
      const attachment = files[0];
      const method = methodForAttachment(attachment, draft.sendMode);
      if (draft.text.trim() && ["sendSticker", "sendVideoNote"].includes(method)) throw new Error("Stickers and video notes do not support captions. Send the text as a separate message.");
      if (draft.text.trim()) form.append("caption", draft.text.trim());
      if (draft.text.trim() && draft.parseMode) form.append("parse_mode", draft.parseMode);
      if (draft.replyMarkup) form.append("reply_markup", JSON.stringify(draft.replyMarkup));
      form.append(uploadFieldForMethod(method), attachment.file, attachment.name);
      return { method, form };
    }
    if (files.some((attachment) => attachment.explicitMethod)) throw new Error("Stickers and video notes must be sent one at a time.");
    const media = files.map((attachment, index) => {
      const method = methodForAttachment(attachment, draft.sendMode);
      const type = { sendPhoto: "photo", sendVideo: "video", sendAudio: "audio" }[method] || "document";
      const field = `media_${index}`;
      return { type, media: `attach://${field}`, ...(index === 0 && draft.text.trim() ? { caption: draft.text.trim(), ...(draft.parseMode ? { parse_mode: draft.parseMode } : {}) } : {}) };
    });
    if (draft.replyMarkup) throw new Error("Telegram media albums do not support inline buttons. Send the buttons with a separate message.");
    const kinds = new Set(media.map((entry) => entry.type));
    const validVisualAlbum = [...kinds].every((type) => ["photo", "video"].includes(type));
    const validSingleKindAlbum = kinds.size === 1 && ["audio", "document"].includes(media[0]?.type);
    if (!validVisualAlbum && !validSingleKindAlbum) throw new Error("Telegram albums can mix photos and videos, but audio and documents must each be sent in their own album.");
    form.append("media", JSON.stringify(media));
    files.forEach((attachment, index) => form.append(`media_${index}`, attachment.file, attachment.name));
    return { method: "sendMediaGroup", form };
  }

  function buildEditMediaUpload(draft) {
    if (draft.files.length !== 1) throw new Error("Choose exactly one replacement file when editing media.");
    const attachment = draft.files[0];
    if (attachment.explicitMethod) throw new Error("Stickers and video notes cannot replace message media here.");
    const sendMethod = methodForAttachment(attachment, draft.sendMode);
    const type = { sendPhoto: "photo", sendVideo: "video", sendAnimation: "animation", sendAudio: "audio", sendVoice: "voice_note", sendDocument: "document" }[sendMethod];
    if (!type) throw new Error("This attachment type cannot replace message media.");
    const ephemeral = draft.edit?.ephemeral_message_id !== "" && draft.edit?.ephemeral_message_id != null;
    const form = new FormData();
    form.append("media", JSON.stringify({ type, media: "attach://media_file", ...(draft.text.trim() ? { caption: draft.text.trim(), ...(draft.parseMode ? { parse_mode: draft.parseMode } : {}) } : {}) }));
    form.append(ephemeral ? "ephemeral_message_id" : "message_id", String(ephemeral ? draft.edit.ephemeral_message_id : draft.edit.message_id));
    if (draft.replyMarkup) form.append("reply_markup", JSON.stringify(draft.replyMarkup));
    form.append("media_file", attachment.file, attachment.name);
    return { method: ephemeral ? "editEphemeralMessageMedia" : "editMessageMedia", form };
  }

  function optimisticMessageFromDraft(draft, clientId) {
    const media = (draft.files || []).map((attachment) => ({ kind: methodForAttachment(attachment, draft.sendMode).replace(/^send/, "").replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`).replace(/^_/, ""), url: attachment.url, file_name: attachment.name, file_size: attachment.size, mime_type: attachment.type }));
    return { id: clientId, client_id: clientId, direction: "outgoing", event_type: draft.edit ? "editMessageText" : draft.files?.length > 1 ? "sendMediaGroup" : draft.files?.length ? methodForAttachment(draft.files[0], draft.sendMode) : "sendMessage", text: draft.files?.length ? null : draft.text, caption: draft.files?.length ? draft.text : null, media, created_at: new Date().toISOString(), status: draft.files?.length ? "uploading" : "sending", _optimistic: true, _draft: draft };
  }

  function removeOptimisticMessage(key, clientId, { revoke = false } = {}) {
    const optimistic = state.botViewOptimisticMessages.get(key) || [];
    const found = optimistic.find((item) => item.client_id === clientId);
    state.botViewOptimisticMessages.set(key, optimistic.filter((item) => item.client_id !== clientId));
    if (revoke && found?._draft) revokeDraftFiles(found._draft);
  }

  async function submitMessage(form) {
    formError(form, "");
    saveBotViewDraftFromDom();
    const conversation = state.conversations.find((item) => conversationId(item) === String(state.selectedConversationId));
    const conversationIdValue = conversationId(conversation);
    const id = String(state.selectedBotId || "");
    const contextVersion = state.botContextVersion;
    const sessionVersion = state.sessionVersion;
    const draft = botViewDraft();
    draft.text = String(draft.text || "").trim();
    if (!conversationIdValue || (!draft.text && !draft.files.length)) return;
    const key = botViewSendKey(id, conversationIdValue);
    if (state.botViewSendsInFlight.has(key)) return;
    if (draft.deliveryUnknown && !window.confirm("Telegram may already have received the previous attempt. Send it again and risk a duplicate?")) return;
    if (draft.retryClientId) removeOptimisticMessage(key, draft.retryClientId);
    const requestKey = `botAction:${conversationIdValue}`;
    const ticket = startRequest(requestKey);
    const clientId = window.crypto?.randomUUID?.() || `pending-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const sendContext = { key, requestKey, botId: id, chatId: conversationIdValue, contextVersion, sessionVersion, ticket };
    const draftSnapshot = { ...draft, files: [...draft.files], reply: effectiveReplyForConversation(draft, conversation) };
    const actionGenerationHeaders = botViewActionGenerationHeaders(draftSnapshot.edit || draftSnapshot.reply);
    const optimistic = optimisticMessageFromDraft(draftSnapshot, clientId);
    if (!reserveBotViewSend(key, sendContext)) return;
    if (!draft.edit) state.botViewOptimisticMessages.set(key, [...(state.botViewOptimisticMessages.get(key) || []), optimistic]);
    if (draftSnapshot.files.length) state.botViewUploadProgress = { key, percent: 0 };
    state.botViewDrafts.set(key, emptyBotViewDraft());
    state.botViewOpenPanel = null;
    renderBotViewLive();
    const timeline = document.querySelector("#chat-timeline");
    if (timeline) timeline.scrollTop = timeline.scrollHeight;
    try {
      let response;
      if (draftSnapshot.edit && draftSnapshot.files.length) {
        const limit = botViewUploadLimit();
        if (draftSnapshot.files[0].size > limit) throw new Error(`This file is larger than the ${formatBytes(limit)} limit for this bot.`);
        if (methodForAttachment(draftSnapshot.files[0], draftSnapshot.sendMode) === "sendPhoto" && draftSnapshot.files[0].size > BOT_VIEW_PHOTO_MAX_FILE_BYTES) throw new Error("Telegram photos must be 10 MB or smaller. Send the image as a file instead.");
        const upload = buildEditMediaUpload(draftSnapshot);
        response = await uploadApi(botViewActionPath(id, conversationIdValue, upload.method), upload.form, (percent) => {
          state.botViewUploadProgress = { key, percent };
          const progress = document.querySelector(".upload-progress");
          if (progress) {
            progress.setAttribute("aria-valuenow", String(percent));
            progress.querySelector("i")?.style.setProperty("--upload-progress", `${percent}%`);
            const label = progress.querySelector("span");
            if (label) label.textContent = `Uploading ${percent}%`;
          }
        }, actionGenerationHeaders);
      } else if (draftSnapshot.edit) {
        const ephemeral = draftSnapshot.edit.ephemeral_message_id !== "" && draftSnapshot.edit.ephemeral_message_id != null;
        const captionEdit = draftSnapshot.edit.has_caption || draftSnapshot.edit.has_media;
        const method = ephemeral ? captionEdit ? "editEphemeralMessageCaption" : "editEphemeralMessageText" : captionEdit ? "editMessageCaption" : "editMessageText";
        const identity = ephemeral ? { ephemeral_message_id: draftSnapshot.edit.ephemeral_message_id } : { message_id: Number(draftSnapshot.edit.message_id) || draftSnapshot.edit.message_id };
        const content = method === "editMessageCaption" ? { caption: draftSnapshot.text, ...(draftSnapshot.parseMode ? { parse_mode: draftSnapshot.parseMode } : {}) } : { text: draftSnapshot.text, ...(draftSnapshot.parseMode ? { parse_mode: draftSnapshot.parseMode } : {}) };
        if (method === "editEphemeralMessageCaption") {
          delete content.text;
          content.caption = draftSnapshot.text;
        }
        response = await api(botViewActionPath(id, conversationIdValue, method), { method: "POST", body: { ...identity, ...content, ...(draftSnapshot.replyMarkup ? { reply_markup: draftSnapshot.replyMarkup } : {}) }, headers: actionGenerationHeaders });
      } else if (draftSnapshot.files.length) {
        const limit = botViewUploadLimit();
        if (draftSnapshot.files.some((attachment) => attachment.size > limit)) throw new Error(`One of these files is larger than the ${formatBytes(limit)} limit for this bot.`);
        const totalLimit = botViewAggregateUploadLimit();
        if (draftSnapshot.files.reduce((sum, attachment) => sum + attachment.size, 0) > totalLimit) throw new Error(`This upload is larger than the ${formatBytes(totalLimit)} combined limit for this bot.`);
        if (draftSnapshot.files.some((attachment) => methodForAttachment(attachment, draftSnapshot.sendMode) === "sendPhoto" && attachment.size > BOT_VIEW_PHOTO_MAX_FILE_BYTES)) throw new Error("Telegram photos must be 10 MB or smaller. Send the image as a file instead.");
        const upload = buildAttachmentUpload(draftSnapshot);
        response = await uploadApi(botViewActionPath(id, conversationIdValue, upload.method), upload.form, (percent) => {
          state.botViewUploadProgress = { key, percent };
          const progress = document.querySelector(".upload-progress");
          if (progress) {
            progress.setAttribute("aria-valuenow", String(percent));
            progress.querySelector("i")?.style.setProperty("--upload-progress", `${percent}%`);
            const label = progress.querySelector("span");
            if (label) label.textContent = `Uploading ${percent}%`;
          }
        }, actionGenerationHeaders);
      } else {
        const guest = Boolean(conversation?.guest_query_id);
        const messageContent = { message_text: draftSnapshot.text, ...(draftSnapshot.parseMode ? { parse_mode: draftSnapshot.parseMode } : {}) };
        const params = guest ? { result: { type: "article", id: clientId.slice(0, 64), title: "Reply from bot", input_message_content: messageContent } } : { text: draftSnapshot.text, ...(draftSnapshot.parseMode ? { parse_mode: draftSnapshot.parseMode } : {}), ...(draftSnapshot.replyMarkup ? { reply_markup: draftSnapshot.replyMarkup } : {}) };
        const replyParameters = replyParametersFromDraft(draftSnapshot);
        if (replyParameters && !guest) params.reply_parameters = replyParameters;
        response = await api(botViewActionPath(id, conversationIdValue, guest ? "answerGuestQuery" : "sendMessage"), { method: "POST", body: params, headers: guest ? {} : actionGenerationHeaders });
      }
      if (state.sessionVersion !== sessionVersion || !botRequestIsCurrent(requestKey, ticket, id, contextVersion)) return;
      const mergedResponseCount = mergeBotViewActionResponse(conversationIdValue, response, draftSnapshot.files);
      removeOptimisticMessage(key, clientId, { revoke: draftSnapshot.files.length > 0 && !mergedResponseCount });
      renderBotViewLive();
      await loadConversationMessages(conversationIdValue);
      if (state.sessionVersion !== sessionVersion || !botRequestIsCurrent(requestKey, ticket, id, contextVersion)) return;
      renderBotViewLive();
      window.requestAnimationFrame(() => { const currentTimeline = document.querySelector("#chat-timeline"); if (currentTimeline) currentTimeline.scrollTop = currentTimeline.scrollHeight; });
      surfaceWarnings(response);
    } catch (error) {
      if (state.sessionVersion !== sessionVersion || !botRequestIsCurrent(requestKey, ticket, id, contextVersion)) return;
      const definitive = botViewDefinitivelyRejected(error);
      optimistic.status = definitive ? "failed" : "delivery_unknown";
      optimistic.error = botViewErrorMessage(error);
      draftSnapshot.retryClientId = clientId;
      draftSnapshot.deliveryUnknown = !definitive;
      state.botViewDrafts.set(key, draftSnapshot);
      renderBotViewLive();
      const currentForm = document.querySelector("#message-form");
      if (currentForm) formError(currentForm, `${optimistic.error}${definitive ? "" : " Delivery may have succeeded; check the timeline before retrying."}`);
    } finally {
      state.botViewUploadProgress = null;
      document.querySelector(".upload-progress")?.remove();
      finishBotViewSend(sendContext);
      if (state.sessionVersion === sessionVersion && state.botContextVersion === contextVersion && String(state.selectedBotId || "") === id && state.route.name === "bot-view") {
        startBotViewRefresh();
      }
    }
  }

  function addBotViewFiles(fileList, { sendMode = null, explicitMethod = "" } = {}) {
    const files = [...(fileList || [])].filter((file) => file instanceof File);
    if (!files.length) return;
    const draft = botViewDraft();
    const conversation = state.conversations.find((item) => conversationId(item) === String(state.selectedConversationId));
    if (conversation?.guest_query_id) { toast("Guest queries can only be answered with a text result.", "error"); return; }
    if (draft.edit?.ephemeral_message_id !== "" && draft.edit?.ephemeral_message_id != null) { toast("Telegram does not allow a new file upload when editing an ephemeral message. Edit its text, caption, or buttons instead.", "error"); return; }
    const maxFiles = conversation?.receiver_user_id != null ? 1 : BOT_VIEW_MAX_FILES;
    const remaining = maxFiles - draft.files.length;
    if (remaining <= 0) { toast(`Telegram albums support up to ${BOT_VIEW_MAX_FILES} files.`, "error"); return; }
    const accepted = files.slice(0, remaining);
    const limit = botViewUploadLimit();
    const tooLarge = accepted.find((file) => file.size > limit);
    if (tooLarge) { toast(`${tooLarge.name} is larger than this bot's ${formatBytes(limit)} upload limit.`, "error"); return; }
    accepted.forEach((file) => draft.files.push({ id: `${Date.now()}-${Math.random().toString(16).slice(2)}`, file, url: URL.createObjectURL(file), name: file.name || "attachment", size: file.size, type: file.type || "application/octet-stream", isVoice: Boolean(file.isVoice), forceDocument: Boolean(file.forceDocument), explicitMethod }));
    if (sendMode) draft.sendMode = sendMode;
    if (files.length > accepted.length) toast(conversation?.receiver_user_id != null ? "Ephemeral messages support one attachment at a time." : `Only the first ${remaining} files were added.`, "warning");
    state.botViewOpenPanel = null;
    renderBotViewLive();
    document.querySelector("#message-form textarea")?.focus({ preventScroll: true });
  }

  async function sendBotViewSpecialAction(method, params, optimisticPayload = {}, suppliedActionGeneration) {
    const id = String(state.selectedBotId || "");
    const conversationIdValue = String(state.selectedConversationId || "");
    if (!id || !conversationIdValue) return;
    const key = botViewSendKey(id, conversationIdValue);
    if (state.botViewSendsInFlight.has(key)) return;
    const conversation = state.conversations.find((item) => conversationId(item) === conversationIdValue);
    if (!conversation) return;
    if (conversation.guest_query_id) { toast("Guest queries can only be answered with text.", "error"); return; }
    if (conversation.receiver_user_id != null && !["sendContact", "sendLocation", "sendVenue"].includes(method)) { toast("That message type is not available for ephemeral recipients.", "error"); return; }
    if (conversation.direct_messages_topic_id != null && ["sendPoll", "sendChatAction"].includes(method)) { toast("That message type is not available in direct-message topics.", "error"); return; }
    if (method === "sendChecklist" && !conversation.business_connection_id) { toast("Checklists require a business conversation.", "error"); return; }
    const contextVersion = state.botContextVersion;
    const sessionVersion = state.sessionVersion;
    const requestKey = `botAction:${conversationIdValue}`;
    const ticket = startRequest(requestKey);
    const clientId = window.crypto?.randomUUID?.() || `pending-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const sendContext = { key, requestKey, botId: id, chatId: conversationIdValue, contextVersion, sessionVersion, ticket };
    const draftReply = effectiveReplyForConversation(botViewDraft(), conversation);
    const actionGeneration = suppliedActionGeneration !== undefined ? suppliedActionGeneration : draftReply?.action_generation;
    const action = { method, params, optimisticPayload, actionGeneration, upload: false };
    const optimistic = { id: clientId, client_id: clientId, direction: "outgoing", event_type: method, created_at: new Date().toISOString(), status: "sending", _optimistic: true, _draft: { text: "", files: [], reply: botViewDraft().reply, edit: null }, _action: action, ...optimisticPayload };
    if (!reserveBotViewSend(key, sendContext)) return;
    state.botViewOptimisticMessages.set(key, [...(state.botViewOptimisticMessages.get(key) || []), optimistic]);
    state.botViewOpenPanel = null;
    renderBotViewLive();
    try {
      const supportsReply = ["sendContact", "sendLocation", "sendVenue", "sendPoll", "sendDice", "sendRichMessage", "sendChecklist"].includes(method);
      const effectiveReply = draftReply;
      const replyParameters = supportsReply ? replyParametersFromDraft({ reply: effectiveReply }) : null;
      const response = await api(botViewActionPath(id, conversationIdValue, method), { method: "POST", body: { ...params, ...(replyParameters ? { reply_parameters: replyParameters } : {}) }, headers: replyParameters?.ephemeral_message_id != null ? botViewActionGenerationHeaders(actionGeneration) : {} });
      if (state.sessionVersion !== sessionVersion || !botRequestIsCurrent(requestKey, ticket, id, contextVersion)) return;
      mergeBotViewActionResponse(conversationIdValue, response);
      removeOptimisticMessage(key, clientId);
      const draft = botViewDraft();
      draft.reply = null;
      renderBotViewLive();
      await loadConversationMessages(conversationIdValue);
      renderBotViewLive();
      window.requestAnimationFrame(() => { const timeline = document.querySelector("#chat-timeline"); if (timeline) timeline.scrollTop = timeline.scrollHeight; });
    } catch (error) {
      if (state.sessionVersion !== sessionVersion || !botRequestIsCurrent(requestKey, ticket, id, contextVersion)) return;
      optimistic.status = botViewDefinitivelyRejected(error) ? "failed" : "delivery_unknown";
      optimistic.error = botViewErrorMessage(error);
      renderBotViewLive();
    } finally {
      finishBotViewSend(sendContext);
      if (state.route.name === "bot-view") startBotViewMessageStream();
    }
  }

  async function sendBotViewMultipartSpecialAction(method, formData, optimisticPayload = {}, previewFiles = [], suppliedActionGeneration) {
    const id = String(state.selectedBotId || "");
    const conversationIdValue = String(state.selectedConversationId || "");
    if (!id || !conversationIdValue) return;
    const conversation = state.conversations.find((item) => conversationId(item) === conversationIdValue);
    if (!conversation || conversation.guest_query_id) { toast("This upload is not available in this conversation.", "error"); return; }
    const key = botViewSendKey(id, conversationIdValue);
    if (state.botViewSendsInFlight.has(key)) { previewFiles.forEach((file) => { if (file?.url?.startsWith?.("blob:")) URL.revokeObjectURL(file.url); }); return; }
    const contextVersion = state.botContextVersion;
    const sessionVersion = state.sessionVersion;
    const requestKey = `botAction:${conversationIdValue}`;
    const ticket = startRequest(requestKey);
    const clientId = window.crypto?.randomUUID?.() || `pending-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const sendContext = { key, requestKey, botId: id, chatId: conversationIdValue, contextVersion, sessionVersion, ticket };
    const actionGeneration = suppliedActionGeneration !== undefined ? suppliedActionGeneration : effectiveReplyForConversation(botViewDraft(), conversation)?.action_generation;
    const action = { method, formData, optimisticPayload, previewFiles, actionGeneration, upload: true };
    const optimistic = { id: clientId, client_id: clientId, direction: "outgoing", event_type: method, created_at: new Date().toISOString(), status: "uploading", _optimistic: true, _draft: { text: "", files: previewFiles }, _action: action, ...optimisticPayload };
    if (!reserveBotViewSend(key, sendContext)) return;
    state.botViewOptimisticMessages.set(key, [...(state.botViewOptimisticMessages.get(key) || []), optimistic]);
    state.botViewUploadProgress = { key, percent: 0 };
    state.botViewOpenPanel = null;
    renderBotViewLive();
    try {
      const response = await uploadApi(botViewActionPath(id, conversationIdValue, method), formData, (percent) => {
        state.botViewUploadProgress = { key, percent };
        const progress = document.querySelector(".upload-progress");
        if (progress) {
          progress.setAttribute("aria-valuenow", String(percent));
          progress.querySelector("i")?.style.setProperty("--upload-progress", `${percent}%`);
          const label = progress.querySelector("span");
          if (label) label.textContent = `Uploading ${percent}%`;
        }
      }, botViewActionGenerationHeaders(actionGeneration));
      if (state.sessionVersion !== sessionVersion || !botRequestIsCurrent(requestKey, ticket, id, contextVersion)) return;
      const mergedResponseCount = mergeBotViewActionResponse(conversationIdValue, response, previewFiles);
      removeOptimisticMessage(key, clientId, { revoke: previewFiles.length > 0 && !mergedResponseCount });
      renderBotViewLive();
      await loadConversationMessages(conversationIdValue);
      renderBotViewLive();
      window.requestAnimationFrame(() => { const timeline = document.querySelector("#chat-timeline"); if (timeline) timeline.scrollTop = timeline.scrollHeight; });
    } catch (error) {
      if (state.sessionVersion !== sessionVersion || !botRequestIsCurrent(requestKey, ticket, id, contextVersion)) return;
      optimistic.status = botViewDefinitivelyRejected(error) ? "failed" : "delivery_unknown";
      optimistic.error = botViewErrorMessage(error);
      renderBotViewLive();
    } finally {
      state.botViewUploadProgress = null;
      finishBotViewSend(sendContext);
      if (state.route.name === "bot-view") startBotViewMessageStream();
    }
  }

  async function deleteBotViewMessage(messageIdValue, ephemeralMessageId = "", actionGeneration = "") {
    const ephemeral = ephemeralMessageId !== "" && ephemeralMessageId != null;
    if ((!ephemeral && messageIdValue === "") || !state.selectedBotId || !state.selectedConversationId) return;
    try {
      const conversation = state.conversations.find((item) => conversationId(item) === String(state.selectedConversationId));
      const business = Boolean(conversation?.business_connection_id);
      const method = ephemeral ? "deleteEphemeralMessage" : business ? "deleteBusinessMessages" : "deleteMessage";
      const body = ephemeral ? { ephemeral_message_id: ephemeralMessageId } : business ? { message_ids: [Number(messageIdValue) || messageIdValue] } : { message_id: Number(messageIdValue) || messageIdValue };
      const response = await api(botViewActionPath(state.selectedBotId, state.selectedConversationId, method), { method: "POST", body, headers: ephemeral ? botViewActionGenerationHeaders(actionGeneration) : {} });
      mergeBotViewActionResponse(state.selectedConversationId, response);
      renderBotViewLive();
      await loadConversationMessages(state.selectedConversationId);
      renderBotViewLive();
      toast("Message deleted.");
    } catch (error) {
      toast(botViewErrorMessage(error), "error");
    }
  }

  function setBotViewBulkMode(enabled) {
    const key = botViewKey();
    state.botViewBulkModeKey = enabled ? key : null;
    if (enabled && !state.botViewBulkSelection.has(key)) state.botViewBulkSelection.set(key, new Set());
    if (!enabled) state.botViewBulkSelection.delete(key);
    renderBotViewLive();
  }

  function toggleBotViewBulkMessage(messageIdValue, checked) {
    const key = botViewKey();
    if (state.botViewBulkModeKey !== key) return;
    const selected = state.botViewBulkSelection.get(key) || new Set();
    if (checked) selected.add(String(messageIdValue));
    else selected.delete(String(messageIdValue));
    state.botViewBulkSelection.set(key, selected);
    renderBotViewLive();
  }

  async function deleteSelectedBotViewMessages() {
    const key = botViewKey();
    const selected = [...(state.botViewBulkSelection.get(key) || [])];
    if (!selected.length || !state.selectedBotId || !state.selectedConversationId) return;
    if (!window.confirm(`Delete ${selected.length} selected message${selected.length === 1 ? "" : "s"}? Telegram's normal deletion limits still apply.`)) return;
    const conversation = state.conversations.find((item) => conversationId(item) === String(state.selectedConversationId));
    const method = conversation?.business_connection_id ? "deleteBusinessMessages" : "deleteMessages";
    try {
      const response = await api(botViewActionPath(state.selectedBotId, state.selectedConversationId, method), { method: "POST", body: { message_ids: selected.map((value) => Number(value) || value) } });
      mergeBotViewActionResponse(state.selectedConversationId, response);
      state.botViewBulkSelection.delete(key);
      state.botViewBulkModeKey = null;
      renderBotViewLive();
      await loadConversationMessages(state.selectedConversationId);
      renderBotViewLive();
      toast(`${selected.length} message${selected.length === 1 ? "" : "s"} deleted.`);
    } catch (error) {
      toast(botViewErrorMessage(error), "error");
    }
  }

  async function startVoiceRecording() {
    if (state.botViewRecorder) return;
    if (!navigator.mediaDevices?.getUserMedia || typeof MediaRecorder !== "function") {
      toast("Voice recording is not supported by this browser. You can attach an audio file instead.", "error");
      return;
    }
    const supported = (type) => !MediaRecorder.isTypeSupported || MediaRecorder.isTypeSupported(type);
    const mimeType = ["audio/ogg;codecs=opus", "audio/mp4;codecs=mp4a.40.2", "audio/mp4", "audio/webm;codecs=opus", "audio/webm"].find(supported) || "";
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true }, video: false });
      if (state.route.name !== "bot-view" || !state.selectedConversationId) { stream.getTracks().forEach((track) => track.stop()); return; }
      const recorder = mimeType ? new MediaRecorder(stream, { mimeType }) : new MediaRecorder(stream);
      const recording = { key: botViewKey(), recorder, stream, chunks: [], startedAt: Date.now(), cancelled: false, timer: null, mimeType: recorder.mimeType || mimeType };
      recorder.addEventListener("dataavailable", (event) => { if (event.data?.size) recording.chunks.push(event.data); });
      recorder.addEventListener("stop", () => {
        window.clearInterval(recording.timer);
        stream.getTracks().forEach((track) => track.stop());
        if (state.botViewRecorder === recording) state.botViewRecorder = null;
        if (!recording.cancelled && recording.chunks.length) {
          const type = recording.mimeType || recording.chunks[0].type || "audio/webm";
          const voiceCompatible = type.startsWith("audio/ogg") || type.startsWith("audio/mp4") || type.startsWith("audio/mpeg") || type.startsWith("audio/x-m4a");
          const extension = type.startsWith("audio/ogg") ? "ogg" : type.startsWith("audio/mp4") ? "m4a" : type.startsWith("audio/mpeg") ? "mp3" : "webm";
          const file = new File(recording.chunks, `voice-${Date.now()}.${extension}`, { type });
          file.isVoice = voiceCompatible;
          file.forceDocument = !voiceCompatible;
          addBotViewFiles([file], { sendMode: voiceCompatible ? "media" : "document" });
          if (!voiceCompatible) toast("This browser records WebM, so the recording will be sent as a file.", "warning");
        } else if (state.route.name === "bot-view") renderBotViewLive();
      });
      state.botViewRecorder = recording;
      recorder.start(250);
      recording.timer = window.setInterval(() => {
        const elapsed = Math.floor((Date.now() - recording.startedAt) / 1000);
        const timer = document.querySelector(".voice-recorder time");
        if (timer) timer.textContent = `${String(Math.floor(elapsed / 60)).padStart(2, "0")}:${String(elapsed % 60).padStart(2, "0")}`;
      }, 250);
      renderBotViewLive();
    } catch (error) {
      toast(error?.name === "NotAllowedError" ? "Microphone access was denied. Allow it in browser settings or attach an audio file." : "Could not start voice recording.", "error");
    }
  }

  function stopVoiceRecording({ cancel = false, renderResult = true } = {}) {
    const recording = state.botViewRecorder;
    if (!recording) return;
    recording.cancelled = Boolean(cancel);
    if (recording.recorder?.state !== "inactive") recording.recorder.stop();
    else {
      window.clearInterval(recording.timer);
      recording.stream?.getTracks?.().forEach((track) => track.stop());
      state.botViewRecorder = null;
      if (renderResult && state.route.name === "bot-view") renderBotViewLive();
    }
  }

  function submitPoll(form) {
    formError(form, "");
    const data = new FormData(form);
    const question = String(data.get("question") || "").trim();
    const options = String(data.get("options") || "").split(/\r?\n/).map((option) => option.trim()).filter(Boolean);
    if (options.length < 1 || options.length > 12) { formError(form, "Enter between 1 and 12 options, one per line."); return; }
    sendBotViewSpecialAction("sendPoll", { question, options: options.map((text) => ({ text })), is_anonymous: data.get("is_anonymous") === "on", allows_multiple_answers: data.get("allows_multiple_answers") === "on" }, { poll: { question, options: options.map((text) => ({ text, voter_count: 0 })), total_voter_count: 0 } });
  }

  function submitLocation(form) {
    formError(form, "");
    const data = new FormData(form);
    const latitude = Number(data.get("latitude"));
    const longitude = Number(data.get("longitude"));
    if (!Number.isFinite(latitude) || latitude < -90 || latitude > 90 || !Number.isFinite(longitude) || longitude < -180 || longitude > 180) { formError(form, "Enter a valid latitude and longitude."); return; }
    sendBotViewSpecialAction("sendLocation", { latitude, longitude }, { location: { latitude, longitude } });
  }

  function submitContact(form) {
    formError(form, "");
    const data = new FormData(form);
    const firstName = String(data.get("first_name") || "").trim();
    const phoneNumber = String(data.get("phone_number") || "").trim();
    if (!firstName || !phoneNumber) { formError(form, "Enter a name and phone number."); return; }
    sendBotViewSpecialAction("sendContact", { first_name: firstName, phone_number: phoneNumber }, { contact: { first_name: firstName, phone_number: phoneNumber } });
  }

  function submitVenue(form) {
    formError(form, "");
    const data = new FormData(form);
    const latitude = Number(data.get("latitude"));
    const longitude = Number(data.get("longitude"));
    const title = String(data.get("title") || "").trim();
    const address = String(data.get("address") || "").trim();
    if (!Number.isFinite(latitude) || latitude < -90 || latitude > 90 || !Number.isFinite(longitude) || longitude < -180 || longitude > 180 || !title || !address) { formError(form, "Enter valid coordinates, a place name, and an address."); return; }
    sendBotViewSpecialAction("sendVenue", { latitude, longitude, title, address }, { venue: { location: { latitude, longitude }, title, address } });
  }

  function submitLivePhoto(form) {
    formError(form, "");
    const data = new FormData(form);
    const video = data.get("live_photo");
    const photo = data.get("photo");
    const caption = String(data.get("caption") || "").trim();
    if (!(video instanceof File) || !video.size || !(photo instanceof File) || !photo.size) { formError(form, "Choose both the short video and its static photo."); return; }
    if (video.size > BOT_VIEW_PHOTO_MAX_FILE_BYTES || photo.size > BOT_VIEW_PHOTO_MAX_FILE_BYTES) { formError(form, "Each live-photo file must be 10 MB or smaller."); return; }
    if (!video.type.startsWith("video/") || !photo.type.startsWith("image/")) { formError(form, "Choose a video file and an image file."); return; }
    const draft = botViewDraft();
    const conversation = state.conversations.find((item) => conversationId(item) === String(state.selectedConversationId));
    const replyParameters = replyParametersFromDraft({ reply: effectiveReplyForConversation(draft, conversation) });
    const actionGeneration = effectiveReplyForConversation(draft, conversation)?.action_generation;
    const body = new FormData();
    if (caption) body.append("caption", caption);
    if (draft.parseMode) body.append("parse_mode", draft.parseMode);
    if (replyParameters) body.append("reply_parameters", JSON.stringify(replyParameters));
    if (draft.replyMarkup) body.append("reply_markup", JSON.stringify(draft.replyMarkup));
    body.append("live_photo", video, video.name || "live-photo.mp4");
    body.append("photo", photo, photo.name || "live-photo.jpg");
    const previewFiles = [
      { file: photo, url: URL.createObjectURL(photo), name: photo.name || "Live photo", size: photo.size, type: photo.type },
      { file: video, url: URL.createObjectURL(video), name: video.name || "Live photo video", size: video.size, type: video.type },
    ];
    sendBotViewMultipartSpecialAction("sendLivePhoto", body, { caption, media: [{ kind: "photo", url: previewFiles[0].url, label: "Live photo" }, { kind: "live_photo", url: previewFiles[1].url, label: "Live photo" }] }, previewFiles, actionGeneration);
  }

  function submitRichMessage(form) {
    formError(form, "");
    const markdown = String(new FormData(form).get("markdown") || "").trim();
    if (!markdown) { formError(form, "Write the rich message first."); return; }
    sendBotViewSpecialAction("sendRichMessage", { rich_message: { markdown } }, { rich_message: { blocks: [{ type: "paragraph", text: markdown }] } });
  }

  function submitChecklist(form) {
    formError(form, "");
    const data = new FormData(form);
    const title = String(data.get("title") || "").trim();
    const tasks = String(data.get("tasks") || "").split(/\r?\n/).map((task) => task.trim()).filter(Boolean);
    if (!title || tasks.length < 1 || tasks.length > 30 || tasks.some((task) => task.length > 100)) { formError(form, "Enter a title and 1–30 tasks of at most 100 characters each."); return; }
    const checklist = { title, tasks: tasks.map((text, index) => ({ id: index + 1, text })), ...(data.get("others_can_add_tasks") === "on" ? { others_can_add_tasks: true } : {}), ...(data.get("others_can_mark_tasks_as_done") === "on" ? { others_can_mark_tasks_as_done: true } : {}) };
    const messageIdValue = String(data.get("message_id") || "");
    if (messageIdValue) performBotViewControlAction("editMessageChecklist", { message_id: Number(messageIdValue) || messageIdValue, checklist }, "Checklist updated.");
    else sendBotViewSpecialAction("sendChecklist", { checklist }, { checklist });
  }

  async function submitSuggestedPostDecision(form, decision) {
    formError(form, "");
    const data = new FormData(form);
    const messageIdValue = String(data.get("message_id") || "");
    const conversation = state.conversations.find((item) => conversationId(item) === String(state.selectedConversationId));
    if (!messageIdValue || conversation?.direct_messages_topic_id == null) { formError(form, "This suggested post is no longer available in the current conversation."); return; }
    const method = decision === "decline" ? "declineSuggestedPost" : "approveSuggestedPost";
    const params = { message_id: Number(messageIdValue) || messageIdValue };
    if (method === "approveSuggestedPost") {
      const rawDate = String(data.get("send_date") || "").trim();
      if (rawDate) {
        const milliseconds = Date.parse(rawDate);
        if (!Number.isFinite(milliseconds)) { formError(form, "Choose a valid publication time."); return; }
        params.send_date = Math.floor(milliseconds / 1000);
      }
    } else {
      const comment = String(data.get("comment") || "").trim();
      if (comment.length > 128) { formError(form, "The decline comment must be 128 characters or fewer."); return; }
      if (comment) params.comment = comment;
    }
    const botIdValue = String(state.selectedBotId || "");
    const conversationIdValue = String(state.selectedConversationId || "");
    const contextVersion = state.botContextVersion;
    const sessionVersion = state.sessionVersion;
    setSubmitting(form, true, method === "approveSuggestedPost" ? "Approving…" : "Declining…");
    try {
      const response = await api(botViewActionPath(botIdValue, conversationIdValue, method), { method: "POST", body: params });
      if (state.sessionVersion !== sessionVersion || state.botContextVersion !== contextVersion || String(state.selectedConversationId || "") !== conversationIdValue) return;
      mergeBotViewActionResponse(conversationIdValue, response);
      state.botViewOpenPanel = null;
      renderBotViewLive();
      await loadConversationMessages(conversationIdValue);
      renderBotViewLive();
      toast(method === "approveSuggestedPost" ? "Suggested post approved." : "Suggested post declined.");
    } catch (error) {
      if (form.isConnected) {
        formError(form, botViewErrorMessage(error));
        setSubmitting(form, false);
      } else {
        toast(botViewErrorMessage(error), "error");
      }
    }
  }

  function markBotViewCallbackAnswered(conversation, actionGeneration) {
    if (!conversation || actionGeneration === "" || actionGeneration == null) return;
    conversationMessages(conversation).forEach((item) => {
      if (String(item?.action_generation ?? "") === String(actionGeneration)) {
        item.actionable = false;
        item._locally_observed = true;
        item._response_pending = true;
      }
    });
  }

  async function submitCallbackAnswer(form) {
    formError(form, "");
    const data = new FormData(form);
    const actionGeneration = String(data.get("action_generation") || "");
    const textValue = String(data.get("text") || "").trim();
    if (!actionGeneration) { formError(form, "This callback is no longer actionable. Refresh the conversation."); return; }
    if (textValue.length > 200) { formError(form, "Callback text must be 200 characters or fewer."); return; }
    const botIdValue = String(state.selectedBotId || "");
    const conversationIdValue = String(state.selectedConversationId || "");
    const contextVersion = state.botContextVersion;
    const sessionVersion = state.sessionVersion;
    setSubmitting(form, true, "Answering…");
    try {
      await api(botViewActionPath(botIdValue, conversationIdValue, "answerCallbackQuery"), {
        method: "POST",
        body: { ...(textValue ? { text: textValue } : {}), ...(data.get("show_alert") === "on" ? { show_alert: true } : {}) },
        headers: botViewActionGenerationHeaders(actionGeneration),
      });
      if (state.sessionVersion !== sessionVersion || state.botContextVersion !== contextVersion || String(state.selectedConversationId || "") !== conversationIdValue) return;
      const conversation = state.conversations.find((item) => conversationId(item) === conversationIdValue);
      markBotViewCallbackAnswered(conversation, actionGeneration);
      state.botViewOpenPanel = null;
      renderBotViewLive();
      await loadConversationMessages(conversationIdValue);
      renderBotViewLive();
      toast("Callback answered.");
    } catch (error) {
      if (form.isConnected) {
        formError(form, botViewErrorMessage(error));
        setSubmitting(form, false);
      } else {
        toast(botViewErrorMessage(error), "error");
      }
    }
  }

  async function submitForwardMessage(form) {
    formError(form, "");
    const data = new FormData(form);
    const targetConversationId = String(data.get("conversation_id") || "");
    const messageIdValue = String(data.get("message_id") || "");
    const fromChatId = String(data.get("from_chat_id") || "");
    const method = data.get("mode") === "copy" ? "copyMessage" : "forwardMessage";
    if (!targetConversationId || !messageIdValue || !fromChatId) { formError(form, "Select a destination."); return; }
    setSubmitting(form, true, "Sending…");
    try {
      const response = await api(botViewActionPath(state.selectedBotId, targetConversationId, method), { method: "POST", body: { from_chat_id: Number(fromChatId) || fromChatId, message_id: Number(messageIdValue) || messageIdValue } });
      mergeBotViewActionResponse(targetConversationId, response);
      state.botViewOpenPanel = null;
      renderBotViewLive();
      toast(method === "copyMessage" ? "Message copied." : "Message forwarded.");
    } catch (error) {
      formError(form, botViewErrorMessage(error));
      setSubmitting(form, false);
    }
  }

  function submitMessageFormat(form) {
    botViewDraft().parseMode = String(new FormData(form).get("parse_mode") || "");
    state.botViewOpenPanel = null;
    renderBotViewLive();
    document.querySelector("#message-form textarea")?.focus({ preventScroll: true });
  }

  function submitMessageButtons(form) {
    formError(form, "");
    const lines = String(new FormData(form).get("buttons") || "").split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
    const buttons = [];
    for (const line of lines) {
      const separator = line.indexOf("|");
      const text = separator >= 0 ? line.slice(0, separator).trim() : "";
      const url = separator >= 0 ? line.slice(separator + 1).trim() : "";
      const parsed = safeExternalLink(url);
      if (!text || !parsed || !parsed.startsWith("https://")) { formError(form, "Use one button per line in the form Label | https://example.com."); return; }
      buttons.push([{ text, url: parsed }]);
    }
    if (!buttons.length || buttons.length > 100) { formError(form, "Add between 1 and 100 buttons."); return; }
    botViewDraft().replyMarkup = { inline_keyboard: buttons };
    state.botViewOpenPanel = null;
    renderBotViewLive();
    document.querySelector("#message-form textarea")?.focus({ preventScroll: true });
  }

  function removeBotViewAttachment(attachmentId) {
    const draft = botViewDraft();
    const attachment = draft.files.find((item) => item.id === attachmentId);
    if (attachment?.url?.startsWith("blob:")) URL.revokeObjectURL(attachment.url);
    draft.files = draft.files.filter((item) => item.id !== attachmentId);
    renderBotViewLive();
  }

  function retryBotViewMessage(clientId) {
    const key = botViewKey();
    const optimistic = (state.botViewOptimisticMessages.get(key) || []).find((item) => item.client_id === clientId);
    if (!optimistic?._draft) return;
    if (optimistic.status === "delivery_unknown" && !window.confirm("Telegram may already have received this message. Retry anyway and risk sending a duplicate?")) return;
    state.botViewDrafts.set(key, { ...optimistic._draft, files: [...(optimistic._draft.files || [])], retryClientId: null, deliveryUnknown: false });
    removeOptimisticMessage(key, clientId);
    renderBotViewLive();
    window.setTimeout(() => document.querySelector("#message-form")?.requestSubmit(), 0);
  }

  function retryBotViewSpecialAction(clientId) {
    const key = botViewKey();
    const optimistic = (state.botViewOptimisticMessages.get(key) || []).find((item) => item.client_id === clientId);
    const action = optimistic?._action;
    if (!action) return;
    if (optimistic.status === "delivery_unknown" && !window.confirm("Telegram may already have received this action. Retry anyway and risk a duplicate?")) return;
    removeOptimisticMessage(key, clientId);
    if (action.upload) sendBotViewMultipartSpecialAction(action.method, action.formData, action.optimisticPayload, action.previewFiles, action.actionGeneration);
    else sendBotViewSpecialAction(action.method, action.params, action.optimisticPayload, action.actionGeneration);
  }

  async function markBusinessMessageRead(messageIdValue) {
    if (!messageIdValue || !state.selectedBotId || !state.selectedConversationId) return;
    try {
      const response = await api(botViewActionPath(state.selectedBotId, state.selectedConversationId, "readBusinessMessage"), { method: "POST", body: { message_id: Number(messageIdValue) || messageIdValue } });
      mergeBotViewActionResponse(state.selectedConversationId, response);
      renderBotViewLive();
      toast("Message marked as read.");
    } catch (error) {
      toast(botViewErrorMessage(error), "error");
    }
  }

  async function performBotViewControlAction(method, params, successMessage) {
    const id = String(state.selectedBotId || "");
    const conversationIdValue = String(state.selectedConversationId || "");
    const contextVersion = state.botContextVersion;
    const sessionVersion = state.sessionVersion;
    if (!id || !conversationIdValue) return;
    try {
      const response = await api(botViewActionPath(id, conversationIdValue, method), { method: "POST", body: params });
      if (state.sessionVersion !== sessionVersion || state.botContextVersion !== contextVersion || String(state.selectedBotId || "") !== id || String(state.selectedConversationId || "") !== conversationIdValue) return;
      mergeBotViewActionResponse(conversationIdValue, response);
      state.botViewOpenPanel = null;
      renderBotViewLive();
      await loadConversationMessages(conversationIdValue);
      renderBotViewLive();
      if (successMessage) toast(successMessage);
    } catch (error) {
      if (state.sessionVersion === sessionVersion && state.botContextVersion === contextVersion) toast(botViewErrorMessage(error), "error");
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

  if (window.__PHENOGRAM_CHAT_TEST_MODE__ === true) {
    window.__PHENOGRAM_CHAT_TEST__ = {
      state,
      isPlatformUnauthorizedPayload,
      isTelegramFailurePayload,
      botViewDefinitivelyRejected,
      safeMediaUrl,
      messageStableId,
      ephemeralMessageIsActionable,
      botViewActionGenerationHeaders,
      emptyBotViewDraft,
      botViewDraft,
      botViewKey,
      botViewNearBottom,
      botViewPrependScrollTop,
      botViewUnreadAfterInsert,
      reserveBotViewSend,
      finishBotViewSend,
      botViewMessageStreamContextIsCurrent,
      mergeConversationMessageSnapshot,
      mergeConversationTimelineItems,
      botViewTimelineMessagesFromResponse,
      botViewAggregateUploadLimit,
      collapseMediaGroups,
      renderMessage,
      renderComposerPanel,
    };
    return;
  }

  document.addEventListener("submit", (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) return;
    if (form.id === "connect-bot-form") { event.preventDefault(); submitConnectBot(form); }
    if (form.id === "managed-webhook-recovery-form") { event.preventDefault(); submitManagedWebhookRecovery(form); }
    if (form.id === "message-form") { event.preventDefault(); submitMessage(form); }
    if (form.id === "poll-form") { event.preventDefault(); submitPoll(form); }
    if (form.id === "location-form") { event.preventDefault(); submitLocation(form); }
    if (form.id === "contact-form") { event.preventDefault(); submitContact(form); }
    if (form.id === "venue-form") { event.preventDefault(); submitVenue(form); }
    if (form.id === "live-photo-form") { event.preventDefault(); submitLivePhoto(form); }
    if (form.id === "rich-message-form") { event.preventDefault(); submitRichMessage(form); }
    if (form.id === "checklist-form") { event.preventDefault(); submitChecklist(form); }
    if (form.id === "suggested-post-approve-form") { event.preventDefault(); submitSuggestedPostDecision(form, "approve"); }
    if (form.id === "suggested-post-decline-form") { event.preventDefault(); submitSuggestedPostDecision(form, "decline"); }
    if (form.id === "callback-answer-form") { event.preventDefault(); submitCallbackAnswer(form); }
    if (form.id === "forward-message-form") { event.preventDefault(); submitForwardMessage(form); }
    if (form.id === "message-format-form") { event.preventDefault(); submitMessageFormat(form); }
    if (form.id === "message-buttons-form") { event.preventDefault(); submitMessageButtons(form); }
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
    else if (action === "open-managed-webhook-recovery") setModal("managed-webhook-recovery");
    else if (action === "close-modal") closeModal();
    else if (action === "open-bot-picker") state.bots.length ? setModal("bot-picker") : setModal("connect");
    else if (action === "pick-bot") { const id = trigger.dataset.botId; selectBot(id); closeModal(); navigate(`/bots/${encodeURIComponent(id)}/overview`); }
    else if (action === "toggle-menu") setMobileMenu(!state.mobileMenu, { restoreFocus: state.mobileMenu });
    else if (action === "close-menu") setMobileMenu(false, { restoreFocus: true });
    else if (action === "logout") logout();
    else if (action === "go-billing") { closeModal(); navigate("/billing"); }
    else if (action === "retry-route") routeChanged();
    else if (action === "refresh-updates") loadUpdates();
    else if (action === "retry-conversations") {
      stopBotViewRefresh();
      Promise.resolve(state.botViewRefreshPromise).catch(() => {}).then(() => loadConversations()).finally(startBotViewRefresh);
    }
    else if (action === "clear-update-filters") { resetFilteredUpdatesReload(); state.filters = { ...state.filters, type: "", query: "" }; loadUpdates(); }
    else if (action === "toggle-updates") toggleUpdatesStream();
    else if (action === "view-update") { const itemId = normalizeJournalId(trigger.dataset.updateId); const item = state.updates.find((candidate) => updateJournalId(candidate) === itemId) || state.updates[Number(trigger.dataset.updateIndex)]; if (item) { state.drawer = { type: "update", itemId: updateJournalId(item), item }; render(); } }
    else if (action === "close-drawer") { state.drawer = null; render(); }
    else if (action === "copy-json") { const itemId = normalizeJournalId(state.drawer?.itemId || updateJournalId(state.drawer?.item)); const item = state.updates.find((candidate) => updateJournalId(candidate) === itemId) || state.drawer?.item; if (item) copyText(JSON.stringify(updatePayload(item), null, 2)); }
    else if (action === "copy-value") copyText(trigger.dataset.copyValue || "");
    else if (action === "select-conversation") {
      stopBotViewMessageStream();
      state.botViewConversationListPinned = false;
      state.botViewBulkModeKey = null;
      state.selectedConversationId = trigger.dataset.conversationId;
      state.botViewOpenPanel = null;
      render();
      loadConversationMessages(state.selectedConversationId).finally(() => {
        render();
        window.setTimeout(() => { const timeline = document.querySelector("#chat-timeline"); if (timeline) timeline.scrollTop = timeline.scrollHeight; startBotViewMessageStream(); }, 10);
      });
    }
    else if (action === "chat-back") {
      stopBotViewMessageStream();
      state.botViewBulkModeKey = null;
      state.selectedConversationId = null;
      state.botViewConversationListPinned = mobileBotViewIsSinglePane();
      render();
    }
    else if (action === "scroll-latest") {
      const timeline = document.querySelector("#chat-timeline");
      if (timeline) timeline.scrollTo({ top: timeline.scrollHeight, behavior: "smooth" });
    }
    else if (action === "toggle-bulk-select") setBotViewBulkMode(state.botViewBulkModeKey !== botViewKey());
    else if (action === "cancel-bulk-select") setBotViewBulkMode(false);
    else if (action === "toggle-selected-message") toggleBotViewBulkMessage(trigger.dataset.telegramMessageId, trigger.checked);
    else if (action === "delete-selected-messages") deleteSelectedBotViewMessages();
    else if (action === "load-older-messages") loadOlderBotViewMessages();
    else if (action === "toggle-composer-panel") {
      const key = botViewKey();
      const panel = trigger.dataset.panel;
      state.botViewOpenPanel = state.botViewOpenPanel?.key === key && state.botViewOpenPanel?.name === panel ? null : { key, name: panel };
      renderBotViewLive();
      window.setTimeout(() => document.querySelector(".composer-panel input, .composer-panel textarea, .composer-panel button")?.focus({ preventScroll: true }), 0);
    }
    else if (action === "close-composer-panel") { state.botViewOpenPanel = null; renderBotViewLive(); }
    else if (action === "open-special-panel") { state.botViewOpenPanel = { key: botViewKey(), name: trigger.dataset.panel }; renderBotViewLive(); }
    else if (action === "pick-attachments") {
      const input = document.querySelector("#message-attachments");
      if (input) {
        input.accept = trigger.dataset.accept || "*/*";
        input.dataset.mode = trigger.dataset.mode || "media";
        input.dataset.method = trigger.dataset.method || "";
        input.click();
      }
    }
    else if (action === "remove-attachment") removeBotViewAttachment(trigger.dataset.attachmentId);
    else if (action === "set-attachment-mode") { botViewDraft().sendMode = trigger.dataset.mode === "document" ? "document" : "media"; renderBotViewLive(); }
    else if (action === "insert-emoji") {
      const textarea = document.querySelector("#message-form textarea");
      if (textarea) {
        const start = textarea.selectionStart ?? textarea.value.length;
        const end = textarea.selectionEnd ?? start;
        textarea.setRangeText(trigger.dataset.emoji || "", start, end, "end");
        textarea.dispatchEvent(new Event("input", { bubbles: true }));
        textarea.focus({ preventScroll: true });
      }
    }
    else if (action === "cancel-message-context") { const draft = botViewDraft(); draft.reply = null; draft.edit = null; draft.suppressEphemeralReply = trigger.dataset.autoEphemeral === "true"; renderBotViewLive(); }
    else if (action === "clear-composer-options") { const draft = botViewDraft(); draft.parseMode = ""; draft.replyMarkup = null; renderBotViewLive(); }
    else if (action === "reply-message" || action === "edit-message") {
      try {
        const message = JSON.parse(trigger.dataset.message || "{}");
        const draft = botViewDraft();
        if (action === "reply-message") { draft.reply = message; draft.edit = null; draft.suppressEphemeralReply = false; }
        else { draft.edit = message; draft.reply = null; draft.text = message.text || ""; draft.replyMarkup = message.reply_markup || null; }
        renderBotViewLive();
        document.querySelector("#message-form textarea")?.focus({ preventScroll: true });
      } catch (_) { toast("Could not select that message.", "error"); }
    }
    else if (action === "copy-message-text") copyText(trigger.dataset.text || "");
    else if (action === "forward-message") { state.botViewOpenPanel = { key: botViewKey(), name: "forward", data: { messageId: trigger.dataset.telegramMessageId, fromChatId: trigger.dataset.fromChatId } }; renderBotViewLive(); }
    else if (action === "open-reaction-panel") { state.botViewOpenPanel = { key: botViewKey(), name: "reaction", data: { messageId: trigger.dataset.telegramMessageId } }; renderBotViewLive(); }
    else if (action === "open-callback-answer") {
      state.botViewOpenPanel = { key: botViewKey(), name: "callback-answer", data: { actionGeneration: trigger.dataset.actionGeneration } };
      renderBotViewLive();
      window.setTimeout(() => document.querySelector("#callback-answer-form textarea")?.focus({ preventScroll: true }), 0);
    }
    else if (action === "review-suggested-post") {
      const decision = trigger.dataset.decision === "decline" ? "decline" : "approve";
      state.botViewOpenPanel = { key: botViewKey(), name: `suggested-post-${decision}`, data: { messageId: trigger.dataset.telegramMessageId } };
      renderBotViewLive();
      window.setTimeout(() => document.querySelector(`#suggested-post-${decision}-form input:not([type="hidden"]), #suggested-post-${decision}-form textarea, #suggested-post-${decision}-form button`)?.focus({ preventScroll: true }), 0);
    }
    else if (action === "set-message-reaction") performBotViewControlAction("setMessageReaction", { message_id: Number(trigger.dataset.telegramMessageId) || trigger.dataset.telegramMessageId, reaction: [{ type: "emoji", emoji: trigger.dataset.reaction }] }, "Reaction updated.");
    else if (action === "remove-message-reaction") performBotViewControlAction("setMessageReaction", { message_id: Number(trigger.dataset.telegramMessageId) || trigger.dataset.telegramMessageId, reaction: [] }, "Reaction removed.");
    else if (action === "stop-poll") performBotViewControlAction("stopPoll", { message_id: Number(trigger.dataset.telegramMessageId) || trigger.dataset.telegramMessageId }, "Poll stopped.");
    else if (action === "stop-live-location") performBotViewControlAction("stopMessageLiveLocation", { message_id: Number(trigger.dataset.telegramMessageId) || trigger.dataset.telegramMessageId }, "Live location stopped.");
    else if (action === "edit-checklist") {
      try {
        state.botViewOpenPanel = { key: botViewKey(), name: "checklist", data: { messageId: trigger.dataset.telegramMessageId, checklist: JSON.parse(trigger.dataset.checklist || "{}") } };
        renderBotViewLive();
      } catch (_) { toast("Could not open that checklist.", "error"); }
    }
    else if (action === "delete-message") {
      trigger.dataset.action = "confirm-delete-message";
      trigger.classList.add("is-confirming");
      trigger.setAttribute("aria-label", "Confirm delete message");
      trigger.innerHTML = "Delete?";
      window.setTimeout(() => {
        if (trigger.isConnected && trigger.dataset.action === "confirm-delete-message") {
          trigger.dataset.action = "delete-message";
          trigger.classList.remove("is-confirming");
          trigger.setAttribute("aria-label", "Delete message");
          trigger.innerHTML = icon("trash");
        }
      }, 3500);
    }
    else if (action === "confirm-delete-message") deleteBotViewMessage(trigger.dataset.telegramMessageId ?? "", trigger.dataset.ephemeralMessageId ?? "", trigger.dataset.actionGeneration ?? "");
    else if (action === "retry-message") retryBotViewMessage(trigger.dataset.clientId);
    else if (action === "retry-special-action") retryBotViewSpecialAction(trigger.dataset.clientId);
    else if (action === "mark-business-message-read") markBusinessMessageRead(trigger.dataset.telegramMessageId);
    else if (action === "send-dice") sendBotViewSpecialAction("sendDice", { emoji: trigger.dataset.emoji || "🎲" }, { dice: { emoji: trigger.dataset.emoji || "🎲" } });
    else if (action === "use-current-location") {
      const form = trigger.closest("form");
      if (!navigator.geolocation) { formError(form, "Location is not supported by this browser."); return; }
      trigger.disabled = true;
      navigator.geolocation.getCurrentPosition((position) => {
        trigger.disabled = false;
        if (!form?.isConnected) return;
        form.elements.latitude.value = String(position.coords.latitude);
        form.elements.longitude.value = String(position.coords.longitude);
      }, (error) => { trigger.disabled = false; formError(form, error.code === 1 ? "Location permission was denied." : "Could not determine your location."); }, { enableHighAccuracy: true, timeout: 10000 });
    }
    else if (action === "start-voice-recording") startVoiceRecording();
    else if (action === "stop-voice-recording") stopVoiceRecording();
    else if (action === "cancel-voice-recording") stopVoiceRecording({ cancel: true });
    else if (action === "retry-stream-keys") loadStreamKeys();
    else if (action === "revoke-stream-key") revokeStreamKey(trigger);
    else if (action === "dismiss-stream-secret") { state.streamKey = null; state.streamKeyId = null; render(); }
    else if (action === "confirm-routing") setModal("routing", { mode: trigger.dataset.mode });
    else if (action === "confirm-delete-bot") setModal("delete-bot");
    else if (action === "request-plan") toast(`${trigger.dataset.plan || "That"} plan checkout is not enabled in this MVP yet. Your current plan is unchanged.`);
  });

  document.addEventListener("input", (event) => {
    if (event.target.matches("#conversation-search")) {
      applyConversationFilter(event.target.value);
    }
    if (event.target.matches("input[aria-invalid='true']")) event.target.removeAttribute("aria-invalid");
    if (event.target.matches("#message-form textarea")) {
      const draft = botViewDraft();
      draft.text = event.target.value;
      resizeBotViewComposer(event.target);
      updateBotViewPrimaryAction();
    }
  });

  document.addEventListener("change", (event) => {
    if (event.target.matches("#message-attachments")) {
      addBotViewFiles(event.target.files, { sendMode: event.target.dataset.mode || "media", explicitMethod: event.target.dataset.method || "" });
      event.target.value = "";
      event.target.dataset.method = "";
    }
  });

  document.addEventListener("paste", (event) => {
    if (!event.target.closest?.(".composer")) return;
    const files = [...(event.clipboardData?.files || [])];
    if (!files.length) return;
    event.preventDefault();
    addBotViewFiles(files, { sendMode: "media" });
  });

  ["dragenter", "dragover"].forEach((type) => document.addEventListener(type, (event) => {
    if (state.route.name !== "bot-view" || !event.dataTransfer?.types?.includes("Files")) return;
    event.preventDefault();
    event.dataTransfer.dropEffect = "copy";
    document.querySelector(".timeline-wrap")?.classList.add("is-dragging");
  }));

  ["dragleave", "drop"].forEach((type) => document.addEventListener(type, (event) => {
    if (state.route.name !== "bot-view") return;
    if (type === "drop" && event.dataTransfer?.files?.length) {
      event.preventDefault();
      addBotViewFiles(event.dataTransfer.files, { sendMode: "media" });
    }
    if (type === "drop" || !event.relatedTarget) document.querySelector(".timeline-wrap")?.classList.remove("is-dragging");
  }));

  document.addEventListener("scroll", (event) => {
    if (event.target?.matches?.("#chat-timeline")) updateScrollLatestControl();
  }, true);

  document.addEventListener("keydown", (event) => {
    if (event.target.matches?.("#message-form textarea") && event.key === "Enter" && !event.shiftKey && !event.isComposing) {
      event.preventDefault();
      const form = event.target.form;
      if (event.target.value.trim() || botViewDraft().files.length || botViewDraft().edit) form?.requestSubmit();
    }
    if (event.key === "Escape") {
      if (state.botViewOpenPanel) { state.botViewOpenPanel = null; renderBotViewLive(); }
      else if (state.modal) closeModal();
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
  window.addEventListener("beforeunload", () => {
    stopUpdatesStream({ status: "idle" });
    stopBotViewRefresh();
    stopBotViewMessageStream();
    stopVoiceRecording({ cancel: true, renderResult: false });
  });
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState !== "visible") {
      stopBotViewRefresh();
      stopBotViewMessageStream();
      return;
    }
    if (document.visibilityState === "visible" && state.route.name === "bot-updates" && !state.updatesPaused && !state.updatesStream && !state.updatesStreamRetryTimer) {
      startUpdatesStream({ reconnecting: state.updatesStreamStatus === "reconnecting" });
    }
    if (state.route.name === "bot-view") startBotViewRefresh();
  });

  bootstrap();
})();
