const monthEl = document.getElementById("month");
const scopeTypeEl = document.getElementById("scopeType");
const scopeKeyEl = document.getElementById("scopeKey");
const refreshBtn = document.getElementById("refreshBtn");
const quickRefreshBtn = document.getElementById("quickRefreshBtn");
const themeToggleBtn = document.getElementById("themeToggleBtn");
const loadDataBtn = document.getElementById("loadDataBtn");
const monitorRefreshBtn = document.getElementById("monitorRefreshBtn");
const ingestRefreshBtn = document.getElementById("ingestRefreshBtn");
const workspacePathEl = document.getElementById("workspacePath");
const workspaceTitleEl = document.getElementById("workspaceTitle");
const workspaceSubEl = document.getElementById("workspaceSub");
const financeHeadScopeEl = document.getElementById("financeHeadScope");
const marketHeadNextEl = document.getElementById("marketHeadNext");
const marketHeadAlertsEl = document.getElementById("marketHeadAlerts");
const monitorHeadComplianceEl = document.getElementById("monitorHeadCompliance");
const monitorHeadAnomalyEl = document.getElementById("monitorHeadAnomaly");
const trendHeadModelEl = document.getElementById("trendHeadModel");
const forecastClassEl = document.getElementById("forecastClass");
const forecastBtn = document.getElementById("forecastBtn");
const approvalStageEl = document.getElementById("approvalStage");
const loadPendingBtn = document.getElementById("loadPendingBtn");
const actorNameEl = document.getElementById("actorName");
const llmPromptEl = document.getElementById("llmPrompt");
const llmBtn = document.getElementById("llmBtn");
const llmResultEl = document.getElementById("llmResult");
const pageStatusEl = document.getElementById("pageStatus");
const toastHostEl = document.getElementById("toastHost");
const copilotBtn = document.getElementById("copilotBtn");
const copilotMetaEl = document.getElementById("copilotMeta");
const copilotActionsEl = document.getElementById("copilotActions");
const copilotLlmEl = document.getElementById("copilotLlm");
const simClassEl = document.getElementById("simClass");
const simOutsrcEl = document.getElementById("simOutsrc");
const simHoursEl = document.getElementById("simHours");
const simFactorEl = document.getElementById("simFactor");
const simulateBtn = document.getElementById("simulateBtn");
const simulateResultEl = document.getElementById("simulateResult");
const aiApprovalStageEl = document.getElementById("aiApprovalStage");
const approvalAiBtn = document.getElementById("approvalAiBtn");
const approvalAiBoxEl = document.getElementById("approvalAiBox");
const askPromptEl = document.getElementById("askPrompt");
const askBtn = document.getElementById("askBtn");
const askSuggestBtn = document.getElementById("askSuggestBtn");
const askQuickPromptsEl = document.getElementById("askQuickPrompts");
const askResultEl = document.getElementById("askResult");
const seedAiTodoBtn = document.getElementById("seedAiTodoBtn");
const clearAiTodoBtn = document.getElementById("clearAiTodoBtn");
const aiTodoMetaEl = document.getElementById("aiTodoMeta");
const aiTodoListEl = document.getElementById("aiTodoList");
const insightStrengthEl = document.getElementById("insightStrength");
const insightRiskEl = document.getElementById("insightRisk");
const insightChanceEl = document.getElementById("insightChance");
const insightActionEl = document.getElementById("insightAction");
const tickerSupportEl = document.getElementById("tickerSupport");
const tickerAlertsEl = document.getElementById("tickerAlerts");
const tickerComplianceEl = document.getElementById("tickerCompliance");
const tickerAnomalyEl = document.getElementById("tickerAnomaly");
const navLinks = Array.from(document.querySelectorAll(".nav-link[data-target]"));
const mainSections = Array.from(document.querySelectorAll(".main-section[id]"));
const dsKeyFabEl = document.getElementById("dsKeyFab");
const dsKeyDotEl = document.getElementById("dsKeyDot");
const providerOverlayEl = document.getElementById("providerOverlay");
const providerModalEl = document.getElementById("providerModal");
const providerCloseBtnEl = document.getElementById("providerCloseBtn");
const providerSelectEl = document.getElementById("providerSelect");
const providerModelInputEl = document.getElementById("providerModelInput");
const providerApiKeyInputEl = document.getElementById("providerApiKeyInput");
const providerHintEl = document.getElementById("providerHint");
const providerTestBtnEl = document.getElementById("providerTestBtn");
const providerSaveBtnEl = document.getElementById("providerSaveBtn");

const AI_PROVIDER_SETTINGS_KEY = "ai_provider_settings_v1";
const AI_EXEC_TODO_STORE_KEY = "ai_exec_todo_store_v1";
const PROVIDER_LABELS = {
  deepseek: "DeepSeek",
  openai: "OpenAI",
};

const MODULE_LAYOUT = {
  "section-overview": ["section-overview-head", "section-overview"],
  "section-finance": ["section-finance-head", "section-finance"],
  "section-monitor": ["section-monitor-head", "section-monitor"],
  "section-market": ["section-market-head", "section-market"],
  "section-risk": ["section-risk-head", "section-risk"],
  "section-simulate": ["section-simulate-head", "section-simulate"],
  "section-trend": ["section-trend-head", "section-trend"],
  "section-ingest": ["section-ingest-head", "section-ingest"],
};

let scopesCache = { departments: [], projects: [] };
let refreshToken = 0;
let lastCopilotData = null;
let runtimeConfig = {
  llm: {
    default_provider: "deepseek",
    default_models: {
      deepseek: "deepseek-chat",
      openai: "gpt-4o-mini",
    },
    server_keys: {
      deepseek: false,
      openai: false,
    },
  },
};

function formatMoney(value, fallback = "--") {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return fallback;
  const v = Number(value);
  return `HK$ ${v.toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
}

function formatPct(value, fallback = "--") {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return fallback;
  return `${(Number(value) * 100).toFixed(2)}%`;
}

function scopeTypeLabel(scopeType) {
  if (scopeType === "department") return "部门";
  if (scopeType === "project") return "项目";
  return "公司";
}

function currentContext() {
  return {
    month: monthEl.value,
    scope_type: scopeTypeEl.value,
    scope_key: scopeKeyEl.value,
  };
}

function setPageStatus(text, busy = false) {
  pageStatusEl.textContent = text;
  pageStatusEl.classList.toggle("busy", busy);
}

function applyTheme(theme, persist = true) {
  const normalized = theme === "dark" ? "dark" : "light";
  document.documentElement.setAttribute("data-theme", normalized);
  if (themeToggleBtn) {
    const symbol = themeToggleBtn.querySelector(".icon-symbol");
    if (symbol) symbol.textContent = normalized === "dark" ? "☀" : "◐";
    themeToggleBtn.title = normalized === "dark" ? "切换到亮色模式" : "切换到暗色模式";
    themeToggleBtn.setAttribute("aria-label", themeToggleBtn.title);
  }
  if (persist) {
    localStorage.setItem("rd_theme", normalized);
  }
}

function initTheme() {
  const stored = localStorage.getItem("rd_theme");
  const preferred =
    window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  applyTheme(stored || preferred, false);
  if (!themeToggleBtn) return;
  themeToggleBtn.addEventListener("click", () => {
    const current = document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light";
    applyTheme(current === "dark" ? "light" : "dark", true);
  });
}

async function loadRuntimeConfig() {
  try {
    const data = await api("/api/runtime-config");
    const llm = data?.llm || {};
    runtimeConfig = {
      llm: {
        default_provider: llm.default_provider === "openai" ? "openai" : "deepseek",
        default_models: {
          deepseek: String(llm?.default_models?.deepseek || "deepseek-chat"),
          openai: String(llm?.default_models?.openai || "gpt-4o-mini"),
        },
        server_keys: {
          deepseek: Boolean(llm?.server_keys?.deepseek),
          openai: Boolean(llm?.server_keys?.openai),
        },
      },
    };
  } catch {
    // keep fallback runtimeConfig
  }
}

function serverHasProviderKey(provider) {
  return Boolean(runtimeConfig?.llm?.server_keys?.[provider]);
}

function defaultAiProviderSettings() {
  const defaultProvider = runtimeConfig?.llm?.default_provider === "openai" ? "openai" : "deepseek";
  const defaultModels = runtimeConfig?.llm?.default_models || {};
  return {
    provider: defaultProvider,
    models: {
      deepseek: String(defaultModels.deepseek || "deepseek-chat"),
      openai: String(defaultModels.openai || "gpt-4o-mini"),
    },
    keys: {
      deepseek: "",
      openai: "",
    },
  };
}

function loadAiProviderSettings() {
  try {
    const raw = localStorage.getItem(AI_PROVIDER_SETTINGS_KEY);
    if (!raw) return defaultAiProviderSettings();
    const parsed = JSON.parse(raw);
    const fallback = defaultAiProviderSettings();
    const provider = parsed?.provider === "openai" ? "openai" : "deepseek";
    return {
      provider,
      models: {
        deepseek: String(parsed?.models?.deepseek || fallback.models.deepseek),
        openai: String(parsed?.models?.openai || fallback.models.openai),
      },
      keys: {
        deepseek: String(parsed?.keys?.deepseek ?? ""),
        openai: String(parsed?.keys?.openai || ""),
      },
    };
  } catch {
    return defaultAiProviderSettings();
  }
}

function saveAiProviderSettings(settings) {
  localStorage.setItem(AI_PROVIDER_SETTINGS_KEY, JSON.stringify(settings));
}

function resolveRuntimeApiKey(provider, rawKey) {
  const key = String(rawKey || "").trim();
  if (key) return key;
  return null;
}

function currentAiConfig() {
  const settings = loadAiProviderSettings();
  const selectedProvider = settings.provider === "openai" ? "openai" : "deepseek";
  const providerOrder = selectedProvider === "deepseek" ? ["deepseek", "openai"] : ["openai", "deepseek"];
  for (const provider of providerOrder) {
    const model = (settings.models?.[provider] || "").trim() || null;
    const apiKey = resolveRuntimeApiKey(provider, settings.keys?.[provider]);
    const hasServerKey = serverHasProviderKey(provider);
    if (apiKey || hasServerKey) {
      return {
        provider,
        model,
        apiKey,
        hasServerKey,
      };
    }
  }
  return {
    provider: selectedProvider,
    model: (settings.models?.[selectedProvider] || "").trim() || null,
    apiKey: null,
    hasServerKey: false,
  };
}

function refreshDsFabState() {
  const cfg = currentAiConfig();
  const hasKey = !!cfg.apiKey || !!cfg.hasServerKey;
  if (dsKeyDotEl) dsKeyDotEl.classList.toggle("ready", hasKey);
  if (dsKeyFabEl) {
    const label = PROVIDER_LABELS[cfg.provider] || cfg.provider;
    if (cfg.apiKey) {
      dsKeyFabEl.title = `${label} 已配置 API Key`;
    } else if (cfg.hasServerKey) {
      dsKeyFabEl.title = `${label} 使用服务端默认 Key`;
    } else {
      dsKeyFabEl.title = `${label} 未配置 API Key`;
    }
    const textNode = dsKeyFabEl.querySelector(".ds-fab-text");
    if (textNode) textNode.textContent = cfg.provider === "openai" ? "OA" : "DS";
  }
  if (trendHeadModelEl) {
    const label = PROVIDER_LABELS[cfg.provider] || cfg.provider;
    trendHeadModelEl.textContent = cfg.model ? `${label} · ${cfg.model}` : `${label} · 未设模型`;
  }
}

function openProviderModal() {
  const settings = loadAiProviderSettings();
  if (!providerModalEl || !providerOverlayEl) return;
  providerModalEl.setAttribute("aria-hidden", "false");
  providerOverlayEl.setAttribute("aria-hidden", "false");
  providerSelectEl.value = settings.provider;
  providerModelInputEl.value = settings.models?.[settings.provider] || "";
  providerApiKeyInputEl.value = settings.keys?.[settings.provider] || "";
  providerOverlayEl.hidden = false;
  providerModalEl.hidden = false;
  updateProviderHint();
  providerApiKeyInputEl.focus();
}

function closeProviderModal() {
  if (providerModalEl) {
    providerModalEl.hidden = true;
    providerModalEl.setAttribute("aria-hidden", "true");
  }
  if (providerOverlayEl) {
    providerOverlayEl.hidden = true;
    providerOverlayEl.setAttribute("aria-hidden", "true");
  }
}

function updateProviderHint() {
  if (!providerHintEl) return;
  const provider = providerSelectEl?.value || "deepseek";
  const serverKeyHint = serverHasProviderKey(provider) ? "当前服务端已配置默认 Key，留空即可使用。" : "如未填写则依赖服务端环境变量。";
  if (provider === "openai") {
    providerHintEl.textContent = `OpenAI 建议模型：gpt-4o-mini / gpt-4.1-mini。${serverKeyHint}`;
  } else {
    providerHintEl.textContent = `DeepSeek 建议模型：deepseek-chat / deepseek-reasoner。${serverKeyHint}`;
  }
}

async function testProviderConnection() {
  const provider = providerSelectEl.value;
  const model = providerModelInputEl.value.trim() || null;
  const apiKey = resolveRuntimeApiKey(provider, providerApiKeyInputEl.value);
  setButtonBusy(providerTestBtnEl, true, "测试中");
  try {
    await api("/api/llm/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        prompt: "请仅回复：连接成功",
        provider,
        model,
        api_key: apiKey,
      }),
    });
    showToast({ title: "连接成功", text: `${PROVIDER_LABELS[provider]} 模型连接正常。`, type: "success" });
  } catch (err) {
    showToast({ title: "连接失败", text: err.message, type: "error", duration: 3400 });
  } finally {
    setButtonBusy(providerTestBtnEl, false);
  }
}

function saveProviderConfig() {
  const provider = providerSelectEl.value;
  const model = providerModelInputEl.value.trim();
  const apiKey = providerApiKeyInputEl.value.trim();
  const settings = loadAiProviderSettings();
  settings.provider = provider;
  settings.models[provider] = model || (provider === "openai" ? "gpt-4o-mini" : "deepseek-chat");
  settings.keys[provider] = apiKey;
  saveAiProviderSettings(settings);
  refreshDsFabState();
  closeProviderModal();
  showToast({ title: "配置已保存", text: `${PROVIDER_LABELS[provider]} 设置已生效。`, type: "success" });
}

function initDsKeyUi() {
  closeProviderModal();
  loadRuntimeConfig()
    .catch(() => undefined)
    .finally(() => refreshDsFabState());
  if (!dsKeyFabEl) return;
  dsKeyFabEl.addEventListener("click", () => {
    dsKeyFabEl.classList.add("is-hit");
    window.setTimeout(() => dsKeyFabEl.classList.remove("is-hit"), 180);
    openProviderModal();
  });
  providerCloseBtnEl?.addEventListener("click", closeProviderModal);
  providerOverlayEl?.addEventListener("click", closeProviderModal);
  providerSelectEl?.addEventListener("change", () => {
    const settings = loadAiProviderSettings();
    const provider = providerSelectEl.value;
    providerModelInputEl.value = settings.models?.[provider] || "";
    providerApiKeyInputEl.value = settings.keys?.[provider] || "";
    updateProviderHint();
  });
  providerTestBtnEl?.addEventListener("click", testProviderConnection);
  providerSaveBtnEl?.addEventListener("click", saveProviderConfig);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeProviderModal();
  });
}

function setWorkspaceHeadFromLink(link) {
  if (!link) return;
  if (workspacePathEl) workspacePathEl.textContent = link.dataset.path || "研发投入治理";
  if (workspaceTitleEl) workspaceTitleEl.textContent = link.dataset.title || "研发投入治理";
  if (workspaceSubEl) workspaceSubEl.textContent = link.dataset.sub || "";
}

function setActiveNavLink(activeLink) {
  if (!activeLink) return;
  navLinks.forEach((link) => {
    link.classList.toggle("active", link === activeLink);
  });
  setWorkspaceHeadFromLink(activeLink);
}

function sectionSetForTarget(targetId) {
  if (MODULE_LAYOUT[targetId]) return MODULE_LAYOUT[targetId];
  if (!targetId) return MODULE_LAYOUT["section-overview"];
  return [targetId];
}

function applyModuleSections(targetId) {
  const visibleSet = new Set(sectionSetForTarget(targetId));
  mainSections.forEach((section) => {
    const visible = visibleSet.has(section.id);
    section.hidden = !visible;
    section.classList.toggle("is-visible", visible);
  });
}

function flashSection(sectionId) {
  const target = document.getElementById(sectionId);
  if (!target) return;
  const focusNode = target.classList.contains("panel") ? target : target.querySelector(".panel") || target;
  focusNode.classList.add("is-focused");
  window.setTimeout(() => focusNode.classList.remove("is-focused"), 700);
}

function navigateToSection(link, smooth = false) {
  const targetId = link?.dataset?.target;
  if (!targetId) return false;
  const target = document.getElementById(targetId);
  if (!target) return false;
  setActiveNavLink(link);
  applyModuleSections(targetId);
  window.scrollTo({ top: 0, behavior: smooth ? "smooth" : "auto" });
  flashSection(targetId);
  return true;
}

function initSidebarNav() {
  if (!navLinks.length) return;
  navLinks.forEach((link) => {
    link.addEventListener("click", (e) => {
      e.preventDefault();
      navigateToSection(link, true);
    });
  });
  const initial = navLinks.find((x) => x.classList.contains("active")) || navLinks[0];
  navigateToSection(initial, false);
}

function showToast({ title, text, type = "success", duration = 2600 }) {
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.innerHTML = `<div class="toast-title">${title}</div><div class="toast-text">${text}</div>`;
  toastHostEl.appendChild(el);
  window.setTimeout(() => {
    el.style.opacity = "0";
    el.style.transform = "translateY(8px)";
    window.setTimeout(() => el.remove(), 260);
  }, duration);
}

function setButtonBusy(btn, busy, busyText) {
  if (!btn) return;
  const isIconOnly = btn.dataset.iconOnly === "1";
  if (busy) {
    if (!isIconOnly) {
      if (!btn.dataset.baseText) btn.dataset.baseText = btn.textContent || "";
      if (busyText) btn.textContent = busyText;
    }
    btn.classList.add("is-busy");
    btn.disabled = true;
  } else {
    btn.classList.remove("is-busy");
    btn.disabled = false;
    if (!isIconOnly && btn.dataset.baseText) btn.textContent = btn.dataset.baseText;
  }
}

function setLoading(ids, loading) {
  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.classList.toggle("loading-box", loading);
  });
}

async function api(path, options = {}) {
  const res = await fetch(path, options);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.detail || "请求失败");
  }
  return data;
}

function metricValueHtml(item) {
  if (item.type === "money") {
    return `<div class="value ${item.cls || ""}" data-kind="money" data-target="${Number(item.raw || 0)}">HK$ 0</div>`;
  }
  if (item.type === "pct") {
    return `<div class="value ${item.cls || ""}" data-kind="pct" data-target="${Number(item.raw || 0)}">0.00%</div>`;
  }
  return `<div class="value ${item.cls || ""}">${item.val}</div>`;
}

function animateValue(el) {
  const kind = el.dataset.kind;
  if (!kind) return;
  const target = Number(el.dataset.target || 0);
  const start = Number(el.dataset.current || 0);
  const duration = 460;
  const startedAt = performance.now();

  function tick(now) {
    const progress = Math.min((now - startedAt) / duration, 1);
    const eased = 1 - (1 - progress) * (1 - progress);
    const value = start + (target - start) * eased;
    if (kind === "money") el.textContent = formatMoney(value, "HK$ 0");
    if (kind === "pct") el.textContent = formatPct(value, "0.00%");
    if (progress < 1) {
      requestAnimationFrame(tick);
    } else {
      el.dataset.current = `${target}`;
    }
  }

  requestAnimationFrame(tick);
}

function setKpi(cardId, metrics) {
  const target = metrics.target_hkd ?? null;
  const completed = metrics.completed_hkd ?? null;
  const supportRate = metrics.support_rate ?? null;
  const gap = metrics.gap_hkd ?? null;
  const mom = metrics.mom ?? null;
  const yoy = metrics.yoy ?? null;
  const list = [
    { key: "目标值", type: "money", raw: target },
    { key: "完成值", type: "money", raw: completed },
    { key: "上月完成值", type: "money", raw: metrics.last_month_hkd },
    { key: "上年完成值", type: "money", raw: metrics.last_year_hkd },
    { key: "环比", type: "pct", raw: mom, cls: mom > 0 ? "warn" : "good" },
    { key: "同比", type: "pct", raw: yoy, cls: yoy > 0 ? "warn" : "good" },
    { key: "目标支撑率", type: "pct", raw: supportRate, cls: supportRate >= 1 ? "good" : "warn" },
    { key: "缺口差距", type: "money", raw: gap, cls: gap <= 0 ? "good" : "bad" },
  ];

  const box = document.getElementById(cardId);
  box.innerHTML = list
    .map(
      (item) => `
      <div class="metric-item">
        <div class="key">${item.key}</div>
        ${metricValueHtml(item)}
      </div>
    `
    )
    .join("");

  box.querySelectorAll(".value[data-kind]").forEach((node) => animateValue(node));
}

function renderTable(targetId, rows, columns, emptyText = "暂无数据") {
  const box = document.getElementById(targetId);
  box.classList.remove("loading-box");
  if (!rows || rows.length === 0) {
    box.innerHTML = `<div style="padding:12px;color:var(--muted);">${emptyText}</div>`;
    return;
  }
  const thead = columns.map((c) => `<th>${c.label}</th>`).join("");
  const tbody = rows
    .map((row) => {
      const tds = columns
        .map((c) => {
          const val = c.render ? c.render(row[c.key], row) : row[c.key];
          return `<td>${val ?? ""}</td>`;
        })
        .join("");
      return `<tr>${tds}</tr>`;
    })
    .join("");
  box.innerHTML = `<table><thead><tr>${thead}</tr></thead><tbody>${tbody}</tbody></table>`;
}

function escapeHtml(input) {
  return String(input ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function inlineMarkdown(text) {
  let out = escapeHtml(text || "");
  out = out.replace(/`([^`]+)`/g, "<code>$1</code>");
  out = out.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  out = out.replace(/__([^_]+)__/g, "<u>$1</u>");
  out = out.replace(/\*(?!\*)([^*]+)\*/g, "<em>$1</em>");
  return out;
}

function markdownToHtml(markdown) {
  const lines = String(markdown || "")
    .replace(/\r/g, "")
    .split("\n");
  const html = [];
  let listType = "";
  const closeList = () => {
    if (!listType) return;
    html.push(`</${listType}>`);
    listType = "";
  };
  for (const raw of lines) {
    const line = String(raw || "");
    const trimmed = line.trim();
    if (!trimmed) {
      closeList();
      continue;
    }
    let m = trimmed.match(/^###\s+(.+)$/);
    if (m) {
      closeList();
      html.push(`<h3>${inlineMarkdown(m[1])}</h3>`);
      continue;
    }
    m = trimmed.match(/^##\s+(.+)$/);
    if (m) {
      closeList();
      html.push(`<h3>${inlineMarkdown(m[1])}</h3>`);
      continue;
    }
    m = trimmed.match(/^\d+\.\s+(.+)$/);
    if (m) {
      if (listType !== "ol") {
        closeList();
        html.push("<ol>");
        listType = "ol";
      }
      html.push(`<li>${inlineMarkdown(m[1])}</li>`);
      continue;
    }
    m = trimmed.match(/^[-*]\s+(.+)$/);
    if (m) {
      if (listType !== "ul") {
        closeList();
        html.push("<ul>");
        listType = "ul";
      }
      html.push(`<li>${inlineMarkdown(m[1])}</li>`);
      continue;
    }
    closeList();
    html.push(`<p>${inlineMarkdown(trimmed)}</p>`);
  }
  closeList();
  return html.join("");
}

function llmMetaHtml(meta = {}) {
  const chips = [];
  if (meta.mode) chips.push(`模式：${escapeHtml(meta.mode)}`);
  if (meta.provider) chips.push(`提供商：${escapeHtml(meta.provider)}`);
  if (meta.model) chips.push(`模型：${escapeHtml(meta.model)}`);
  if (meta.status) chips.push(`状态：${escapeHtml(meta.status)}`);
  if (!chips.length) return "";
  return `<div class="llm-meta">${chips.map((x) => `<span class="llm-meta-chip">${x}</span>`).join("")}</div>`;
}

function renderMarkdownResult(el, markdown, meta = {}, streaming = false) {
  if (!el) return;
  const body = markdownToHtml(markdown || "");
  const cursor = streaming ? '<span class="stream-caret" aria-hidden="true"></span>' : "";
  el.classList.toggle("is-streaming", streaming);
  el.innerHTML = `${llmMetaHtml(meta)}<div class="llm-md">${body}${cursor}</div>`;
}

async function streamNdjson(path, payload, handlers = {}) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    throw new Error(data.detail || "请求失败");
  }
  if (!res.body) {
    throw new Error("当前环境不支持流式响应");
  }
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let index = buffer.indexOf("\n");
      while (index >= 0) {
        const line = buffer.slice(0, index).trim();
        buffer = buffer.slice(index + 1);
        if (line) {
          try {
            const event = JSON.parse(line);
            handlers.onEvent?.(event);
          } catch {
            handlers.onMalformed?.(line);
          }
        }
        index = buffer.indexOf("\n");
      }
    }
    const tail = buffer.trim();
    if (tail) {
      try {
        const event = JSON.parse(tail);
        handlers.onEvent?.(event);
      } catch {
        handlers.onMalformed?.(tail);
      }
    }
  } catch (err) {
    try {
      await reader.cancel();
    } catch {
      // ignore cancel failure
    }
    throw err;
  }
}

function cleanBulletText(line) {
  return String(line ?? "")
    .replace(/^[\s\-•·●\u2022]+/, "")
    .trim();
}

function parseLlmSummarySections(summary) {
  const text = String(summary || "").trim();
  if (!text) return [];
  const blocks = text.split(/\n\s*\n+/).map((x) => x.trim()).filter(Boolean);
  const sections = blocks
    .map((block) => {
      const lines = block.split("\n").map((x) => x.trim()).filter(Boolean);
      if (!lines.length) return null;
      const title = lines[0].replace(/[：:]$/, "").trim() || "模型总结";
      const source = lines.length > 1 ? lines.slice(1) : lines;
      const items = source.map(cleanBulletText).filter(Boolean);
      if (!items.length) return null;
      return { title, items };
    })
    .filter(Boolean);
  if (sections.length) return sections;
  const fallbackItems = text.split("\n").map(cleanBulletText).filter(Boolean);
  return fallbackItems.length ? [{ title: "模型总结", items: fallbackItems }] : [];
}

function sectionToneByTitle(title) {
  const text = String(title || "");
  if (text.includes("风险")) return "risk";
  if (text.includes("动作") || text.includes("行动")) return "action";
  if (text.includes("协调") || text.includes("计划")) return "chance";
  return "neutral";
}

function sectionCardHtml(section) {
  const tone = section.tone || sectionToneByTitle(section.title);
  const title = escapeHtml(section.title || "模型总结");
  const items = (section.items || []).map((x) => cleanBulletText(x)).filter(Boolean);
  if (!items.length) return "";
  const lis = items.map((x) => `<li>${escapeHtml(x)}</li>`).join("");
  return `
    <article class="brief-card tone-${tone}">
      <h4>${title}</h4>
      <ul>${lis}</ul>
    </article>
  `;
}

function renderCopilotLlm(data) {
  if (!copilotLlmEl) return;
  if (data?.llm_summary_error) {
    copilotLlmEl.innerHTML = `<div class="brief-state error">LLM 输出失败：${escapeHtml(data.llm_summary_error)}</div>`;
    return;
  }

  const structured = data?.llm_structured || null;
  const sections = [];
  if (structured) {
    const risks = Array.isArray(structured.key_risks) ? structured.key_risks : [];
    const actions = Array.isArray(structured.two_week_actions) ? structured.two_week_actions : [];
    const nextMonth = Array.isArray(structured.next_month_coordination) ? structured.next_month_coordination : [];
    if (risks.length) sections.push({ title: "关键风险", items: risks, tone: "risk" });
    if (actions.length) sections.push({ title: "两周动作", items: actions, tone: "action" });
    if (nextMonth.length) sections.push({ title: "下月协同", items: nextMonth, tone: "chance" });
  }
  if (!sections.length && data?.llm_summary) {
    sections.push(...parseLlmSummarySections(data.llm_summary));
  }

  if (!sections.length) {
    copilotLlmEl.innerHTML = `<div class="brief-state">本次未返回模型摘要，已保留规则引擎结论（可点击“刷新洞察”重试）。</div>`;
    return;
  }

  const chips = [];
  if (structured?.model) chips.push(`<span class="brief-chip">模型：${escapeHtml(structured.model)}</span>`);
  if (Number.isFinite(Number(structured?.confidence))) {
    const pct = Math.max(0, Math.min(100, Number(structured.confidence) * 100));
    chips.push(`<span class="brief-chip">置信度：${pct.toFixed(0)}%</span>`);
  }
  const chipHtml = chips.length ? `<div class="brief-meta">${chips.join("")}</div>` : "";
  const cards = sections.map((x) => sectionCardHtml(x)).filter(Boolean).join("");
  copilotLlmEl.innerHTML = `${chipHtml}<div class="brief-grid">${cards}</div>`;
}

function askContextKey(ctx = currentContext()) {
  return `${ctx.month}|${ctx.scope_type}|${ctx.scope_key}`;
}

function loadAiTodoStore() {
  try {
    const raw = localStorage.getItem(AI_EXEC_TODO_STORE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function saveAiTodoStore(store) {
  localStorage.setItem(AI_EXEC_TODO_STORE_KEY, JSON.stringify(store));
}

function loadAiTodosForCurrentContext() {
  const store = loadAiTodoStore();
  const key = askContextKey();
  const items = Array.isArray(store[key]) ? store[key] : [];
  return items
    .map((x) => ({
      id: String(x?.id || ""),
      text: cleanBulletText(x?.text || ""),
      done: Boolean(x?.done),
      owner: String(x?.owner || ""),
      due: String(x?.due || ""),
    }))
    .filter((x) => x.id && x.text);
}

function saveAiTodosForCurrentContext(items) {
  const store = loadAiTodoStore();
  store[askContextKey()] = items;
  saveAiTodoStore(store);
}

function todoProgressPct(items) {
  const total = items.length;
  if (!total) return 0;
  const done = items.filter((x) => x.done).length;
  return (done / total) * 100;
}

function renderAiTodoBoard() {
  if (!aiTodoListEl || !aiTodoMetaEl) return;
  const items = loadAiTodosForCurrentContext();
  const done = items.filter((x) => x.done).length;
  const total = items.length;
  const pct = todoProgressPct(items);
  aiTodoMetaEl.innerHTML = `
    执行进度：${done}/${total || 0}（${pct.toFixed(0)}%）
    <div class="todo-progress"><span style="width:${pct.toFixed(2)}%"></span></div>
  `;
  if (!items.length) {
    aiTodoListEl.innerHTML = `<div class="todo-empty">当前范围暂无执行项，点击“从本次洞察生成”可自动创建。</div>`;
    return;
  }
  aiTodoListEl.innerHTML = items
    .map((item) => {
      const doneCls = item.done ? " is-done" : "";
      return `
        <article class="todo-item${doneCls}" data-id="${escapeHtml(item.id)}">
          <div class="todo-row">
            <input class="todo-check" type="checkbox" ${item.done ? "checked" : ""} />
            <div class="todo-text">${escapeHtml(item.text)}</div>
            <button class="ghost-btn todo-del" type="button">删除</button>
          </div>
          <div class="todo-meta">
            <input class="todo-owner" type="text" value="${escapeHtml(item.owner)}" placeholder="责任人（可选）" />
            <input class="todo-due" type="date" value="${escapeHtml(item.due)}" />
          </div>
        </article>
      `;
    })
    .join("");
}

function upsertTodoById(id, updater) {
  const items = loadAiTodosForCurrentContext();
  const next = items.map((x) => (x.id === id ? updater(x) : x));
  saveAiTodosForCurrentContext(next);
  renderAiTodoBoard();
}

function deleteTodoById(id) {
  const items = loadAiTodosForCurrentContext().filter((x) => x.id !== id);
  saveAiTodosForCurrentContext(items);
  renderAiTodoBoard();
}

function clearCompletedTodos() {
  const items = loadAiTodosForCurrentContext().filter((x) => !x.done);
  saveAiTodosForCurrentContext(items);
  renderAiTodoBoard();
}

function uniqueCleanLines(lines) {
  const out = [];
  const seen = new Set();
  lines
    .map((x) => cleanBulletText(x))
    .filter(Boolean)
    .forEach((line) => {
      const key = line.toLowerCase();
      if (seen.has(key)) return;
      seen.add(key);
      out.push(line);
    });
  return out;
}

function todoCandidatesFromCopilot(data) {
  if (!data) return [];
  const fromStructured = Array.isArray(data?.llm_structured?.two_week_actions) ? data.llm_structured.two_week_actions : [];
  const fromRules = Array.isArray(data?.suggested_actions) ? data.suggested_actions : [];
  const merged = uniqueCleanLines([...fromStructured, ...fromRules]);
  return merged.slice(0, 10);
}

function seedTodosFromCopilot() {
  const actions = todoCandidatesFromCopilot(lastCopilotData);
  if (!actions.length) {
    showToast({ title: "暂无可生成动作", text: "请先点击“刷新洞察”生成最新 AI 动作。", type: "error" });
    return;
  }
  const now = Date.now();
  const items = actions.map((text, idx) => ({
    id: `todo_${now}_${idx}`,
    text,
    done: false,
    owner: "",
    due: "",
  }));
  saveAiTodosForCurrentContext(items);
  renderAiTodoBoard();
  showToast({ title: "执行清单已生成", text: `已生成 ${items.length} 条可跟进动作。` });
}

function parseQuestionsFromAiText(content) {
  const text = String(content || "").trim();
  if (!text) return [];
  const lines = text
    .split("\n")
    .map((x) => x.trim())
    .filter(Boolean)
    .map((line) => line.replace(/^[\-\d\.\)\s、]+/, "").trim())
    .filter(Boolean)
    .map((line) => line.replace(/[？?]+$/, ""))
    .map((line) => `${line}？`);
  return uniqueCleanLines(lines).slice(0, 6);
}

function fallbackQuickQuestions() {
  const ctx = currentContext();
  const scopeText = `${scopeTypeLabel(ctx.scope_type)} ${ctx.scope_key}`;
  const reason = cleanBulletText((lastCopilotData?.risk_reasons || [])[0] || "");
  const riskSnippet = reason ? `，尤其围绕“${reason}”` : "";
  return [
    `请给出 ${ctx.month} ${scopeText} 的 TOP3 缺口项目及差距金额${riskSnippet}。`,
    `如果本月要把目标支撑率提升到 80%，最优先的三条动作是什么？`,
    `请按预算、工时、审批三个维度给出风险排序和负责人建议。`,
    `基于当前预警与违规，未来两周最容易落地的纠偏方案是什么？`,
  ];
}

function renderAskQuickPrompts(questions) {
  if (!askQuickPromptsEl) return;
  const list = uniqueCleanLines(questions || []).slice(0, 6);
  if (!list.length) {
    askQuickPromptsEl.innerHTML = "";
    return;
  }
  askQuickPromptsEl.innerHTML = list
    .map((q) => `<button class="ask-chip" type="button" data-q="${escapeHtml(q)}">${escapeHtml(q)}</button>`)
    .join("");
}

async function generateAiQuickQuestions() {
  if (!askSuggestBtn) return;
  setButtonBusy(askSuggestBtn, true, "生成中");
  try {
    const ctx = currentContext();
    const cfg = currentAiConfig();
    const fallback = fallbackQuickQuestions();
    const prompt = [
      "请基于给定研发投入上下文，生成 5 条高价值管理追问。",
      "输出要求：仅输出 5 行问题，每行一个中文问句，不要编号，不要解释。",
      "不要生成依赖缺失字段的问题，例如责任人姓名、周关闭计划负责人、未提供的组织任命信息。",
      "优先围绕：预算缺口、超预算项目、合规违规、CAPEX/OPEX执行、审批优先级。",
      `月份: ${ctx.month}`,
      `范围: ${scopeTypeLabel(ctx.scope_type)} / ${ctx.scope_key}`,
      `风险原因: ${(lastCopilotData?.risk_reasons || []).join("；") || "暂无"}`,
      `建议动作: ${(lastCopilotData?.suggested_actions || []).join("；") || "暂无"}`,
      "问题应可直接用于“数据问答”输入框，并聚焦预算缺口、执行顺序、责任分配。",
    ].join("\n");
    const data = await api("/api/llm/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        prompt,
        provider: cfg.provider,
        model: cfg.model,
        api_key: cfg.apiKey,
      }),
    });
    const questions = parseQuestionsFromAiText(data?.content || "");
    renderAskQuickPrompts(questions.length ? questions : fallback);
    showToast({ title: "推荐问题已更新", text: `已生成 ${questions.length || fallback.length} 条追问。`, type: "success" });
  } catch (err) {
    renderAskQuickPrompts(fallbackQuickQuestions());
    showToast({ title: "生成失败", text: `已切换为本地推荐：${err.message}`, type: "error" });
  } finally {
    setButtonBusy(askSuggestBtn, false);
  }
}

function initAiEnhancements() {
  renderAiTodoBoard();
  renderAskQuickPrompts(fallbackQuickQuestions());

  seedAiTodoBtn?.addEventListener("click", seedTodosFromCopilot);
  clearAiTodoBtn?.addEventListener("click", () => {
    clearCompletedTodos();
    showToast({ title: "已清理完成项", text: "执行清单已更新。", type: "success", duration: 1400 });
  });

  aiTodoListEl?.addEventListener("click", (e) => {
    const target = e.target;
    const row = target.closest(".todo-item");
    if (!row) return;
    const id = row.dataset.id;
    if (!id) return;
    if (target.classList.contains("todo-del")) {
      deleteTodoById(id);
    }
  });

  aiTodoListEl?.addEventListener("change", (e) => {
    const target = e.target;
    const row = target.closest(".todo-item");
    if (!row) return;
    const id = row.dataset.id;
    if (!id) return;
    if (target.classList.contains("todo-check")) {
      upsertTodoById(id, (item) => ({ ...item, done: Boolean(target.checked) }));
      return;
    }
    if (target.classList.contains("todo-owner")) {
      upsertTodoById(id, (item) => ({ ...item, owner: target.value.trim() }));
      return;
    }
    if (target.classList.contains("todo-due")) {
      upsertTodoById(id, (item) => ({ ...item, due: target.value || "" }));
    }
  });

  askQuickPromptsEl?.addEventListener("click", (e) => {
    const target = e.target.closest(".ask-chip");
    if (!target) return;
    const q = (target.dataset.q || "").trim();
    if (!q) return;
    askPromptEl.value = q;
    runAsk();
  });

  askSuggestBtn?.addEventListener("click", generateAiQuickQuestions);
}

function renderTrend(history, forecast) {
  const container = document.getElementById("trendCanvas");
  container.classList.remove("loading-box");
  const all = [
    ...history.map((x) => ({ ...x, type: "history", y: x.amount_hkd })),
    ...forecast.map((x) => ({ ...x, type: "forecast", y: x.predicted_hkd })),
  ];
  if (!all.length) {
    container.innerHTML = `<div style="padding:16px;color:var(--muted);">历史数据不足，无法绘制趋势。</div>`;
    return;
  }

  const width = container.clientWidth || 1000;
  const height = 180;
  const padding = 28;
  const maxY = Math.max(...all.map((d) => d.y), 1);
  const minY = Math.min(...all.map((d) => d.y), 0);
  const domain = maxY - minY || 1;
  const points = all.map((d, i) => {
    const x = padding + (i * (width - padding * 2)) / Math.max(all.length - 1, 1);
    const y = height - padding - ((d.y - minY) * (height - padding * 2)) / domain;
    return { ...d, x, y };
  });
  const histPoints = points.filter((x) => x.type === "history");
  const futPoints = points.filter((x) => x.type === "forecast");
  const histPath = histPoints.map((p, i) => `${i ? "L" : "M"} ${p.x} ${p.y}`).join(" ");
  const futPath = futPoints.map((p, i) => `${i ? "L" : "M"} ${p.x} ${p.y}`).join(" ");
  const labels = points
    .map((p) => `<text x="${p.x}" y="${height - 8}" font-size="10" text-anchor="middle" fill="var(--muted)">${p.month}</text>`)
    .join("");

  container.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
      <defs>
        <linearGradient id="lineA" x1="0" x2="1">
          <stop offset="0%" stop-color="var(--primary)"/>
          <stop offset="100%" stop-color="var(--accent)"/>
        </linearGradient>
      </defs>
      <line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" stroke="var(--border-strong)" />
      <path d="${histPath}" fill="none" stroke="url(#lineA)" stroke-width="2.2" />
      <path d="${futPath}" fill="none" stroke="var(--warning)" stroke-width="2.2" stroke-dasharray="6 4" />
      ${labels}
    </svg>
  `;
}

async function refreshScopes() {
  const data = await api("/api/scopes");
  scopesCache = data;
  renderScopeOptions();
}

function renderScopeOptions() {
  const t = scopeTypeEl.value;
  let options = [];
  if (t === "company") {
    options = [{ code: "COMPANY", name: "公司整体" }];
  } else if (t === "department") {
    options = scopesCache.departments.map((x) => ({ code: x.code, name: `${x.code} - ${x.name}` }));
  } else {
    options = scopesCache.projects.map((x) => ({ code: x.code, name: `${x.code} - ${x.name}` }));
  }
  scopeKeyEl.innerHTML = options.map((o) => `<option value="${o.code}">${o.name}</option>`).join("");
  if (financeHeadScopeEl) {
    financeHeadScopeEl.textContent = scopeKeyEl.options[scopeKeyEl.selectedIndex]?.text || "公司整体";
  }
}

async function refreshDashboard() {
  setLoading(["kpi-TOTAL", "kpi-OPEX", "kpi-CAPEX"], true);
  try {
    const month = monthEl.value;
    const scopeType = scopeTypeEl.value;
    const scopeKey = scopeKeyEl.value;
    const data = await api(`/api/dashboard?month=${month}&scope_type=${scopeType}&scope_key=${scopeKey}`);
    setKpi("kpi-TOTAL", data.metrics.TOTAL);
    setKpi("kpi-OPEX", data.metrics.OPEX);
    setKpi("kpi-CAPEX", data.metrics.CAPEX);
    if (tickerSupportEl) {
      tickerSupportEl.textContent = formatPct(data.metrics.TOTAL.support_rate, "--");
    }
    if (financeHeadScopeEl) {
      const label = scopeKeyEl.options[scopeKeyEl.selectedIndex]?.text || scopeKeyEl.value || "公司整体";
      financeHeadScopeEl.textContent = label;
    }
  } finally {
    setLoading(["kpi-TOTAL", "kpi-OPEX", "kpi-CAPEX"], false);
  }
}

async function refreshForecast() {
  setLoading(["trendCanvas", "forecastTable", "anomalyBox", "alertBox"], true);
  try {
    const month = monthEl.value;
    const scopeType = scopeTypeEl.value;
    const scopeKey = scopeKeyEl.value;
    const costClass = forecastClassEl.value;
    const data = await api(`/api/forecast?scope_type=${scopeType}&scope_key=${scopeKey}&cost_class=${costClass}&horizon=2`);
    renderTrend(data.history || [], data.forecast || []);
    if (marketHeadNextEl) {
      const next = (data.forecast || [])[0];
      marketHeadNextEl.textContent = next ? formatMoney(next.predicted_hkd) : "样本不足";
    }
    const rows = [
      ...(data.history || []).map((x) => ({ ...x, predicted_hkd: "", kind: "历史" })),
      ...(data.forecast || []).map((x) => ({ month: x.month, amount_hkd: "", predicted_hkd: x.predicted_hkd, kind: "预测" })),
    ];
    renderTable(
      "forecastTable",
      rows,
      [
        { key: "kind", label: "类型" },
        { key: "month", label: "月份" },
        { key: "amount_hkd", label: "历史金额", render: (v) => (v ? formatMoney(v) : "") },
        { key: "predicted_hkd", label: "预测金额", render: (v) => (v ? formatMoney(v) : "") },
      ],
      "暂无预测数据"
    );

    const anomaly = await api(`/api/anomalies/labor?month=${month}`);
    const anomalyRows = [
      ...anomaly.company.map((x) => ({ level: "公司", ...x })),
      ...anomaly.department.map((x) => ({ level: "部门", ...x })),
      ...anomaly.project.map((x) => ({ level: "项目", ...x })),
      ...anomaly.person.map((x) => ({ level: "人员", ...x })),
    ];
    if (tickerAnomalyEl) tickerAnomalyEl.textContent = `${anomalyRows.length} 条`;
    if (monitorHeadAnomalyEl) monitorHeadAnomalyEl.textContent = `${anomalyRows.length} 条`;
    renderTable(
      "anomalyBox",
      anomalyRows,
      [
        { key: "level", label: "层级" },
        { key: "key", label: "编码" },
        { key: "name", label: "名称" },
        { key: "current_hkd", label: "本月", render: (v, r) => (r.level === "人员" ? r.current_hours : formatMoney(v)) },
        { key: "previous_hkd", label: "上月", render: (v, r) => (r.level === "人员" ? r.previous_hours : formatMoney(v)) },
        { key: "growth", label: "环比", render: (v) => formatPct(v) },
      ],
      "本月没有超过 100% 的异常增长。"
    );

    const alerts = await api(`/api/alerts?scope_type=${scopeType}&scope_key=${scopeKey}&month=${month}`);
    if (tickerAlertsEl) tickerAlertsEl.textContent = `${(alerts.items || []).length} 条`;
    if (marketHeadAlertsEl) marketHeadAlertsEl.textContent = `${(alerts.items || []).length} 条`;
    renderTable(
      "alertBox",
      alerts.items || [],
      [
        { key: "level", label: "级别" },
        { key: "cost_class", label: "分类" },
        { key: "scope", label: "对象" },
        { key: "message", label: "提醒内容" },
      ],
      "当前范围暂无预警。"
    );
  } finally {
    setLoading(["trendCanvas", "forecastTable", "anomalyBox", "alertBox"], false);
  }
}

async function refreshComplianceAndSuggestion() {
  setLoading(["complianceBox", "suggestionBox"], true);
  try {
    const month = monthEl.value;
    const compliance = await api(`/api/compliance?month=${month}`);
    const rows = compliance.checks.flatMap((check) =>
      (check.violations || []).map((v) => ({
        rule: check.rule,
        detail: JSON.stringify(v),
      }))
    );
    if (tickerComplianceEl) tickerComplianceEl.textContent = `${rows.length} 条`;
    if (monitorHeadComplianceEl) monitorHeadComplianceEl.textContent = `${rows.length} 条`;
    renderTable(
      "complianceBox",
      rows,
      [
        { key: "rule", label: "规则" },
        { key: "detail", label: "违规明细" },
      ],
      "当前月份未检出违规，或缺少考勤数据。"
    );

    const suggestion = await api(`/api/suggestions/hours?month=${month}`);
    renderTable(
      "suggestionBox",
      suggestion.suggestions.slice(0, 30),
      [
        { key: "project_code", label: "项目编码" },
        { key: "project_name", label: "项目名称" },
        { key: "remain_hkd", label: "剩余预算", render: (v) => formatMoney(v) },
        { key: "avg_cost_per_hour_hkd", label: "历史每工时成本", render: (v) => formatMoney(v) },
        { key: "recommended_next_month_hours", label: "建议下月工时", render: (v) => Number(v).toFixed(2) },
      ],
      "暂无可用预算或工时历史，暂无法建议。"
    );
  } finally {
    setLoading(["complianceBox", "suggestionBox"], false);
  }
}

async function refreshPending() {
  setLoading(["pendingBox"], true);
  try {
    const stage = approvalStageEl.value;
    const month = monthEl.value;
    const actor = actorNameEl.value.trim();
    const data = await api(`/api/approvals/pending?stage=${stage}&month=${month}`);
    const rows = data.items || [];

    if (!rows.length) {
      renderTable("pendingBox", [], [], "当前环节没有待审批工时。");
      return;
    }

    const html = `
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>员工</th>
          <th>项目</th>
          <th>工时</th>
          <th>部门</th>
          <th>动作</th>
        </tr>
      </thead>
      <tbody>
        ${rows
          .map(
            (r) => `
          <tr>
            <td>${r.id}</td>
            <td>${r.employee_name}</td>
            <td>${r.project_code || ""}</td>
            <td>${r.declared_hours}</td>
            <td>${r.dept_name || ""}</td>
            <td>
              <button class="ghost-btn" data-id="${r.id}" data-action="approved">通过</button>
              <button class="ghost-btn" data-id="${r.id}" data-action="rejected">驳回</button>
            </td>
          </tr>
        `
          )
          .join("")}
      </tbody>
    </table>
  `;
    const box = document.getElementById("pendingBox");
    box.classList.remove("loading-box");
    box.innerHTML = html;

    document.querySelectorAll("#pendingBox button[data-id]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const id = btn.getAttribute("data-id");
        const decision = btn.getAttribute("data-action");
        if (!actor) {
          showToast({ title: "缺少审批人", text: "请先输入审批人姓名。", type: "error" });
          return;
        }
        setButtonBusy(btn, true, decision === "approved" ? "通过中" : "驳回中");
        try {
          await api(`/api/approvals/${id}/action`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              stage,
              actor,
              decision,
              comment: decision === "rejected" ? "驳回，请补充说明后重提。" : "",
              human_confirmed: true,
            }),
          });
          showToast({ title: "审批已提交", text: `工时记录 ${id} 已${decision === "approved" ? "通过" : "驳回"}。` });
          await refreshPending();
        } catch (err) {
          showToast({ title: "审批失败", text: err.message, type: "error" });
        } finally {
          setButtonBusy(btn, false);
        }
      });
    });
  } finally {
    setLoading(["pendingBox"], false);
  }
}

async function refreshImports() {
  setLoading(["importLogBox"], true);
  try {
    const data = await api("/api/imports");
    renderTable(
      "importLogBox",
      data.items,
      [
        { key: "source_name", label: "来源文件" },
        { key: "loaded_rows", label: "导入行数" },
        { key: "loaded_at", label: "导入时间" },
        { key: "note", label: "说明" },
      ],
      "暂无导入记录。"
    );
  } finally {
    setLoading(["importLogBox"], false);
  }
}

function riskLabel(level) {
  if (level === "high") return "高";
  if (level === "medium") return "中";
  return "低";
}

async function runCopilot(silent = false) {
  setLoading(["copilotActions"], true);
  if (!silent) setButtonBusy(copilotBtn, true, "生成中");
  try {
    const ctx = currentContext();
    const cfg = currentAiConfig();
    const data = await api("/api/ai/copilot/brief", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ...ctx,
        provider: cfg.provider,
        model: cfg.model,
        api_key: cfg.apiKey,
      }),
    });
    lastCopilotData = data;
    copilotMetaEl.innerHTML = `
      <div>范围：${data.scope_name} ｜ 风险分：<strong>${data.risk_score}</strong> ｜ 风险等级：<strong>${riskLabel(
      data.risk_level
    )}</strong> ｜ 违规：${data.violation_count} ｜ 异常：${data.anomaly_count} ｜ 预警：${data.alert_count}</div>
    `;
    const rows = (data.suggested_actions || []).map((x, idx) => ({
      no: idx + 1,
      action: x,
    }));
    renderTable(
      "copilotActions",
      rows,
      [
        { key: "no", label: "#" },
        { key: "action", label: "建议动作" },
      ],
      "暂无建议动作"
    );
    renderCopilotLlm(data);
    if (!loadAiTodosForCurrentContext().length) {
      renderAiTodoBoard();
    }
    renderAskQuickPrompts(fallbackQuickQuestions());

    const reasons = data.risk_reasons || [];
    const fc = data.forecast?.forecast || [];
    if (insightStrengthEl) {
      insightStrengthEl.textContent =
        data.risk_level === "low"
          ? "目标执行风险较低，当前节奏稳定，可维持滚动复盘机制。"
          : `风险分 ${data.risk_score}，但已形成可执行动作清单，可在本周期纠偏。`;
    }
    if (insightRiskEl) {
      insightRiskEl.textContent = reasons.length ? reasons[0] : "未检出高强度结构性风险。";
    }
    if (insightChanceEl) {
      insightChanceEl.textContent = fc.length
        ? `${fc[0].month} 预测投入约 ${formatMoney(fc[0].predicted_hkd)}，可提前做预算与人力联动配置。`
        : "历史样本偏少，补齐月度数据后可显著提升预测稳定性。";
    }
    if (insightActionEl) {
      insightActionEl.textContent = rows.length
        ? rows[0].action
        : "持续监控预算、工时、考勤的一致性，保持每月闭环。";
    }

    if (!silent) {
      showToast({ title: "决策简报已生成", text: `风险分 ${data.risk_score}，建议动作 ${rows.length} 条。` });
    }
  } catch (err) {
    if (!silent) showToast({ title: "决策简报失败", text: err.message, type: "error" });
  } finally {
    setLoading(["copilotActions"], false);
    if (!silent) setButtonBusy(copilotBtn, false);
  }
}

async function runSimulation(silent = false) {
  setLoading(["simulateResult"], true);
  if (!silent) setButtonBusy(simulateBtn, true, "推演中");
  try {
    const ctx = currentContext();
    const data = await api("/api/ai/simulate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ...ctx,
        cost_class: simClassEl.value,
        add_outsourced_hkd: Number(simOutsrcEl.value || 0),
        add_labor_hours: Number(simHoursEl.value || 0),
        labor_cost_factor: Number(simFactorEl.value || 1),
      }),
    });
    const base = data.base || {};
    const proj = data.projection || {};
    const rows = [
      { metric: "当前完成值", before: formatMoney(base.completed_hkd), after: formatMoney(proj.projected_completed_hkd) },
      { metric: "目标支撑率", before: formatPct(base.support_rate), after: formatPct(proj.projected_support_rate) },
      { metric: "缺口", before: formatMoney(base.gap_hkd), after: formatMoney(proj.projected_gap_hkd) },
      { metric: "平均每工时成本", before: "--", after: formatMoney(data.model_params?.avg_labor_cost_per_hour_hkd) },
      {
        metric: "补齐缺口所需额外工时",
        before: "--",
        after:
          proj.needed_extra_hours_to_close_gap === null || proj.needed_extra_hours_to_close_gap === undefined
            ? "--"
            : Number(proj.needed_extra_hours_to_close_gap).toFixed(2),
      },
    ];
    renderTable(
      "simulateResult",
      rows,
      [
        { key: "metric", label: "指标" },
        { key: "before", label: "当前" },
        { key: "after", label: "推演后" },
      ],
      "暂无推演结果"
    );
    if (!silent) showToast({ title: "推演完成", text: "已输出预算与工时推演结果。" });
  } catch (err) {
    if (!silent) showToast({ title: "推演失败", text: err.message, type: "error" });
  } finally {
    setLoading(["simulateResult"], false);
    if (!silent) setButtonBusy(simulateBtn, false);
  }
}

async function runApprovalAi(silent = false) {
  setLoading(["approvalAiBox"], true);
  if (!silent) setButtonBusy(approvalAiBtn, true, "生成中");
  try {
    const month = monthEl.value;
    const stage = aiApprovalStageEl.value;
    const data = await api(`/api/ai/approvals/recommend?stage=${stage}&month=${month}`);
    const rows = (data.items || []).map((x) => ({
      timesheet_id: x.timesheet_id,
      employee_name: x.employee_name,
      project_code: x.project_code || "",
      risk_score: x.risk_score,
      recommendation: x.recommendation === "approved" ? "建议通过" : "建议驳回",
      reasons: (x.reasons || []).join("；"),
    }));
    renderTable(
      "approvalAiBox",
      rows,
      [
        { key: "timesheet_id", label: "工时ID" },
        { key: "employee_name", label: "员工" },
        { key: "project_code", label: "项目编码" },
        { key: "risk_score", label: "风险分" },
        { key: "recommendation", label: "建议" },
        { key: "reasons", label: "原因" },
      ],
      "当前阶段没有待审批数据。"
    );
    if (!silent) showToast({ title: "审批建议已更新", text: `共 ${rows.length} 条待审批建议。` });
  } catch (err) {
    if (!silent) showToast({ title: "审批建议失败", text: err.message, type: "error" });
  } finally {
    setLoading(["approvalAiBox"], false);
    if (!silent) setButtonBusy(approvalAiBtn, false);
  }
}

async function runAsk() {
  const question = askPromptEl.value.trim();
  if (!question) {
    showToast({ title: "提示", text: "请输入问题。", type: "error" });
    return;
  }
  setButtonBusy(askBtn, true, "分析中");
  let answer = "";
  const meta = { mode: "llm", status: "连接中" };
  renderMarkdownResult(askResultEl, "*正在连接模型，请稍候...*", meta, true);
  try {
    const ctx = currentContext();
    const cfg = currentAiConfig();
    await streamNdjson("/api/ai/ask/stream", {
      question,
      ...ctx,
      provider: cfg.provider,
      model: cfg.model,
      api_key: cfg.apiKey,
    }, {
      onEvent: (event) => {
        if (!event || typeof event !== "object") return;
        if (event.type === "status") {
          meta.status = String(event.text || "处理中");
          const tmp = answer || `*${meta.status}*`;
          renderMarkdownResult(askResultEl, tmp, meta, true);
          return;
        }
        if (event.type === "meta") {
          if (event.mode) meta.mode = String(event.mode);
          if (event.provider) meta.provider = String(event.provider);
          if (event.model) meta.model = String(event.model);
          renderMarkdownResult(askResultEl, answer || "*开始生成...*", meta, true);
          return;
        }
        if (event.type === "delta") {
          answer += String(event.content || "");
          meta.status = "生成中";
          renderMarkdownResult(askResultEl, answer, meta, true);
          return;
        }
        if (event.type === "error") {
          throw new Error(String(event.message || "流式问答失败"));
        }
        if (event.type === "done") {
          if (event.mode) meta.mode = String(event.mode);
          meta.status = "完成";
        }
      },
    });
    const finalText = answer.trim() || "数据不足：未返回可解析内容，请稍后重试。";
    renderMarkdownResult(askResultEl, finalText, meta, false);
    showToast({ title: "问答完成", text: "已基于当前真实数据输出答案。" });
  } catch (err) {
    renderMarkdownResult(askResultEl, `**调用失败：** ${err.message}`, { mode: "error", status: "失败" }, false);
    showToast({ title: "问答失败", text: err.message, type: "error" });
  } finally {
    setButtonBusy(askBtn, false);
  }
}

async function refreshAll() {
  const token = ++refreshToken;
  setPageStatus("数据状态：刷新中…", true);
  setButtonBusy(refreshBtn, true, "刷新中");
  setButtonBusy(quickRefreshBtn, true, "刷新中");
  try {
    await Promise.all([
      refreshDashboard(),
      refreshForecast(),
      refreshComplianceAndSuggestion(),
      refreshPending(),
      refreshImports(),
      runCopilot(true),
      runApprovalAi(true),
      runSimulation(true),
    ]);
    if (token === refreshToken) {
      setPageStatus(`数据状态：已更新（${new Date().toLocaleTimeString("zh-CN")}）`, false);
      showToast({ title: "刷新完成", text: "看板与明细已更新。", type: "success", duration: 1800 });
    }
  } catch (err) {
    setPageStatus("数据状态：刷新失败", false);
    showToast({ title: "刷新失败", text: err.message, type: "error", duration: 3200 });
    throw err;
  } finally {
    setButtonBusy(refreshBtn, false);
    setButtonBusy(quickRefreshBtn, false);
  }
}

async function triggerRefresh() {
  try {
    await refreshAll();
  } catch (err) {
    console.warn(err.message);
  }
}

function uploadForm(formId, endpoint, label) {
  const form = document.getElementById(formId);
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const submitBtn = form.querySelector("button[type='submit']");
    const fd = new FormData(form);
    setButtonBusy(submitBtn, true, "上传中");
    try {
      const result = await api(endpoint, { method: "POST", body: fd });
      showToast({
        title: "上传成功",
        text: `${label} 已入库，导入 ${result.loaded_rows ?? 0} 行。`,
        type: "success",
      });
      await refreshAll();
      form.reset();
    } catch (err) {
      showToast({ title: "上传失败", text: err.message, type: "error" });
    } finally {
      setButtonBusy(submitBtn, false);
    }
  });
}

async function runLlm() {
  const prompt = llmPromptEl.value.trim();
  if (!prompt) {
    showToast({ title: "提示", text: "请输入分析问题。", type: "error" });
    return;
  }
  setButtonBusy(llmBtn, true, "生成中");
  let content = "";
  const meta = { mode: "llm", status: "连接中" };
  renderMarkdownResult(llmResultEl, "*正在调用模型，请稍候...*", meta, true);
  try {
    const cfg = currentAiConfig();
    await streamNdjson("/api/llm/analyze/stream", {
      prompt,
      provider: cfg.provider,
      model: cfg.model,
      api_key: cfg.apiKey,
    }, {
      onEvent: (event) => {
        if (!event || typeof event !== "object") return;
        if (event.type === "status") {
          meta.status = String(event.text || "处理中");
          const tmp = content || `*${meta.status}*`;
          renderMarkdownResult(llmResultEl, tmp, meta, true);
          return;
        }
        if (event.type === "meta") {
          if (event.provider) meta.provider = String(event.provider);
          if (event.model) meta.model = String(event.model);
          renderMarkdownResult(llmResultEl, content || "*开始生成...*", meta, true);
          return;
        }
        if (event.type === "delta") {
          content += String(event.content || "");
          meta.status = "生成中";
          renderMarkdownResult(llmResultEl, content, meta, true);
          return;
        }
        if (event.type === "error") {
          throw new Error(String(event.message || "流式分析失败"));
        }
        if (event.type === "done") {
          meta.status = "完成";
        }
      },
    });
    renderMarkdownResult(llmResultEl, content || "数据不足：未返回可解析内容。", meta, false);
    showToast({ title: "分析完成", text: "LLM 建议已生成。", type: "success" });
  } catch (err) {
    renderMarkdownResult(llmResultEl, `**调用失败：** ${err.message}`, { mode: "error", status: "失败" }, false);
    showToast({ title: "分析失败", text: err.message, type: "error" });
  } finally {
    setButtonBusy(llmBtn, false);
  }
}

initTheme();
initDsKeyUi();
initSidebarNav();
initAiEnhancements();

loadDataBtn.addEventListener("click", async () => {
  setButtonBusy(loadDataBtn, true, "初始化中");
  setPageStatus("数据状态：初始化真实数据中…", true);
  try {
    const data = await api("/api/load-initial-data", { method: "POST" });
    await refreshScopes();
    await refreshAll();
    const loadedTotal = Object.values(data.stats || {}).reduce((sum, x) => sum + Number(x || 0), 0);
    showToast({
      title: "初始化完成",
      text: `已从源文件导入 ${loadedTotal} 条有效记录。`,
      type: "success",
      duration: 3200,
    });
  } catch (err) {
    setPageStatus("数据状态：初始化失败", false);
    showToast({ title: "初始化失败", text: err.message, type: "error", duration: 3600 });
  } finally {
    setButtonBusy(loadDataBtn, false);
  }
});

scopeTypeEl.addEventListener("change", async () => {
  renderScopeOptions();
  renderAiTodoBoard();
  renderAskQuickPrompts(fallbackQuickQuestions());
  await triggerRefresh();
});

monthEl.addEventListener("change", async () => {
  renderAiTodoBoard();
  renderAskQuickPrompts(fallbackQuickQuestions());
  await triggerRefresh();
});
scopeKeyEl.addEventListener("change", async () => {
  renderAiTodoBoard();
  renderAskQuickPrompts(fallbackQuickQuestions());
  await triggerRefresh();
});
if (refreshBtn) refreshBtn.addEventListener("click", triggerRefresh);
if (quickRefreshBtn) quickRefreshBtn.addEventListener("click", triggerRefresh);
if (forecastBtn) forecastBtn.addEventListener("click", refreshForecast);
if (forecastClassEl) {
  forecastClassEl.addEventListener("change", () => {
    refreshForecast().catch((err) => {
      showToast({ title: "刷新失败", text: err.message, type: "error" });
    });
  });
}
if (loadPendingBtn) loadPendingBtn.addEventListener("click", refreshPending);
if (approvalStageEl) {
  approvalStageEl.addEventListener("change", () => {
    refreshPending().catch((err) => {
      showToast({ title: "加载待办失败", text: err.message, type: "error" });
    });
  });
}
llmBtn.addEventListener("click", runLlm);
copilotBtn.addEventListener("click", () => runCopilot(false));
simulateBtn.addEventListener("click", () => runSimulation(false));
approvalAiBtn.addEventListener("click", () => runApprovalAi(false));
askBtn.addEventListener("click", runAsk);
aiApprovalStageEl.addEventListener("change", () => runApprovalAi(false));
if (monitorRefreshBtn) {
  monitorRefreshBtn.addEventListener("click", async () => {
    setButtonBusy(monitorRefreshBtn, true, "刷新中");
    try {
      await Promise.all([refreshForecast(), refreshComplianceAndSuggestion()]);
      showToast({ title: "监控已刷新", text: "异常与合规数据已更新。", type: "success", duration: 1600 });
    } catch (err) {
      showToast({ title: "刷新失败", text: err.message, type: "error" });
    } finally {
      setButtonBusy(monitorRefreshBtn, false);
    }
  });
}
if (ingestRefreshBtn) {
  ingestRefreshBtn.addEventListener("click", async () => {
    setButtonBusy(ingestRefreshBtn, true, "刷新中");
    try {
      await refreshImports();
      showToast({ title: "导入记录已刷新", text: "最新导入日志已更新。", type: "success", duration: 1400 });
    } catch (err) {
      showToast({ title: "刷新失败", text: err.message, type: "error" });
    } finally {
      setButtonBusy(ingestRefreshBtn, false);
    }
  });
}

uploadForm("erpForm", "/api/upload/erp", "ERP 文件");
uploadForm("attendanceForm", "/api/upload/attendance", "考勤文件");

(async () => {
  setPageStatus("数据状态：加载中…", true);
  try {
    await refreshScopes();
    await refreshAll();
    showToast({
      title: "页面就绪",
      text: "可直接点击“初始化真实数据”开始正式使用。",
      type: "success",
      duration: 2400,
    });
  } catch (err) {
    setPageStatus("数据状态：初始化失败", false);
    showToast({ title: "加载失败", text: err.message, type: "error" });
  }
})();
