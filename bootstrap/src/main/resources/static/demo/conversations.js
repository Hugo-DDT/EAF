import { createConversationApi } from "/demo/conversation-api.js";
import { el, renderTurn, renderComparison, renderUsage, renderFollowups } from "/demo/conversation-view.js";

const $ = (selector) => document.querySelector(selector);
const api = createConversationApi();
const terminal = new Set(["SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"]);
const state = { generation: 0, statusFilter: "ACTIVE", sessions: [], selectedId: null, current: null,
  brief: null, turns: [], pendingSend: null, pendingBrief: null, pendingFollowups: new Map(),
  suggestionTaskId: null, timer: null, feedbacks: new Map(), experienceDrafts: new Map(),
  feedbackKeys: new Map(), draftKeys: new Map(), cards: [], teamResults: [], teamResultIds: new Set(), editingCard: null, cardSource: null,
  pendingCard: null, cardActionKeys: new Map(), cardCursor: null };

function workspacePath(path) { return `/conversations${path}`; }
function apiTask(path) { return `/tasks${path}`; }
function taskIdPath(id, suffix = "") { return `${apiTask(`/${encodeURIComponent(id)}`)}${suffix}`; }
function isActive(status) { return status && !terminal.has(status); }
function setStatus(selector, text, error = false) { const item = $(selector); item.textContent = text || ""; item.classList.toggle("error-text", error); }
function stopPolling() { if (state.timer !== null) window.clearTimeout(state.timer); state.timer = null; }
function selectedUrl(id) {
  const url = new URL(window.location.href);
  if (id) url.searchParams.set("conversationId", id); else url.searchParams.delete("conversationId");
  window.history.replaceState(null, "", url);
}
function selectedFromUrl() { return new URL(window.location.href).searchParams.get("conversationId"); }

function clearPrivateData({ preserveSelectionUrl = false } = {}) {
  stopPolling();
  state.sessions = []; state.selectedId = null; state.current = null; state.brief = null; state.turns = [];
  state.pendingSend = null; state.pendingBrief = null; state.suggestionTaskId = null;
  state.pendingFollowups.clear();
  state.feedbacks.clear(); state.experienceDrafts.clear(); state.feedbackKeys.clear(); state.draftKeys.clear();
  state.cards = []; state.cardCursor = null; state.editingCard = null; state.cardSource = null; state.pendingCard = null; state.cardActionKeys.clear();
  state.teamResults = []; state.teamResultIds.clear();
  // URL 只保留非敏感会话 ID，供刷新后的重新认证恢复会话；访问令牌仍只留在页面内存。
  if (!preserveSelectionUrl) selectedUrl(null);
  $("#session-list").replaceChildren(el("p", "quiet", "连接身份后加载会话。"));
  $("#conversation-mode").textContent = "NO SESSION SELECTED";
  $("#conversation-title").textContent = "选择一个会话";
  $("#conversation-meta").textContent = "连续问题、当前任务和结果会显示在这里。";
  $("#message-list").replaceChildren(el("div", "welcome-note", "会话消息在这里按轮次恢复。"));
  $("#brief-content").value = ""; $("#brief-revision").textContent = "R—";
  $("#suggestion").hidden = true; $("#comparison-section").hidden = true;
  $("#comparison").replaceChildren(); $("#usage").replaceChildren(el("p", "quiet", "暂无计量数据。"));
  $("#followups").replaceChildren(el("p", "quiet", "已确认的跟进流程会显示在这里。"));
  $("#experience-card-list").replaceChildren(el("p", "quiet", "连接身份后加载个人卡片。"));
  $("#load-more-experience-cards").hidden = true;
  resetExperienceEditor(); setStatus("#experience-status", "");
  $("#notice").textContent = ""; setStatus("#brief-status", "");
  $("#message-input").value = ""; $("#message-input").disabled = true; $("#send-message").disabled = true;
  $("#brief-content").disabled = true; $("#save-brief").disabled = true;
  $("#save-and-analyze").hidden = true; $("#rename-session").disabled = true; $("#archive-session").disabled = true;
  $("#cancel-turn").hidden = true; $("#poll-state").textContent = "终态任务停止轮询";
  document.body.classList.add("disconnected");
}

function setConnected(value) {
  document.body.classList.toggle("disconnected", !value);
  $("#identity-status").textContent = value ? "身份已连接；令牌留在页面内存中。" : "未连接";
}

async function connect(event) {
  event.preventDefault();
  const requested = selectedFromUrl();
  clearPrivateData();
  const token = $("#token").value.trim();
  const workspace = $("#workspace-id").value.trim();
  $("#token").value = "";
  if (!token || !workspace) { setConnected(false); setStatus("#identity-status", "需要令牌和 Workspace ID。", true); return; }
  state.generation = api.connect(token, workspace);
  setConnected(true);
  setStatus("#identity-status", "身份已连接；正在读取私人会话…");
  try {
    await Promise.all([refreshSessions(), refreshExperienceCards()]);
    if (state.generation !== api.generation()) return;
    if (requested) await selectSession(requested);
    setStatus("#identity-status", "已连接；刷新后请重新认证。令牌仅保存在当前页面内存中。");
  } catch (error) {
    if (error.name !== "AbortError") setStatus("#identity-status", error.message, true);
  }
}

function renderSessions() {
  const target = $("#session-list"); target.replaceChildren();
  if (!state.sessions.length) { target.append(el("p", "quiet", state.statusFilter === "ACTIVE" ? "没有进行中的会话。" : "没有已归档的会话。")); return; }
  for (const session of state.sessions) {
    const button = el("button", `session-item${session.id === state.selectedId ? " active" : ""}`);
    button.type = "button";
    button.append(el("b", "", session.title));
    const kind = session.mode === "CUSTOMER_ASSISTANT" ? `客户 · ${session.customerId}` : "知识问答";
    button.append(el("small", session.status === "ARCHIVED" ? "archived" : "", `${kind} · ${session.lastTurnNo} 轮 · R${session.currentBriefRevision}`));
    button.addEventListener("click", () => selectSession(session.id));
    target.append(button);
  }
}

async function refreshSessions() {
  if (!api.connected()) return;
  const generation = state.generation;
  const data = await api.request(`${workspacePath("")}?status=${state.statusFilter}&limit=50`);
  if (generation !== state.generation || generation !== api.generation()) return;
  state.sessions = data?.items || [];
  renderSessions();
}

function updateModeFields() {
  const customer = $("#session-mode").value === "CUSTOMER_ASSISTANT";
  $("#customer-field").hidden = !customer;
  $("#new-customer").required = customer;
}

async function createSession(event) {
  event.preventDefault();
  if (!api.connected()) return setStatus("#identity-status", "请先连接身份。", true);
  const mode = $("#session-mode").value;
  const request = { mode, title: $("#session-title").value.trim() || null,
    customerId: mode === "CUSTOMER_ASSISTANT" ? $("#new-customer").value.trim() : null };
  try {
    const result = await api.request(workspacePath(""), { method: "POST", headers: { "Idempotency-Key": api.key() }, body: JSON.stringify(request) });
    $("#new-session").reset(); updateModeFields();
    state.statusFilter = result.status || "ACTIVE";
    document.querySelectorAll(".filter").forEach((button) => button.classList.toggle("active", button.dataset.sessionStatus === state.statusFilter));
    await refreshSessions();
    await selectSession(result.id);
  } catch (error) { setStatus("#identity-status", error.message, true); }
}

function renderBriefSuggestion() {
  state.suggestionTaskId = null;
  const turn = [...state.turns].reverse().find((item) => item.status === "SUCCEEDED" && item.briefRevision === state.current?.currentBriefRevision
    && item.result?.briefSuggestion?.content);
  const panel = $("#suggestion");
  if (!turn || state.current?.mode !== "CUSTOMER_ASSISTANT") {
    panel.hidden = true; state.suggestionTaskId = null; return;
  }
  panel.hidden = false;
  $("#suggestion-summary").textContent = `${turn.result.briefSuggestion.changeSummary || "建议更新客户简报"} · 来源第 ${turn.turnNo} 轮`;
  const actions = panel.querySelector(".suggestion-actions");
  if (actions) actions.remove();
  const controls = el("div", "suggestion-actions");
  const apply = el("button", "button button-sage", "载入建议到编辑框"); apply.type = "button";
  apply.addEventListener("click", () => { $("#brief-content").value = turn.result.briefSuggestion.content; state.suggestionTaskId = turn.taskId; });
  const ignore = el("button", "text-button", "忽略本条建议"); ignore.type = "button";
  ignore.addEventListener("click", () => { panel.hidden = true; state.suggestionTaskId = null; });
  controls.append(apply, ignore); panel.append(controls);
}

async function loadSources(taskId) {
  try { const result = await api.request(taskIdPath(taskId, "/sources")); return result?.items || []; }
  catch { return []; }
}

async function renderTurns(loadSourceContent = true) {
  const target = $("#message-list"); target.replaceChildren();
  if (!state.turns.length) { target.append(el("div", "welcome-note", "这是一个空会话。提出一个问题或补充一段合成客户材料即可开始。")); return; }
  const sourceTasks = loadSourceContent
    ? state.turns.slice(-5).filter((turn) => turn.result?.citations?.length).map((turn) => turn.taskId) : [];
  const sources = new Map(await Promise.all(sourceTasks.map(async (taskId) => [taskId, await loadSources(taskId)])));
  const feedbackTasks = state.turns.filter((turn) => turn.status === "SUCCEEDED" && !state.feedbacks.has(turn.taskId));
  await Promise.all(feedbackTasks.map(async (turn) => {
    try { state.feedbacks.set(turn.taskId, await api.request(taskIdPath(turn.taskId, "/feedback"))); }
    catch { state.feedbacks.set(turn.taskId, []); }
  }));
  for (const turn of state.turns) renderTurn(target, turn, state.current.mode, sources.has(turn.taskId) ? sources.get(turn.taskId) : null, {
    briefRevision: state.current.currentBriefRevision,
    active: isActive(state.current.activeTaskStatus),
    onConfirmFollowup: submitFollowup,
    feedbackItems: state.feedbacks.get(turn.taskId) || [],
    experienceDrafts: new Map((state.feedbacks.get(turn.taskId) || []).map((item) => [item.id, state.experienceDrafts.get(item.id)]).filter(([, draft]) => draft)),
    onSubmitFeedback: submitExperienceFeedback,
    onCreateExperienceDraft: createExperienceDraft,
    onEditExperience: editExperienceFromFeedback
  });
  target.scrollTop = target.scrollHeight;
}

function renderCurrent(loadSourceContent = true) {
  if (!state.current) return;
  const item = state.current;
  $("#conversation-mode").textContent = `${item.mode === "CUSTOMER_ASSISTANT" ? "CUSTOMER ASSISTANT" : "KNOWLEDGE QA"} · ${item.status}`;
  $("#conversation-title").textContent = item.title;
  $("#conversation-meta").textContent = item.mode === "CUSTOMER_ASSISTANT"
    ? `合成客户 ${item.customerId} · 已确认简报 R${item.currentBriefRevision} · ${item.lastTurnNo} 轮`
    : `已确认主题简报 R${item.currentBriefRevision} · ${item.lastTurnNo} 轮`;
  const archived = item.status === "ARCHIVED";
  const busy = isActive(item.activeTaskStatus);
  $("#rename-session").disabled = archived;
  $("#archive-session").disabled = busy;
  $("#archive-session").textContent = archived ? "恢复" : "归档";
  $("#message-input").disabled = archived || busy;
  $("#send-message").disabled = archived || busy;
  $("#message-input").placeholder = item.mode === "CUSTOMER_ASSISTANT"
    ? "补充合成客户材料，例如近期联系、投诉处理或待核实情况。" : "例如：那延期规则也适用于加急产品吗？";
  $("#brief-content").disabled = archived || busy;
  $("#save-brief").disabled = archived || busy;
  $("#save-and-analyze").hidden = item.mode !== "CUSTOMER_ASSISTANT" || archived || busy;
  $("#save-and-analyze").disabled = archived || busy;
  $("#cancel-turn").hidden = !busy || !item.activeTaskId;
  $("#poll-state").textContent = busy ? `正在查询轮次状态 · ${item.activeTaskStatus}` : "终态任务停止轮询";
  const currentTurn = [...state.turns].reverse().find((turn) => turn.taskId === item.activeTaskId);
  $("#cancel-turn").onclick = () => cancelTurn(currentTurn);
  $("#brief-revision").textContent = state.brief ? `R${state.brief.revision}` : `R${item.currentBriefRevision}`;
  $("#brief-content").value = state.brief?.content || "";
  $("#comparison-section").hidden = item.mode !== "CUSTOMER_ASSISTANT";
  renderBriefSuggestion(); renderSessions();
  void renderTurns(loadSourceContent);
}

async function loadSideData(conversationId, generation) {
  const [usage, followups, comparison] = await Promise.allSettled([
    api.request(workspacePath(`/${encodeURIComponent(conversationId)}/usage`)),
    api.request(workspacePath(`/${encodeURIComponent(conversationId)}/followups`)),
    api.request(workspacePath(`/${encodeURIComponent(conversationId)}/analysis-comparison`))
  ]);
  if (generation !== state.generation || state.selectedId !== conversationId) return;
  if (usage.status === "fulfilled") renderUsage($("#usage"), usage.value);
  else $("#usage").replaceChildren(el("p", "quiet", usage.reason.message));
  if (followups.status === "fulfilled") renderFollowups($("#followups"), followups.value);
  else $("#followups").replaceChildren(el("p", "quiet", followups.reason.message));
  if (state.current?.mode === "CUSTOMER_ASSISTANT" && comparison.status === "fulfilled")
    renderComparison($("#comparison"), comparison.value);
  await refreshTeamResults();
}

async function loadConversation(conversationId, generation = state.generation, updateList = false, pollOnly = false) {
  if (!api.connected() || !conversationId) return;
  const [conversation, turns, brief] = await Promise.all([
    api.request(workspacePath(`/${encodeURIComponent(conversationId)}`)),
    api.request(workspacePath(`/${encodeURIComponent(conversationId)}/turns?limit=50`)),
    api.request(workspacePath(`/${encodeURIComponent(conversationId)}/brief`))
  ]);
  if (generation !== state.generation || conversationId !== state.selectedId) return;
  state.current = conversation;
  state.turns = turns?.items || [];
  state.brief = brief;
  if (state.pendingSend && state.turns.some((turn) => turn.input === state.pendingSend.input
    && turn.turnNo > state.pendingSend.afterTurnNo)) state.pendingSend = null;
  if (updateList) await refreshSessions();
  if (generation !== state.generation) return;
  renderCurrent(!pollOnly);
  if (!pollOnly) await loadSideData(conversationId, generation);
}

async function selectSession(id, pushUrl = true) {
  if (!api.connected()) return;
  stopPolling();
  state.selectedId = id;
  state.current = null; state.brief = null; state.turns = [];
  if (pushUrl) selectedUrl(id);
  const found = state.sessions.find((item) => item.id === id);
  if (found && found.status !== state.statusFilter) {
    state.statusFilter = found.status;
    document.querySelectorAll(".filter").forEach((button) => button.classList.toggle("active", button.dataset.sessionStatus === state.statusFilter));
    await refreshSessions();
  }
  try {
    await loadConversation(id);
    schedulePoll();
  } catch (error) {
    $("#notice").textContent = error.message;
    if (error.name !== "AbortError") {
      clearPrivateSelection(); api.disconnect(); setConnected(false);
    }
  }
}

function clearPrivateSelection() {
  stopPolling(); state.selectedId = null; state.current = null; state.brief = null; state.turns = [];
  selectedUrl(null); clearPrivateData();
}

function schedulePoll() {
  stopPolling();
  const pollConversation = Boolean(state.current && isActive(state.current.activeTaskStatus));
  const pollDraft = [...state.experienceDrafts.values()].some((task) => task?.id && isActive(task.status));
  if (!api.connected() || (!pollConversation && !pollDraft) || document.hidden) return;
  state.timer = window.setTimeout(async () => {
    state.timer = null;
    const generation = state.generation;
    try {
      if (pollConversation) await loadConversation(state.selectedId, generation, false, true);
      await refreshExperienceDrafts();
    } catch (error) { if (generation === state.generation && error.name !== "AbortError") $("#notice").textContent = `查询失败，可手动刷新：${error.message}`; }
    schedulePoll();
  }, 2_000);
}

async function refreshSelected() {
  if (!state.selectedId) return refreshSessions();
  try { await loadConversation(state.selectedId, state.generation, true); schedulePoll(); }
  catch (error) { $("#notice").textContent = `查询失败：${error.message}`; }
}

async function sendTurn(input) {
  if (!state.current || isActive(state.current.activeTaskStatus)) return;
  let pending = state.pendingSend;
  if (pending && pending.input !== input) {
    $("#notice").textContent = "上一条发送结果仍待核对。请刷新会话；确认该轮未创建后再开始新问题。";
    return;
  }
  if (!pending) pending = state.pendingSend = { key: api.key(), input, afterTurnNo: state.current.lastTurnNo,
    followupResultIds: [...state.teamResultIds] };
  $("#send-message").disabled = true;
  $("#notice").textContent = "正在提交本轮…";
  try {
    await api.request(workspacePath(`/${encodeURIComponent(state.current.id)}/turns`), {
      method: "POST", headers: { "Idempotency-Key": pending.key }, body: JSON.stringify({ input: pending.input,
        followupResultIds: pending.followupResultIds })
    });
    state.pendingSend = null;
    state.teamResultIds.clear(); renderTeamResults();
    $("#message-input").value = "";
    $("#notice").textContent = "轮次已创建；结果将按 Task 状态更新。";
    await loadConversation(state.selectedId, state.generation, true);
    schedulePoll();
  } catch (error) {
    $("#send-message").disabled = false;
    $("#notice").textContent = `发送状态可能未知。刷新核对后可用同一内容重试：${error.message}`;
  }
}

async function refreshTeamResults() {
  const section = $("#team-results-section");
  if (!section || !api.connected()) return;
  const supported = state.current?.mode === "CUSTOMER_ASSISTANT" && state.current.capabilityVersion === "1.2.0";
  section.hidden = !supported;
  if (!supported) { state.teamResults = []; state.teamResultIds.clear(); return; }
  setStatus("#team-result-status", "正在读取当前客户的共享结果…");
  const customerId = encodeURIComponent(state.current.customerId);
  try {
    const cards = await api.request(`/customer-followups?scope=team&customerId=${customerId}&limit=20`);
    const details = await Promise.all((cards?.items || []).map((card) =>
      api.request(`/customer-followups/${encodeURIComponent(card.id)}`).catch(() => null)));
    const next = details.filter(Boolean).flatMap((detail) => (detail.recentResults || []).map((result) => ({
      ...result, customerId: detail.card.customerId, cardSummary: detail.card.summary,
      cardCreatorId: detail.card.creatorId, assigneeId: detail.card.assigneeId
    }))).sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
    const allowed = new Set(next.map((item) => item.id));
    state.teamResultIds = new Set([...state.teamResultIds].filter((id) => allowed.has(id)));
    state.teamResults = next;
    renderTeamResults();
    setStatus("#team-result-status", `${next.length} 条近期结果可供本轮选择；所选内容将在发送时重新检查权限。`);
  } catch (error) {
    state.teamResults = []; state.teamResultIds.clear(); renderTeamResults();
    setStatus("#team-result-status", `团队结果暂不可读：${error.message}`, true);
  }
}

function renderTeamResults() {
  const target = $("#team-result-list");
  if (!target) return;
  target.replaceChildren();
  if (!state.teamResults.length) { target.append(el("p", "quiet", "当前客户还没有可见的团队结果。")); return; }
  for (const item of state.teamResults) {
    const label = el("label", "team-result-option");
    const checkbox = document.createElement("input"); checkbox.type = "checkbox"; checkbox.value = item.id;
    checkbox.checked = state.teamResultIds.has(item.id);
    checkbox.disabled = !checkbox.checked && state.teamResultIds.size >= 3;
    checkbox.addEventListener("change", () => {
      if (checkbox.checked && state.teamResultIds.size >= 3) { checkbox.checked = false; return; }
      if (checkbox.checked) state.teamResultIds.add(item.id); else state.teamResultIds.delete(item.id);
      renderTeamResults();
    });
    const copy = el("span", "team-result-copy");
    copy.append(el("b", "", `结果 ${item.resultNo} · ${item.outcomeCode} · ${item.syncStatus || "本地记录"}`));
    copy.append(el("span", "", item.summary));
    copy.append(el("small", "", `负责人记录 · ${new Date(item.createdAt).toLocaleString()}${item.correctsResultId ? " · 更正记录" : ""}`));
    label.append(checkbox, copy); target.append(label);
  }
  setStatus("#team-result-status", `已选 ${state.teamResultIds.size}/3 条。带入记录会作为独立业务资料展示，不会成为知识证据。`);
}

async function sendMessage(event) {
  event.preventDefault();
  const input = $("#message-input").value.trim();
  if (!input) return;
  if (state.pendingSend) return sendTurn(state.pendingSend.input);
  await sendTurn(input);
}

async function saveBrief({ suggestionTaskId = null } = {}) {
  if (!state.current || !state.brief) return false;
  const content = $("#brief-content").value.trim();
  let pending = state.pendingBrief;
  if (pending && (pending.content !== content || pending.suggestionTaskId !== suggestionTaskId)) {
    setStatus("#brief-status", "上一条简报保存仍待核对，请先刷新。", true); return false;
  }
  if (!pending) pending = state.pendingBrief = { key: api.key(), content, suggestionTaskId, revision: state.brief.revision };
  $("#save-brief").disabled = true;
  try {
    const saved = await api.request(workspacePath(`/${encodeURIComponent(state.current.id)}/brief`), {
      method: "PUT", headers: { "Idempotency-Key": pending.key },
      body: JSON.stringify({ expectedRevision: pending.revision, content: pending.content, suggestionTaskId: pending.suggestionTaskId })
    });
    state.pendingBrief = null; state.suggestionTaskId = null;
    state.brief = saved.result;
    setStatus("#brief-status", saved.changed ? `已保存简报修订 R${saved.currentRevision}。` : `内容未变化，仍为 R${saved.currentRevision}。`);
    await loadConversation(state.selectedId, state.generation);
    return true;
  } catch (error) {
    setStatus("#brief-status", `保存可能已提交；刷新核对后用相同内容重试：${error.message}`, true);
    return false;
  } finally { if (state.current) $("#save-brief").disabled = isActive(state.current.activeTaskStatus) || state.current.status === "ARCHIVED"; }
}

async function saveOnly() { await saveBrief({ suggestionTaskId: state.suggestionTaskId }); }

async function saveAndAnalyze() {
  const saved = await saveBrief({ suggestionTaskId: state.suggestionTaskId });
  if (!saved) return;
  const note = "请根据已确认的当前简报重新分析该客户。把新补充作为待核实资料，不作事实承诺。";
  $("#message-input").value = note;
  await sendTurn(note);
  if (state.current?.activeTaskStatus) setStatus("#brief-status", `简报已保存为 R${state.current.currentBriefRevision}；分析轮次已创建。`);
}

function resetExperienceEditor() {
  state.editingCard = null; state.cardSource = null;
  const form = $("#experience-card-form"); if (form) form.reset();
  $("#experience-type").value = "PROCEDURAL";
  $("#experience-applicability").value = "GENERAL";
  $("#experience-customer-field").hidden = true;
  $("#experience-source-note").textContent = "手工卡不绑定反馈来源。";
  $("#save-experience-card").textContent = "保存草稿";
  $("#cancel-experience-edit").hidden = true;
}

function renderExperienceCards() {
  const target = $("#experience-card-list"); target.replaceChildren();
  if (!state.cards.length) { target.append(el("p", "quiet", "还没有个人经验卡。可手工新建，或从反馈生成草稿。")); return; }
  for (const card of state.cards) {
    const row = el("article", "experience-card");
    row.append(el("div", "experience-card-head", `${card.applicability === "CUSTOMER" ? `客户 · ${card.customerId}` : "个人通用"} · ${card.status}`));
    row.append(el("h3", "", card.latest?.title || "未命名经验"));
    row.append(el("p", "", card.latest?.content || ""));
    row.append(el("small", "", `修订 ${card.latestRevision} · ${card.latest?.type === "PREFERENCE" ? "表达偏好" : "操作经验"} · 到期 ${new Date(card.latest?.expiresAt).toLocaleDateString()}`));
    const actions = el("div", "experience-card-actions");
    const edit = el("button", "text-button", "编辑"); edit.type = "button"; edit.addEventListener("click", () => editExperienceCard(card)); actions.append(edit);
    if (card.latest?.status === "DRAFT") {
      const publish = el("button", "text-button", "本人确认启用"); publish.type = "button"; publish.addEventListener("click", () => publishExperienceCard(card)); actions.append(publish);
    }
    if (card.activeRevision != null) {
      const revoke = el("button", "text-button", "撤回"); revoke.type = "button"; revoke.addEventListener("click", () => revokeExperienceCard(card)); actions.append(revoke);
    }
    row.append(actions); target.append(row);
  }
}

async function refreshExperienceCards() {
  if (!api.connected()) return;
  state.cardCursor = null;
  const page = await requestExperienceCardPage(null);
  state.cards = page?.items || [];
  state.cardCursor = page?.nextCursorUpdatedAt && page?.nextCursorId
    ? { updatedAt: page.nextCursorUpdatedAt, id: page.nextCursorId } : null;
  $("#load-more-experience-cards").hidden = !state.cardCursor;
  renderExperienceCards();
}

async function loadMoreExperienceCards() {
  if (!state.cardCursor || !api.connected()) return;
  const button = $("#load-more-experience-cards"); button.disabled = true;
  try {
    const page = await requestExperienceCardPage(state.cardCursor);
    const known = new Set(state.cards.map((card) => card.id));
    state.cards.push(...(page?.items || []).filter((card) => !known.has(card.id)));
    state.cardCursor = page?.nextCursorUpdatedAt && page?.nextCursorId
      ? { updatedAt: page.nextCursorUpdatedAt, id: page.nextCursorId } : null;
    button.hidden = !state.cardCursor;
    renderExperienceCards();
  } catch (error) { setStatus("#experience-status", error.message, true); }
  finally { button.disabled = false; }
}

function requestExperienceCardPage(cursor) {
  const query = new URLSearchParams({ limit: "50" });
  if (cursor) { query.set("cursorUpdatedAt", cursor.updatedAt); query.set("cursorId", cursor.id); }
  return api.request(`/experience-cards?${query.toString()}`);
}

function editExperienceCard(card) {
  state.editingCard = card; state.cardSource = null;
  $("#experience-title").value = card.latest?.title || "";
  $("#experience-content").value = card.latest?.content || "";
  $("#experience-type").value = card.latest?.type || "PROCEDURAL";
  $("#experience-applicability").value = card.applicability;
  $("#experience-customer").value = card.customerId || "";
  $("#experience-customer-field").hidden = card.applicability !== "CUSTOMER";
  $("#experience-expiry").value = card.latest?.expiresAt ? toLocalDateTime(card.latest.expiresAt) : "";
  $("#experience-source-note").textContent = `编辑卡片 ${card.id} · 来源绑定沿用当前修订。`;
  $("#save-experience-card").textContent = "保存新修订";
  $("#cancel-experience-edit").hidden = false;
  $("#experience-title").focus();
}

function toLocalDateTime(value) {
  const date = new Date(value); if (Number.isNaN(date.valueOf())) return "";
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}

function editExperienceFromFeedback(turn, feedback, draft) {
  resetExperienceEditor();
  state.cardSource = { sourceTaskId: turn.taskId, sourceFeedbackId: feedback.id, draftTaskId: draft?.id || null };
  $("#experience-title").value = draft?.result?.title || "";
  $("#experience-content").value = draft?.result?.content || feedback.correction;
  $("#experience-type").value = "PROCEDURAL";
  const customer = state.current?.mode === "CUSTOMER_ASSISTANT";
  $("#experience-applicability").value = customer ? "CUSTOMER" : "GENERAL";
  $("#experience-customer").value = customer ? state.current.customerId : "";
  $("#experience-customer-field").hidden = !customer;
  $("#experience-source-note").textContent = `来源反馈 ${feedback.id}${draft ? ` · 整理 Task ${draft.id}` : " · 可手工编辑"}；保存后仍是草稿。`;
  $("#save-experience-card").textContent = "保存为个人卡草稿";
  $("#cancel-experience-edit").hidden = false;
  $("#experience-title").focus();
}

async function saveExperienceCard(event) {
  event.preventDefault();
  if (!api.connected()) return setStatus("#experience-status", "请先连接身份。", true);
  const applicability = $("#experience-applicability").value;
  const expiry = $("#experience-expiry").value;
  const body = { title: $("#experience-title").value.trim(), content: $("#experience-content").value.trim(),
    type: $("#experience-type").value, applicability,
    customerId: applicability === "CUSTOMER" ? $("#experience-customer").value.trim() : null,
    expiresAt: expiry ? new Date(expiry).toISOString() : null,
    ...(state.cardSource || {}) };
  if (applicability === "CUSTOMER" && !body.customerId) return setStatus("#experience-status", "指定客户卡需要客户 ID。", true);
  const operation = state.editingCard ? "save" : "create";
  const signature = JSON.stringify({ operation, cardId: state.editingCard?.id, version: state.editingCard?.version, body });
  if (state.pendingCard && state.pendingCard.signature !== signature)
    return setStatus("#experience-status", "上一条卡片写入结果仍待核对；请恢复原内容并使用相同请求键重试。", true);
  const pending = state.pendingCard || { signature, key: api.key() };
  state.pendingCard = pending;
  $("#save-experience-card").disabled = true;
  try {
    const path = state.editingCard ? `/experience-cards/${encodeURIComponent(state.editingCard.id)}/versions` : "/experience-cards";
    if (state.editingCard) body.expectedVersion = state.editingCard.version;
    const receipt = await api.request(path, { method: "POST", headers: { "Idempotency-Key": pending.key }, body: JSON.stringify(body) });
    state.pendingCard = null;
    setStatus("#experience-status", `草稿已保存 · 修订 ${receipt.revision}。启用前可继续修改。`);
    resetExperienceEditor(); await refreshExperienceCards();
  } catch (error) { setStatus("#experience-status", `保存状态可能未知；用相同内容重试，或刷新卡片列表核对：${error.message}`, true); }
  finally { $("#save-experience-card").disabled = false; }
}

async function experienceCardAction(card, action) {
  const version = card.version; const revision = card.latestRevision;
  const signature = `${card.id}:${action}:${version}:${revision}`;
  const key = state.cardActionKeys.get(signature) || api.key(); state.cardActionKeys.set(signature, key);
  try {
    const path = action === "publish"
      ? `/experience-cards/${encodeURIComponent(card.id)}/versions/${revision}/publish`
      : `/experience-cards/${encodeURIComponent(card.id)}/revoke`;
    const body = action === "publish" ? { expectedVersion: version } : { expectedVersion: version, expectedActiveRevision: card.activeRevision };
    const receipt = await api.request(path, { method: "POST", headers: { "Idempotency-Key": key }, body: JSON.stringify(body) });
    state.cardActionKeys.delete(signature);
    setStatus("#experience-status", action === "publish" ? `已由本人启用修订 ${receipt.revision}。` : "个人经验已撤回。");
    await refreshExperienceCards();
  } catch (error) { setStatus("#experience-status", `操作结果可能未知；刷新确认状态后仍使用此操作键：${error.message}`, true); }
}

function publishExperienceCard(card) { return experienceCardAction(card, "publish"); }
function revokeExperienceCard(card) { return experienceCardAction(card, "revoke"); }

async function submitExperienceFeedback(turn, correction, evidence, button) {
  const body = { correction: correction.trim(), evidence: evidence.trim() };
  const signature = JSON.stringify(body); const prior = state.feedbackKeys.get(turn.taskId);
  if (prior && prior.signature !== signature) { $("#notice").textContent = "上一条反馈结果仍待核对；请使用相同纠正与依据重试。"; return; }
  const pending = prior || { signature, key: api.key() }; state.feedbackKeys.set(turn.taskId, pending);
  button.disabled = true;
  try {
    const feedback = await api.request(taskIdPath(turn.taskId, "/feedback"), {
      method: "POST", headers: { "Idempotency-Key": pending.key }, body: JSON.stringify(body)
    });
    state.feedbackKeys.delete(turn.taskId);
    const items = state.feedbacks.get(turn.taskId) || [];
    state.feedbacks.set(turn.taskId, [...items.filter((item) => item.id !== feedback.id), feedback]);
    $("#notice").textContent = "反馈已保存。你可以手工编辑个人卡，或点击一次整理生成可编辑建议。";
    await renderTurns(false);
  } catch (error) { $("#notice").textContent = `反馈结果可能未知；用相同内容重试：${error.message}`; button.disabled = false; }
}

async function createExperienceDraft(turn, feedback, draftText, button) {
  const signature = JSON.stringify({ feedbackId: feedback.id, draftText: draftText.trim() });
  const prior = state.draftKeys.get(feedback.id);
  if (prior && prior.signature !== signature) { $("#notice").textContent = "上一条整理请求结果仍待核对；请使用相同草稿提示重试。"; return; }
  const pending = prior || { signature, key: api.key() }; state.draftKeys.set(feedback.id, pending);
  button.disabled = true;
  try {
    const task = await api.request(taskIdPath(turn.taskId, "/experience-drafts"), {
      method: "POST", headers: { "Idempotency-Key": pending.key },
      body: JSON.stringify({ feedbackId: feedback.id, draftText: draftText.trim() || null })
    });
    state.draftKeys.delete(feedback.id);
    state.experienceDrafts.set(feedback.id, task);
    $("#notice").textContent = `整理 Task ${task.id} · ${task.status}。整理结果需要你编辑并保存为卡片草稿。`;
    await renderTurns(false); schedulePoll();
  } catch (error) { $("#notice").textContent = `整理请求状态可能未知；用相同提示重试：${error.message}`; button.disabled = false; }
}

async function refreshExperienceDrafts() {
  const active = [...state.experienceDrafts.entries()].filter(([, task]) => task?.id && isActive(task.status));
  if (!active.length) return false;
  let changed = false;
  const completed = [];
  await Promise.all(active.map(async ([feedbackId, prior]) => {
    try {
      const next = await api.request(taskIdPath(prior.id));
      if (next.status !== prior.status || next.result !== prior.result) changed = true;
      if (next.status !== prior.status && !isActive(next.status)) completed.push(next);
      state.experienceDrafts.set(feedbackId, next);
    } catch (error) { if (error.status !== 404) console.warn("experience draft status unavailable"); }
  }));
  if (completed.length) {
    const task = completed[0];
    const nextStep = task.status === "SUCCEEDED"
      ? "整理结果需要你编辑并保存为卡片草稿。" : "保留原反馈后可使用新请求键重新整理。";
    $("#notice").textContent = `整理 Task ${task.id} · ${task.status}。${nextStep}`;
  }
  if (changed) await renderTurns(false);
  return changed;
}

async function submitFollowup(turn, summary, button) {
  if (!state.current || turn.briefRevision !== state.current.currentBriefRevision || isActive(state.current.activeTaskStatus)) return;
  button.disabled = true;
  const pendingId = `${turn.taskId}\u001f${summary}`;
  const sameTaskPending = [...state.pendingFollowups.keys()].find((key) => key.startsWith(`${turn.taskId}\u001f`));
  if (sameTaskPending && sameTaskPending !== pendingId) {
    $("#notice").textContent = "此分析已有跟进请求待核对；请用原摘要重试，避免创建重复流程。";
    button.disabled = false;
    return;
  }
  const idempotencyKey = state.pendingFollowups.get(pendingId) || api.key();
  state.pendingFollowups.set(pendingId, idempotencyKey);
  try {
    const task = await api.request(taskIdPath(turn.taskId));
    const response = await api.request(workspacePath(`/${encodeURIComponent(state.current.id)}/followups`), {
      method: "POST", headers: { "Idempotency-Key": idempotencyKey },
      body: JSON.stringify({ sourceTaskId: turn.taskId, expectedTaskVersion: task.version,
        expectedBriefRevision: turn.briefRevision, summary })
    });
    state.pendingFollowups.delete(pendingId);
    $("#notice").textContent = `已提交独立审批流程 ${response.instanceId} · ${response.status}。`;
    await loadSideData(state.current.id, state.generation);
  } catch (error) {
    $("#notice").textContent = `跟进状态可能未知；可用相同摘要和请求键重试：${error.message}`;
    button.disabled = false;
  }
}

async function cancelTurn() {
  const current = state.current;
  if (!current?.activeTaskId) return;
  try {
    const task = await api.request(taskIdPath(current.activeTaskId));
    await api.request(taskIdPath(current.activeTaskId, "/cancel"), { method: "POST", body: JSON.stringify({ expectedVersion: task.version }) });
    $("#notice").textContent = "取消请求已提交。";
    await loadConversation(current.id, state.generation);
    schedulePoll();
  } catch (error) { $("#notice").textContent = `取消失败：${error.message}`; }
}

async function updateSession(patch) {
  if (!state.current) return;
  try {
    const changed = await api.request(workspacePath(`/${encodeURIComponent(state.current.id)}`), {
      method: "PATCH", body: JSON.stringify({ expectedVersion: state.current.version, ...patch })
    });
    state.current = changed;
    if (changed.status !== state.statusFilter) {
      state.statusFilter = changed.status;
      document.querySelectorAll(".filter").forEach((button) => button.classList.toggle("active", button.dataset.sessionStatus === state.statusFilter));
    }
    await refreshSessions(); await loadConversation(changed.id, state.generation);
  } catch (error) { $("#notice").textContent = `会话更新失败：${error.message}`; }
}

async function renameSession() {
  if (!state.current) return;
  const title = window.prompt("会话标题", state.current.title);
  if (title === null || !title.trim()) return;
  await updateSession({ title: title.trim() });
}

async function confirmWorkflowStatus() {
  if (state.selectedId) await loadSideData(state.selectedId, state.generation);
}

function bindEvents() {
  $("#identity-form").addEventListener("submit", connect);
  $("#experience-card-form").addEventListener("submit", saveExperienceCard);
  $("#new-experience-card").addEventListener("click", resetExperienceEditor);
  $("#cancel-experience-edit").addEventListener("click", resetExperienceEditor);
  $("#experience-applicability").addEventListener("change", () => {
    $("#experience-customer-field").hidden = $("#experience-applicability").value !== "CUSTOMER";
  });
  $("#refresh-experience-cards").addEventListener("click", async () => {
    try { await refreshExperienceCards(); setStatus("#experience-status", "卡片列表已刷新。"); }
    catch (error) { setStatus("#experience-status", error.message, true); }
  });
  $("#load-more-experience-cards").addEventListener("click", loadMoreExperienceCards);
  $("#new-session").addEventListener("submit", createSession);
  $("#session-mode").addEventListener("change", updateModeFields);
  $("#refresh-sessions").addEventListener("click", refreshSessions);
  $("#composer").addEventListener("submit", sendMessage);
  $("#save-brief").addEventListener("click", saveOnly);
  $("#save-and-analyze").addEventListener("click", saveAndAnalyze);
  $("#cancel-turn").addEventListener("click", cancelTurn);
  $("#rename-session").addEventListener("click", renameSession);
  $("#archive-session").addEventListener("click", () => {
    if (!state.current) return;
    const status = state.current.status === "ARCHIVED" ? "ACTIVE" : "ARCHIVED";
    void updateSession({ status });
  });
  $("#refresh-comparison").addEventListener("click", confirmWorkflowStatus);
  $("#refresh-side-data").addEventListener("click", confirmWorkflowStatus);
  $("#refresh-conversation").addEventListener("click", refreshSelected);
  document.querySelectorAll(".filter").forEach((button) => button.addEventListener("click", async () => {
    state.statusFilter = button.dataset.sessionStatus;
    document.querySelectorAll(".filter").forEach((item) => item.classList.toggle("active", item === button));
    try { await refreshSessions(); } catch (error) { $("#notice").textContent = error.message; }
  }));
  document.addEventListener("visibilitychange", () => document.hidden ? stopPolling() : schedulePoll());
  window.addEventListener("pagehide", stopPolling);
}

bindEvents();
updateModeFields();
clearPrivateData({ preserveSelectionUrl: true });
