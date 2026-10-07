const $ = (selector) => document.querySelector(selector);
const workspace = () => $("#workspace-id").value.trim();
const state = { token: "", generation: 0, cards: [], selectedId: null, detail: null,
  cursor: null, sourceByKey: new Map(), pendingCreate: null, pendingWrites: new Map(), syncByResult: new Map() };
const terminal = new Set(["SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"]);

function el(tag, className = "", text = null) {
  const item = document.createElement(tag);
  if (className) item.className = className;
  if (text !== null && text !== undefined) item.textContent = String(text);
  return item;
}
function message(selector, text, error = false) {
  const item = $(selector); if (!item) return;
  item.textContent = text || ""; item.classList.toggle("error-text", error);
}
function key() { return crypto.randomUUID(); }
async function request(path, options = {}) {
  if (!state.token) throw new Error("请先连接本地身份。");
  const headers = new Headers(options.headers || {});
  headers.set("Authorization", `Bearer ${state.token}`);
  if (options.body !== undefined && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  const response = await fetch(`/api/v1/workspaces/${encodeURIComponent(workspace())}${path}`, { ...options, headers });
  const raw = await response.text(); let data = null;
  if (raw) { try { data = JSON.parse(raw); } catch { data = { message: raw }; } }
  if (!response.ok) throw new Error([response.status, data?.code || data?.errorCode, data?.message || data?.detail || response.statusText].filter(Boolean).join(" · "));
  return data;
}
function date(value) { return value ? new Date(value).toLocaleString() : "未设置"; }
function localToIso(value) { return value ? new Date(value).toISOString() : null; }
function stateLabel(status) { return ({ OPEN: "待处理", IN_PROGRESS: "跟进中", CLOSED: "已结束" })[status] || status || "未知"; }
function isOverdue(card) { return card.dueAt && Date.parse(card.dueAt) < Date.now() && card.businessStatus !== "CLOSED"; }

async function connect(event) {
  event.preventDefault();
  const token = $("#token").value.trim();
  if (!token || !workspace()) { message("#identity-status", "需要令牌和 Workspace ID。", true); return; }
  state.token = token; state.generation += 1; $("#token").value = "";
  document.body.classList.remove("disconnected");
  message("#identity-status", "身份已连接；令牌只保存在此页面内存。正在加载客户分析与跟进…");
  await Promise.allSettled([loadSources(), loadCards(true), loadApprovals()]);
  message("#identity-status", "已连接。刷新页面后需重新认证；服务端会逐次检查客户权限。");
}

async function loadSources() {
  const select = $("#source-task");
  select.replaceChildren(new Option("读取本人客户分析…", ""));
  state.sourceByKey.clear();
  try {
    const pages = await Promise.all(["ACTIVE", "ARCHIVED"].map((status) =>
      request(`/conversations?status=${status}&limit=50`).catch(() => ({ items: [] }))));
    const conversations = pages.flatMap((page) => page.items || []).filter((item) =>
      item.mode === "CUSTOMER_ASSISTANT" && item.capabilityVersion === "1.2.0");
    const candidates = [];
    for (const conversation of conversations) {
      const turns = await request(`/conversations/${encodeURIComponent(conversation.id)}/turns?limit=50`).catch(() => ({ items: [] }));
      for (const turn of turns.items || []) {
        if (turn.status !== "SUCCEEDED" || !turn.result?.followupDraft?.summary || turn.result?.clarificationQuestion) continue;
        const keyValue = `${conversation.id}|${turn.taskId}`;
        state.sourceByKey.set(keyValue, { conversation, turn }); candidates.push({ keyValue, conversation, turn });
      }
    }
    select.replaceChildren(new Option(candidates.length ? "选择本人成功分析" : "暂无可创建跟进的成功分析", ""));
    for (const item of candidates) select.add(new Option(
      `${item.conversation.customerId} · 第 ${item.turn.turnNo} 轮 · ${date(item.turn.createdAt)}`,
      item.keyValue));
    select.disabled = candidates.length === 0;
    $("#create-form button[type=submit]").disabled = candidates.length === 0;
    message("#create-status", candidates.length ? `${candidates.length} 条本人客户分析可选。` : "没有符合条件的本人成功分析。", !candidates.length);
  } catch (error) { message("#create-status", `读取本人会话失败：${error.message}`, true); }
}

async function chooseSource() {
  const source = state.sourceByKey.get($("#source-task").value);
  $("#assignee").replaceChildren(new Option("由我负责", ""));
  if (!source) { $("#create-summary").value = ""; return; }
  $("#create-summary").value = source.turn.result.followupDraft.summary;
  const customer = encodeURIComponent(source.conversation.customerId);
  try {
    const assignees = await request(`/customer-followup-assignees?customerId=${customer}&limit=50`);
    for (const person of assignees || []) $("#assignee").add(new Option(person.displayName || person.id, person.id));
  } catch (error) { message("#create-status", `来源可用，但未能读取负责人名单：${error.message}`, true); }
}

function queryForCards(cursor = null) {
  const query = new URLSearchParams({ scope: $("#scope").value, limit: "20" });
  const status = $("#status-filter").value; if (status) query.set("status", status);
  if (cursor) { query.set("cursorUpdatedAt", cursor.updatedAt); query.set("cursorId", cursor.id); }
  return `/customer-followups?${query.toString()}`;
}
async function loadCards(reset = false) {
  if (!state.token) return;
  try {
    const page = await request(queryForCards(reset ? null : state.cursor));
    const items = page.items || [];
    if (reset) state.cards = items;
    else {
      const ids = new Set(state.cards.map((card) => card.id)); state.cards.push(...items.filter((card) => !ids.has(card.id)));
    }
    state.cursor = page.nextUpdatedAt && page.nextId ? { updatedAt: page.nextUpdatedAt, id: page.nextId } : null;
    $("#load-more").hidden = !state.cursor; renderCards();
    if (state.selectedId && reset) await loadDetail(state.selectedId);
  } catch (error) { $("#card-list").replaceChildren(el("p", "empty", `读取待办失败：${error.message}`)); }
}
function renderCards() {
  const list = $("#card-list"); list.replaceChildren();
  $("#queue-count").textContent = `${state.cards.length} 条`;
  if (!state.cards.length) { list.append(el("p", "empty", "当前筛选下没有可见跟进。")); return; }
  for (const card of state.cards) {
    const button = el("button", `queue-card${card.id === state.selectedId ? " active" : ""}`); button.type = "button";
    const top = el("div", "queue-card-top"); top.append(el("span", "state-pill" + (card.businessStatus === "CLOSED" ? " closed" : ""), stateLabel(card.businessStatus)));
    if (isOverdue(card)) top.append(el("span", "state-pill overdue", "逾期"));
    button.append(top, el("h3", "", card.customerId), el("p", "", card.summary));
    button.append(el("small", "", `负责人 ${card.assigneeId} · 结果 ${card.lastResultNo || 0} 条 · 更新 ${date(card.updatedAt)}`));
    button.addEventListener("click", () => loadDetail(card.id)); list.append(button);
  }
}

async function loadDetail(id) {
  if (!state.token) return;
  state.selectedId = id;
  try {
    const detail = await request(`/customer-followups/${encodeURIComponent(id)}`);
    state.detail = detail; renderCards(); await renderDetail(detail);
  } catch (error) { $("#detail").replaceChildren(el("p", "empty", `无法读取此跟进：${error.message}`)); }
}

async function renderDetail(detail) {
  const target = $("#detail"); target.replaceChildren();
  const card = detail.card; const head = el("div", "detail-head"); const title = document.createElement("div");
  title.append(el("p", "overline", `CUSTOMER ${card.customerId}`), el("h2", "", card.summary),
    el("p", "", `创建人 ${card.creatorId} · 负责人 ${card.assigneeId} · 版本 ${card.rowVersion}`));
  head.append(title, el("span", "state-pill" + (card.businessStatus === "CLOSED" ? " closed" : ""), stateLabel(card.businessStatus))); target.append(head);
  const grid = el("div", "detail-grid");
  for (const [label, value] of [["计划日期", date(card.dueAt)], ["来源会话", card.sourceConversationId],
    ["来源分析", card.sourceTaskId], ["来源简报修订", card.sourceBriefRevision], ["原创建 Workflow", card.creationWorkflowId || "未关联"],
    ["结果条数", card.lastResultNo]]) {
    const cell = el("div", "detail-cell"); cell.append(el("span", "", label), el("b", "", value ?? "—")); grid.append(cell);
  }
  target.append(grid, el("p", "shared-summary", `团队共享摘要\n${card.summary}`));
  const actions = el("div", "detail-actions");
  if (detail.allowedActions?.includes("UPDATE")) {
    const assign = el("button", "button button-light", "调整负责人 / 日期"); assign.type = "button";
    assign.addEventListener("click", () => showUpdateForm(target, detail)); actions.append(assign);
  }
  target.append(actions);
  const sourceLabel = el("p", "overline", "RESULT TIMELINE · IMMUTABLE BUSINESS RECORDS"); sourceLabel.style.marginTop = "17px"; target.append(sourceLabel);
  const timeline = el("div", "result-timeline");
  const resultRows = [...(detail.recentResults || [])].sort((a, b) => b.resultNo - a.resultNo);
  for (const result of resultRows) timeline.append(await renderResult(card, result, detail.allowedActions || []));
  if (!resultRows.length) timeline.append(el("p", "empty", "还没有处理结果。保存本地记录不会触发 CRM 或模型。"));
  target.append(timeline);
  if (detail.allowedActions?.includes("SAVE_RESULT")) target.append(makeResultForm(card));
}

async function renderResult(card, result, actions) {
  const box = el("article", "result-card");
  const head = el("div", "result-card-head");
  head.append(el("span", "", `#${result.resultNo} · ${result.outcomeCode} · ${result.disposition}`));
  head.append(el("span", "state-pill", result.syncStatus || "NOT_REQUESTED")); box.append(head);
  box.append(el("p", "", result.summary));
  if (result.nextAction) box.append(el("p", "", `下一步：${result.nextAction}`));
  box.append(el("small", "", `记录人 ${result.recordedBy} · ${date(result.createdAt)}${result.correctsResultId ? ` · 更正 ${result.correctsResultId}` : ""}`));
  if (actions.includes("SYNC_RESULT")) {
    const controls = el("div", "detail-actions");
    const syncState = state.syncByResult.get(result.id);
    const executionStatus = syncState?.execution?.status;
    if (["NOT_REQUESTED", "FAILED_SAFE"].includes(result.syncStatus)) {
      const submit = el("button", "button button-quiet", "申请同步到模拟 CRM"); submit.type = "button";
      submit.addEventListener("click", () => startSync(card.id, result.id)); controls.append(submit);
    }
    if (["PENDING", "UNKNOWN", "FAILED_SAFE"].includes(result.syncStatus) || syncState) {
      const check = el("button", "button button-light", "查询同步状态"); check.type = "button";
      check.addEventListener("click", () => refreshSync(card.id, result.id)); controls.append(check);
    }
    if (["UNKNOWN", "FAILED_SAFE"].includes(result.syncStatus) || ["UNKNOWN", "VERIFICATION_FAILED"].includes(executionStatus)) {
      const verify = el("button", "button button-light", "按原操作核验"); verify.type = "button";
      verify.addEventListener("click", () => verifySync(card.id, result.id)); controls.append(verify);
    }
    box.append(controls);
    if (syncState) box.append(el("small", "", `同步尝试 ${syncState.attempt?.attemptNo} · Workflow ${syncState.workflowStatus || "排队中"} · 执行 ${executionStatus || "尚无执行记录"}`));
  }
  return box;
}

function showUpdateForm(target, detail) {
  const prior = target.querySelector(".update-form"); if (prior) return prior.remove();
  const form = el("form", "result-form update-form");
  const heading = el("h3", "", "调整分派和计划");
  const owner = document.createElement("select"); owner.setAttribute("aria-label", "负责人");
  const due = document.createElement("input"); due.type = "datetime-local"; due.setAttribute("aria-label", "计划日期");
  const assigneeLabel = el("label", "", "负责人"); assigneeLabel.append(owner);
  const dueLabel = el("label", "", "计划日期"); dueLabel.append(due);
  const button = el("button", "button button-dark full", "保存分派"); button.type = "submit";
  form.append(heading, assigneeLabel, dueLabel, button);
  request(`/customer-followup-assignees?customerId=${encodeURIComponent(detail.card.customerId)}&limit=50`)
    .then((people) => { for (const person of people || []) owner.add(new Option(person.displayName || person.id, person.id)); owner.value = detail.card.assigneeId; })
    .catch((error) => form.prepend(el("p", "empty full", error.message)));
  due.value = detail.card.dueAt ? new Date(Date.parse(detail.card.dueAt) - new Date().getTimezoneOffset() * 60_000).toISOString().slice(0, 16) : "";
  form.addEventListener("submit", async (event) => {
    event.preventDefault(); button.disabled = true;
    try {
      await request(`/customer-followups/${encodeURIComponent(detail.card.id)}`, { method: "PATCH",
        body: JSON.stringify({ expectedVersion: detail.card.rowVersion, assigneeId: owner.value || null, dueAt: localToIso(due.value) }) });
      await loadCards(true);
    } catch (error) { button.disabled = false; form.prepend(el("p", "empty", `分派未完成：${error.message}`)); }
  });
  target.querySelector(".detail-actions")?.after(form) || target.append(form);
  return form;
}

function makeResultForm(card) {
  const form = el("form", "result-form"); form.append(el("h3", "", "登记处理结果 · 保存即追加一条不可变记录"));
  const outcome = document.createElement("select"); outcome.setAttribute("aria-label", "结果类型");
  for (const [value, label] of [["CONTACTED", "已联系"], ["NO_RESPONSE", "未联系上"], ["RESOLVED", "问题已处理"], ["OTHER", "其他"]]) outcome.add(new Option(label, value));
  const disposition = document.createElement("select"); disposition.setAttribute("aria-label", "处理后状态");
  disposition.add(new Option("继续跟进", "CONTINUE"));
  disposition.add(new Option("结束跟进", "CLOSE"));
  const summary = document.createElement("textarea"); summary.required = true; summary.maxLength = 2000; summary.placeholder = "本次实际处理情况";
  const next = document.createElement("textarea"); next.maxLength = 500; next.placeholder = "下一步（可选）";
  const contact = document.createElement("input"); contact.type = "datetime-local";
  const field = (label, input, full = false) => { const wrap = el("label", full ? "full" : ""); wrap.append(el("span", "", label), input); return wrap; };
  const button = el("button", "button button-dark full", "保存本地结果"); button.type = "submit";
  form.append(field("结果类型", outcome), field("后续状态", disposition), field("处理摘要", summary, true),
    field("下一步", next, true), field("下次联系时间", contact), button);
  form.addEventListener("submit", async (event) => {
    event.preventDefault(); if (!summary.value.trim()) return;
    button.disabled = true;
    const payload = { expectedVersion: state.detail?.card.rowVersion ?? card.rowVersion,
      outcomeCode: outcome.value, summary: summary.value.trim(), nextAction: next.value.trim() || null,
      nextContactAt: localToIso(contact.value), disposition: disposition.value, correctsResultId: null };
    const signature = `${card.id}:result:${JSON.stringify(payload)}`;
    const pending = state.pendingWrites.get(signature) || { key: key() }; state.pendingWrites.set(signature, pending);
    try {
      await request(`/customer-followups/${encodeURIComponent(card.id)}/results`, { method: "POST",
        headers: { "Idempotency-Key": pending.key }, body: JSON.stringify(payload) });
      state.pendingWrites.delete(signature); await loadDetail(card.id); await loadCards(true);
    } catch (error) { button.disabled = false; form.prepend(el("p", "empty full", `保存状态可能未知；保留内容并用同一请求键重试：${error.message}`)); }
  });
  return form;
}

async function startSync(followupId, resultId) {
  const signature = `${followupId}:${resultId}:sync`;
  const pending = state.pendingWrites.get(signature) || { key: key() }; state.pendingWrites.set(signature, pending);
  try {
    const projection = await request(`/customer-followups/${encodeURIComponent(followupId)}/results/${encodeURIComponent(resultId)}/sync`,
      { method: "POST", headers: { "Idempotency-Key": pending.key }, body: "{}" });
    state.pendingWrites.delete(signature); state.syncByResult.set(resultId, projection); await loadDetail(followupId);
  } catch (error) { message("#identity-status", `同步申请状态待核对；请查询结果状态后用相同操作恢复：${error.message}`, true); }
}
async function refreshSync(followupId, resultId) {
  try {
    const projection = await request(`/customer-followups/${encodeURIComponent(followupId)}/results/${encodeURIComponent(resultId)}/sync`);
    state.syncByResult.set(resultId, projection); await loadDetail(followupId);
  } catch (error) { message("#identity-status", `同步状态查询失败：${error.message}`, true); }
}
async function verifySync(followupId, resultId) {
  const signature = `${followupId}:${resultId}:verify`;
  const pending = state.pendingWrites.get(signature) || { key: key() }; state.pendingWrites.set(signature, pending);
  try {
    const projection = await request(`/customer-followups/${encodeURIComponent(followupId)}/results/${encodeURIComponent(resultId)}/verify`,
      { method: "POST", headers: { "Idempotency-Key": pending.key }, body: JSON.stringify({ reason: "负责人请求按原 operationId 核验结果写入" }) });
    state.pendingWrites.delete(signature); state.syncByResult.set(resultId, projection); await loadDetail(followupId);
  } catch (error) { message("#identity-status", `核验未完成；未知写入不会换 operationId 自动重试：${error.message}`, true); }
}

async function createFollowup(event) {
  event.preventDefault();
  const source = state.sourceByKey.get($("#source-task").value); if (!source) return;
  const summary = $("#create-summary").value.trim(); if (!summary) return;
  const task = await request(`/tasks/${encodeURIComponent(source.turn.taskId)}`);
  const body = { conversationId: source.conversation.id, sourceTaskId: source.turn.taskId,
    expectedTaskVersion: task.version, expectedBriefRevision: source.turn.briefRevision, summary,
    assigneeId: $("#assignee").value || null, dueAt: localToIso($("#due-at").value) };
  const signature = JSON.stringify(body);
  if (state.pendingCreate && state.pendingCreate.signature !== signature) {
    message("#create-status", "上一条创建结果仍待核对；请恢复原字段并使用原请求键重试。", true); return;
  }
  const pending = state.pendingCreate || { signature, key: key() }; state.pendingCreate = pending;
  const button = $("#create-form button[type=submit]"); button.disabled = true;
  try {
    const created = await request("/customer-followups", { method: "POST",
      headers: { "Idempotency-Key": pending.key }, body: JSON.stringify(body) });
    state.pendingCreate = null; $("#create-form").reset();
    message("#create-status", `团队跟进已建立 · 创建 Workflow ${created.creationWorkflow?.instanceId || created.card?.creationWorkflowId || "已关联"}。`);
    await Promise.all([loadCards(true), loadSources()]);
    if (created.card?.id) await loadDetail(created.card.id);
  } catch (error) { message("#create-status", `创建状态可能未知；保留原内容和请求键后重试：${error.message}`, true); }
  finally { button.disabled = false; }
}

async function loadApprovals() {
  const target = $("#approval-list"); target.replaceChildren();
  try {
    const page = await request("/approvals?limit=20");
    const pending = (page.items || []).filter((item) => item.state === "PENDING");
    if (!pending.length) { target.append(el("p", "empty", "当前身份没有待处理审批。")); return; }
    for (const item of pending) {
      const box = el("article", "approval-item"); box.append(el("p", "", `审批 ${item.id} · ${date(item.createdAt)}`));
      const open = el("button", "button button-light", "读取审批"); open.type = "button";
      open.addEventListener("click", async () => {
        try {
          const approval = await request(`/approvals/${encodeURIComponent(item.id)}`);
          box.replaceChildren(el("p", "", `审批 ${approval.state} · 版本 ${approval.version} · ${approval.id}`));
          const rawPreview = approval.binding?.previewJson;
          let preview = null;
          try { preview = typeof rawPreview === "string" ? JSON.parse(rawPreview) : rawPreview; } catch { /* 保留原始预览文本。 */ }
          const previewPanel = el("div", "approval-preview");
          previewPanel.append(el("p", "", "执行预览（绑定到此审批）"));
          const previewFields = [["工具", preview?.tool], ["客户", preview?.customerId],
            ["目标 CRM 跟进", preview?.externalId], ["结果序号", preview?.resultNo],
            ["结果类型", preview?.outcomeCode], ["处理摘要", preview?.summary],
            ["下一步", preview?.nextAction], ["后续状态", preview?.disposition],
            ["原操作编号", preview?.operationId]];
          for (const [label, value] of previewFields) if (value !== null && value !== undefined && value !== "")
            previewPanel.append(el("p", "", `${label}：${value}`));
          if (!previewFields.some(([, value]) => value !== null && value !== undefined && value !== "") && rawPreview)
            previewPanel.append(el("p", "", String(rawPreview)));
          box.append(previewPanel);
          if (approval.state === "PENDING") for (const [decision, label] of [["APPROVED", "批准"], ["REJECTED", "拒绝"]]) {
            const button = el("button", "button button-quiet", label); button.type = "button";
            button.addEventListener("click", async () => {
              button.disabled = true;
              try { await request(`/approvals/${encodeURIComponent(item.id)}/decisions`, { method: "POST",
                headers: { "Idempotency-Key": key() }, body: JSON.stringify({ decision, expectedVersion: approval.version }) });
                await loadApprovals(); if (state.selectedId) await loadDetail(state.selectedId);
              } catch (error) { button.disabled = false; box.append(el("p", "empty", `审批未完成：${error.message}`)); }
            }); box.append(button);
          }
        } catch (error) { box.append(el("p", "empty", `无权读取或决定此审批：${error.message}`)); }
      }); box.append(open); target.append(box);
    }
  } catch (error) { target.append(el("p", "empty", `无法读取审批队列：${error.message}`)); }
}

$("#identity-form").addEventListener("submit", connect);
$("#refresh").addEventListener("click", () => void loadCards(true));
$("#scope").addEventListener("change", () => void loadCards(true));
$("#status-filter").addEventListener("change", () => void loadCards(true));
$("#load-more").addEventListener("click", () => void loadCards(false));
$("#source-task").addEventListener("change", () => void chooseSource());
$("#create-form").addEventListener("submit", createFollowup);
