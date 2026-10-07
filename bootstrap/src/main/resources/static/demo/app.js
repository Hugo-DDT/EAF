const $ = (selector) => document.querySelector(selector);
const state = { token: "", workspaceId: "", controller: null, selectedTask: null, nextTaskCursor: null,
  pendingFollowup: null, pollingWorkflow: null };
const TERMINAL = new Set(["SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"]);
const FOLLOWUP_AGENT = "20000000-0000-4000-8000-00000000000a";
const QA_AGENT = "20000000-0000-4000-8000-000000000009";
const FOLLOWUP_WORKFLOW = "58000000-0000-4000-8000-000000000009";

function node(tag, className, text) {
  const value = document.createElement(tag);
  if (className) value.className = className;
  if (text !== undefined && text !== null) value.textContent = String(text);
  return value;
}

function status(selector, message, error = false) {
  const target = $(selector);
  target.textContent = message || "";
  target.classList.toggle("error-text", error);
}

function workspacePath(path) {
  return `/api/v1/workspaces/${encodeURIComponent(state.workspaceId)}${path}`;
}

async function api(path, options = {}) {
  if (!state.token) throw new Error("请先连接本地身份。");
  const headers = new Headers(options.headers || {});
  headers.set("Authorization", `Bearer ${state.token}`);
  if (options.body !== undefined && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  const response = await fetch(workspacePath(path), {
    ...options,
    headers,
    signal: state.controller?.signal
  });
  const text = await response.text();
  let data = null;
  if (text) {
    try { data = JSON.parse(text); } catch { data = { message: text }; }
  }
  if (!response.ok) {
    const code = data?.code || data?.errorCode;
    const detail = data?.message || data?.detail || response.statusText;
    throw new Error([response.status, code, detail].filter(Boolean).join(" · "));
  }
  return data;
}

function key() {
  return crypto.randomUUID();
}

function setConnected(value) {
  document.body.classList.toggle("connected", value);
}

function clearWorkspace() {
  state.selectedTask = null;
  state.nextTaskCursor = null;
  state.pendingFollowup = null;
  state.pollingWorkflow = null;
  $("#document-list").replaceChildren(node("p", "empty-state", "连接身份后加载文档。"));
  $("#search-results").replaceChildren(node("p", "empty-state", "检索结果会显示标题路径、来源和命中通道。"));
  $("#task-list").replaceChildren(node("p", "empty-state", "连接身份后加载任务。"));
  $("#task-detail").replaceChildren(node("p", "empty-state", "选择一个任务查看可见结果与跟进进度。"));
  $("#qa-answer").textContent = "提交问题后，回答会出现在这里。";
  $("#qa-answer").className = "answer-copy empty-state";
  $("#qa-missing").replaceChildren();
  $("#qa-sources").replaceChildren(node("p", "empty-state", "尚无引用。"));
  $("#followup-result").textContent = "分析结果和可编辑草稿会显示在这里。";
  $("#followup-result").className = "empty-state";
  $("#draft-summary").value = "";
  $("#draft-summary").disabled = true;
  $("#confirm-followup").disabled = true;
  $("#followup-sources").replaceChildren(node("p", "empty-state", "客户分析引用会显示在这里。"));
  $("#approval-actions").replaceChildren();
  $("#approval-actions").append(createApprovalLookup());
  status("#qa-status", "");
  status("#followup-status", "");
  status("#workflow-status", "");
  status("#document-status", "");
}

async function connect(event) {
  event.preventDefault();
  state.controller?.abort();
  state.controller = new AbortController();
  state.token = $("#token-input").value.trim();
  state.workspaceId = $("#workspace-id").value.trim();
  $("#token-input").value = "";
  clearWorkspace();
  if (!state.token || !state.workspaceId) {
    state.token = "";
    setConnected(false);
    status("#connection-status", "需要令牌和 Workspace ID。", true);
    return;
  }
  setConnected(true);
  status("#connection-status", "身份已连接。正在读取当前授权范围…");
  await Promise.allSettled([refreshDocuments(), refreshTasks(), refreshPendingApprovals()]);
  status("#connection-status", "已连接；令牌仅保存在当前页面内存中。");
}

function navigate(viewName) {
  document.querySelectorAll(".nav-button").forEach((button) => {
    const active = button.dataset.view === viewName;
    button.classList.toggle("active", active);
    button.setAttribute("aria-current", active ? "page" : "false");
  });
  document.querySelectorAll(".view").forEach((view) => { view.hidden = view.id !== `view-${viewName}`; });
  if (!state.token) return;
  if (viewName === "knowledge") refreshDocuments();
  if (viewName === "history") refreshTasks();
  if (viewName === "followup") refreshPendingApprovals();
}

function renderDocuments(page) {
  const list = $("#document-list");
  list.replaceChildren();
  if (!page?.items?.length) {
    list.append(node("p", "empty-state", "当前身份没有可读取的文档。"));
    return;
  }
  for (const item of page.items) {
    const row = node("article", "document-row");
    const content = node("div");
    content.append(node("h4", "", item.title));
    content.append(node("p", "", `版本 ${item.version ?? "未记录"} · ${item.status || "未知状态"}`));
    const open = node("button", "button button-quiet", "读取正文");
    open.type = "button";
    open.addEventListener("click", async () => {
      try {
        const doc = await api(`/knowledge/documents/${encodeURIComponent(item.id)}`);
        const dialog = document.createElement("dialog");
        dialog.className = "document-dialog";
        const title = node("h3", "", doc.title);
        const meta = node("p", "muted", `版本 ${doc.version} · ${doc.status}`);
        const body = node("pre", "document-body", doc.content);
        const close = node("button", "button button-dark", "关闭");
        close.addEventListener("click", () => dialog.close());
        dialog.append(title, meta, body, close);
        document.body.append(dialog);
        dialog.addEventListener("close", () => dialog.remove(), { once: true });
        dialog.showModal();
      } catch (error) { status("#document-status", error.message, true); }
    });
    row.append(content, open);
    list.append(row);
  }
  if (page.nextCreatedAt && page.nextId) {
    const more = node("button", "button button-quiet", "加载更多文档 ↓");
    more.type = "button";
    more.addEventListener("click", async () => {
      more.disabled = true;
      try {
        const next = await api(`/knowledge/documents?limit=20&cursorCreatedAt=${encodeURIComponent(page.nextCreatedAt)}&cursorId=${encodeURIComponent(page.nextId)}`);
        renderDocuments({ items: [...page.items, ...next.items], nextCreatedAt: next.nextCreatedAt, nextId: next.nextId });
      } catch (error) { status("#document-status", error.message, true); }
    });
    list.append(more);
  }
}

async function refreshDocuments() {
  if (!state.token) return;
  try { renderDocuments(await api("/knowledge/documents?limit=20")); }
  catch (error) { $("#document-list").replaceChildren(node("p", "empty-state", error.message)); }
}

async function importDocument(event) {
  event.preventDefault();
  const file = $("#document-file").files?.[0];
  if (!file) return status("#document-status", "请选择 Markdown 或 TXT 文件。", true);
  if (file.size > 100 * 1024) return status("#document-status", "文件超过服务端 100 KiB 限制。", true);
  const name = file.name.split(/[\\/]/).pop() || "合成知识材料.txt";
  const title = $("#document-title").value.trim() || name;
  status("#document-status", "读取 UTF-8 文件…");
  try {
    const content = new TextDecoder("utf-8", { fatal: true }).decode(await file.arrayBuffer());
    const idempotencyKey = key();
    const document = await api("/knowledge/documents", {
      method: "POST", headers: { "Idempotency-Key": idempotencyKey },
      body: JSON.stringify({ title, sourceRef: `demo://local/${key()}`, content, metadata: { classification: "synthetic-demo" } })
    });
    const version = document.version;
    const chunks = await api(`/knowledge/documents/${document.id}/chunks?assetVersion=${version}&chunkingVersion=p9-structure-1`, { method: "POST" });
    status("#document-status", `${chunks.length} 个结构片段已生成；正在构建向量和词法索引…`);
    let build = await api(`/knowledge/documents/${document.id}/index-builds?assetVersion=${version}&chunkingVersion=p9-structure-1`, {
      method: "POST", headers: { "Idempotency-Key": `${idempotencyKey}-index-p9-structure-1` }
    });
    const deadline = Date.now() + 120000;
    while (!["READY", "FAILED"].includes(build.status) && Date.now() < deadline) {
      await new Promise((resolve) => window.setTimeout(resolve, 800));
      build = await api(`/knowledge/documents/${document.id}/index-builds/${build.id}`);
    }
    if (build.status !== "READY") throw new Error(`索引未就绪：${build.failureCode || build.status}`);
    const publication = await api(`/knowledge/documents/${document.id}/publish?expectedVersion=${document.rowVersion}&buildId=${build.id}`, {
      method: "POST", headers: { "Idempotency-Key": `${idempotencyKey}-publish` }
    });
    status("#document-status", `已发布版本 ${publication.assetVersion}；${chunks.length} 个片段可参与检索。`);
    $("#document-form").reset();
    await refreshDocuments();
  } catch (error) { status("#document-status", error.message, true); }
}

function renderSearch(result) {
  const container = $("#search-results");
  container.replaceChildren();
  if (!result?.hits?.length) {
    container.append(node("p", "empty-state", "没有找到当前身份可读取的已发布知识片段。"));
    return;
  }
  for (const hit of result.hits) {
    const article = node("article", "search-result");
    article.append(node("h4", "", (hit.headingPath || []).join(" / ") || hit.sourceRef || "知识片段"));
    article.append(node("p", "", hit.content));
    const tags = node("div", "tag-row");
    for (const channel of hit.channels || []) tags.append(node("span", "tag", channel));
    if (hit.vectorRank) tags.append(node("span", "tag", `向量 #${hit.vectorRank}`));
    if (hit.lexicalRank) tags.append(node("span", "tag", `词法 #${hit.lexicalRank}`));
    tags.append(node("span", "tag", `${hit.startOffset ?? "?"}–${hit.endOffset ?? "?"} ${hit.offsetUnit || ""}`));
    article.append(tags);
    container.append(article);
  }
}

async function searchKnowledge(event) {
  event.preventDefault();
  const query = $("#search-query").value.trim();
  if (!query) return;
  try { renderSearch(await api("/knowledge/search", { method: "POST", body: JSON.stringify({ query, topK: 5, mode: "HYBRID" }) })); }
  catch (error) { $("#search-results").replaceChildren(node("p", "empty-state", error.message)); }
}

function sourceCard(source) {
  const card = node("article", "source-row");
  const heading = (source.headingPath || []).join(" / ");
  card.append(node("h4", "", `${source.citationId} · ${source.sourceType}${heading ? ` · ${heading}` : ""}`));
  const location = source.documentVersion ? `文档版本 ${source.documentVersion} · ${source.startOffset ?? "?"}–${source.endOffset ?? "?"} ${source.offsetUnit || ""}` : source.memoryVersion ? `Memory ${source.memoryVersion}` : source.sourceRef || "来源定位未记录";
  card.append(node("p", "", location));
  if (source.content) card.append(node("p", "source-content", source.content));
  return card;
}

function renderSources(container, sources) {
  container.replaceChildren();
  if (sources?.unavailable) {
    container.append(node("p", "empty-state", "引用来源已撤回或当前身份失去读取权限，正文已隐藏。"));
    return;
  }
  if (!sources?.items?.length) {
    container.append(node("p", "empty-state", "本次运行没有可显示的上下文来源。"));
    return;
  }
  for (const source of sources.items) container.append(sourceCard(source));
}

function renderJsonResult(container, result) {
  container.replaceChildren();
  if (!result) { container.append(node("p", "empty-state", "结果尚未就绪。")); return; }
  if (result.answer) {
    const answer = node("p", "answer-copy", result.answer);
    container.append(answer);
    container.append(node("span", "tag", `回答状态 · ${result.answerStatus || "未知"}`));
    container.append(node("span", "tag", `证据判断 · ${result.evidenceReview?.status || "未记录"} / ${result.evidenceReview?.mode || "未知"}`));
    const missing = node("div", "missing-list", (result.missingInformation || []).join("；"));
    if (missing.textContent) container.append(missing);
    return;
  }
  if (result.followupDraft) {
    container.append(node("span", "tag", `风险 · ${result.riskLevel || "未知"}`));
    container.append(node("p", "answer-copy", result.summary || ""));
    container.append(node("p", "muted", (result.reasons || []).join("；")));
    container.append(node("p", "missing-list", (result.uncertainties || []).join("；")));
    const draft = result.followupDraft;
    $("#draft-summary").value = draft.summary || "";
    $("#draft-summary").disabled = !draft.customerId;
    $("#confirm-followup").disabled = !draft.customerId;
    if (draft.customerId) state.selectedTask = { ...state.selectedTask, followupCustomerId: draft.customerId };
    if (draft.missingInformation?.length) container.append(node("p", "missing-list", `需补充：${draft.missingInformation.join("；")}`));
    return;
  }
  const pre = node("pre", "document-body", JSON.stringify(result, null, 2));
  container.append(pre);
}

async function createTask(agentId, input, businessEntity) {
  const body = { agentId, agentVersion: "1.0.0", input };
  if (businessEntity) body.businessEntity = businessEntity;
  return api("/tasks", { method: "POST", headers: { "Idempotency-Key": key() }, body: JSON.stringify(body) });
}

async function startQa(event) {
  event.preventDefault();
  const question = $("#qa-question").value.trim();
  if (!question) return;
  status("#qa-status", "已提交；等待检索、证据判断和回答…");
  try {
    const task = await createTask(QA_AGENT, question, null);
    state.selectedTask = task;
    await showTask(task.id, "qa");
    pollTask(task.id, "qa");
  } catch (error) { status("#qa-status", error.message, true); }
}

async function startFollowup(event) {
  event.preventDefault();
  const customerId = $("#customer-id").value.trim();
  const material = $("#customer-material").value.trim();
  if (!customerId || !material) return;
  const input = `客户 ID: ${customerId}\n客户材料：\n${material}`;
  status("#followup-status", "已提交；正在形成只读分析和草稿…");
  try {
    const task = await createTask(FOLLOWUP_AGENT, input, { type: "CUSTOMER", id: customerId });
    state.selectedTask = task;
    await showTask(task.id, "followup");
    pollTask(task.id, "followup");
  } catch (error) { status("#followup-status", error.message, true); }
}

async function confirmFollowup() {
  if (!state.selectedTask?.id) return;
  const summary = $("#draft-summary").value.trim();
  if (!summary) return status("#workflow-status", "请先填写跟进摘要。", true);
  if (state.pendingFollowup && (state.pendingFollowup.taskId !== state.selectedTask.id
      || state.pendingFollowup.summary !== summary)) {
    return status("#workflow-status", "上次确认结果尚不能确定。请保持摘要不变并用原请求键重试，或到任务记录查询实例。", true);
  }
  if (!state.pendingFollowup) state.pendingFollowup = { taskId: state.selectedTask.id, summary, key: key() };
  $("#draft-summary").disabled = true;
  $("#confirm-followup").disabled = true;
  status("#workflow-status", "确认已提交；正在读取固定审批流程…");
  try {
    const instance = await api(`/tasks/${state.selectedTask.id}/followups`, {
      method: "POST", headers: { "Idempotency-Key": state.pendingFollowup.key },
      body: JSON.stringify({ expectedVersion: state.selectedTask.version, summary })
    });
    state.pendingFollowup = null;
    await showWorkflow(instance.workflowId, instance.instanceId, "#workflow-status");
    pollWorkflow(instance.workflowId, instance.instanceId);
  } catch (error) {
    status("#workflow-status", `${error.message} · 可保持摘要不变并使用原请求键重试。`, true);
    $("#confirm-followup").disabled = false;
  }
}

function taskStatusText(task) {
  const labels = { QUEUED: "排队中", RUNNING: "处理中", WAITING_APPROVAL: "等待审批", WAITING_VERIFICATION: "等待核验", WAITING_REMOTE: "等待远端", SUCCEEDED: "已完成", FAILED: "失败", TIMED_OUT: "超时", CANCELLED: "已取消" };
  return labels[task.status] || task.status || "未知状态";
}

function renderTasks(items, append = false) {
  const container = $("#task-list");
  if (!append) container.replaceChildren();
  if (!items?.length && !append) {
    container.append(node("p", "empty-state", "当前身份暂时没有可见任务。"));
    return;
  }
  for (const task of items || []) {
    const row = node("article", "task-row");
    const text = node("div");
    const title = task.result?.answer ? "知识问答" : task.result?.followupDraft ? "客户跟进分析" : task.agentId === FOLLOWUP_AGENT ? "客户跟进" : task.agentId === QA_AGENT ? "知识问答" : "Agent 任务";
    text.append(node("h4", "", title));
    text.append(node("p", "", `${taskStatusText(task)} · ${task.createdAt ? new Date(task.createdAt).toLocaleString() : task.id}`));
    const open = node("button", "button button-quiet", "查看记录 →");
    open.type = "button";
    open.addEventListener("click", () => showTask(task.id, "history"));
    row.append(text, open);
    container.append(row);
  }
  if (state.nextTaskCursor) {
    const more = node("button", "button button-quiet", "加载更多任务 ↓");
    more.type = "button";
    more.addEventListener("click", loadMoreTasks, { once: true });
    container.append(more);
  }
}

async function refreshTasks() {
  if (!state.token) return;
  try {
    const page = await api("/tasks?limit=20");
    state.nextTaskCursor = page.nextCursor;
    renderTasks(page.items);
  } catch (error) { $("#task-list").replaceChildren(node("p", "empty-state", error.message)); }
}

async function loadMoreTasks(event) {
  if (!state.nextTaskCursor) return;
  event.currentTarget.disabled = true;
  try {
    const cursor = state.nextTaskCursor;
    const query = new URLSearchParams({ limit: "20", cursorUpdatedAt: cursor.updatedAt, cursorTaskId: cursor.taskId });
    const page = await api(`/tasks?${query}`);
    state.nextTaskCursor = page.nextCursor;
    event.currentTarget.remove();
    renderTasks(page.items, true);
  } catch (error) { status("#connection-status", error.message, true); }
}

async function showTask(taskId, targetView = "history") {
  if (!state.token) return;
  if (targetView !== "history") navigate(targetView);
  state.selectedTask = { id: taskId };
  const detail = $("#task-detail");
  detail.replaceChildren(node("p", "empty-state", "读取当前任务和授权来源…"));
  try {
    const task = await api(`/tasks/${encodeURIComponent(taskId)}`);
    state.selectedTask = task;
    if (targetView === "qa") {
      renderJsonResult($("#qa-answer"), task.result);
      if (task.status === "SUCCEEDED") status("#qa-status", `回答任务 ${task.id} · ${taskStatusText(task)}`);
      const sources = await api(`/tasks/${taskId}/sources`);
      renderSources($("#qa-sources"), sources);
      return task;
    }
    if (targetView === "followup") {
      renderJsonResult($("#followup-result"), task.result);
      if (task.status === "SUCCEEDED") status("#followup-status", `分析任务 ${task.id} · ${taskStatusText(task)}`);
      const sources = await api(`/tasks/${taskId}/sources`);
      renderSources($("#followup-sources"), sources);
      return task;
    }
    await renderTaskDetail(task);
    return task;
  } catch (error) {
    detail.replaceChildren(node("p", "empty-state", error.message));
    return null;
  }
}

async function renderTaskDetail(task) {
  const detail = $("#task-detail");
  detail.replaceChildren();
  const heading = node("div", "card-heading");
  const title = node("div");
  title.append(node("p", "eyebrow", `TASK JOURNAL / ATTEMPT ${task.attempt}`));
  title.append(node("h3", "", task.result?.answer ? "知识问答" : task.result?.followupDraft ? "客户跟进分析" : "Agent 任务"));
  heading.append(title, node("span", "tag", taskStatusText(task)));
  detail.append(heading);
  const meta = node("div", "meta-grid");
  for (const [label, value] of [["AGENT VERSION", `${task.agentId}@${task.agentVersion}`], ["SOURCE", task.source || "未知"], ["ATTEMPT", task.attempt]]) {
    const cell = node("div", "meta-cell"); cell.append(node("span", "", label), node("b", "", value)); meta.append(cell);
  }
  detail.append(meta);
  if (task.errorCode) detail.append(node("p", "missing-list", `${task.errorCode}${task.errorDetail ? ` · ${task.errorDetail}` : ""}`));
  const resultArea = node("div", "answer-copy");
  if (task.result?.answer) resultArea.textContent = task.result.answer;
  else if (task.result?.followupDraft) resultArea.textContent = `${task.result.summary || ""}\n\n草稿：${task.result.followupDraft.summary || ""}`;
  else if (task.result) resultArea.textContent = JSON.stringify(task.result, null, 2);
  else resultArea.textContent = "结果尚未就绪。";
  detail.append(resultArea);
  const sourceHeading = node("p", "eyebrow", "CONTEXT SOURCES"); detail.append(sourceHeading);
  const sourceContainer = node("div"); detail.append(sourceContainer);
  const usageHeading = node("p", "eyebrow", "USAGE / COST"); usageHeading.style.marginTop = "16px"; detail.append(usageHeading);
  const usageContainer = node("div", "tag-row"); detail.append(usageContainer);
  try {
    renderSources(sourceContainer, await api(`/tasks/${task.id}/sources`));
    const usage = await api(`/tasks/${task.id}/usage`);
    usageContainer.append(node("span", "tag", `${usage.calls} 次调用`));
    usageContainer.append(node("span", "tag", usage.tokenStatus === "NOT_RECORDED" ? "用量未记录" : `Token ${usage.inputTokens ?? "?"} + ${usage.outputTokens ?? "?"} · ${usage.tokenStatus}`));
    for (const cost of usage.costs || []) usageContainer.append(node("span", "tag", `${cost.amount ?? "未知费用"} ${cost.currency || "币种未记录"} · ${cost.status || "未知"}`));
  } catch (error) { sourceContainer.append(node("p", "empty-state", error.message)); }
  const controls = node("div", "approval-actions");
  if (task.status === "WAITING_APPROVAL" || task.status === "WAITING_VERIFICATION") {
    const reload = node("button", "button button-quiet", "刷新任务状态"); reload.type = "button"; reload.addEventListener("click", () => showTask(task.id)); controls.append(reload);
    // 审批只记录决定；发起人需恢复原 Task，固定 Execution 才会继续。
    const resume = node("button", "button button-dark", "审批或核验完成后继续原任务"); resume.type = "button";
    let resumeKey = key();
    resume.addEventListener("click", async () => {
      resume.disabled = true;
      try {
        const latest = await api(`/tasks/${encodeURIComponent(task.id)}`);
        await api(`/tasks/${encodeURIComponent(task.id)}/resume`, { method: "POST",
          headers: { "Idempotency-Key": resumeKey }, body: JSON.stringify({ expectedVersion: latest.version }) });
        resumeKey = key();
        await showTask(task.id);
        pollTask(task.id, "history");
      } catch (error) {
        resume.disabled = false;
        controls.append(node("p", "empty-state", `原任务仍待处理：${error.message}`));
      }
    });
    controls.append(resume);
  }
  detail.append(controls);
  const followups = await api(`/tasks/${task.id}/followups?limit=20`).catch(() => null);
  if (followups?.items?.length) {
    const label = node("p", "eyebrow", "FOLLOW-UP INSTANCES"); label.style.marginTop = "20px"; detail.append(label);
    for (const item of followups.items) {
      const flow = node("article", "workflow-row");
      const info = node("p", "", `${item.instanceId} · ${item.status} · ${item.currentStepId || "等待开始"}`);
      flow.append(info);
      const open = node("button", "button button-quiet", "查看审批与写入状态 →"); open.type = "button";
      open.addEventListener("click", () => showWorkflow(item.workflowId, item.instanceId, null));
      flow.append(open);
      detail.append(flow);
    }
  }
}

async function pollTask(taskId, targetView) {
  while (state.token && state.selectedTask?.id === taskId) {
    if (document.hidden) await new Promise((resolve) => document.addEventListener("visibilitychange", resolve, { once: true }));
    await new Promise((resolve) => window.setTimeout(resolve, 2000));
    if (!state.token || document.hidden) continue;
    if (state.selectedTask?.id !== taskId) return;
    const task = await showTask(taskId, targetView);
    if (!task || TERMINAL.has(task.status)) {
      refreshTasks();
      return;
    }
  }
}

async function confirmDecision(approvalId, decision) {
  try {
    const approval = await api(`/approvals/${encodeURIComponent(approvalId)}`);
    const updated = await api(`/approvals/${encodeURIComponent(approvalId)}/decisions`, {
      method: "POST", body: JSON.stringify({ decision, expectedVersion: approval.version })
    });
    status("#workflow-status", `审批已记录：${updated.state || decision}。执行仍由原流程继续。`);
    await loadApproval(approvalId);
  } catch (error) { status("#workflow-status", error.message, true); }
}

async function loadApproval(approvalId) {
  const panel = $("#approval-actions");
  panel.replaceChildren();
  try {
    const approval = await api(`/approvals/${encodeURIComponent(approvalId)}`);
    panel.append(node("p", "tag", `审批 ${approval.state || "状态未知"} · ${approval.id}`));
    if (approval.state === "PENDING") {
      for (const [decision, label, style] of [["APPROVED", "批准", "button button-accent"], ["REJECTED", "拒绝", "button button-quiet"]]) {
        const button = node("button", style, label); button.type = "button";
        button.addEventListener("click", () => confirmDecision(approvalId, decision));
        panel.append(button);
      }
    }
    panel.append(createApprovalLookup());
    await refreshPendingApprovals();
  } catch (error) {
    panel.append(node("p", "empty-state", `当前身份不能读取或决定此审批：${error.message}`));
    panel.append(createApprovalLookup());
    await refreshPendingApprovals();
  }
}

async function refreshPendingApprovals() {
  const panel = $("#approval-actions");
  panel.querySelector(".pending-approval-list")?.remove();
  const queue = node("div", "pending-approval-list");
  queue.append(node("p", "eyebrow", "待我处理的审批"));
  try {
    const page = await api("/approvals?limit=20");
    if (!page?.items?.length) queue.append(node("p", "muted", "当前没有待处理审批。"));
    for (const item of page.items || []) {
      const open = node("button", "button button-quiet", `查看待审请求 · ${item.createdAt ? new Date(item.createdAt).toLocaleString() : item.id}`);
      open.type = "button";
      open.addEventListener("click", () => loadApproval(item.id));
      queue.append(open);
    }
  } catch {
    queue.append(node("p", "muted", "当前身份没有待审批列表权限。"));
  }
  panel.append(queue);
}

async function showWorkflow(workflowId, instanceId, messageSelector = "#workflow-status") {
  try {
    const instance = await api(`/workflows/${encodeURIComponent(workflowId)}/instances/${encodeURIComponent(instanceId)}`);
    const message = [instance.status, instance.currentStepId, instance.businessEffectStatus, instance.errorCode].filter(Boolean).join(" · ");
    if (messageSelector) status(messageSelector, `流程 ${instance.id}：${message}`);
    const target = $("#workflow-status");
    if (target && messageSelector !== "#workflow-status") status("#workflow-status", `流程 ${instance.id}：${message}`);
    if (instance.approvalId) await loadApproval(instance.approvalId);
    else {
      const panel = $("#approval-actions");
      panel.replaceChildren(node("p", "empty-state", instance.status === "SUCCEEDED" ? "已完成；请核对实际 CRM 结果。" : "审批编号尚未提供；可由有权限的审批人输入编号查询。"));
      panel.append(createApprovalLookup());
      await refreshPendingApprovals();
    }
    if (instance.childTaskId && instance.childTaskVersion) {
      const child = await api(`/tasks/${encodeURIComponent(instance.childTaskId)}`).catch(() => null);
      if (child?.status === "WAITING_APPROVAL" && instance.status === "WAITING_CHILD") {
        const resume = node("button", "button button-dark", "审批后续办任务"); resume.type = "button";
        resume.addEventListener("click", async () => {
          try {
            const latest = await api(`/tasks/${child.id}`);
            await api(`/tasks/${child.id}/resume`, { method: "POST", headers: { "Idempotency-Key": key() }, body: JSON.stringify({ expectedVersion: latest.version }) });
            await showWorkflow(workflowId, instanceId);
          } catch (error) { status("#workflow-status", error.message, true); }
        });
        $("#approval-actions").append(resume);
      }
    }
    return instance;
  } catch (error) {
    if (messageSelector) status(messageSelector, error.message, true);
    return null;
  }
}

async function pollWorkflow(workflowId, instanceId) {
  const pollingKey = `${workflowId}/${instanceId}`;
  state.pollingWorkflow = pollingKey;
  while (state.token && state.pollingWorkflow === pollingKey) {
    if (document.hidden) await new Promise((resolve) => document.addEventListener("visibilitychange", resolve, { once: true }));
    await new Promise((resolve) => window.setTimeout(resolve, 2000));
    if (!state.token || document.hidden) continue;
    if (state.pollingWorkflow !== pollingKey) return;
    const instance = await showWorkflow(workflowId, instanceId);
    if (!instance || ["SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"].includes(instance.status)) return;
  }
}

function createApprovalLookup() {
  const form = node("form", "approval-lookup");
  const label = node("label", "", "审批人可粘贴待处理审批 ID");
  const input = node("input"); input.type = "text"; input.autocomplete = "off"; input.placeholder = "approval ID";
  const button = node("button", "button button-quiet", "查询审批"); button.type = "submit";
  label.append(input); form.append(label, button);
  form.addEventListener("submit", async (event) => { event.preventDefault(); if (input.value.trim()) await loadApproval(input.value.trim()); });
  return form;
}

$("#identity-form").addEventListener("submit", connect);
document.querySelectorAll(".nav-button").forEach((button) => button.addEventListener("click", () => navigate(button.dataset.view)));
$("#document-form").addEventListener("submit", importDocument);
$("#search-form").addEventListener("submit", searchKnowledge);
$("#qa-form").addEventListener("submit", startQa);
$("#followup-form").addEventListener("submit", startFollowup);
$("#confirm-followup").addEventListener("click", confirmFollowup);
$("#refresh-documents").addEventListener("click", refreshDocuments);
$("#refresh-tasks").addEventListener("click", refreshTasks);
document.addEventListener("visibilitychange", () => { if (!document.hidden && state.token && state.selectedTask?.id) refreshTasks(); });
$("#approval-actions").append(createApprovalLookup());
refreshPendingApprovals();
