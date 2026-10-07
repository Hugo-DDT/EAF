export function el(tag, className = "", text = null) {
  const item = document.createElement(tag);
  if (className) item.className = className;
  if (text !== null && text !== undefined) item.textContent = String(text);
  return item;
}

export function renderTurn(container, turn, mode, sourceItems, { briefRevision, active, onConfirmFollowup,
  feedbackItems = [], experienceDrafts = new Map(), onSubmitFeedback, onCreateExperienceDraft, onEditExperience }) {
  const row = el("article", "turn");
  row.append(el("div", "turn-question", turn.input));
  const answer = el("div", "turn-answer");
  const stateText = turn.status === "SUCCEEDED" ? `第 ${turn.turnNo} 轮 · ${new Date(turn.createdAt).toLocaleString()}`
    : `第 ${turn.turnNo} 轮 · ${turn.status || "处理中"}`;
  answer.append(el("p", "answer-label", stateText));
  if (turn.result && mode === "KNOWLEDGE_QA") {
    if (turn.result.clarificationQuestion) {
      answer.append(el("p", "answer-text", `需要澄清：${turn.result.clarificationQuestion}`));
    } else {
      answer.append(el("p", "answer-text", turn.result.answer || "本轮没有可显示的回答。"));
      if (turn.result.answerStatus) answer.append(el("p", "turn-detail", `状态：${turn.result.answerStatus}`));
    }
  } else if (turn.result) {
    const risk = turn.result.riskLevel ? `风险意见：${turn.result.riskLevel}` : "客户分析";
    answer.append(el("p", "answer-text", turn.result.clarificationQuestion
      ? `需要补充：${turn.result.clarificationQuestion}` : `${risk}\n${turn.result.summary || ""}`));
    const uncertain = turn.result.uncertainties || [];
    if (uncertain.length) answer.append(el("p", "turn-detail", `待核实：${uncertain.join("；")}`));
    const draft = turn.result.followupDraft;
    if (draft?.summary && turn.status === "SUCCEEDED") {
      const draftBox = el("div", "draft-action");
      draftBox.append(el("p", "turn-detail", `跟进草稿 · 简报修订 R${turn.briefRevision}`));
      const summary = el("textarea", "turn-draft");
      summary.maxLength = 2_000;
      summary.rows = 3;
      summary.value = draft.summary;
      summary.setAttribute("aria-label", "确认前编辑跟进摘要");
      const submit = el("button", "button button-sage", "确认最新草稿并提交审批");
      submit.type = "button";
      const sourceCurrent = turn.briefRevision === briefRevision && !active;
      submit.disabled = !sourceCurrent;
      if (!sourceCurrent) draftBox.append(el("p", "turn-detail", "基于旧资料或有活动轮次；重新分析当前简报后才能提交。"));
      submit.addEventListener("click", () => onConfirmFollowup(turn, summary.value, submit));
      draftBox.append(summary, submit);
      answer.append(draftBox);
    }
  } else if (turn.errorCode) {
    answer.append(el("p", "turn-error", `${turn.errorCode} · ${turn.errorDetail || "本轮未完成"}`));
  } else if (turn.status === "SUCCEEDED") {
    // 来源撤回会隐藏已完成 Task 的结果；不能把不可见结果误报成仍在运行。
    answer.append(el("p", "turn-detail", "本轮任务已完成，但引用来源已失效，历史回答当前不可见。"));
  } else {
    answer.append(el("p", "turn-detail", "任务仍在运行；页面会继续查询状态。"));
  }
  const ids = turn.result?.citations || [];
  if (ids.length) {
    const chips = el("div", "source-chips");
    for (const id of ids) {
      const item = sourceItems?.find((source) => source.citationId === id);
      const label = item ? `${(item.headingPath || []).join(" / ") || "本轮来源"} · ${id}`
        : sourceItems == null ? `本轮引用 · ${id}` : `来源当前不可读取 · ${id}`;
      chips.append(el("span", "source-chip", label));
      if (item?.content) chips.append(el("p", "turn-detail", item.content));
    }
    answer.append(chips);
  }
  const context = turn.result?.conversationContext;
  if (context) answer.append(el("p", "turn-detail", `上下文：确认简报 R${context.briefRevision} · 采用 ${context.includedTurnIds?.length || 0} 轮 · 省略 ${context.omittedTurnCount || 0} 轮`));
  if (turn.result?.retrievalQuery) answer.append(el("p", "turn-detail", `本轮检索问题：${turn.result.retrievalQuery}`));
  const experienceUsage = turn.result?.experienceUsage;
  if (experienceUsage) {
    const box = el("div", "experience-usage");
    const included = experienceUsage.included || [];
    box.append(el("p", "overline", `个人经验 · 实际带入 ${included.length} 张`));
    if (included.length) {
      for (const item of included) {
        const cited = (experienceUsage.citedCitationIds || []).includes(item.citationId);
        box.append(el("p", "experience-usage-item", `${item.title || "个人经验"} · ${cited ? "本轮被引用" : "已带入参考"}`));
      }
    } else box.append(el("p", "experience-usage-item", "本轮没有适用的个人经验卡。"));
    const omitted = (experienceUsage.omittedByLimit || 0) + (experienceUsage.omittedByBudget || 0);
    if (omitted) box.append(el("p", "turn-detail", `另有 ${omitted} 张因数量或预算限制未带入。`));
    answer.append(box);
  }
  const followupUsage = turn.result?.followupUsage;
  if (followupUsage) {
    const box = el("div", "team-result-usage");
    const included = followupUsage.included || [];
    box.append(el("p", "overline", `团队跟进结果 · 实际带入 ${included.length} 条`));
    if (included.length) for (const item of included)
      box.append(el("p", "team-result-usage-item", `结果 ${item.resultNo} · ${item.outcomeCode} · ${item.syncStatus || "本地记录"} · ${item.summary}`));
    else box.append(el("p", "team-result-usage-item", "本轮没有选择团队跟进结果。"));
    box.append(el("p", "turn-detail", "带入表示作为业务资料提供给本轮分析，不表示模型引用或事实核验。"));
    answer.append(box);
  }
  if (turn.status === "SUCCEEDED" && onSubmitFeedback) {
    const box = el("section", "feedback-box");
    box.append(el("p", "overline", "YOUR CORRECTION · 仅保存你提交的内容"));
    const form = document.createElement("form"); form.className = "feedback-form";
    const correction = document.createElement("textarea"); correction.maxLength = 4_000; correction.required = true;
    correction.placeholder = "纠正这次回答中的遗漏或表达问题"; correction.setAttribute("aria-label", "用户纠正");
    const evidence = document.createElement("textarea"); evidence.maxLength = 4_000; evidence.required = true;
    evidence.placeholder = "补充你确认的依据"; evidence.setAttribute("aria-label", "用户依据");
    const submit = el("button", "button button-light", "记录反馈"); submit.type = "submit";
    form.append(correction, evidence, submit);
    form.addEventListener("submit", (event) => {
      event.preventDefault(); void onSubmitFeedback(turn, correction.value, evidence.value, submit);
    });
    box.append(form);
    for (const feedback of feedbackItems) {
      const item = el("details", "feedback-item");
      item.append(el("summary", "", `已记录反馈 · ${new Date(feedback.createdAt).toLocaleString()}`));
      item.append(el("p", "turn-detail", `纠正：${feedback.correction}`));
      item.append(el("p", "turn-detail", `依据：${feedback.evidence}`));
      const existingDraft = experienceDrafts.get(feedback.id);
      if (existingDraft) {
        item.append(el("p", "draft-status", `整理 Task · ${existingDraft.status || "已提交"}`));
        if (existingDraft.status === "SUCCEEDED" && existingDraft.result?.title && existingDraft.result?.content) {
          item.append(el("p", "turn-detail", `${existingDraft.result.title}\n${existingDraft.result.content}`));
          const use = el("button", "button button-sage", "载入卡片编辑器"); use.type = "button";
          use.addEventListener("click", () => onEditExperience(turn, feedback, existingDraft)); item.append(use);
        } else if (["FAILED", "TIMED_OUT", "CANCELLED"].includes(existingDraft.status))
          item.append(el("p", "turn-error", `${existingDraft.errorCode || "整理未完成"} · 可使用新请求键再次创建整理 Task。`));
      }
      if (!existingDraft || ["FAILED", "TIMED_OUT", "CANCELLED"].includes(existingDraft.status)) {
        const draftText = document.createElement("textarea"); draftText.maxLength = 800;
        draftText.placeholder = "可选：补充你希望草稿采用的表述"; draftText.setAttribute("aria-label", "整理草稿提示");
        const createDraft = el("button", "button button-sage", "一次整理成草稿"); createDraft.type = "button";
        createDraft.addEventListener("click", () => void onCreateExperienceDraft(turn, feedback, draftText.value, createDraft));
        item.append(draftText, createDraft);
      }
      const card = el("button", "text-button", "编辑为经验卡"); card.type = "button";
      card.addEventListener("click", () => onEditExperience(turn, feedback, null)); item.append(card);
      box.append(item);
    }
    answer.append(box);
  }
  row.append(answer);
  container.append(row);
}

export function renderComparison(container, data) {
  container.replaceChildren();
  if (!data?.available) { container.append(el("p", "quiet", "至少两次当前可见的成功分析后显示前后变化。")); return; }
  const rows = [
    ["资料修订", `R${data.beforeBriefRevision}`, `R${data.afterBriefRevision}`],
    ["风险意见", data.beforeRiskLevel, data.afterRiskLevel],
    ["新增待核实", data.addedMissingInformation?.join("；") || "无", ""],
    ["已解决待核实", data.resolvedMissingInformation?.join("；") || "无", ""]
  ];
  for (const [label, before, after] of rows) {
    const row = el("div", "compare-row");
    row.append(el("span", "", label), el("b", "", before), el("em", "", after));
    container.append(row);
  }
  for (const [label, text] of [["前次摘要", data.beforeSummary], ["本次摘要", data.afterSummary],
    ["前次草稿", data.beforeDraft], ["本次草稿", data.afterDraft]]) {
    if (!text) continue;
    const row = el("div", "followup-row");
    row.append(el("b", "", `${label}：`), document.createTextNode(text));
    container.append(row);
  }
  container.append(el("p", "turn-detail", "这是字段差异，不代表变化由简报修订导致。"));
}

export function renderUsage(container, data) {
  container.replaceChildren();
  if (!data) { container.append(el("p", "quiet", "暂无计量数据。")); return; }
  container.append(el("p", "", `模型调用 ${data.calls} · Token ${data.tokenStatus === "KNOWN" ? `${data.inputTokens ?? 0} 输入 / ${data.outputTokens ?? 0} 输出` : data.tokenStatus}`));
  if (data.costs?.length) for (const cost of data.costs) {
    const value = cost.amount == null ? "费用未知" : `${cost.amount} ${cost.currency || ""}`;
    container.append(el("p", "", `${value} · ${cost.status} · ${cost.calls} 次`));
  }
  if (!data.costs?.length) container.append(el("p", "", "费用未记录"));
}

export function renderFollowups(container, items) {
  container.replaceChildren();
  if (!items?.length) { container.append(el("p", "quiet", "已确认的跟进流程会显示在这里。")); return; }
  for (const item of items) {
    const row = el("div", "followup-row");
    row.append(el("b", "", `${item.status} · ${item.instanceId}`));
    row.append(document.createTextNode(`\nWorkflow ${item.workflowVersion || ""} · ${new Date(item.createdAt).toLocaleString()}`));
    container.append(row);
  }
}
