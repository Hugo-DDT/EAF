package io.eaf.model.infrastructure;

import io.eaf.model.api.ModelFailure;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelRequest;
import io.eaf.model.api.ModelResult;
import io.eaf.model.api.ModelToolCall;
import java.util.Locale;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.atomic.AtomicInteger;

public final class DeterministicModelGateway implements ModelGateway {
    private static final Pattern CUSTOMER_ID = Pattern.compile("(?:customer[-_ ]?id|客户)\\s*[:=：]?\\s*([A-Za-z0-9._:-]{1,160})", Pattern.CASE_INSENSITIVE);
    private static final Pattern CITATION_ID = Pattern.compile("\"citationId\"\\s*:\\s*\"([^\"]{1,80})\"");
    private static final Pattern EVIDENCE_ID = Pattern.compile("\"evidenceId\"\\s*:\\s*\"([A-Z][A-Z0-9]{0,19})\"");
    private static final Pattern KNOWLEDGE_CITATION = Pattern.compile("\\{[^{}]*\"citationId\"\\s*:\\s*\"([^\"]{1,80})\"[^{}]*\"sourceType\"\\s*:\\s*\"KNOWLEDGE\"[^{}]*}", Pattern.DOTALL);
    private final String scenario;
    private final AtomicInteger calls = new AtomicInteger();

    public DeterministicModelGateway(String scenario) { this.scenario = scenario; }

    @Override
    public ModelResult call(ModelRequest request) {
        if (request.messages().stream().mapToInt(m -> m.content().length()).sum() >= request.tokenBudget())
            throw new ModelFailure("BUDGET_EXCEEDED", "模型输入预算不足。", false, false);
        if (request.deadline().isBefore(java.time.Instant.now()))
            throw new ModelFailure("UPSTREAM_TIMEOUT", "模型截止时间已到。", true, false);
        if ("BUDGET_EXCEEDED".equals(scenario)) throw new ModelFailure("BUDGET_EXCEEDED", "测试模型预算不足。", false, false);
        calls.incrementAndGet();
        if ("REJECT".equals(scenario)) throw new ModelFailure("UPSTREAM_FAILURE", "测试模型拒绝请求。", false, true);
        if ("TIMEOUT".equals(scenario)) throw new ModelFailure("UPSTREAM_TIMEOUT", "测试模型超时。", true, true);
        if ("INVALID_JSON".equals(scenario)) return new ModelResult("deterministic", "p1-test", "not-json", 12, 3, "KNOWN");
        var latestText = request.messages().get(request.messages().size() - 1).content();
        if ((isToolScenario() || (!request.tools().isEmpty() && customerId(latestText) != null)) &&
                (request.messages().stream().noneMatch(m -> "tool".equals(m.role())) || "REPEAT_TOOL".equals(scenario))) {
            var id = request.messages().stream().filter(m -> "user".equals(m.role())).map(m -> customerId(m.content())).filter(java.util.Objects::nonNull).findFirst().orElse(customerId(latestText));
            var callId = "call-%d".formatted(calls.get());
            var name = switch (scenario) {
                case "UNKNOWN_TOOL" -> "crm.customer.delete";
                case "WRITE" -> "crm.followup.create";
                default -> "crm.customer.query";
            };
            //  写工具必须携带完整摘要；参数仍由 Execution 重新规范化，模型输出不是授权事实。
            var args = "MISSING_PARAMETER".equals(scenario) || id == null ? "{}"
                    : "WRITE".equals(scenario) ? "{\"customerId\":\"%s\",\"summary\":\"安排一次客户跟进。\"}".formatted(id)
                    : "{\"customerId\":\"%s\"}".formatted(id);
            return new ModelResult("deterministic", "p2-test", null, 120, 16, "KNOWN",
                    List.of(new ModelToolCall(callId, name, args)), "TOOL_CALLS");
        }
        var marker = "\n用户任务（决定本次输出）：\n";
        var taskMarker = latestText.indexOf(marker);
        var taskText = (taskMarker >= 0 ? latestText.substring(taskMarker + marker.length()) : latestText).toLowerCase(Locale.ROOT);
        var toolResult = request.messages().stream().filter(m -> "tool".equals(m.role())).reduce((first, second) -> second).orElse(null);
        if (toolResult != null && !"REPEAT_TOOL".equals(scenario)) {
            var content = toolResult.content().toLowerCase(Locale.ROOT);
            var riskFromCustomer = content.contains("active") || content.contains("续约") ? "LOW" : content.contains("complaint") ? "HIGH" : "UNKNOWN";
            var output = output(riskFromCustomer, request);
            return new ModelResult("deterministic", "p2-test", output, 120, 32, "KNOWN");
        }
        // 确定性替身只覆盖有限分类词根以验证协议与状态流，不代表自然语言泛化能力。
        var lowRiskSignal = taskText.contains("续约") || taskText.contains("续期") || taskText.contains("延续服务");
        var unknownRiskSignal = taskText.contains("信息不足") || taskText.contains("冲突")
                || taskText.contains("没有足够证据") || taskText.contains("无法确认风险") || taskText.contains("不一致");
        var risk = taskText.contains("明确高风险") ? "HIGH" : lowRiskSignal ? "LOW" :
                unknownRiskSignal ? "UNKNOWN" : "MEDIUM";
        // ponytail: 确定性替身只模拟已知的续约中断问题；更广泛的语义泛化由真实模型评测验证。
        var candidateRule = request.messages().stream().map(m -> m.content())
                .filter(content -> content.contains("隔离候选实验上下文"))
                .anyMatch(content -> content.contains("续约中断") && content.contains("高风险"));
        if (candidateRule && taskText.contains("续约中断")) risk = "HIGH";
        var profileMarker = request.messages().stream().filter(message -> "system".equals(message.role()))
                .map(message -> message.content()).findFirst().orElse("");
        if (profileMarker.contains("MY_P16_WORK_DIGEST_RESPONSE_V1")) {
            var matcher = EVIDENCE_ID.matcher(latestText);
            var evidence = matcher.find() ? matcher.group(1) : "W1";
            return new ModelResult("deterministic", "p30-test",
                    ("{\"overview\":\"已按当前快照整理本人待办；请对照证据核验状态、截止时间和摘要预览。\","
                            + "\"attentionItems\":[{\"text\":\"优先核对当前状态与截止时间；摘要预览不代表处理结果。\",\"evidenceIds\":[\"%s\"]}]}")
                            .formatted(evidence), 48, 19, "KNOWN");
        }
        if (profileMarker.contains("PROJECT_BRIEF_PREPARE_V1")) {
            var matcher = EVIDENCE_ID.matcher(latestText);
            var citation = matcher.find() ? matcher.group(1) : null;
            var citations = citation == null ? "[]"
                    : "[{\"evidenceId\":\"%s\",\"reason\":\"依据已选来源整理\"}]".formatted(citation);
            return new ModelResult("deterministic", "p29-test",
                    ("{\"overview\":\"依据用户选择的资料与待办整理项目简报草稿，事实仍以来源快照为准。\","
                            + "\"attentionItems\":[],\"citations\":%s}").formatted(citations), 48, 18, "KNOWN");
        }
        if (profileMarker.contains("P15_SERVICE_REQUEST_PLAN_V1")) {
            var citation = citation(request);
            if ("SERVICE_REQUEST_SEARCH".equals(scenario) && calls.get() == 1)
                return new ModelResult("deterministic", "p15-test",
                        "{\"action\":\"SEARCH\",\"query\":\"内部服务请求登记所需信息\",\"missingInformation\":[\"请求处理规范\"]}",
                        64, 18, "KNOWN");
            if ("SERVICE_REQUEST_NEEDS_INPUT".equals(scenario))
                return new ModelResult("deterministic", "p15-test",
                        "{\"action\":\"FINAL\",\"outcome\":\"NEEDS_INPUT\",\"citations\":[],\"questions\":[\"请补充受影响设备或地点。\"]}",
                        64, 18, "KNOWN");
            if ("SERVICE_REQUEST_INVALID".equals(scenario))
                return new ModelResult("deterministic", "p15-test", "{\"action\":\"FINAL\",\"outcome\":\"READY\",\"category\":\"IT\",\"title\":\"无效引用\",\"summary\":\"测试\",\"handlingSuggestion\":\"测试\",\"citations\":[\"kb-forged\"],\"questions\":[]}",
                        64, 18, "KNOWN");
            if (citation == null || "SERVICE_REQUEST_NO_EVIDENCE".equals(scenario))
                return new ModelResult("deterministic", "p15-test",
                        "{\"action\":\"FINAL\",\"outcome\":\"INSUFFICIENT_EVIDENCE\",\"citations\":[],\"questions\":[],\"missingInformation\":\"没有足够的当前正式知识证据。\"}",
                        64, 16, "KNOWN");
            return new ModelResult("deterministic", "p15-test",
                    ("{\"action\":\"FINAL\",\"outcome\":\"READY\",\"category\":\"IT\","
                            + "\"title\":\"办公设备维修请求\",\"summary\":\"合成员工反馈设备无法正常使用，希望登记检修。\","
                            + "\"handlingSuggestion\":\"按设备处理规范核对故障现象后受理。\",\"citations\":[\"%s\"],\"questions\":[]}")
                            .formatted(citation), 72, 22, "KNOWN");
        }
        if (profileMarker.contains("P16_SERVICE_REQUEST_PREPARE_V1"))
            return new ModelResult("deterministic", "p16-test",
                    "{\"handlingAdvice\":\"先按简报核对影响范围与设备状态，再执行必要检查。\","
                            + "\"cautions\":\"未经处理人核实前，不要承诺问题已经解决。\"}", 40, 18, "KNOWN");
        if (profileMarker.contains("P17_SERVICE_REQUEST_PREPARE_V2"))
            return new ModelResult("deterministic", "p17-test",
                    "{\"handlingAdvice\":\"按明确选择的团队经验核对适用条件，再检查设备状态并记录结果。\","
                            + "\"cautions\":\"团队经验是参考建议，处理人需核实当前情况。\"}", 42, 19, "KNOWN");
        if (profileMarker.contains("P16_SERVICE_REQUEST_SUMMARY_V1")) {
            var outcome = jsonString(latestText, "outcome");
            var remaining = "COMPLETED".equals(outcome) ? "" : "仍有人工记录的后续事项待完成，请核对 nextAction。";
            return new ModelResult("deterministic", "p16-test",
                    ("{\"resultSummary\":\"已按本轮人工记录整理，具体处理内容请核对人工原文。\",\"remainingWork\":\"%s\"}")
                            .formatted(remaining),
                    48, 20, "KNOWN");
        }
        if (profileMarker.contains("P21_KNOWLEDGE_READ_ONLY_V1")) {
            var citation = knowledgeCitation(request);
            return citation == null
                    ? new ModelResult("deterministic", "p21-test",
                            "{\"category\":\"OTHER\",\"title\":\"\",\"knowledgeAdvice\":\"\","
                                    + "\"outcome\":\"INSUFFICIENT_EVIDENCE\",\"questions\":[],\"citations\":[]}", 40, 8, "KNOWN")
                    : new ModelResult("deterministic", "p21-test",
                            ("{\"category\":\"IT\",\"title\":\"办公设备处理建议\","
                                    + "\"knowledgeAdvice\":\"按正式知识核对设备故障现象和位置，再按对应流程处理。\","
                                    + "\"outcome\":\"ANALYZED\",\"questions\":[],\"citations\":[\"%s\"]}").formatted(citation),
                            48, 16, "KNOWN");
        }
        if (profileMarker.contains("P21_TEAM_EXPERIENCE_READ_ONLY_V1"))
            return new ModelResult("deterministic", "p21-test",
                    "{\"experienceAdvice\":\"参考明确选择的团队经验核对适用条件，再记录当前事实。\","
                            + "\"cautions\":\"团队经验仅供参考，处理人需核实实际情况。\"}", 42, 14, "KNOWN");
        if (profileMarker.contains("P23_TEAM_EXPERIENCE_IMPROVEMENT_V1"))
            return new ModelResult("deterministic", "p23-test",
                    "{\"title\":\"先核实现象与影响范围\",\"appliesWhen\":\"处理服务请求且需要补齐现场信息时\","
                            + "\"content\":\"先记录可观察现象、影响范围和已核实信息；资料不足时提出补充问题，不把推测当作事实。\"}",
                    48, 20, "KNOWN");
        if (profileMarker.contains("P10_QUERY_PREPARATION_V1")) {
            var ambiguous = latestText.contains("它") && latestText.contains("标准") && latestText.contains("加急");
            if (ambiguous) return new ModelResult("deterministic", "p10-demo",
                    "{\"retrievalQuery\":null,\"clarificationQuestion\":\"你指的是标准产品还是加急产品？\"}", 45, 12, "KNOWN");
            var current = jsonString(latestText, "currentQuestion");
            var query = current == null ? "" : current;
            return new ModelResult("deterministic", "p10-demo", "{\"retrievalQuery\":\""
                    + escapeJson(query) + "\",\"clarificationQuestion\":null}", 50, 10, "KNOWN");
        }
        if (profileMarker.contains("P10_CONVERSATIONAL_KNOWLEDGE_QA_V1")
                || profileMarker.contains("P11_CONVERSATIONAL_KNOWLEDGE_QA_V2")) {
            var citation = profileMarker.contains("P11_CONVERSATIONAL_KNOWLEDGE_QA_V2")
                    ? knowledgeCitation(request) : citation(request);
            return new ModelResult("deterministic", "p10-demo", "{\"answer\":\"已依据本轮授权知识片段整理；请打开来源引用核对适用细节。\"," 
                    + "\"answerStatus\":\"ANSWERED\",\"citations\":"
                    + (citation == null ? "[]" : "[\"" + citation + "\"]")
                    + ",\"missingInformation\":[]}", 85, 24, "KNOWN");
        }
        if (profileMarker.contains("P10_CONVERSATIONAL_CUSTOMER_FOLLOWUP_V1")
                || profileMarker.contains("P11_CONVERSATIONAL_CUSTOMER_FOLLOWUP_V2")
                || profileMarker.contains("P12_CUSTOMER_ASSISTANT_V3")) {
            var citation = citation(request);
            var citations = citation == null ? "[]" : "[\"" + citation + "\"]";
            var currentTurn = jsonString(latestText, "turnId");
            var revision = jsonNumber(latestText, "briefRevision");
            var suggestion = currentTurn == null ? "null" : "{\"baseRevision\":" + revision
                    + ",\"content\":\"建议将本轮客户补充登记为待核实信息。\","
                    + "\"changeSummary\":\"建议核实并更新本轮新增信息。\",\"sourceTurnIds\":[\""
                    + currentTurn + "\"]}";
            var response = ("{\"riskLevel\":\"%s\",\"summary\":\"基于合成客户材料形成的风险提示，请先核实客户事实。\","
                    + "\"reasons\":[\"确定性演示仅覆盖固定合成样例\"],\"uncertainties\":[],\"citations\":%s,"
                    + "\"followupDraft\":{\"summary\":\"建议联系客户核实当前情况；未核实前不作服务承诺。\","
                    + "\"missingInformation\":[],\"citations\":%s},\"briefSuggestion\":%s}")
                    .formatted(risk, citations, citations, suggestion);
            return new ModelResult("deterministic", "p10-demo", response, 110, 30, "KNOWN");
        }
        if (profileMarker.contains("P11_EXPERIENCE_DRAFT_V1")) {
            return new ModelResult("deterministic", "p11-demo",
                    "{\"title\":\"先说明依据与缺口\",\"content\":\"分析时先列出已知资料，再单独说明尚待核实的信息；资料不足时不要补写事实。\"}",
                    72, 24, "KNOWN");
        }
        if (profileMarker.contains("P9_KNOWLEDGE_QA_V1")) {
            var citation = citation(request);
            return new ModelResult("deterministic", "p9-demo", "{\"answer\":\"已依据授权知识片段整理；请打开来源引用核对适用细节。\","
                    + "\"answerStatus\":\"ANSWERED\",\"citations\":" + (citation == null ? "[]" : "[\"" + citation + "\"]")
                    + ",\"missingInformation\":[]}", 80, 24, "KNOWN");
        }
        if (profileMarker.contains("P9_CUSTOMER_FOLLOWUP_V1")) {
            var citation = citation(request);
            var citations = citation == null ? "[]" : "[\"" + citation + "\"]";
            var gaps = taskText.contains("信息不足") || taskText.contains("缺少") ? "[\"最近一次联系结果\"]" : "[]";
            var p9 = ("{\"riskLevel\":\"%s\",\"summary\":\"基于合成材料整理的风险提示，提交前请核实客户事实。\","
                    + "\"reasons\":[\"确定性演示仅覆盖固定合成样例\"],\"uncertainties\":[],\"citations\":%s,"
                    + "\"followupDraft\":{\"customerId\":\"model-value-ignored\","
                    + "\"summary\":\"建议联系客户核实当前续约状态及投诉处理情况；未核实前不作服务承诺。\","
                    + "\"missingInformation\":%s,\"citations\":%s}}").formatted(risk, citations, gaps, citations);
            return new ModelResult("deterministic", "p9-demo", p9, 100, 32, "KNOWN");
        }
        var output = output(risk, request);
        if ("UNKNOWN_USAGE".equals(scenario)) return new ModelResult("deterministic", "p1-test", output, null, null, "UNKNOWN");
        return new ModelResult("deterministic", "p1-test", output, 120, 32, "KNOWN");
    }

    @Override public int callCount() { return calls.get(); }

    private boolean isToolScenario() { return List.of("TOOL_QUERY", "REPEAT_TOOL", "UNKNOWN_TOOL", "MISSING_PARAMETER", "WRITE", "MALICIOUS_TOOL_RESULT").contains(scenario); }

    private String output(String risk, ModelRequest request) {
        var contextMessage = request.messages().stream().filter(m -> m.content().contains("正式知识上下文")
                || m.content().contains("隔离候选实验上下文")).findFirst().orElse(null);
        var citation = contextMessage == null ? null : request.messages().stream().map(m -> {
                    var matcher = CITATION_ID.matcher(m.content());
                    return matcher.find() ? matcher.group(1) : null;
                }).filter(java.util.Objects::nonNull).findFirst().orElse(null);
        return contextMessage == null
                ? "{\"riskLevel\":\"%s\",\"summary\":\"基于用户提供摘要的建议，不是外部事实。\",\"reasons\":[\"材料已按固定 Prompt 处理\"],\"uncertainties\":[\"尚未查询业务系统\"]}".formatted(risk)
                : "{\"riskLevel\":\"%s\",\"summary\":\"基于本次授权上下文生成建议。\",\"reasons\":[\"结论引用了本次授权上下文\"],\"uncertainties\":[],\"citations\":%s}".formatted(risk,
                citation == null ? "[]" : "[\"%s\"]".formatted(citation));
    }

    private String citation(ModelRequest request) {
        return request.messages().stream().map(message -> {
            var matcher = CITATION_ID.matcher(message.content());
            return matcher.find() ? matcher.group(1) : null;
        }).filter(java.util.Objects::nonNull).findFirst().orElse(null);
    }

    private String knowledgeCitation(ModelRequest request) {
        return request.messages().stream().map(message -> {
            var matcher = KNOWLEDGE_CITATION.matcher(message.content());
            return matcher.find() ? matcher.group(1) : null;
        }).filter(java.util.Objects::nonNull).findFirst().orElse(null);
    }

    private String customerId(String text) {
        if (text == null) return null;
        Matcher matcher = CUSTOMER_ID.matcher(text);
        return matcher.find() ? matcher.group(1) : null;
    }

    private String jsonString(String source, String field) {
        var matcher = Pattern.compile("\\\"" + Pattern.quote(field) + "\\\"\\s*:\\s*\\\"((?:\\\\.|[^\\\"\\\\])*)\\\"")
                .matcher(source);
        return matcher.find() ? matcher.group(1).replace("\\\\n", "\n").replace("\\\\\"", "\"").replace("\\\\\\\\", "\\\\") : null;
    }

    private int jsonNumber(String source, String field) {
        var matcher = Pattern.compile("\\\"" + Pattern.quote(field) + "\\\"\\s*:\\s*(\\d+)").matcher(source);
        return matcher.find() ? Integer.parseInt(matcher.group(1)) : 0;
    }

    private String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }
}
// 本文件负责实现 EAF 的 DeterministicModelGateway.java 相关代码。
