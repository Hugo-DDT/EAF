package io.eaf.agentprotocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.capability.api.CapabilityDefinition;
import io.eaf.capability.api.CapabilityService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.task.api.ConversationPromptContext;
import io.eaf.task.api.CustomerFollowupService;
import io.eaf.task.api.TaskService;
import io.eaf.usage.api.UsageRecord;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** 协议应用层固定资产，并组合 task/runtime/workflow 的公开 API。 */
@Service
public class ConversationApplicationService {
    private static final UUID QA_CAPABILITY = UUID.fromString("54000000-0000-4000-8000-00000000000c");
    private static final UUID CUSTOMER_CAPABILITY = UUID.fromString("54000000-0000-4000-8000-00000000000d");

    private final TaskService conversations;
    private final CapabilityService capabilities;
    private final RuntimeQuery runtime;
    private final TaskApplicationService tasks;
    private final CustomerFollowupService customerFollowups;
    private final ObjectMapper json;

    public ConversationApplicationService(TaskService conversations, CapabilityService capabilities,
                                          RuntimeQuery runtime, TaskApplicationService tasks,
                                          CustomerFollowupService customerFollowups, ObjectMapper json) {
        this.conversations = conversations;
        this.capabilities = capabilities;
        this.runtime = runtime;
        this.tasks = tasks;
        this.customerFollowups = customerFollowups;
        this.json = json;
    }

    public ConversationResponse create(ActorContext actor, UUID workspaceId, CreateRequest request, String idempotencyKey) {
        if (request == null || request.mode() == null)
            throw EafException.invalid("会话 mode 必填。");
        var capabilityId = switch (request.mode()) {
            case "KNOWLEDGE_QA" -> QA_CAPABILITY;
            case "CUSTOMER_ASSISTANT" -> CUSTOMER_CAPABILITY;
            default -> throw EafException.invalid("不支持的会话 mode。");
        };
        if ("CUSTOMER_ASSISTANT".equals(request.mode())
                ? request.customerId() == null || request.customerId().isBlank()
                : request.customerId() != null)
            throw EafException.invalid("客户助手必须绑定客户；知识问答不能绑定客户。");
        // 新建会话固定版本；已有会话继续使用 Task 中保存的绑定快照。
        var capabilityVersion = "CUSTOMER_ASSISTANT".equals(request.mode()) ? "1.2.0" : "1.1.0";
        var capability = capabilities.requirePublished(actor, workspaceId, capabilityId, capabilityVersion);
        return response(conversations.createConversation(new TaskService.CreateConversationCommand(actor, workspaceId,
                request.mode(), request.title() == null ? defaultTitle(request.mode(), request.customerId()) : request.title(),
                request.customerId(), capability.id(), capability.version(), capability.contentHash(), capability.agentId(),
                capability.agentVersion(), capability.skillId(), capability.skillVersion(), capability.skillContentHash(),
                idempotencyKey)));
    }

    public TaskService.ConversationPage list(ActorContext actor, UUID workspaceId, String status,
                                              Instant cursorUpdatedAt, UUID cursorId, int limit) {
        return conversations.listConversations(actor, workspaceId, status, cursorUpdatedAt, cursorId, limit);
    }

    public ConversationResponse get(ActorContext actor, UUID workspaceId, UUID conversationId) {
        return response(conversations.getConversation(actor, workspaceId, conversationId));
    }

    public ConversationResponse update(ActorContext actor, UUID workspaceId, UUID conversationId,
                                       UpdateRequest request) {
        return response(conversations.updateConversation(actor, workspaceId, conversationId,
                request.expectedVersion(), request.title(), request.status()));
    }

    public SendResponse send(ActorContext actor, UUID workspaceId, UUID conversationId, SendRequest request,
                             String idempotencyKey) {
        if (request == null || request.input() == null || request.input().isBlank() || request.input().length() > 8_000)
            throw EafException.invalid("会话输入必须为 1—8,000 字符。");
        var conversation = conversations.getConversation(actor, workspaceId, conversationId);
        var selectedResults = List.<ConversationPromptContext.SelectedFollowupResult>of();
        var requestedResultIds = request.followupResultIds() == null ? List.<UUID>of() : request.followupResultIds();
        var p12CustomerConversation = "CUSTOMER_ASSISTANT".equals(conversation.mode())
                && "1.2.0".equals(conversation.capabilityVersion());
        if (!p12CustomerConversation && !requestedResultIds.isEmpty())
            throw EafException.invalid("仅客户助手会话可以带入团队跟进结果。");
        if (p12CustomerConversation && !requestedResultIds.isEmpty()) {
            selectedResults = customerFollowups.selectForAnalysis(actor, workspaceId, conversation.customerId(),
                    requestedResultIds).stream().map(item -> {
                        var result = item.result();
                        return new ConversationPromptContext.SelectedFollowupResult(result.id(), result.followupId(),
                                result.resultNo(), result.recordedBy(), item.creatorId(), result.outcomeCode(),
                                result.summary(), result.nextAction(), result.nextContactAt(), result.disposition(),
                                result.correctsResultId(), result.createdAt(), item.syncStatus());
                    }).toList();
        }
        var brief = conversations.getConversationBrief(actor, workspaceId, conversationId);
        var prior = conversations.listConversationTurns(actor, workspaceId, conversationId, null, 50).items();
        var history = new ArrayList<ConversationPromptContext.HistoryTurn>();
        var includedTasks = new ArrayList<UUID>();
        for (var turn : prior) {
            if (!"SUCCEEDED".equals(turn.status()) || !runtime.canExposeResult(actor, workspaceId, turn.taskId())) continue;
            var task = conversations.get(actor, workspaceId, turn.taskId());
            var summary = summarize(task.resultJson(), conversation.mode());
            if (summary.isBlank()) continue;
            history.add(new ConversationPromptContext.HistoryTurn(turn.id(), turn.taskId(), turn.input(), summary));
        }
        if (history.size() > 3) history = new ArrayList<>(history.subList(history.size() - 3, history.size()));
        while (history.stream().mapToInt(item -> item.input().length() + item.publicSummary().length()).sum() > 3_000
                && !history.isEmpty()) history.remove(0);
        for (var item : history) includedTasks.add(item.taskId());
        var omitted = Math.max(0, conversation.lastTurnNo() - history.size());
        var turnId = UUID.randomUUID();
        var context = new ConversationPromptContext(conversation.id(), turnId, conversation.mode(),
                conversation.customerId(), brief.revision(), brief.content(), history, omitted, null, null,
                selectedResults);
        final String snapshot;
        try { snapshot = json.writeValueAsString(context); }
        catch (Exception failure) { throw EafException.invalid("会话上下文无法序列化。"); }
        var turn = conversations.createConversationTurn(new TaskService.CreateConversationTurnCommand(actor,
                workspaceId, conversationId, turnId, request.input(), idempotencyKey, conversation.currentBriefRevision(),
                conversation.lastTurnNo(), snapshot, includedTasks));
        var task = tasks.get(actor, workspaceId, turn.taskId());
        return new SendResponse(turn.id(), turn.turnNo(), turn.taskId(), turn.briefRevision(), turn.status(),
                task.result(), task.errorCode(), task.errorDetail(), includedTasks.size(), omitted, selectedResults);
    }

    public TurnPageResponse turns(ActorContext actor, UUID workspaceId, UUID conversationId,
                                  Integer beforeTurnNo, int limit) {
        var page = conversations.listConversationTurns(actor, workspaceId, conversationId, beforeTurnNo, limit);
        var items = page.items().stream().map(turn -> {
            var response = tasks.get(actor, workspaceId, turn.taskId());
            return new TurnResponse(turn.id(), turn.turnNo(), turn.input(), turn.briefRevision(), turn.status(),
                    turn.taskId(), turn.createdAt(), response.result(), response.errorCode(), response.errorDetail());
        }).toList();
        return new TurnPageResponse(items, page.nextBeforeTurnNo());
    }

    public TaskService.ConversationBrief brief(ActorContext actor, UUID workspaceId, UUID conversationId) {
        return conversations.getConversationBrief(actor, workspaceId, conversationId);
    }

    public TaskService.ConversationBriefSave saveBrief(ActorContext actor, UUID workspaceId, UUID conversationId,
                                                        SaveBriefRequest request, String idempotencyKey) {
        var sourceTurns = List.<UUID>of();
        if (request.suggestionTaskId() != null) {
            var conversation = conversations.getConversation(actor, workspaceId, conversationId);
            if ("KNOWLEDGE_QA".equals(conversation.mode()))
                throw EafException.conflict("BRIEF_SUGGESTION_UNAVAILABLE", "知識問答只允許手工维护主题简报。");
            var result = tasks.get(actor, workspaceId, request.suggestionTaskId()).result();
            if (!runtime.canExposeResult(actor, workspaceId, request.suggestionTaskId()))
                throw EafException.conflict("CONTEXT_SNAPSHOT_UNAVAILABLE", "简报建议来源已失效。");
            var suggestion = result == null ? null : result.path("briefSuggestion");
            if (suggestion == null || !suggestion.isObject() || !suggestion.path("baseRevision").canConvertToInt()
                    || suggestion.path("baseRevision").asInt() != request.expectedRevision()
                    || !suggestion.path("content").isTextual() || !suggestion.path("sourceTurnIds").isArray())
                throw EafException.conflict("BRIEF_SUGGESTION_STALE", "当前分析没有可用的同版本简报建议。");
            for (var item : suggestion.path("sourceTurnIds")) {
                if (!item.isTextual()) throw EafException.conflict("BRIEF_SUGGESTION_INVALID", "简报建议来源格式无效。");
                try { sourceTurns = append(sourceTurns, UUID.fromString(item.asText())); }
                catch (IllegalArgumentException invalid) { throw EafException.conflict("BRIEF_SUGGESTION_INVALID", "简报建议来源轮次无效。"); }
            }
        }
        return conversations.saveConversationBrief(new TaskService.SaveConversationBriefCommand(actor, workspaceId,
                conversationId, request.expectedRevision(), request.content(), request.suggestionTaskId(), sourceTurns,
                idempotencyKey));
    }

    @Transactional
    public TaskApplicationService.FollowupResponse confirmFollowup(ActorContext actor, UUID workspaceId,
            UUID conversationId, ConfirmFollowupRequest request, String idempotencyKey) {
        // Task 应用层先按原稳定请求键恢复已创建流程；只在首次创建前锁定并复核会话最新性。
        return tasks.confirmFollowup(actor, workspaceId, request.sourceTaskId(), request.expectedTaskVersion(),
                request.summary(), idempotencyKey, conversationId, request.expectedBriefRevision());
    }

    public AnalysisComparisonResponse comparison(ActorContext actor, UUID workspaceId, UUID conversationId) {
        var conversation = conversations.getConversation(actor, workspaceId, conversationId);
        if (!"CUSTOMER_ASSISTANT".equals(conversation.mode()))
            throw EafException.conflict("ANALYSIS_UNAVAILABLE", "知识问答会话没有客户分析对比。");
        var recent = conversations.listConversationTurns(actor, workspaceId, conversationId, null, 50).items();
        var analyses = new ArrayList<AnalysisPoint>();
        for (var turn : recent) {
            if (!"SUCCEEDED".equals(turn.status()) || !runtime.canExposeResult(actor, workspaceId, turn.taskId())) continue;
            var task = tasks.get(actor, workspaceId, turn.taskId());
            var result = task.result();
            if (result == null || result.hasNonNull("clarificationQuestion") || result.path("riskLevel").asText().isBlank()) continue;
            analyses.add(new AnalysisPoint(turn, task, result));
        }
        analyses.sort(Comparator.comparingInt(item -> item.turn().turnNo()));
        if (analyses.size() < 2) return AnalysisComparisonResponse.unavailable();
        var before = analyses.get(analyses.size() - 2);
        var after = analyses.get(analyses.size() - 1);
        var beforeMissing = strings(before.result().path("uncertainties"));
        var afterMissing = strings(after.result().path("uncertainties"));
        var beforeDraft = text(before.result().path("followupDraft").path("summary"));
        var afterDraft = text(after.result().path("followupDraft").path("summary"));
        return new AnalysisComparisonResponse(true, before.task().id(), after.task().id(), before.turn().briefRevision(),
                after.turn().briefRevision(), before.result().path("riskLevel").asText("UNKNOWN"),
                after.result().path("riskLevel").asText("UNKNOWN"), difference(afterMissing, beforeMissing),
                difference(beforeMissing, afterMissing), beforeDraft, afterDraft,
                text(before.result().path("summary")), text(after.result().path("summary")));
    }

    public ConversationUsageResponse usage(ActorContext actor, UUID workspaceId, UUID conversationId) {
        conversations.getConversation(actor, workspaceId, conversationId);
        var turns = conversations.listConversationTurns(actor, workspaceId, conversationId, null, 50).items();
        var calls = 0;
        var input = 0;
        var output = 0;
        var knownRows = 0;
        var totalRows = 0;
        var costs = new LinkedHashMap<String, UsageAccumulator>();
        for (var turn : turns) {
            var usage = tasks.usage(actor, workspaceId, turn.taskId());
            calls += usage.calls();
            totalRows += usage.calls();
            if (usage.inputTokens() != null && usage.outputTokens() != null) {
                input += usage.inputTokens(); output += usage.outputTokens(); knownRows += usage.calls();
            }
            for (var cost : usage.costs()) {
                var key = String.valueOf(cost.currency()) + "\u001f" + cost.status();
                var accumulator = costs.computeIfAbsent(key, ignored -> new UsageAccumulator(cost.currency(), cost.status()));
                accumulator.calls += cost.calls();
                if (cost.amount() == null) accumulator.known = false;
                else accumulator.amount = accumulator.amount.add(cost.amount());
            }
        }
        var tokenStatus = totalRows == 0 ? "NOT_RECORDED" : knownRows == totalRows ? "KNOWN" : knownRows == 0 ? "UNKNOWN" : "PARTIAL";
        var projectedCosts = costs.values().stream().map(item -> new ConversationUsageCost(item.currency,
                item.known ? item.amount : null, item.status, item.calls)).toList();
        return new ConversationUsageResponse(calls, knownRows == 0 ? null : input, knownRows == 0 ? null : output,
                tokenStatus, projectedCosts);
    }

    public List<TaskApplicationService.FollowupResponse> followups(ActorContext actor, UUID workspaceId,
                                                                    UUID conversationId) {
        conversations.getConversation(actor, workspaceId, conversationId);
        return conversations.listConversationTurns(actor, workspaceId, conversationId, null, 50).items().stream()
                .flatMap(turn -> tasks.followups(actor, workspaceId, turn.taskId(), null, null, 50).items().stream())
                .sorted(Comparator.comparing(TaskApplicationService.FollowupResponse::createdAt).reversed()).toList();
    }

    private ConversationResponse response(TaskService.ConversationSnapshot value) {
        return new ConversationResponse(value.id(), value.mode(), value.title(), value.customerId(), value.status(),
                value.currentBriefRevision(), value.version(), value.lastTurnNo(), value.activeTaskId(),
                value.activeTaskStatus(), value.createdAt(), value.updatedAt(),
                value.capabilityId(), value.capabilityVersion());
    }

    private String summarize(String rawResult, String mode) {
        if (rawResult == null || rawResult.isBlank()) return "";
        try {
            var result = json.readTree(rawResult);
            if ("KNOWLEDGE_QA".equals(mode)) {
                if (result.hasNonNull("clarificationQuestion")) return "";
                var answer = text(result.path("answer"));
                var missing = strings(result.path("missingInformation"));
                return answer + (missing.isEmpty() ? "" : "\n待补充：" + String.join("；", missing));
            }
            if (result.hasNonNull("clarificationQuestion")) return "";
            var summary = text(result.path("summary"));
            var risk = text(result.path("riskLevel"));
            var uncertainties = strings(result.path("uncertainties"));
            return summary + (risk.isBlank() ? "" : "\n风险意见：" + risk)
                    + (uncertainties.isEmpty() ? "" : "\n待核实：" + String.join("；", uncertainties));
        } catch (Exception invalid) { return ""; }
    }

    private String defaultTitle(String mode, String customerId) {
        return "CUSTOMER_ASSISTANT".equals(mode) ? "客户 · " + customerId : "知识问答 · 新会话";
    }

    private List<UUID> append(List<UUID> values, UUID next) {
        var result = new ArrayList<>(values); result.add(next); return List.copyOf(result);
    }

    private String text(JsonNode node) { return node != null && node.isTextual() ? node.asText() : ""; }

    private List<String> strings(JsonNode node) {
        if (node == null || !node.isArray()) return List.of();
        var result = new ArrayList<String>(); node.forEach(item -> { if (item.isTextual()) result.add(item.asText()); });
        return List.copyOf(result);
    }

    private List<String> difference(List<String> left, List<String> right) {
        return left.stream().filter(item -> !right.contains(item)).toList();
    }

    private record AnalysisPoint(TaskService.ConversationTurn turn, TaskResponse task, JsonNode result) { }

    private static final class UsageAccumulator {
        final String currency;
        final String status;
        BigDecimal amount = BigDecimal.ZERO;
        int calls;
        boolean known = true;
        UsageAccumulator(String currency, String status) { this.currency = currency; this.status = status; }
    }

    public record CreateRequest(String mode, String title, String customerId) { }
    public record UpdateRequest(long expectedVersion, String title, String status) { }
    public record SaveBriefRequest(int expectedRevision, String content, UUID suggestionTaskId) { }
    public record SendRequest(String input, List<UUID> followupResultIds) { }
    public record ConfirmFollowupRequest(UUID sourceTaskId, long expectedTaskVersion, int expectedBriefRevision,
                                         String summary) { }
    public record ConversationResponse(UUID id, String mode, String title, String customerId, String status,
                                      int currentBriefRevision, long version, int lastTurnNo, UUID activeTaskId,
                                      String activeTaskStatus, Instant createdAt, Instant updatedAt,
                                      UUID capabilityId, String capabilityVersion) { }
    public record TurnResponse(UUID id, int turnNo, String input, int briefRevision, String status, UUID taskId,
                               Instant createdAt, JsonNode result, String errorCode, String errorDetail) { }
    public record TurnPageResponse(List<TurnResponse> items, int nextBeforeTurnNo) { }
    public record SendResponse(UUID turnId, int turnNo, UUID taskId, int briefRevision, String status,
                               JsonNode result, String errorCode, String errorDetail,
                               int includedTurnCount, int omittedTurnCount,
                               List<ConversationPromptContext.SelectedFollowupResult> includedFollowupResults) { }
    public record AnalysisComparisonResponse(boolean available, UUID beforeTaskId, UUID afterTaskId,
            Integer beforeBriefRevision, Integer afterBriefRevision, String beforeRiskLevel, String afterRiskLevel,
            List<String> addedMissingInformation, List<String> resolvedMissingInformation,
            String beforeDraft, String afterDraft, String beforeSummary, String afterSummary) {
        static AnalysisComparisonResponse unavailable() {
            return new AnalysisComparisonResponse(false, null, null, null, null, null, null,
                    List.of(), List.of(), null, null, null, null);
        }
    }
    public record ConversationUsageCost(String currency, BigDecimal amount, String status, int calls) { }
    public record ConversationUsageResponse(int calls, Integer inputTokens, Integer outputTokens, String tokenStatus,
                                             List<ConversationUsageCost> costs) { }
}
