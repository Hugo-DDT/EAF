package io.eaf.context.infrastructure;

import io.eaf.context.api.ContextItem;
import io.eaf.context.api.ContextQuery;
import io.eaf.context.api.ContextService;
import io.eaf.context.api.ContextTaskScope;
import io.eaf.context.api.EnterpriseContext;
import io.eaf.context.api.ExperienceUsage;
import io.eaf.context.api.TeamExperienceUsage;
import io.eaf.knowledge.api.KnowledgeSearchHit;
import io.eaf.knowledge.api.KnowledgeSearchResult;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.memory.api.MemoryDefinition;
import io.eaf.memory.api.MemoryService;
import io.eaf.memory.api.ExperienceCardService;
import io.eaf.memory.api.TeamExperienceService;
import io.eaf.memory.api.TeamExperienceService.ExperienceRef;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.shared.ActorType;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EnterpriseContextService implements ContextService {
    private static final int DEFAULT_TOP_K = 5;
    private static final int MAX_TOKEN_BUDGET = 2_000;
    private final KnowledgeService knowledge;
    private final MemoryService memories;
    private final ExperienceCardService experiences;
    private final TeamExperienceService teamExperiences;
    private final WorkspaceAuthorization workspaces;

    public EnterpriseContextService(KnowledgeService knowledge, MemoryService memories, WorkspaceAuthorization workspaces) {
        this(knowledge, memories, workspaces, memories instanceof ExperienceCardService cards ? cards : null, null);
    }

    public EnterpriseContextService(KnowledgeService knowledge, MemoryService memories, WorkspaceAuthorization workspaces,
                                   ExperienceCardService experiences) {
        this(knowledge, memories, workspaces, experiences, null);
    }

    @Autowired
    public EnterpriseContextService(KnowledgeService knowledge, MemoryService memories, WorkspaceAuthorization workspaces,
                                    ExperienceCardService experiences, TeamExperienceService teamExperiences) {
        this.knowledge = knowledge;
        this.memories = memories;
        this.experiences = experiences;
        this.workspaces = workspaces;
        this.teamExperiences = teamExperiences;
    }

    @Override
    public EnterpriseContext query(ActorContext actor, UUID workspaceId, ContextQuery request) {
        return query(actor, workspaceId, request, null);
    }

    @Override
    public EnterpriseContext query(ActorContext actor, UUID workspaceId, ContextQuery request, ContextTaskScope taskScope) {
        if (request == null || request.query() == null || request.query().isBlank())
            throw EafException.invalid("context query 不能为空。");
        if ((request.businessEntityType() == null) != (request.businessEntityId() == null)
                || request.businessEntityType() != null && (request.businessEntityType().isBlank()
                || request.businessEntityType().length() > 80 || request.businessEntityId().isBlank()
                || request.businessEntityId().length() > 160))
            throw EafException.invalid("业务实体类型与标识必须同时提供且不超过长度限制。");
        var topK = request.topK() == null ? DEFAULT_TOP_K : request.topK();
        var budget = request.tokenBudget() == null ? MAX_TOKEN_BUDGET : request.tokenBudget();
        if (topK < 1 || topK > 10) throw EafException.invalid("context topK 必须在 1-10 之间。");
        if (budget < 1 || budget > MAX_TOKEN_BUDGET) throw EafException.invalid("tokenBudget 必须在 1-2000 之间。");
        requireMemoryPolicy(request, topK, budget);
        requireMcpReadonlyRequest(actor, request);

        // context 有自己的入口授权，再由 knowledge 对每个文档执行 read 过滤，拒绝借上下文绕过边界。
        workspaces.require(actor, workspaceId, "context:read");
        if (taskScope != null && (!actor.tenantId().equals(taskScope.tenantId())
                || !actor.actorId().equals(taskScope.actorId()) || !workspaceId.equals(taskScope.workspaceId())
                || taskScope.taskId() == null || taskScope.runId() == null || taskScope.source() == null))
            throw EafException.forbidden("Context Task 计量归属与当前委托不一致。");
        var knowledgeReadable = workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "knowledge:read");
        var personalExperience = personalExperience(request);
        var searchTopK = personalExperience ? 5 : topK;
        KnowledgeSearchResult search = knowledgeReadable
                ? knowledge.search(actor, workspaceId, request.query(), searchTopK, knowledgeScope(taskScope),
                        request.retrievalMode() == null ? "VECTOR" : request.retrievalMode())
                : new KnowledgeSearchResult(topK, java.util.List.of());
        if (personalExperience) {
            var candidates = new ArrayList<ContextItem>();
            var seen = new HashSet<UUID>();
            for (var hit : search.hits()) {
                if (!seen.add(hit.chunkId())) continue;
                candidates.add(item(hit, candidates.size() + 1, estimateTokens(hit.content())));
            }
            return assemblePersonalExperience(actor, workspaceId, request, candidates, 0);
        }
        var items = new ArrayList<ContextItem>();
        Set<UUID> seenChunks = new HashSet<>();
        var used = 0;
        var omitted = 0;
        for (var hit : search.hits()) {
            if (!seenChunks.add(hit.chunkId())) { omitted++; continue; }
            var tokens = estimateTokens(hit.content());
            if (used + tokens > budget) { omitted++; continue; }
            used += tokens;
            items.add(item(hit, items.size() + 1, tokens));
        }
        // Memory 没有向量排序；仅在明确的实体绑定与剩余预算内，按模块给出的稳定顺序补入。
        if (!mcpReadonly(actor) && items.size() < topK && memories != null
                && workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "memory:read")) {
            for (var memory : memories.findApplicable(actor, workspaceId, request.businessEntityType(), request.businessEntityId())) {
                var tokens = estimateTokens(memory.content());
                if (used + tokens > budget) { omitted++; continue; }
                used += tokens;
                items.add(item(memory, items.size() + 1, tokens));
                if (items.size() == topK) break;
            }
        }
        if (items.isEmpty() && omitted == 0)
            return new EnterpriseContext("NO_EVIDENCE", topK, budget, 0, 0, null, java.util.List.of());
        var status = items.isEmpty() ? "BUDGET_EXHAUSTED" : omitted == 0 ? "READY" : "BUDGET_TRUNCATED";
        return new EnterpriseContext(status, topK, budget, used, omitted,
                omitted == 0 ? null : "TOKEN_BUDGET", java.util.List.copyOf(items));
    }

    @Override
    public EnterpriseContext resolveTeamExperiences(ActorContext actor, UUID workspaceId, String scenarioKey,
                                                     java.util.List<ExperienceRef> selections) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("团队经验上下文只允许直接 HUMAN 身份。");
        workspaces.require(actor, workspaceId, "memory:read");
        if (teamExperiences == null || scenarioKey == null || !scenarioKey.matches("[a-z][a-z0-9-]{0,63}")
                || selections == null || selections.isEmpty() || selections.size() > 3
                || selections.stream().anyMatch(ref -> ref == null || ref.cardId() == null || ref.revision() < 1)
                || selections.stream().map(ExperienceRef::cardId).distinct().count() != selections.size())
            throw EafException.invalid("团队经验选择必须是同场景下 1 到 3 个不重复的版本。");
        var items = new ArrayList<ContextItem>();
        var included = new ArrayList<TeamExperienceUsage.Included>();
        var used = 0;
        var chars = 0;
        for (var ref : selections) {
            var selected = teamExperiences.requireCurrent(actor, workspaceId, scenarioKey, ref.cardId(), ref.revision());
            var content = "团队经验：" + selected.title() + "\n适用条件：" + selected.appliesWhen()
                    + "\n建议：" + selected.content();
            chars += content.length();
            if (chars > 3_000) throw EafException.invalid("所选团队经验格式化正文合计不能超过 3000 字符。");
            var tokens = estimateTokens(content);
            used += tokens;
            var citation = "team-experience-" + (items.size() + 1);
            var memory = selected.memory();
            items.add(new ContextItem(citation, "MEMORY", null, 0, null, null, memory.id(), memory.version(),
                    memory.sourceRef(), memory.contentHash(), content, null, tokens, "TEAM", memory.evidenceRefs(),
                    memory.expiresAt(), "SERVICE_REQUEST", scenarioKey));
            included.add(new TeamExperienceUsage.Included(selected.cardId(), selected.revision(), memory.id(),
                    memory.version(), memory.contentHash()));
        }
        return new EnterpriseContext("READY", items.size(), 2_000, used, 0, null, List.copyOf(items), null,
                new TeamExperienceUsage(included));
    }

    @Override
    public EnterpriseContext prepare(ActorContext actor, UUID workspaceId, ContextQuery request, ContextTaskScope taskScope) {
        validateRequest(actor, workspaceId, request, taskScope);
        var topK = request.topK() == null ? DEFAULT_TOP_K : request.topK();
        var budget = request.tokenBudget() == null ? MAX_TOKEN_BUDGET : request.tokenBudget();
        var canReadKnowledge = workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "knowledge:read");
        if (!canReadKnowledge) return new EnterpriseContext("NO_EVIDENCE", topK, budget, 0, 0, null, java.util.List.of());
        var search = knowledge.search(actor, workspaceId, request.query(), Math.min(10, topK), knowledgeScope(taskScope),
                request.retrievalMode() == null ? "VECTOR" : request.retrievalMode());
        var items = new ArrayList<ContextItem>();
        var used = 0;
        var omitted = 0;
        var seen = new HashSet<UUID>();
        for (var hit : search.hits()) {
            if (!seen.add(hit.chunkId())) { omitted++; continue; }
            var tokens = estimateTokens(hit.content());
            if (used + tokens > budget) { omitted++; continue; }
            used += tokens;
            items.add(item(hit, items.size() + 1, tokens));
        }
        var status = items.isEmpty() ? "NO_EVIDENCE" : omitted == 0 ? "READY" : "BUDGET_TRUNCATED";
        return new EnterpriseContext(status, topK, budget, used, omitted,
                omitted == 0 ? null : "TOKEN_BUDGET", java.util.List.copyOf(items));
    }

    @Override
    public EnterpriseContext assemble(ActorContext actor, UUID workspaceId, EnterpriseContext candidates,
                                      java.util.Map<String, String> selections, ContextQuery request,
                                      ContextTaskScope taskScope) {
        validateRequest(actor, workspaceId, request, taskScope);
        if (candidates == null || candidates.items() == null || candidates.items().size() > 10
                || !isCurrent(actor, workspaceId, candidates))
            throw EafException.conflict("CONTEXT_CANDIDATES_UNAVAILABLE", "证据候选已失效或无法验证。");
        var allowed = candidates.items().stream().map(ContextItem::citationId).collect(java.util.stream.Collectors.toSet());
        if (selections != null && selections.keySet().stream().anyMatch(id -> !allowed.contains(id)))
            throw EafException.invalid("证据判断包含本次候选之外的片段。");
        var topK = request.topK() == null ? DEFAULT_TOP_K : request.topK();
        var budget = request.tokenBudget() == null ? MAX_TOKEN_BUDGET : request.tokenBudget();
        if (personalExperience(request)) {
            var selectedKnowledge = new ArrayList<ContextItem>();
            var omittedKnowledge = 0;
            var seen = new HashSet<String>();
            for (var item : candidates.items()) {
                var choice = selections == null ? "ANSWERS" : selections.get(item.citationId());
                if (!"ANSWERS".equals(choice) && !"CONTRADICTS".equals(choice)
                        || !"KNOWLEDGE".equals(item.sourceType()) || !seen.add(item.citationId())) {
                    omittedKnowledge++;
                    continue;
                }
                if (selectedKnowledge.size() >= 5) { omittedKnowledge++; continue; }
                selectedKnowledge.add(item);
            }
            return assemblePersonalExperience(actor, workspaceId, request, selectedKnowledge, omittedKnowledge);
        }
        var selected = new ArrayList<ContextItem>();
        var used = 0;
        var omitted = 0;
        for (var item : candidates.items()) {
            var choice = selections == null ? "ANSWERS" : selections.get(item.citationId());
            if (!"ANSWERS".equals(choice) && !"CONTRADICTS".equals(choice)) { omitted++; continue; }
            if (selected.size() >= Math.min(5, topK)) { omitted++; continue; }
            if (used + item.estimatedTokens() > budget) { omitted++; continue; }
            used += item.estimatedTokens();
            selected.add(item);
        }
        if (!mcpReadonly(actor) && selected.size() < topK && memories != null
                && workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "memory:read")) {
            for (var memory : memories.findApplicable(actor, workspaceId, request.businessEntityType(), request.businessEntityId())) {
                var tokens = estimateTokens(memory.content());
                if (used + tokens > budget) { omitted++; continue; }
                used += tokens;
                selected.add(item(memory, selected.size() + 1, tokens));
                if (selected.size() == topK) break;
            }
        }
        var status = selected.isEmpty() ? "NO_EVIDENCE" : omitted == 0 ? "READY" : "BUDGET_TRUNCATED";
        return new EnterpriseContext(status, topK, budget, used, omitted,
                omitted == 0 ? null : "EVIDENCE_SELECTION_OR_TOKEN_BUDGET", java.util.List.copyOf(selected));
    }

    private void validateRequest(ActorContext actor, UUID workspaceId, ContextQuery request, ContextTaskScope taskScope) {
        if (request == null || request.query() == null || request.query().isBlank())
            throw EafException.invalid("context query 不能为空。");
        if ((request.businessEntityType() == null) != (request.businessEntityId() == null)
                || request.businessEntityType() != null && (request.businessEntityType().isBlank()
                || request.businessEntityType().length() > 80 || request.businessEntityId().isBlank()
                || request.businessEntityId().length() > 160))
            throw EafException.invalid("业务实体类型与标识必须同时提供且不超过长度限制。");
        var topK = request.topK() == null ? DEFAULT_TOP_K : request.topK();
        var budget = request.tokenBudget() == null ? MAX_TOKEN_BUDGET : request.tokenBudget();
        requireMemoryPolicy(request, topK, budget);
        requireMcpReadonlyRequest(actor, request);
        if (topK < 1 || topK > 10) throw EafException.invalid("context topK 必须在 1-10 之间。");
        if (budget < 1 || budget > MAX_TOKEN_BUDGET) throw EafException.invalid("tokenBudget 必须在 1-2000 之间。");
        workspaces.require(actor, workspaceId, "context:read");
        if (taskScope != null && (!actor.tenantId().equals(taskScope.tenantId())
                || !actor.actorId().equals(taskScope.actorId()) || !workspaceId.equals(taskScope.workspaceId())
                || taskScope.taskId() == null || taskScope.runId() == null || taskScope.source() == null))
            throw EafException.forbidden("Context Task 计量归属与当前委托不一致。");
    }

    private io.eaf.knowledge.api.KnowledgeSearchScope knowledgeScope(ContextTaskScope scope) {
        if (scope == null) return null;
        var scopeType = scope.qualityRunId() != null || "EVALUATION".equals(scope.source()) ? "EVALUATION" : "TASK";
        return new io.eaf.knowledge.api.KnowledgeSearchScope(scope.tenantId(), scope.workspaceId(), scope.actorId(),
                scope.taskId(), scope.runId(), scope.source(), scopeType,
                scope.qualityRunId() != null ? scope.qualityRunId()
                        : scope.rootTaskId() == null ? scope.taskId() : scope.rootTaskId(), 0);
    }

    @Override
    public boolean isCurrent(ActorContext actor, UUID workspaceId, EnterpriseContext context) {
        workspaces.require(actor, workspaceId, "context:read");
        if (context == null || context.items() == null) return false;
        if (context.teamExperienceUsage() != null) {
            if (teamExperiences == null || context.teamExperienceUsage().included().size() != context.items().size()) return false;
            var scenario = context.items().isEmpty() ? null : context.items().getFirst().businessEntityId();
            if (scenario == null) return false;
            for (var included : context.teamExperienceUsage().included()) {
                var selected = teamExperiences.requireCurrent(actor, workspaceId, scenario,
                        included.cardId(), included.revision());
                if (!selected.memory().id().equals(included.memoryId())
                        || !selected.memory().version().equals(included.memoryVersion())
                        || !selected.memory().contentHash().equals(included.contentHash())) return false;
            }
        }
        return context.items().stream().allMatch(item -> {
            if (item == null) return false;
            var sourceType = item.sourceType() == null ? "KNOWLEDGE" : item.sourceType();
            try {
                if ("KNOWLEDGE".equals(sourceType))
                    return item.documentId() != null && item.chunkId() != null && item.buildId() != null
                            && knowledge.isUsable(actor, workspaceId, item.documentId(), item.documentVersion(),
                            item.chunkId(), item.buildId(), item.contentHash());
                if (!mcpReadonly(actor) && "MEMORY".equals(sourceType) && memories != null
                        && item.memoryId() != null && item.memoryVersion() != null) {
                    var current = memories.requireUsable(actor, workspaceId, item.memoryId(), item.memoryVersion());
                    return current.contentHash().equals(item.contentHash());
                }
                return false;
            } catch (RuntimeException unavailableOrDenied) { return false; }
        });
    }

    private ContextItem item(KnowledgeSearchHit hit, int order, int tokens) {
        return new ContextItem("kb-" + order, "KNOWLEDGE", hit.documentId(), hit.documentVersion(), hit.chunkId(), hit.buildId(),
                null, null, hit.sourceRef(), hit.contentHash(), hit.content(), hit.distance(), tokens, null, null, null, null, null,
                hit.headingPath(), hit.startOffset(), hit.endOffset(), hit.offsetUnit());
    }

    private ContextItem item(MemoryDefinition memory, int order, int tokens) {
        return new ContextItem("mem-" + order, "MEMORY", null, 0, null, null, memory.id(), memory.version(),
                memory.sourceRef(), memory.contentHash(), memory.content(), null, tokens, memory.scope(),
                memory.evidenceRefs(), memory.expiresAt(), memory.businessEntityType(), memory.businessEntityId());
    }

    private boolean personalExperience(ContextQuery request) {
        return request != null && "PERSONAL_EXPERIENCE_V1".equals(request.memoryPolicy());
    }

    private void requireMcpReadonlyRequest(ActorContext actor, ContextQuery request) {
        if (mcpReadonly(actor) && (request.businessEntityType() != null || personalExperience(request)))
            throw EafException.forbidden("MCP 只读委托只允许读取所选 Knowledge，不允许业务实体或 Memory 上下文。");
    }

    private boolean mcpReadonly(ActorContext actor) {
        return actor != null && actor.delegated() && "eaf:mcp".equals(actor.delegationAudience());
    }

    private void requireMemoryPolicy(ContextQuery request, int topK, int budget) {
        var policy = request.memoryPolicy() == null ? "LEGACY" : request.memoryPolicy();
        if (!Set.of("LEGACY", "PERSONAL_EXPERIENCE_V1").contains(policy))
            throw EafException.invalid("Context memoryPolicy 无效。");
        if ("PERSONAL_EXPERIENCE_V1".equals(policy) && (topK != 8 || budget != MAX_TOKEN_BUDGET))
            throw EafException.invalid("经验上下文固定为 topK=8 与 tokenBudget=2000。");
    }

    private EnterpriseContext assemblePersonalExperience(ActorContext actor, UUID workspaceId, ContextQuery request,
                                                          java.util.List<ContextItem> knowledgeCandidates,
                                                          int initiallyOmittedKnowledge) {
        var budget = MAX_TOKEN_BUDGET;
        var used = 0;
        var omittedByLimit = 0;
        var omittedByBudget = 0;
        var omittedKnowledge = initiallyOmittedKnowledge;
        var items = new ArrayList<ContextItem>();
        var included = new ArrayList<ExperienceUsage.IncludedExperience>();
        var canReadPersonal = experiences != null && actor.type() == ActorType.HUMAN && !actor.delegated()
                && workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "memory:read");
        if (canReadPersonal) {
            var customerId = "CUSTOMER".equals(request.businessEntityType()) ? request.businessEntityId() : null;
            var cards = experiences.findApplicable(actor, workspaceId, customerId, 100);
            omittedByLimit = Math.max(0, cards.size() - 3);
            var selectedCards = 0;
            for (var card : cards) {
                if (selectedCards >= 3) break;
                var content = card.title() + "\n" + card.memory().content();
                var tokens = estimateTokens(content);
                if (tokens > 600 || used + tokens > 600 || used + tokens > budget) {
                    omittedByBudget++;
                    continue;
                }
                used += tokens;
                selectedCards++;
                var citation = "mem-" + selectedCards;
                items.add(new ContextItem(citation, "MEMORY", null, 0, null, null, card.cardId(),
                        card.memoryVersion(), card.memory().sourceRef(), card.memory().contentHash(), content,
                        null, tokens, "PERSONAL", card.memory().evidenceRefs(), card.memory().expiresAt(),
                        card.memory().businessEntityType(), card.memory().businessEntityId()));
                included.add(new ExperienceUsage.IncludedExperience(card.cardId(), card.revision(),
                        card.memoryVersion(), citation, card.applicability()));
            }
        }
        var selectedKnowledge = 0;
        var seenChunks = new HashSet<UUID>();
        for (var candidate : knowledgeCandidates) {
            if (candidate.chunkId() != null && !seenChunks.add(candidate.chunkId())) { omittedKnowledge++; continue; }
            if (selectedKnowledge >= 5 || items.size() >= 8) { omittedKnowledge++; continue; }
            if (used + candidate.estimatedTokens() > budget) { omittedKnowledge++; continue; }
            used += candidate.estimatedTokens();
            selectedKnowledge++;
            items.add(candidate);
        }
        var omitted = omittedByLimit + omittedByBudget + omittedKnowledge;
        var status = items.isEmpty() ? "NO_EVIDENCE" : omitted == 0 ? "READY" : "BUDGET_TRUNCATED";
        var usage = new ExperienceUsage(included, java.util.List.of(), omittedByLimit, omittedByBudget);
        return new EnterpriseContext(status, 8, budget, used, omitted,
                omitted == 0 ? null : "EXPERIENCE_LIMIT_OR_TOKEN_BUDGET", java.util.List.copyOf(items), usage);
    }

    private int estimateTokens(String content) { return Math.max(1, (content.codePointCount(0, content.length()) + 3) / 4); }
}
// 预算裁剪只丢弃完整 Chunk；如需摘要或跨片段压缩，应先建立独立、可评测的派生能力。
