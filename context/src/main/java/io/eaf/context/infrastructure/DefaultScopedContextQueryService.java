package io.eaf.context.infrastructure;

import io.eaf.context.api.ScopedContextItem;
import io.eaf.context.api.ScopedContextOrigin;
import io.eaf.context.api.ScopedContextOmitted;
import io.eaf.context.api.ScopedContextQueryService;
import io.eaf.context.api.ScopedContextResult;
import io.eaf.context.api.ScopedContextSource;
import io.eaf.knowledge.api.KnowledgeSearchHit;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.knowledge.api.ScopedKnowledgeSearchHit;
import io.eaf.memory.api.MemoryDefinition;
import io.eaf.memory.api.MemoryService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.workspace.api.ResolvedContextSources;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.eaf.workspace.api.WorkspaceCatalog;
import io.eaf.workspace.api.WorkspaceSummary;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class DefaultScopedContextQueryService implements ScopedContextQueryService {
    private static final int DEFAULT_TOP_K = 5;
    private static final int DEFAULT_TOKEN_BUDGET = 2_000;
    private final WorkspaceCatalog catalog;
    private final WorkspaceAuthorization authorization;
    private final KnowledgeService knowledge;
    private final MemoryService memories;

    public DefaultScopedContextQueryService(WorkspaceCatalog catalog, WorkspaceAuthorization authorization,
                                            KnowledgeService knowledge, MemoryService memories) {
        this.catalog = catalog;
        this.authorization = authorization;
        this.knowledge = knowledge;
        this.memories = memories;
    }

    @Override
    public ScopedContextResult query(ActorContext actor, UUID targetWorkspaceId, String query,
                                     Integer requestedTopK, Integer requestedBudget,
                                     List<UUID> sourceWorkspaceIds) {
        requireHuman(actor);
        if (query == null || query.isBlank()) throw EafException.invalid("query 不能为空。");
        var topK = requestedTopK == null ? DEFAULT_TOP_K : requestedTopK;
        var budget = requestedBudget == null ? DEFAULT_TOKEN_BUDGET : requestedBudget;
        if (topK < 1 || topK > 10) throw EafException.invalid("topK 必须在 1-10 之间。");
        if (budget < 1 || budget > DEFAULT_TOKEN_BUDGET) throw EafException.invalid("tokenBudget 必须在 1-2000 之间。");

        var resolved = catalog.resolveContextSources(actor, targetWorkspaceId, sourceWorkspaceIds);
        var knowledgeBySource = new ArrayList<List<Candidate>>();
        var memoryBySource = new ArrayList<List<Candidate>>();
        for (var source : resolved.sources()) {
            // 每个来源先检查自己的动作；源空间被选中或目标空间可读都不会代替 Knowledge/Memory 权限。
            var knowledgeCandidates = authorization.isAuthorized(actor.tenantId(), actor.actorId(), source.workspaceId(), "knowledge:read")
                    ? knowledge.searchForScopedContext(actor, source.workspaceId(), query, topK).hits().stream()
                            .map(hit -> Candidate.knowledge(source, hit, estimateTokens(hit.hit().content()))).toList()
                    : List.<Candidate>of();
            knowledgeBySource.add(knowledgeCandidates);
            var memoryCandidates = authorization.isAuthorized(actor.tenantId(), actor.actorId(), source.workspaceId(), "memory:read")
                    ? memories.findApplicable(actor, source.workspaceId(), null, null).stream().limit(topK)
                            .map(memory -> Candidate.memory(source, memory, estimateTokens(memory.content()))).toList()
                    : List.<Candidate>of();
            memoryBySource.add(memoryCandidates);
        }

        var ordered = new ArrayList<Candidate>();
        var maxKnowledge = knowledgeBySource.stream().mapToInt(List::size).max().orElse(0);
        for (var rank = 0; rank < maxKnowledge; rank++)
            for (var source = 0; source < knowledgeBySource.size(); source++)
                if (rank < knowledgeBySource.get(source).size()) ordered.add(knowledgeBySource.get(source).get(rank));
        // Knowledge 按各自排名轮转，避免比较不同来源的距离；普通 Memory 按来源顺序补入。
        memoryBySource.forEach(ordered::addAll);

        var current = new LinkedHashMap<String, Aggregate>();
        var staleCount = 0;
        var duplicateCount = 0;
        for (var candidate : ordered) {
            // 结果返回前重新询问资源 Owner；撤权、换版或过期会剔除当前出处。
            if (!isCurrent(actor, candidate)) { staleCount++; continue; }
            var key = candidate.dedupeKey();
            var aggregate = current.get(key);
            if (aggregate == null) current.put(key, new Aggregate(candidate));
            else {
                duplicateCount++;
                aggregate.origins.add(candidate.origin());
            }
        }

        var itemLimitCount = 0;
        var tokenBudgetCount = 0;
        var used = 0;
        var selected = new ArrayList<Aggregate>();
        for (var candidate : current.values()) {
            if (selected.size() >= topK) { itemLimitCount++; continue; }
            // 超预算条目跳过后继续尝试短条目，不截断原文或破坏引用。
            if (candidate.candidate.estimatedTokens() > budget - used) {
                tokenBudgetCount++;
                continue;
            }
            used += candidate.candidate.estimatedTokens();
            selected.add(candidate);
        }
        var items = new ArrayList<ScopedContextItem>();
        var sourceCounts = new LinkedHashMap<UUID, Integer>();
        for (var index = 0; index < selected.size(); index++) {
            var aggregate = selected.get(index);
            items.add(new ScopedContextItem("ctx-" + (index + 1), aggregate.candidate.sourceType(),
                    aggregate.candidate.content(), aggregate.candidate.estimatedTokens(), List.copyOf(aggregate.origins)));
            aggregate.origins.forEach(origin -> sourceCounts.merge(origin.sourceWorkspaceId(), 1, Integer::sum));
        }
        var sources = resolved.sources().stream().map(source -> new ScopedContextSource(source.workspaceId(), source.name(),
                source.kind().name(), sourceCounts.getOrDefault(source.workspaceId(), 0))).toList();
        var status = items.isEmpty()
                ? tokenBudgetCount > 0 ? "BUDGET_EXHAUSTED" : "NO_EVIDENCE"
                : itemLimitCount > 0 || tokenBudgetCount > 0 ? "BUDGET_TRUNCATED" : "READY";
        return new ScopedContextResult(status, topK, budget, used, List.copyOf(items), sources,
                new ScopedContextOmitted(duplicateCount, itemLimitCount, tokenBudgetCount, staleCount),
                resolved.unavailableSourceCount());
    }

    private boolean isCurrent(ActorContext actor, Candidate candidate) {
        try {
            if (!authorization.isAuthorized(actor.tenantId(), actor.actorId(), candidate.source.workspaceId(), "context:read"))
                return false;
            if (candidate.knowledgeHit != null)
                return knowledge.isScopedContextHitUsable(actor, candidate.source.workspaceId(), candidate.knowledgeHit);
            var current = memories.requireUsable(actor, candidate.source.workspaceId(), candidate.memory.id(), candidate.memory.version());
            return current.contentHash().equals(candidate.memory.contentHash());
        } catch (EafException staleOrRevoked) {
            return false;
        }
    }

    private int estimateTokens(String content) {
        return Math.max(1, (content.codePointCount(0, content.length()) + 3) / 4);
    }

    private void requireHuman(ActorContext actor) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("多来源上下文仅接受本人 HUMAN 身份。");
    }

    private static final class Aggregate {
        private final Candidate candidate;
        private final List<ScopedContextOrigin> origins = new ArrayList<>();
        private Aggregate(Candidate candidate) { this.candidate = candidate; this.origins.add(candidate.origin()); }
    }

    private static final class Candidate {
        private final WorkspaceSummary source;
        private final String sourceType;
        private final String content;
        private final String contentHash;
        private final int estimatedTokens;
        private final String scopeKey;
        private final ScopedKnowledgeSearchHit knowledgeHit;
        private final MemoryDefinition memory;
        private final ScopedContextOrigin origin;

        private Candidate(WorkspaceSummary source, String sourceType, String content, String contentHash,
                          int estimatedTokens, String scopeKey, ScopedKnowledgeSearchHit knowledgeHit,
                          MemoryDefinition memory, ScopedContextOrigin origin) {
            this.source = source;
            this.sourceType = sourceType;
            this.content = content;
            this.contentHash = contentHash;
            this.estimatedTokens = estimatedTokens;
            this.scopeKey = scopeKey;
            this.knowledgeHit = knowledgeHit;
            this.memory = memory;
            this.origin = origin;
        }

        static Candidate knowledge(WorkspaceSummary source, ScopedKnowledgeSearchHit scoped, int estimatedTokens) {
            var hit = scoped.hit();
            var origin = new ScopedContextOrigin(source.workspaceId(), source.kind().name(), hit.sourceRef(), hit.contentHash(),
                    scoped.accessPath(), hit.documentId(), hit.documentVersion(), hit.chunkId(), hit.buildId(), hit.startOffset(),
                    hit.endOffset(), hit.offsetUnit(), null, null, null, null, scoped.shareId(), scoped.shareVersion());
            return new Candidate(source, "KNOWLEDGE", hit.content(), hit.contentHash(), estimatedTokens, null,
                    scoped, null, origin);
        }

        static Candidate memory(WorkspaceSummary source, MemoryDefinition memory, int estimatedTokens) {
            var origin = new ScopedContextOrigin(source.workspaceId(), source.kind().name(), memory.sourceRef(),
                    memory.contentHash(), "DIRECT", null, null, null, null, null, null, null, memory.id(),
                    memory.version(), memory.scope(), memory.expiresAt(), null, null);
            return new Candidate(source, "MEMORY", memory.content(), memory.contentHash(), estimatedTokens,
                    memory.scope(), null, memory, origin);
        }

        private String dedupeKey() {
            // 只精确合并同类型同 hash/正文；Memory 还保留 scope，PERSONAL 与 TEAM 不互相折叠。
            return sourceType + "|" + contentHash + "|" + content + ("MEMORY".equals(sourceType) ? "|" + scopeKey : "");
        }

        private ScopedContextOrigin origin() { return origin; }
        private String sourceType() { return sourceType; }
        private String content() { return content; }
        private int estimatedTokens() { return estimatedTokens; }
    }

}
