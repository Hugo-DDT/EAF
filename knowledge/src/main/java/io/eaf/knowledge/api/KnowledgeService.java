package io.eaf.knowledge.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface KnowledgeService {
    KnowledgeDocument create(CreateKnowledgeDocumentCommand command);

    ManagedKnowledgeSource createManagedSource(ManagedKnowledgeSource.CreateCommand command);

    List<ManagedKnowledgeSource> listManagedSources(ActorContext actor, UUID workspaceId, int limit, int offset);

    ManagedKnowledgeSource getManagedSource(ActorContext actor, UUID workspaceId, UUID sourceId);

    ManagedKnowledgeSource.StateReceipt changeManagedSourceState(ActorContext actor, UUID workspaceId,
            UUID sourceId, long expectedSourceRevision, String status, String idempotencyKey);

    ManagedKnowledgeSource.SyncReceipt applyManagedSourceSync(ManagedKnowledgeSource.BatchCommand command);

    ManagedKnowledgeSource.SyncReceipt getManagedSourceSync(ActorContext actor, UUID workspaceId,
            UUID sourceId, UUID syncId);

    List<ManagedKnowledgeSource.ItemSummary> listManagedSourceItems(ActorContext actor, UUID workspaceId,
            UUID sourceId, int limit, int offset);

    ManagedKnowledgeSource.SourceVersion getManagedSourceVersion(ActorContext actor, UUID workspaceId,
            UUID documentId, int documentVersion);

    KnowledgeDocument getManagedSourceVersionForOwner(ActorContext actor, UUID workspaceId,
            UUID sourceId, String itemId, int documentVersion);

    ManagedKnowledgeSource.Neighborhood neighborhood(ManagedKnowledgeSource.NeighborhoodRequest request);

    // 追加不可变原文版本；必须用当前文档 rowVersion 防止基线过期。
    KnowledgeDocument createVersion(CreateKnowledgeVersionCommand command);

    KnowledgeDocument get(ActorContext actor, UUID workspaceId, UUID documentId);

    KnowledgeDocumentPage listDocuments(ActorContext actor, UUID workspaceId, Instant cursorCreatedAt,
                                         UUID cursorId, int pageSize);

    // 只返回本域发布 outbox 的状态引用；无当前知识读取权限时由 Owner 拒绝。
    KnowledgeOutboxPage listOutboxOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                             Instant createdAfter, Instant cursorCreatedAt,
                                             UUID cursorEventId, int pageSize);
    /** 只将本域 FAILED 发布事实重置为 PENDING，保留原 eventId、payload 和尝试次数。 */
    KnowledgeOutboxReplayReceipt replayOutbox(ActorContext actor, UUID workspaceId, UUID eventId,
                                              String requestKey, String reason);

    // 历史和草稿版本按资源 read 权限读取，供发布审查与证据追溯。
    KnowledgeDocument getVersion(ActorContext actor, UUID workspaceId, UUID documentId, int assetVersion);

    default List<KnowledgeChunk> chunk(ActorContext actor, UUID workspaceId, UUID documentId, String chunkingVersion) {
        return chunk(actor, workspaceId, documentId, 1, chunkingVersion);
    }

    List<KnowledgeChunk> chunk(ActorContext actor, UUID workspaceId, UUID documentId, int assetVersion,
                               String chunkingVersion);

    default KnowledgeIndexBuild buildIndex(ActorContext actor, UUID workspaceId, UUID documentId,
                                           String chunkingVersion, String idempotencyKey) {
        return buildIndex(actor, workspaceId, documentId, 1, chunkingVersion, idempotencyKey);
    }

    KnowledgeIndexBuild buildIndex(ActorContext actor, UUID workspaceId, UUID documentId, int assetVersion,
                                   String chunkingVersion, String idempotencyKey);

    KnowledgeIndexBuild getIndexBuild(ActorContext actor, UUID workspaceId, UUID documentId, UUID buildId);

    KnowledgeIndexBuild retryIndex(ActorContext actor, UUID workspaceId, UUID documentId, UUID buildId);

    KnowledgePublication publish(ActorContext actor, UUID workspaceId, UUID documentId,
                                 long expectedRowVersion, UUID buildId, String idempotencyKey);

    KnowledgePublication publish(ActorContext actor, UUID workspaceId, UUID documentId, long expectedRowVersion,
                                 Integer expectedBaseVersion, UUID buildId, String idempotencyKey);

    // 仅 Learning 发布入口使用；Knowledge 仍复核权限、基线、READY 索引和候选正文摘要。
    KnowledgePublication publishCandidate(ActorContext actor, UUID workspaceId, UUID documentId,
                                          long expectedRowVersion, int expectedBaseVersion, UUID buildId,
                                          UUID candidateId, int candidateRevision, String contentHash);

    KnowledgePublication revoke(ActorContext actor, UUID workspaceId, UUID documentId,
                                long expectedRowVersion, String idempotencyKey);

    KnowledgePublication revokeVersion(ActorContext actor, UUID workspaceId, UUID documentId, int assetVersion,
                                       long expectedRowVersion, String idempotencyKey);

    // Learning 撤回只允许撤回来源完全匹配的具体候选版本，并保留候选来源供崩溃恢复。
    KnowledgePublication revokeCandidateVersion(ActorContext actor, UUID workspaceId, UUID documentId,
                                                int assetVersion, long expectedRowVersion,
                                                UUID candidateId, int candidateRevision);

    KnowledgePublication findWithdrawalByOrigin(ActorContext actor, UUID workspaceId, UUID candidateId,
                                                int candidateRevision);

    // 只按 Learning 来源键读取本域发布事实；调用方不能借此访问候选正文。
    KnowledgePublication findReleaseByOrigin(ActorContext actor, UUID workspaceId, UUID candidateId,
                                             int candidateRevision);

    KnowledgePublication getCurrentPublication(ActorContext actor, UUID workspaceId, UUID documentId);

    // 只验证具体快照的文档、Chunk、哈希和 READY build，供 Context 做实时撤回/权限复核。
    boolean isUsable(ActorContext actor, UUID workspaceId, UUID documentId, int documentVersion,
                     UUID chunkId, UUID buildId, String contentHash);

    List<PublishedKnowledgeChunk> readPublishedChunks(ActorContext actor, UUID workspaceId,
            List<PublishedKnowledgeChunk.Ref> refs);

    ContextShare shareContext(ActorContext actor, UUID workspaceId, UUID documentId, UUID recipientId,
                              int documentVersion, long expectedVersion);
    ContextSharePage listContextShares(ActorContext actor, UUID workspaceId, UUID documentId, int limit, int offset);
    ContextShare revokeContextShare(ActorContext actor, UUID workspaceId, UUID documentId, UUID recipientId,
                                    long expectedVersion);
    ScopedKnowledgeSearchResult searchForScopedContext(ActorContext actor, UUID workspaceId, String query, int topK);
    boolean isScopedContextHitUsable(ActorContext actor, UUID workspaceId, ScopedKnowledgeSearchHit scopedHit);

    KnowledgeSearchResult search(ActorContext actor, UUID workspaceId, String query, int topK);

    // Context 只传递服务端派生的计量归属；文档授权仍由 Knowledge 本域逐次检查。
    default KnowledgeSearchResult search(ActorContext actor, UUID workspaceId, String query, int topK,
                                         KnowledgeSearchScope scope) {
        return search(actor, workspaceId, query, topK);
    }

    // HYBRID 只切换召回排序，不传递或改变任何授权条件。
    default KnowledgeSearchResult search(ActorContext actor, UUID workspaceId, String query, int topK,
                                         KnowledgeSearchScope scope, String mode) {
        if (mode != null && !"VECTOR".equals(mode) && !"HYBRID".equals(mode))
            throw io.eaf.shared.EafException.invalid("检索 mode 必须为 VECTOR 或 HYBRID。");
        return search(actor, workspaceId, query, topK, scope);
    }
}
// 本接口只公开知识模块所需的导入和授权详情查询，不泄露 JDBC 实体。
