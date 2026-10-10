package io.eaf.knowledge.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Knowledge 本域管理文本来源的公开合同，不暴露数据库记录。 */
public record ManagedKnowledgeSource(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId,
                                     String name, String type, String status, long sourceRevision,
                                     Instant createdAt, Instant updatedAt) {
    public record CreateCommand(ActorContext actor, UUID workspaceId, String name, String type,
                                String idempotencyKey) { }
    public record Change(String changeType, String itemId, String sourceVersion, String title,
                         String format, String content, String aclVersion, Set<UUID> readActorIds,
                         String reasonCode) { }
    public record BatchCommand(ActorContext actor, UUID workspaceId, UUID sourceId,
                               long expectedSourceRevision, List<Change> changes,
                               String idempotencyKey) { }
    public record ChangeResult(String itemId, String changeType, String result, UUID documentId,
                               Integer documentVersion, String sourceVersion, String availability,
                               String reasonCode) { }
    public record SyncReceipt(UUID syncId, UUID sourceId, String status, long previousSourceRevision,
                              long sourceRevision, Instant observedAt, List<ChangeResult> items,
                              boolean replayed) { }
    public record StateReceipt(UUID operationId, UUID sourceId, String status,
                               long previousSourceRevision, long sourceRevision,
                               Instant observedAt, boolean replayed) { }
    public record ItemSummary(String itemId, UUID documentId, String title, String sourceVersion,
                              String contentHash, String aclVersion, String availability,
                              String reasonCode, Instant observedAt, Integer draftVersion) { }
    public record SourceVersion(UUID sourceId, String itemId, String sourceVersion,
                                String contentHash, String aclVersion, String aclHash, UUID syncId,
                                Instant observedAt, String currentSourceVersion, String currentContentHash,
                                String availability, String reasonCode, String sourceStatus) { }
    public record NeighborhoodRequest(ActorContext actor, UUID workspaceId, UUID documentId,
                                      int documentVersion, UUID buildId, UUID chunkId, int tokenBudget) { }
    public record NeighborhoodItem(KnowledgeChunk chunk, String relation, int estimatedTokens) { }
    public record Neighborhood(UUID documentId, int documentVersion, UUID buildId,
                              List<NeighborhoodItem> items, List<String> omittedReasons) { }
}
