package io.eaf.knowledge.api;

import java.time.Instant;
import java.util.UUID;

/** 发布/撤回命令的不可变事实；当前指针和历史事件都不暴露原始向量。 */
public record KnowledgePublication(UUID eventId, UUID tenantId, UUID workspaceId, UUID documentId,
                                   int assetVersion, UUID buildId, String action,
                                   String documentStatus, long documentRowVersion,
                                   Instant occurredAt, String releaseOrigin, UUID candidateId,
                                   Integer candidateRevision, Integer baseVersion, String contentHash) { }
// 发布事实绑定精确版本和 build，撤回后旧事件仍可用于受控审计与回放。
