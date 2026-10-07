package io.eaf.knowledge.api;

import java.time.Instant;
import java.util.UUID;

/** Knowledge Owner 对值班查询公开的发布事件状态，不包含原文或 payload。 */
public record KnowledgeOutboxItem(UUID eventId, UUID documentId, int assetVersion, String action,
                                  String eventType, String status, int attempts, String errorCode,
                                  Instant nextAttemptAt, Instant createdAt) { }
