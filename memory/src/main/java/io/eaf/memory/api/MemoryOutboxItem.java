package io.eaf.memory.api;

import java.time.Instant;
import java.util.UUID;

/** Memory Owner 对值班查询公开的发布事件状态，不包含记忆正文或 payload。 */
public record MemoryOutboxItem(UUID eventId, UUID memoryId, String memoryVersion, String action,
                               String eventType, String status, int attempts, String errorCode,
                               Instant nextAttemptAt, Instant createdAt) { }
