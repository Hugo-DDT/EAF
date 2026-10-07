package io.eaf.execution.api;

import java.time.Instant;
import java.util.UUID;

/** Execution Owner 对值班查询公开的事件状态，不包含 outbox payload。 */
public record ExecutionOutboxItem(UUID eventId, UUID executionId, String eventType, String status,
                                  int attempts, Instant nextAttemptAt, Instant createdAt) { }
