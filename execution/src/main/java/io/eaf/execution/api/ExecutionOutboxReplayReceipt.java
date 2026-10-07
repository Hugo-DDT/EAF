package io.eaf.execution.api;

import java.time.Instant;
import java.util.UUID;

/** Execution Outbox Owner 返回的重投回执，只暴露原事件 ID 和重试计数。 */
public record ExecutionOutboxReplayReceipt(UUID commandId, UUID eventId, String status,
                                           int attempts, Instant createdAt, boolean replayed) { }
