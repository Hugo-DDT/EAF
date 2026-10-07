package io.eaf.approval.api;

import java.time.Instant;
import java.util.UUID;

/** Approval Owner 对值班查询公开的事件状态，不包含 outbox payload。 */
public record ApprovalOutboxItem(UUID eventId, UUID approvalId, String eventType, String status,
                                 int attempts, Instant nextAttemptAt, Instant createdAt) { }
