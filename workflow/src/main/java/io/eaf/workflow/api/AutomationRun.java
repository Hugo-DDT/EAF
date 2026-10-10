package io.eaf.workflow.api;

import java.time.Instant;
import java.util.UUID;

public record AutomationRun(UUID id, UUID subscriptionId, String triggerKind, String triggerKey, String status,
        String reason, Instant plannedAt, Instant occurredAt, Long sourceRowVersion, UUID taskId,
        String taskStatus, int attempts, Instant nextAttemptAt, Instant admittedAt, Instant finishedAt,
        Instant createdAt) { }
