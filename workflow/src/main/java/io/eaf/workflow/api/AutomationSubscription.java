package io.eaf.workflow.api;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public record AutomationSubscription(UUID id, UUID workspaceId, UUID ownerId, String name, String triggerKind,
        String dayOfWeek, String localTime, String timeZone, UUID workItemId, int maxItems, Instant expiresAt,
        int maxRuns, int admittedRuns, Instant nextFireAt, String status, String reason, UUID activeRunId,
        long version, Instant createdAt, Instant updatedAt, Set<String> allowedActions) {
    public AutomationSubscription { allowedActions = allowedActions == null ? Set.of() : Set.copyOf(allowedActions); }
}
