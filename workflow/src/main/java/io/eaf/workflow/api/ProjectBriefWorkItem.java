package io.eaf.workflow.api;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public record ProjectBriefWorkItem(UUID id, UUID instanceId, String kind, String stepId,
        UUID creatorId, UUID assigneeId, String status, long rowVersion, Instant deadlineAt,
        String workflowStatus, String currentStep, Integer artifactVersion, String decision,
        String disposition, String notes, String nextAction, String note, UUID completedBy,
        Instant completedAt, Set<String> allowedActions, boolean blocked) {
    public ProjectBriefWorkItem { allowedActions = allowedActions == null ? Set.of() : Set.copyOf(allowedActions); }
}
