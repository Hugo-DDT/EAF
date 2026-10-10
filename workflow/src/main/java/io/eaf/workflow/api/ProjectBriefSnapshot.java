package io.eaf.workflow.api;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public record ProjectBriefSnapshot(UUID id, UUID briefId, String title, UUID creatorId, UUID reviewerId,
        UUID recipientId, String status, String currentStep, String waitingReason, long rowVersion,
        Instant createdAt, Instant deadlineAt, UUID prepareTaskId, List<Integer> artifactVersions,
        boolean blocked, Set<String> allowedActions) {
    public ProjectBriefSnapshot {
        artifactVersions = artifactVersions == null ? List.of() : List.copyOf(artifactVersions);
        allowedActions = allowedActions == null ? Set.of() : Set.copyOf(allowedActions);
    }
}
