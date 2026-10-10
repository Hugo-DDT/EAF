package io.eaf.task.api;

import java.time.Instant;
import java.util.UUID;

/** Immutable Workflow-owned source used by the fixed P27 Tools. */
public record P27BusinessTaskSource(String kind, String bindingRef, String bindingVersion, UUID workItemId,
        long workItemVersion, String requestId, String registrationOperationId, String sourceResultHash,
        UUID completedBy, Instant completedAt, String outcome, String summary, String nextAction,
        String externalSubjectId, String expectedExternalVersion) { }
