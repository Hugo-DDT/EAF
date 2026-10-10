package io.eaf.workflow.api;

import java.time.Instant;
import java.util.UUID;

public record ProjectBriefArtifact(UUID briefId, int version, String kind, String templateVersion,
        UUID generationTaskId, int generationAttempt, String contentHash, Instant createdAt,
        boolean blocked, String markdown) { }
