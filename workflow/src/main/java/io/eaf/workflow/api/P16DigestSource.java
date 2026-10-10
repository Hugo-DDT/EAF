package io.eaf.workflow.api;

import java.time.Instant;
import java.util.UUID;

public record P16DigestSource(UUID workItemId, UUID instanceId, String requestId, String status,
        long rowVersion, UUID assigneeId, Instant deadlineAt, Instant createdAt, String sharedBrief,
        String handlingAdvice, String cautions) { }
