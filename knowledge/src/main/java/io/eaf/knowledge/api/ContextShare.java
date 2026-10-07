package io.eaf.knowledge.api;

import java.time.Instant;
import java.util.UUID;

public record ContextShare(UUID id, UUID tenantId, UUID workspaceId, UUID documentId, UUID recipientId,
                           int documentVersion, UUID buildId, String contentHash, String status,
                           long rowVersion, UUID createdBy, Instant createdAt, Instant updatedAt,
                           boolean effective, String validityReason) { }
