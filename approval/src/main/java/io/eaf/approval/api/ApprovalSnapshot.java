package io.eaf.approval.api;

import java.time.Instant;
import java.util.UUID;

public record ApprovalSnapshot(UUID id, UUID tenantId, UUID workspaceId, UUID taskId,
                               ApprovalBinding binding, String state, UUID decidedBy,
                               Instant decidedAt, long version, Instant createdAt) { }
