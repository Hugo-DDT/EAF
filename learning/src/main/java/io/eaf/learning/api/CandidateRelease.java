package io.eaf.learning.api;

import java.time.Instant;
import java.util.UUID;

/** Learning 持有的不可变发布意图及目标模块返回的正式发布事实。 */
public record CandidateRelease(UUID id, UUID tenantId, UUID workspaceId, UUID candidateId,
                               int candidateRevision, UUID approvalId,
                               String targetType, UUID targetId, String baseVersion, String status,
                               UUID targetReleaseId, String targetVersion, String targetContentHash,
                               String failureCode, int attempts, Instant createdAt, Instant updatedAt) { }
