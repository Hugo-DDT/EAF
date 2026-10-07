package io.eaf.learning.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** 候选仅保存隔离草稿及精确基线快照，不代表正式 Knowledge 或 Memory。 */
public record LearningCandidate(UUID id, UUID tenantId, UUID workspaceId, UUID proposerId,
                                UUID sourceFeedbackId, int revision, long rowVersion, String status,
                                String targetType, UUID targetId, String baseVersion, UUID ownerId,
                                String scope, JsonNode baseSnapshot, JsonNode proposedContent,
                                List<String> evidenceRefs, boolean evidenceGap, String contentHash,
                                List<CandidateReview> reviews, List<CandidateApproval> approvals,
                                Instant createdAt, Instant updatedAt) { }
