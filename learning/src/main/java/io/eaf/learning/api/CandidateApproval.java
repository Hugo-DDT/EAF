package io.eaf.learning.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/** Learning 的不可变决定记录，绑定候选修订、目标基线和批准时的评测报告。 */
public record CandidateApproval(UUID id, int candidateRevision, UUID approverId, String decision,
                                String targetType, UUID targetId, String baseVersion, UUID ownerId, String scope,
                                String candidateContentHash, String evidenceHash, UUID evaluationReportId,
                                String evaluationReportKind, String evaluationReportHash,
                                String evaluationConfigurationHash, String datasetHash,
                                JsonNode evaluationSummary, String authorizationAction, Instant validUntil,
                                String reason, Instant createdAt) { }
