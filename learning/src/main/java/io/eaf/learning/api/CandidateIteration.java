package io.eaf.learning.api;

import io.eaf.context.api.ContextSourceRef;
import java.time.Instant;
import java.util.UUID;

/** 一次发布后的 USER Task 使用事实；模型报告与经核实的事实结论分开保存。 */
public record CandidateIteration(UUID id, UUID candidateId, int candidateRevision,
                                 UUID sourceFeedbackId, UUID sourceTaskId,
                                 UUID evaluationReportId, String evaluationStatus,
                                 UUID approvalId, UUID releaseId, UUID targetReleaseId,
                                 String releaseStatus, UUID withdrawalId, String withdrawalStatus,
                                 String targetType, UUID targetId, String targetVersion,
                                 UUID followupTaskId, int followupAttempt, String taskStatus,
                                 String usageStatus, ContextSourceRef targetSource,
                                 String inputHash, String resultHash, String reportedRiskLevel,
                                 String factOutcome, Instant taskCreatedAt, Instant recordedAt) { }
