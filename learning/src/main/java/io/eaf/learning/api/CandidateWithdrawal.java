package io.eaf.learning.api;

import java.time.Instant;
import java.util.UUID;

/** 撤回是发布后的独立流程；原 CandidateRelease 与正式版本历史保持不变。 */
public record CandidateWithdrawal(UUID id, UUID candidateId, int candidateRevision, UUID candidateReleaseId,
                                  String targetType, UUID targetId, String targetVersion,
                                  UUID targetWithdrawalId, String status, String reasonRef,
                                  String failureCode, int attempts, Instant createdAt, Instant updatedAt) { }
