package io.eaf.evaluation.api;

import java.time.Instant;
import java.util.UUID;

/** Evaluation 固定保存的隔离上下文对标识，不暴露保留集答案。 */
public record CandidateContextSnapshot(UUID id, UUID candidateId, int candidateRevision,
                                       String targetType, String candidateContentHash,
                                       Instant createdAt) { }
