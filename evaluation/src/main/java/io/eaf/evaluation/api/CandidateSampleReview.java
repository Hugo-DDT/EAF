package io.eaf.evaluation.api;

import java.time.Instant;
import java.util.UUID;

/** 独立人工复核记录；保留评审人、事实结论、引用蕴含判断与理由。 */
public record CandidateSampleReview(UUID reportId, String caseId, int sampleNo, UUID reviewerId,
                                    String factualDecision, String citationSemanticSupport,
                                    String rationale, Instant createdAt) { }
