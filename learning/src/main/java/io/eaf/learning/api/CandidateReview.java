package io.eaf.learning.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** 事实审核按候选 revision 追加保存，旧版本审阅不会覆盖新版本。 */
public record CandidateReview(int candidateRevision, UUID reviewerId, String decision,
                              String reason, List<String> factEvidenceRefs, Instant createdAt) { }
