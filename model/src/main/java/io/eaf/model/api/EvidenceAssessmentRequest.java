package io.eaf.model.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record EvidenceAssessmentRequest(String question, List<EvidencePassage> passages, Instant deadline,
                                        int tokenBudget, UUID taskId, String traceId, int callNo) {
    public EvidenceAssessmentRequest {
        passages = passages == null ? List.of() : List.copyOf(passages);
    }
}
