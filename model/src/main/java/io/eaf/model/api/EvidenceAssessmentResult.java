package io.eaf.model.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record EvidenceAssessmentResult(String provider, String model, Map<String, EvidenceChoice> assessments,
                                       Integer inputTokens, Integer outputTokens, String usageStatus) {
    public EvidenceAssessmentResult {
        assessments = assessments == null ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(assessments));
    }
}
