package io.eaf.model.infrastructure;

import io.eaf.model.api.EvidenceAssessmentGateway;
import io.eaf.model.api.EvidenceAssessmentRequest;
import io.eaf.model.api.EvidenceAssessmentResult;
import io.eaf.model.api.ModelFailure;

final class MissingEvidenceAssessmentGateway implements EvidenceAssessmentGateway {
    private final String detail;
    MissingEvidenceAssessmentGateway(String detail) { this.detail = detail; }
    @Override public boolean enabled() { return true; }
    @Override public EvidenceAssessmentResult assess(EvidenceAssessmentRequest request) {
        throw new ModelFailure("DEPENDENCY_UNAVAILABLE", detail, false, false);
    }
    @Override public int callCount() { return 0; }
}
