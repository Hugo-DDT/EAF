package io.eaf.model.infrastructure;

import io.eaf.model.api.EvidenceAssessmentGateway;
import io.eaf.model.api.EvidenceAssessmentRequest;
import io.eaf.model.api.EvidenceAssessmentResult;
import io.eaf.model.api.ModelFailure;

public final class DisabledEvidenceAssessmentGateway implements EvidenceAssessmentGateway {
    @Override public boolean enabled() { return false; }
    @Override public EvidenceAssessmentResult assess(EvidenceAssessmentRequest request) {
        throw new ModelFailure("EVIDENCE_ASSESSMENT_DISABLED", "Jev 证据判断未启用。", false, false);
    }
    @Override public int callCount() { return 0; }
}
