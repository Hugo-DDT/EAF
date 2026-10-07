package io.eaf.model.api;

public interface EvidenceAssessmentGateway {
    boolean enabled();
    default boolean external() { return false; }
    default ModelBillingProfile billingProfile() { return null; }
    default String outboundDataScope() { return null; }
    EvidenceAssessmentResult assess(EvidenceAssessmentRequest request);
    int callCount();
}
