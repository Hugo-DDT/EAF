package io.eaf.model.api;

public interface TypedDecisionGateway {
    boolean enabled();

    default boolean external() { return false; }

    default ModelBillingProfile billingProfile() { return null; }

    default String outboundDataScope() { return null; }

    TypedDecisionResult decide(TypedDecisionRequest request);

    int callCount();
}
