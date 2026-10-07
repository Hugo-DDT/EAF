package io.eaf.model.infrastructure;

import io.eaf.model.api.ModelFailure;
import io.eaf.model.api.TypedDecisionGateway;
import io.eaf.model.api.TypedDecisionRequest;
import io.eaf.model.api.TypedDecisionResult;

final class MissingTypedDecisionGateway implements TypedDecisionGateway {
    private final String detail;

    MissingTypedDecisionGateway(String detail) { this.detail = detail; }

    @Override public boolean enabled() { return true; }
    @Override public TypedDecisionResult decide(TypedDecisionRequest request) {
        throw new ModelFailure("DECISION_CONFIGURATION_INVALID", detail, false, false);
    }
    @Override public int callCount() { return 0; }
}
