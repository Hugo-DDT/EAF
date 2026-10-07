package io.eaf.model.infrastructure;

import io.eaf.model.api.ModelFailure;
import io.eaf.model.api.TypedDecisionGateway;
import io.eaf.model.api.TypedDecisionRequest;
import io.eaf.model.api.TypedDecisionResult;

final class DisabledTypedDecisionGateway implements TypedDecisionGateway {
    @Override public boolean enabled() { return false; }
    @Override public TypedDecisionResult decide(TypedDecisionRequest request) {
        throw new ModelFailure("DECISION_DISABLED", "Jev 分类未启用。", false, false);
    }
    @Override public int callCount() { return 0; }
}
