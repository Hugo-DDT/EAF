package io.eaf.model.api;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record TypedDecisionResult(String provider, String model, String choice,
                                  Map<String, BigDecimal> probabilities,
                                  Integer inputTokens, Integer outputTokens, String usageStatus) {
    public TypedDecisionResult {
        probabilities = probabilities == null ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(probabilities));
    }
}
