package io.eaf.model.api;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record EvidenceChoice(String choice, Map<String, BigDecimal> probabilities) {
    public EvidenceChoice {
        probabilities = probabilities == null ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(probabilities));
    }
}
