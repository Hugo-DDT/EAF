package io.eaf.model.api;

import java.time.Instant;
import java.util.UUID;

public record TypedDecisionRequest(String state, Instant deadline, int tokenBudget,
                                   UUID taskId, String traceId, int callNo) { }
