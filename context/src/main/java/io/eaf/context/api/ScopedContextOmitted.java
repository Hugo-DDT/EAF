package io.eaf.context.api;

public record ScopedContextOmitted(int duplicateCount, int itemLimitCount,
                                   int tokenBudgetCount, int staleCount) { }
