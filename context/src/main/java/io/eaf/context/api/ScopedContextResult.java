package io.eaf.context.api;

import java.util.List;

public record ScopedContextResult(String status, int topK, int tokenBudget, int usedTokens,
                                  List<ScopedContextItem> items, List<ScopedContextSource> sources,
                                  ScopedContextOmitted omitted, int unavailableSourceCount) { }
