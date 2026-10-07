package io.eaf.context.api;

import java.util.List;

public record ScopedContextItem(String citationId, String sourceType, String content,
                                int estimatedTokens, List<ScopedContextOrigin> origins) { }
