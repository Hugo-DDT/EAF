package io.eaf.knowledge.api;

import java.util.UUID;

public record ScopedKnowledgeSearchHit(KnowledgeSearchHit hit, String accessPath,
                                       UUID shareId, Long shareVersion) { }
