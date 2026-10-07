package io.eaf.knowledge.api;

import java.util.List;

public record ScopedKnowledgeSearchResult(int topK, List<ScopedKnowledgeSearchHit> hits) { }
