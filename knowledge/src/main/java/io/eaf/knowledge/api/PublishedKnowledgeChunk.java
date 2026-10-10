package io.eaf.knowledge.api;

import java.util.List;
import java.util.UUID;

public record PublishedKnowledgeChunk(Ref ref, String documentTitle, String content, List<String> headingPath) {
    public PublishedKnowledgeChunk { headingPath = headingPath == null ? List.of() : List.copyOf(headingPath); }
    public record Ref(UUID documentId, int documentVersion, UUID chunkId, UUID buildId, String contentHash) { }
}
