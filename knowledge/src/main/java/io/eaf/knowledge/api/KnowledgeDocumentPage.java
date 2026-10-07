package io.eaf.knowledge.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Knowledge Owner 过滤文档授权后的轻量分页。 */
public record KnowledgeDocumentPage(List<Item> items, long totalSize, Instant nextCreatedAt, UUID nextId) {
    public record Item(UUID id, String title, String status, Integer version, Instant createdAt) { }
}
