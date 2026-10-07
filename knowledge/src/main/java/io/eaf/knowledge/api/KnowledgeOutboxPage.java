package io.eaf.knowledge.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Knowledge Owner 按 Workspace 权限返回的 outbox 分页。 */
public record KnowledgeOutboxPage(List<KnowledgeOutboxItem> items, long totalSize,
                                  Instant nextCreatedAt, UUID nextEventId) { }
