package io.eaf.memory.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Memory Owner 按 Workspace 权限返回的 outbox 分页。 */
public record MemoryOutboxPage(List<MemoryOutboxItem> items, long totalSize,
                               Instant nextCreatedAt, UUID nextEventId) { }
