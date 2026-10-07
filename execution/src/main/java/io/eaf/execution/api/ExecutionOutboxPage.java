package io.eaf.execution.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Execution Owner 按 Workspace 权限返回的 outbox 分页。 */
public record ExecutionOutboxPage(List<ExecutionOutboxItem> items, long totalSize,
                                 Instant nextCreatedAt, UUID nextEventId) { }
