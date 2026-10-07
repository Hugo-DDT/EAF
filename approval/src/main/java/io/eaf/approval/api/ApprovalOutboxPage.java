package io.eaf.approval.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Approval Owner 按 Workspace 权限返回的 outbox 分页。 */
public record ApprovalOutboxPage(List<ApprovalOutboxItem> items, long totalSize,
                                Instant nextCreatedAt, UUID nextEventId) { }
