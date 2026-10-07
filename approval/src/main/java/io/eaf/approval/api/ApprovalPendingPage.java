package io.eaf.approval.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** 当前审批人可见的待决定项索引，不带审批绑定正文。 */
public record ApprovalPendingPage(List<Item> items, Instant nextCreatedAt, UUID nextId) {
    public record Item(UUID id, UUID taskId, String state, Instant expiresAt, Instant createdAt) { }
}
