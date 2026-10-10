package io.eaf.approval.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ApprovalActionablePage(List<Item> items, Instant nextCreatedAt, UUID nextId) {
    public ApprovalActionablePage { items = items == null ? List.of() : List.copyOf(items); }
    public record Item(UUID id, UUID taskId, Instant expiresAt, long rowVersion, Instant createdAt) { }
}
