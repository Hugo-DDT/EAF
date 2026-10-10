package io.eaf.task.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record UserTaskResultPage(List<Item> items, Instant nextUpdatedAt, UUID nextId) {
    public UserTaskResultPage { items = items == null ? List.of() : List.copyOf(items); }
    public record Item(UUID id, UUID agentId, String agentVersion, String status, Instant updatedAt) { }
}
