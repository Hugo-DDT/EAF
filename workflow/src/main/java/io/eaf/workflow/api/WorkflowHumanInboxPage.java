package io.eaf.workflow.api;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public record WorkflowHumanInboxPage(List<Item> items, Instant nextUpdatedAt, UUID nextId) {
    public WorkflowHumanInboxPage { items = items == null ? List.of() : List.copyOf(items); }
    public record Item(UUID id, String kind, String title, UUID ownerId, UUID assigneeId, String state,
            String waitingReason, Instant updatedAt, String detailRef, Set<String> allowedActions, boolean blocked) {
        public Item { allowedActions = allowedActions == null ? Set.of() : Set.copyOf(allowedActions); }
    }
}
