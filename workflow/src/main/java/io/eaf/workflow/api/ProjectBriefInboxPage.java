package io.eaf.workflow.api;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public record ProjectBriefInboxPage(List<Item> items, Instant nextUpdatedAt, UUID nextId) {
    public ProjectBriefInboxPage { items = items == null ? List.of() : List.copyOf(items); }
    public record Item(UUID id, String title, UUID ownerId, String state, String waitingReason,
            Instant updatedAt, String detailRef, Set<String> allowedActions, boolean blocked) {
        public Item { allowedActions = allowedActions == null ? Set.of() : Set.copyOf(allowedActions); }
    }
}
