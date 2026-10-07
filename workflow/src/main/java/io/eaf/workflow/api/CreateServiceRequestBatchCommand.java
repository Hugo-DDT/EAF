package io.eaf.workflow.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record CreateServiceRequestBatchCommand(ActorContext actor, UUID workspaceId, String idempotencyKey,
        Integer maxActiveItems, Instant deadlineAt, List<Item> items) {
    public CreateServiceRequestBatchCommand {
        items = items == null ? null : List.copyOf(items);
    }
    public record Item(String itemKey, String requestText, String scenarioKey, List<TeamExperienceRef> experienceRefs) {
        public Item { experienceRefs = experienceRefs == null ? List.of() : List.copyOf(experienceRefs); }
    }
}
