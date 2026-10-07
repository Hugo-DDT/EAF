package io.eaf.workflow.api;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public record ServiceRequestBatchSnapshot(UUID id, UUID workspaceId, String status, long version,
        Instant createdAt, Instant deadlineAt, int maxActiveItems, int itemCount,
        Map<String, Integer> counts, List<Item> items) {
    public ServiceRequestBatchSnapshot {
        counts = counts == null ? Map.of() : Map.copyOf(counts);
        items = items == null ? List.of() : List.copyOf(items);
    }
    public record Item(String itemKey, int ordinal, String status, long version,
            UUID workflowInstanceId, String errorCode, UUID knowledgeTaskId, UUID experienceTaskId) { }
    public record Detail(Item item, String requestText, String scenarioKey,
            List<TeamExperienceRef> experienceRefs) {
        public Detail { experienceRefs = experienceRefs == null ? List.of() : List.copyOf(experienceRefs); }
    }
    public record Created(ServiceRequestBatchSnapshot batch, boolean replayed) { }
}
