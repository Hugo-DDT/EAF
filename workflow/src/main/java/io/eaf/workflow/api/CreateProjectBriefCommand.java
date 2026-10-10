package io.eaf.workflow.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record CreateProjectBriefCommand(ActorContext actor, UUID workspaceId, String title, String goal,
        List<KnowledgeRef> knowledgeRefs, List<WorkItemRef> workItemRefs, OaQueryRef oaQueryRef,
        UUID reviewerId, UUID recipientId, Instant deadlineAt, String idempotencyKey) {
    public CreateProjectBriefCommand {
        knowledgeRefs = knowledgeRefs == null ? List.of() : List.copyOf(knowledgeRefs);
        workItemRefs = workItemRefs == null ? List.of() : List.copyOf(workItemRefs);
    }

    public record KnowledgeRef(UUID documentId, int documentVersion, UUID chunkId, UUID buildId, String contentHash) { }
    public record WorkItemRef(UUID workItemId, long expectedRowVersion) { }
    public record OaQueryRef(UUID queryId, long expectedRowVersion) { }
}
