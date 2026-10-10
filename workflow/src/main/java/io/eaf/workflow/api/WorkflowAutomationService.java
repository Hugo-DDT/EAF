package io.eaf.workflow.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.UUID;

public interface WorkflowAutomationService {
    AutomationSubscription create(CreateAutomationSubscriptionCommand command);
    AutomationSubscriptionPage list(ActorContext actor, UUID workspaceId, Instant cursorCreatedAt, UUID cursorId, int pageSize);
    AutomationSubscription get(ActorContext actor, UUID workspaceId, UUID subscriptionId);
    AutomationSubscription pause(ActorContext actor, UUID workspaceId, UUID subscriptionId, long expectedVersion, String idempotencyKey);
    AutomationSubscription resume(ActorContext actor, UUID workspaceId, UUID subscriptionId, long expectedVersion, String idempotencyKey);
    AutomationSubscription delete(ActorContext actor, UUID workspaceId, UUID subscriptionId, long expectedVersion, String idempotencyKey);
    AutomationRunPage listRuns(ActorContext actor, UUID workspaceId, UUID subscriptionId,
            Instant cursorCreatedAt, UUID cursorId, int pageSize);
    AutomationRun getRun(ActorContext actor, UUID workspaceId, UUID subscriptionId, UUID runId);
    boolean dispatchOne();
    boolean consumeOneEvent();
    AutomationTaskSource requireTaskSource(ActorContext actor, UUID workspaceId, UUID taskId, int attempt);
    void requireTaskExecutionCurrent(ActorContext actor, UUID workspaceId, UUID taskId, int attempt);
    boolean beginGeneration(ActorContext actor, UUID workspaceId, UUID taskId, int attempt);
    String recordGenerationResult(ActorContext actor, UUID workspaceId, UUID taskId, int attempt, String resultJson);
}
