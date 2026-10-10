package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

public interface P30AutomationTaskSourceVerifier {
    void requireTaskCreation(CreateAutomationReadTaskCommand command);
    void requireTaskResultCurrent(ActorContext actor, UUID workspaceId, UUID taskId, int attempt);
    void requireTaskExecutionCurrent(ActorContext actor, UUID workspaceId, UUID taskId, int attempt);
}
