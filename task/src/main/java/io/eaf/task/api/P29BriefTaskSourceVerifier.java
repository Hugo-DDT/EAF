package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

/** Workflow verifies fixed P29 Task creation and result currentness without exposing its tables to Task. */
public interface P29BriefTaskSourceVerifier {
    void requireTaskCreation(CreateWorkflowTaskCommand command);
    void requireTaskResultCurrent(ActorContext actor, UUID workspaceId, UUID taskId, int attempt);
}
