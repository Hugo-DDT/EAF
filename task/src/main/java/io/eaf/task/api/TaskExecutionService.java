package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

/** USER 后台与显式 Evaluation 共用的本地有界执行入口。 */
public interface TaskExecutionService {
    boolean dispatchNext();

    TaskSnapshot executeEvaluation(ActorContext actor, UUID workspaceId, UUID taskId);
}
