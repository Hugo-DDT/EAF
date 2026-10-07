package io.eaf.context.api;

import io.eaf.shared.ActorContext;
import java.util.Optional;
import java.util.UUID;

/** Runtime 仅为绑定的 EVALUATION Task 读取不可变实验上下文快照。 */
public interface EvaluationContextSnapshotReader {
    Optional<EnterpriseContext> readForTask(ActorContext actor, UUID workspaceId, UUID taskId, UUID snapshotId);

    boolean isCurrentForTask(ActorContext actor, UUID workspaceId, UUID taskId, UUID snapshotId,
                             EnterpriseContext context);
}
