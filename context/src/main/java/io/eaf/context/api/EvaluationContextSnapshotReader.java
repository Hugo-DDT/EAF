package io.eaf.context.api;

import io.eaf.shared.ActorContext;
import java.util.Optional;
import java.util.UUID;

/** Runtime 仅为绑定的 EVALUATION Task 读取不可变实验上下文快照。 */
public interface EvaluationContextSnapshotReader {
    Optional<EnterpriseContext> readForTask(ActorContext actor, UUID workspaceId, UUID taskId, UUID snapshotId);

    boolean isCurrentForTask(ActorContext actor, UUID workspaceId, UUID taskId, UUID snapshotId,
                             EnterpriseContext context);

    /** 只为与固定 P31 Prompt 报告绑定的候选侧 EVALUATION Task 返回模板精确来源。 */
    Optional<PromptCandidateTaskBinding> promptCandidateForTask(ActorContext actor, UUID workspaceId, UUID taskId);

    record PromptCandidateTaskBinding(UUID candidateId, int candidateRevision) { }
}
