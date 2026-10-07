package io.eaf.learning.api;

import io.eaf.context.api.ContextSourceRef;
import io.eaf.task.api.TaskAssetBinding;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** 不可变反馈记录，附带服务端读取的 Task、资产和可选 Execution 来源快照。 */
public record Feedback(UUID id, UUID tenantId, UUID workspaceId, UUID taskId, int taskAttempt,
                       String taskStatus, UUID actorId, String sourceType, String correction,
                       String evidence, Source source, Instant createdAt) {
    public record Source(UUID agentId, String agentVersion, String promptVersion,
                         TaskAssetBinding assetBinding, List<ContextSourceRef> contextSources,
                         ExecutionEvidence execution) { }

    public record ExecutionEvidence(UUID executionId, int attempt, String status, boolean verified) { }
}
