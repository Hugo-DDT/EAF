package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.UUID;

/** 只有经 evaluation:run 授权的服务端评测编排可以创建带质量运行标记的 Task。 */
public record CreateQualityRunTaskCommand(ActorContext actor, UUID workspaceId, UUID qualityRunId,
                                          UUID agentId, String agentVersion, String input,
                                          String businessEntityType, String businessEntityId,
                                          String idempotencyKey, String traceId, String source,
                                          UUID scenarioSampleId, TaskAssetBinding assetBinding, Instant deadlineAt) {
    public CreateQualityRunTaskCommand(ActorContext actor, UUID workspaceId, UUID qualityRunId,
                                       UUID agentId, String agentVersion, String input,
                                       String businessEntityType, String businessEntityId,
                                       String idempotencyKey, String traceId, String source) {
        this(actor, workspaceId, qualityRunId, agentId, agentVersion, input, businessEntityType,
                businessEntityId, idempotencyKey, traceId, source, null, null, null);
    }
}
