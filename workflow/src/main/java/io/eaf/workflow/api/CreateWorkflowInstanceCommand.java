package io.eaf.workflow.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.UUID;

// 启动请求显式绑定定义版本、原始输入、来源与幂等键。
public record CreateWorkflowInstanceCommand(ActorContext actor, UUID workspaceId, UUID workflowId,
                                            String workflowVersion, String inputJson,
                                            String idempotencyKey, String source, Instant deadlineAt) {
    public CreateWorkflowInstanceCommand(ActorContext actor, UUID workspaceId, UUID workflowId,
                                        String workflowVersion, String inputJson,
                                        String idempotencyKey, String source) {
        this(actor, workspaceId, workflowId, workflowVersion, inputJson, idempotencyKey, source, null);
    }
}
