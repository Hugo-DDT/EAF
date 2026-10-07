package io.eaf.workflow.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.UUID;

/** Evaluation 内部启动已注册的质量流程；普通 Workflow 命令不接受验收标记。 */
public record CreateQualityRunWorkflowCommand(ActorContext actor, UUID workspaceId, UUID qualityRunId,
                                              UUID workflowId, String workflowVersion, String inputJson,
                                              String idempotencyKey, String source, Instant deadlineAt) { }
