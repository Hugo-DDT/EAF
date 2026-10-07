package io.eaf.workflow.api;

import java.time.Instant;
import java.util.UUID;

// 实例固定原定义、依赖、输入、预算范围和发起人；历史读取仍由 Workspace 授权。
public record WorkflowInstance(UUID id, UUID tenantId, UUID workspaceId, UUID actorId,
                               UUID initiatorId, UUID delegationId, String source,
                               UUID workflowId, String workflowVersion, String definitionHash,
                               UUID rootBudgetScopeId, String inputJson, String inputHash,
                               String definitionSnapshot, String dependencySnapshot,
                               String status, String resultJson, String currentStepId, UUID rootTaskId,
                               String errorCode, long rowVersion, Instant createdAt, Instant deadlineAt,
                               UUID childTaskId, String waitingReason, String businessEffectStatus,
                               String cancelTargetStatus, UUID qualityRunId) { }
