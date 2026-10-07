package io.eaf.workflow.api;

import java.time.Instant;
import java.util.UUID;

/** Workflow Owner 发布的安全运行摘要，不包含输入、结果或定义快照。 */
public record WorkflowOperationsItem(UUID instanceId, UUID workflowId, String workflowVersion,
                                     String status, String currentStepId, UUID rootTaskId, UUID childTaskId,
                                     String errorCode, String waitingReason, String businessEffectStatus,
                                     long version, Instant createdAt, Instant deadlineAt) { }
