package io.eaf.execution.api;

import java.time.Instant;
import java.util.UUID;

/** Execution Owner 暴露的状态摘要，不包含参数、预览、结果或错误正文。 */
public record ExecutionOperationsItem(UUID executionId, UUID taskId, int attempt, String toolName,
                                      String toolVersion, String status, String errorCode, UUID operationId,
                                      UUID approvalId, long version, Instant createdAt, Instant endedAt) { }
