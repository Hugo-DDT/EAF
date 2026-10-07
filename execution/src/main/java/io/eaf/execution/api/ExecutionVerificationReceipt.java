package io.eaf.execution.api;

import java.util.UUID;

/** UNKNOWN 核验运维命令的有限回执，不暴露写入参数或外部响应正文。 */
public record ExecutionVerificationReceipt(UUID commandId, UUID executionId, UUID operationId,
                                           String executionStatus, long executionVersion,
                                           boolean completed, boolean replayed) { }
