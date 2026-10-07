package io.eaf.execution.api;

import java.time.Instant;
import java.util.UUID;

/** Execution 运维列表的稳定创建时间游标。 */
public record ExecutionOperationsCursor(Instant createdAt, UUID executionId) { }
