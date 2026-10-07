package io.eaf.workflow.api;

import java.time.Instant;
import java.util.UUID;

/** Workflow 运维列表使用的稳定创建时间游标。 */
public record WorkflowOperationsCursor(Instant createdAt, UUID instanceId) { }
