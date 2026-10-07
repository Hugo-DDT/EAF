package io.eaf.workflow.infrastructure;

import java.util.UUID;

// 租约代次隔离过期 Workflow Worker 的迟到写入。
public record WorkflowLease(UUID instanceId, UUID ownerId, long fence) { }
