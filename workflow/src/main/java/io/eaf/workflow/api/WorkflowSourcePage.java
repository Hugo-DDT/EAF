package io.eaf.workflow.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Workflow Owner 按实例输入中的 sourceTaskId 过滤当前调用者可读的跟进流程。 */
public record WorkflowSourcePage(List<WorkflowInstance> items, long totalSize, Instant nextCreatedAt,
                                 UUID nextInstanceId) { }
