package io.eaf.workflow.api;

import java.util.List;
import java.util.UUID;

// 对外只返回版本化定义视图，不暴露 JDBC 行或 Mapper。
public record WorkflowDefinition(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId,
                                 String name, String description, String version, String status,
                                 long rowVersion, String inputSchema, String outputSchema,
                                 String entryStepId, List<WorkflowStepSpec> steps,
                                 List<WorkflowDependency> dependencies, String contentHash) { }
