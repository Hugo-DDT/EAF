package io.eaf.task.api;

import java.util.UUID;

/** 保存 Workflow 实例、固定版本和步骤关联，供 Task/Execution 复核评测步骤边界。 */
public record WorkflowTaskProvenance(UUID workflowInstanceId, UUID workflowId,
                                     String workflowVersion, String stepId) {

    public boolean complete() {
        return workflowInstanceId != null && workflowId != null
                && workflowVersion != null && !workflowVersion.isBlank()
                && stepId != null && !stepId.isBlank();
    }

    public boolean p21ParallelBranch() {
        return workflowId != null && workflowId.equals(UUID.fromString("58000000-0000-4000-8000-000000000018"))
                && "1.0.0".equals(workflowVersion)
                && java.util.Set.of("gather-knowledge", "gather-experience").contains(stepId);
    }
}
