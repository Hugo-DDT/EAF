package io.eaf.workflow.api;

import java.util.Map;
import java.util.UUID;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonInclude;

// 映射值只能引用流程输入、祖先步骤输出或受限布尔常量。
public record WorkflowStepSpec(String id, WorkflowStepType type, String nextStepId,
                               UUID capabilityId, String capabilityVersion,
                               String toolName, String toolVersion,
                               Map<String, String> inputMapping,
                               String conditionPath, String conditionValue,
                               String whenTrueStepId, String whenFalseStepId,
                               Map<String, String> outputMapping,
                               @JsonInclude(JsonInclude.Include.NON_NULL) List<WorkflowParallelBranchSpec> parallelBranches) {
    public WorkflowStepSpec(String id, WorkflowStepType type, String nextStepId, UUID capabilityId,
                            String capabilityVersion, String toolName, String toolVersion,
                            Map<String, String> inputMapping, String conditionPath, String conditionValue,
                            String whenTrueStepId, String whenFalseStepId, Map<String, String> outputMapping) {
        this(id, type, nextStepId, capabilityId, capabilityVersion, toolName, toolVersion, inputMapping,
                conditionPath, conditionValue, whenTrueStepId, whenFalseStepId, outputMapping, null);
    }

    public WorkflowStepSpec {
        parallelBranches = parallelBranches == null ? null : List.copyOf(parallelBranches);
    }
}
