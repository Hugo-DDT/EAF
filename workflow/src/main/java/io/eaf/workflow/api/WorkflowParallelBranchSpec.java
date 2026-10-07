package io.eaf.workflow.api;

import java.util.Map;
import java.util.UUID;

/** 发布流程的两个固定只读分支；不支持递归或动态分支。 */
public record WorkflowParallelBranchSpec(String role, UUID capabilityId, String capabilityVersion,
                                         Map<String, String> inputMapping) { }
