package io.eaf.workflow.infrastructure;

import java.util.UUID;

// 步骤意图保存的输入和来源键在恢复时保持不变。
public record WorkflowStepRuntime(String stepId, String stepType, String status,
                                  String inputJson, String inputHash, String dispatchKey,
                                  UUID childTaskId, String outputJson,
                                  String selectedNextStepId) { }
