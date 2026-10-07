package io.eaf.workflow.api;

import java.util.List;

// 每个定义版本固定自己的输入/输出 Schema、入口和有向步骤图。
public record CreateWorkflowVersionCommand(String version, String inputSchema, String outputSchema,
                                           String entryStepId, List<WorkflowStepSpec> steps) { }
