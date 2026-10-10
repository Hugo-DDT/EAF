package io.eaf.task.api;

/** Workflow 来源校验需要的 Task 状态元数据，不包含输入、结果或上下文。 */
public record WorkflowExecutionSource(TaskStatus status, int attempt, long rowVersion,
                                      WorkflowTaskProvenance provenance) { }
