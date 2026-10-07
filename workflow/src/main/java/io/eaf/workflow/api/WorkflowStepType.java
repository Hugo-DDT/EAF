package io.eaf.workflow.api;

// 只允许固定人工步骤进入保留流程，不接收任意运行期节点。
public enum WorkflowStepType { RUN_CAPABILITY, PARALLEL_READ, BRANCH, RUN_TOOL, HUMAN_TASK, COMPLETE }
