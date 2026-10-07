package io.eaf.workflow.infrastructure;

import io.eaf.workflow.api.WorkflowInstance;

// Worker 只取得持久实例快照和当前租约，不依赖进程内对话状态。
public record WorkflowRuntimeState(WorkflowInstance instance, String authorizationHash,
                                   WorkflowLease lease) { }
