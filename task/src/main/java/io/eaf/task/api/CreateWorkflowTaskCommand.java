package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

// Workflow 每个执行步骤创建一个真实 Task，共享实例预算但不创建空的编排 Task。
public record CreateWorkflowTaskCommand(ActorContext actor, UUID workspaceId, UUID budgetScopeId,
                                        UUID rootTaskId, String dispatchKey, UUID agentId,
                                        String agentVersion, String input, String source,
                                        TaskAssetBinding assetBinding, String toolName,
                                        String toolVersion, String toolArgumentsJson, String traceId,
                                        UUID qualityRunId, WorkflowTaskProvenance workflowProvenance) {
    public CreateWorkflowTaskCommand(ActorContext actor, UUID workspaceId, UUID budgetScopeId,
                                     UUID rootTaskId, String dispatchKey, UUID agentId,
                                     String agentVersion, String input, String source,
                                     TaskAssetBinding assetBinding, String toolName,
                                     String toolVersion, String toolArgumentsJson, String traceId) {
        this(actor, workspaceId, budgetScopeId, rootTaskId, dispatchKey, agentId, agentVersion,
                input, source, assetBinding, toolName, toolVersion, toolArgumentsJson, traceId, null, null);
    }

    public CreateWorkflowTaskCommand(ActorContext actor, UUID workspaceId, UUID budgetScopeId,
                                     UUID rootTaskId, String dispatchKey, UUID agentId,
                                     String agentVersion, String input, String source,
                                     TaskAssetBinding assetBinding, String toolName,
                                     String toolVersion, String toolArgumentsJson, String traceId,
                                     UUID qualityRunId) {
        this(actor, workspaceId, budgetScopeId, rootTaskId, dispatchKey, agentId, agentVersion,
                input, source, assetBinding, toolName, toolVersion, toolArgumentsJson, traceId, qualityRunId, null);
    }
}
