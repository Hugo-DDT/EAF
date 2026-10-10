package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

/** 仅允许 Workflow 为已登记的自动化运行创建固定只读 USER 根 Task。 */
public record CreateAutomationReadTaskCommand(ActorContext actor, UUID workspaceId, UUID runId,
        UUID subscriptionId, long authorizationEpoch, UUID agentId, String agentVersion, String input, TaskAssetBinding assetBinding,
        String inputHash, String profileHash, String traceId) { }
