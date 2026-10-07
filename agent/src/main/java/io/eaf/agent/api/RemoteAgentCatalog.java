package io.eaf.agent.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

/** 只向经 Workspace 授权的调用者提供启用的远端 Agent 登记。 */
public interface RemoteAgentCatalog {
    RemoteAgentRegistration requireActive(ActorContext actor, UUID workspaceId, String agentKey);
}
