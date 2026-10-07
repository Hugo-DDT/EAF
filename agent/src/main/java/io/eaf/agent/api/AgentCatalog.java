package io.eaf.agent.api;

import java.util.List;
import java.util.UUID;

public interface AgentCatalog {
    AgentDefinition requirePublished(UUID tenantId, UUID workspaceId, UUID agentId, String version);
    List<AgentDefinition> list(UUID tenantId, UUID workspaceId);
    List<ToolBinding> tools(UUID tenantId, UUID workspaceId, UUID agentId, String version);
}
// 本文件负责实现 EAF 的 AgentCatalog.java 相关代码。
