package io.eaf.tool.api;

import io.eaf.shared.ActorContext;
import java.util.List;
import java.util.UUID;

public interface ToolCatalog {
    List<ToolDefinition> list(ActorContext actor, UUID workspaceId);
    ToolDefinition requirePublished(UUID tenantId, UUID workspaceId, String name, String version);
}
// 本文件负责实现 EAF 的 ToolCatalog.java 相关代码。
