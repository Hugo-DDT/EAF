package io.eaf.workspace.api;

import io.eaf.shared.ActorContext;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface WorkspaceAuthorization {
    WorkspaceAccess require(ActorContext actor, UUID workspaceId, String action);
    boolean isAuthorized(UUID tenantId, UUID actorId, UUID workspaceId, String action);
    // Identity 以此公共 API 计算委托时的当前动作授权，不直接读取 Workspace 表。
    Set<String> actions(UUID tenantId, UUID actorId);
    Set<String> actions(UUID tenantId, UUID actorId, UUID workspaceId);
    List<WorkspaceAccess> list(ActorContext actor, String action);
}
// 本文件负责实现 EAF 的 WorkspaceAuthorization.java 相关代码。
