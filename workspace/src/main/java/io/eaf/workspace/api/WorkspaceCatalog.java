package io.eaf.workspace.api;

import io.eaf.shared.ActorContext;
import java.util.List;
import java.util.UUID;

/** Workspace profile、来源选择与生命周期的公开 Owner API。 */
public interface WorkspaceCatalog {
    WorkspaceCreateResult create(ActorContext actor, UUID workspaceId, String name, WorkspaceKind kind,
                                 UUID parentWorkspaceId);
    WorkspacePage list(ActorContext actor, WorkspaceKind kind, int limit, int offset);
    WorkspaceProfile profile(UUID tenantId, UUID workspaceId);
    ContextSourceSelection listContextSources(ActorContext actor, UUID targetWorkspaceId, int limit, int offset);
    ContextSourceSelection replaceContextSources(ActorContext actor, UUID targetWorkspaceId,
                                                  List<UUID> sourceWorkspaceIds);
    ResolvedContextSources resolveContextSources(ActorContext actor, UUID targetWorkspaceId,
                                                 List<UUID> explicitSourceWorkspaceIds);
    /** 成员停用事务内关闭本人空间并清理本人来源偏好；保留空间与 owner 唯一记录。 */
    void deactivatePersonalSpaceForMember(UUID tenantId, UUID ownerId);
}
