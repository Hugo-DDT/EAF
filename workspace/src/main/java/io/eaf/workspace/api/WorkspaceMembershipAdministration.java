package io.eaf.workspace.api;

import io.eaf.shared.ActorContext;
import java.util.Set;
import java.util.UUID;

/** Workspace 授权的唯一业务写入口；调用方不能直接修改 grant 表。 */
public interface WorkspaceMembershipAdministration {
    /** 目标成员必须有效；只允许转授管理者在该 Workspace 当前持有的动作。 */
    int grant(ActorContext administrator, UUID workspaceId, UUID subjectId, Set<String> actions);

    /** 撤权可移除目标当前动作，但不能移除本人管理资格或最后一位 Workspace 管理员。 */
    int revoke(ActorContext administrator, UUID workspaceId, UUID subjectId, Set<String> actions);

    /** 仅供租户成员停用事务使用；会保留已撤销行并追加一次变更审计。 */
    int revokeAllForTenantMember(ActorContext tenantAdministrator, UUID tenantId, UUID subjectId);
}
