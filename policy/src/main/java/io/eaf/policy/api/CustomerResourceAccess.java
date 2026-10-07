package io.eaf.policy.api;

import io.eaf.shared.ActorContext;
import java.util.List;
import java.util.UUID;

/** Policy 对客户授权提供只读资格查询；调用方不直接连接客户授权表。 */
public interface CustomerResourceAccess {
    boolean canRead(ActorContext actor, UUID workspaceId, String customerId);

    /** 返回当前仍有效且具有指定 Workspace 动作的客户授权主体。 */
    List<UUID> activeReaders(UUID tenantId, UUID workspaceId, String customerId);
}
