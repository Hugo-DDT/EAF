package io.eaf.policy.api;

import io.eaf.shared.ActorContext;
import java.util.Set;
import java.util.UUID;

/** Policy 客户资源授权的域内写 API；目标客户集合始终受调用者现有范围约束。 */
public interface PolicyCustomerGrantAdministration {
    /** 客户范围不得超出管理者自己当前可读的范围，且目标须为同租户有效成员。 */
    int grantCustomers(ActorContext administrator, UUID workspaceId, UUID subjectId, Set<String> customerIds);

    /** Workspace Policy 管理权限可撤销本空间客户授权，重复撤销不会产生新审计事实。 */
    int revokeCustomers(ActorContext administrator, UUID workspaceId, UUID subjectId, Set<String> customerIds);

    /** 仅供租户成员停用事务调用，按租户清除成员在所有 Workspace 的客户授权。 */
    int revokeAllForTenantMember(ActorContext tenantAdministrator, UUID tenantId, UUID subjectId);
}
