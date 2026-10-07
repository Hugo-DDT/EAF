package io.eaf.identity.api;

import java.util.UUID;

public interface DelegationResourceAuthorizer {
    // 委托不得扩大资源访问；资源域只按自己的有效授权表作决定。
    boolean mayDelegateCustomerRead(UUID tenantId, UUID ownerId, UUID delegateId, UUID workspaceId, String customerId);
}
// 资源域在自身授权表中确认 Owner 与受托 Agent 都可读取目标客户。
