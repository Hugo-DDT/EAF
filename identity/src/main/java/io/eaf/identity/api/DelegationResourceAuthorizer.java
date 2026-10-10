package io.eaf.identity.api;

import java.util.UUID;
import java.util.Optional;

public interface DelegationResourceAuthorizer {
    // 委托不得扩大资源访问；资源域只按自己的有效授权表作决定。
    default boolean mayDelegateCustomerRead(UUID tenantId, UUID ownerId, UUID delegateId,
                                             UUID workspaceId, String customerId) {
        return false;
    }

    default Optional<McpCapabilityReference> resolveMcpReadonlyCapability(UUID tenantId, UUID ownerId,
            UUID delegateId, UUID workspaceId, UUID capabilityId, String capabilityVersion) {
        return Optional.empty();
    }

    default boolean mayDelegateKnowledgeRead(UUID tenantId, UUID ownerId, UUID delegateId,
                                              UUID workspaceId, UUID documentId) {
        return false;
    }
}
// 资源域在自身授权表中确认 Owner 与受托 Agent 都可读取目标客户。
