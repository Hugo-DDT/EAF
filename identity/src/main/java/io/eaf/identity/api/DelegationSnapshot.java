package io.eaf.identity.api;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public record DelegationSnapshot(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, UUID delegateId,
                                String audience, Set<String> actions, Set<String> customerIds,
                                Instant createdAt, Instant expiresAt, Instant revokedAt, String scopeHash) {
    public DelegationSnapshot {
        actions = Set.copyOf(actions);
        customerIds = Set.copyOf(customerIds);
    }
}
// 委托快照只返回固定范围及摘要，不包含可重放的秘密或访问令牌。
