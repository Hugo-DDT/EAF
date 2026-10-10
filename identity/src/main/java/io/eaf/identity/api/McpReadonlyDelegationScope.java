package io.eaf.identity.api;

import java.util.Set;
import java.util.UUID;

public record McpReadonlyDelegationScope(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, UUID delegateId,
                                         String profile, UUID capabilityId, String capabilityVersion,
                                         String capabilityHash, Set<UUID> knowledgeDocumentIds, String scopeHash) {
    public McpReadonlyDelegationScope {
        knowledgeDocumentIds = Set.copyOf(knowledgeDocumentIds);
    }
}
