package io.eaf.identity.api;

import java.util.Set;
import java.util.UUID;

public record McpReadonlyDelegationSnapshot(DelegationSnapshot delegation, String profile,
                                            UUID capabilityId, String capabilityVersion, String capabilityHash,
                                            Set<UUID> knowledgeDocumentIds) {
    public McpReadonlyDelegationSnapshot {
        knowledgeDocumentIds = Set.copyOf(knowledgeDocumentIds);
    }
}
