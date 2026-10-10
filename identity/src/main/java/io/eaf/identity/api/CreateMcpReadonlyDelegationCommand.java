package io.eaf.identity.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public record CreateMcpReadonlyDelegationCommand(ActorContext owner, UUID workspaceId, UUID delegateId,
                                                 UUID capabilityId, String capabilityVersion,
                                                 Set<UUID> knowledgeDocumentIds, Instant expiresAt) {
    public CreateMcpReadonlyDelegationCommand {
        knowledgeDocumentIds = knowledgeDocumentIds == null ? Set.of() : Set.copyOf(knowledgeDocumentIds);
    }
}
