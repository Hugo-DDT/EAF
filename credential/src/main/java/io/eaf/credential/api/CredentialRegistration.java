package io.eaf.credential.api;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/** Workspace 凭据登记只接收 Secret 后端引用，不接收秘密明文。 */
public record CredentialRegistration(UUID workspaceId, String credentialRef, String secretRef,
                                     String audience, Set<String> allowedUses, Set<String> permissions,
                                     Instant expiresAt) {
    public CredentialRegistration {
        allowedUses = allowedUses == null ? Set.of() : Set.copyOf(allowedUses);
        permissions = permissions == null ? Set.of() : Set.copyOf(permissions);
    }
}
