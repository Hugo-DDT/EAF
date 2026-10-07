package io.eaf.credential.api;

import io.eaf.shared.ActorContext;

/** 不暴露为匿名 REST；只由受信 Workspace 管理流程调用并重新校验当前授权。 */
public interface CredentialAdministration {
    long register(ActorContext actor, CredentialRegistration registration);
    long rotate(ActorContext actor, java.util.UUID workspaceId, String credentialRef,
                String secretRef, long expectedVersion, java.time.Instant expiresAt);
    void disable(ActorContext actor, java.util.UUID workspaceId, String credentialRef, long expectedVersion);
}
