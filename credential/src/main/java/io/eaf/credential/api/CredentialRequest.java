package io.eaf.credential.api;

import java.util.UUID;

/** 一次出站所需的精确凭据范围；不接受调用方任意指定 Secret 后端路径。 */
public record CredentialRequest(UUID tenantId, UUID workspaceId, String credentialRef,
                                String audience, String use, String permission) { }
