package io.eaf.identity.api;

import java.util.UUID;

/** 固定企业主体映射的脱敏结果，不包含原始 JWT 或认证凭证。 */
public record ExternalIdentityBinding(UUID actorId, boolean active) { }
