package io.eaf.provisioning.api;

import java.util.UUID;

/** 返回内部主体标识及本次是否创建了新主体，不返回企业令牌或外部 subject。 */
public record ProvisionedHuman(UUID actorId, boolean created) { }
