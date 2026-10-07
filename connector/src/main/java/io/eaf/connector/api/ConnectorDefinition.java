package io.eaf.connector.api;

import java.util.UUID;
import java.util.Set;

/** Connector 暴露端点登记及凭据授权元数据；不包含解析后的明文秘密。 */
public record ConnectorDefinition(UUID id, UUID tenantId, UUID workspaceId, String provider,
                                  String baseUrl, String status, String credentialRef, String audience,
                                  Set<String> allowedUses, Set<String> permissions) {
    public ConnectorDefinition {
        allowedUses = allowedUses == null ? Set.of() : Set.copyOf(allowedUses);
        permissions = permissions == null ? Set.of() : Set.copyOf(permissions);
    }
}
// 本文件负责实现 EAF 的 ConnectorDefinition.java 相关代码。
