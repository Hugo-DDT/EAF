package io.eaf.memory.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

// Memory Owner 只能从已验证身份取得；客户端不能声明来源类型或替换所有者。
public record CreateMemoryCommand(ActorContext actor, UUID workspaceId, String logicalKey, String version,
                                  String type, String scope, String content, Double confidence,
                                  Instant expiresAt, String sourceRef, List<String> evidenceRefs,
                                  String businessEntityType, String businessEntityId) {
    public CreateMemoryCommand(ActorContext actor, UUID workspaceId, String logicalKey, String version,
                               String type, String scope, String content, Double confidence, Instant expiresAt,
                               String sourceRef, List<String> evidenceRefs) {
        this(actor, workspaceId, logicalKey, version, type, scope, content, confidence, expiresAt, sourceRef,
                evidenceRefs, null, null);
    }
}
