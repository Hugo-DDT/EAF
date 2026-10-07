package io.eaf.memory.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

// 跨模块只返回经过 Workspace 授权的 Memory 版本快照。
public record MemoryDefinition(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, String logicalKey,
                               String version, String type, String scope, String businessEntityType,
                               String businessEntityId, String content, double confidence,
                               Instant expiresAt, String sourceType, String sourceRef, List<String> evidenceRefs,
                               String status, long rowVersion, String contentHash) { }
