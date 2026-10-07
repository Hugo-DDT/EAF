package io.eaf.capability.api;

import java.util.List;
import java.util.UUID;

// Capability 是对既有权限边界的固定选择，不会为 Agent 增加工具授权。
public record CapabilityDefinition(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId,
                                   String name, String description, String version,
                                   UUID agentId, String agentVersion, UUID skillId, String skillVersion,
                                   String skillContentHash, UUID promptId, String promptVersion,
                                   List<CapabilityToolDependency> toolDependencies, String evaluationRef,
                                   String status, long rowVersion, String contentHash) { }
