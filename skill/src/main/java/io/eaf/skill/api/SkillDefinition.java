package io.eaf.skill.api;

import java.util.List;
import java.util.UUID;

// Skill 是不可变的版本化行为契约，不携带工具执行凭证或权限授予。
public record SkillDefinition(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId,
                              String name, String description, String version,
                              String inputSchema, String outputSchema,
                              UUID promptId, String promptVersion,
                              List<SkillToolDependency> toolDependencies,
                              String evaluationRef, String status, long rowVersion,
                              String contentHash) { }
