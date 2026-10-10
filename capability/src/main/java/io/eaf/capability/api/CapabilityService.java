package io.eaf.capability.api;

import io.eaf.shared.ActorContext;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

// 跨域只公开经 Workspace 授权解析的 Capability Manifest。
public interface CapabilityService {
    CapabilityDefinition create(CreateCapabilityCommand command);
    CapabilityDefinition addVersion(ActorContext actor, UUID workspaceId, UUID capabilityId, CreateCapabilityVersionCommand command);
    List<CapabilityDefinition> list(ActorContext actor, UUID workspaceId);
    CapabilityDefinition get(ActorContext actor, UUID workspaceId, UUID capabilityId, String version);
    CapabilityDefinition requirePublished(ActorContext actor, UUID workspaceId, UUID capabilityId, String version);
    CapabilityDefinition publish(ActorContext actor, UUID workspaceId, UUID capabilityId, String version, long expectedVersion);
    CapabilityDefinition revoke(ActorContext actor, UUID workspaceId, UUID capabilityId, String version, long expectedVersion);
    PromptVariant publishPromptAnalysisVariant(ActorContext actor, UUID workspaceId, UUID candidateId,
            int candidateRevision, UUID adoptionId, UUID agentId, String agentVersion, UUID skillId,
            String skillVersion, UUID promptId, String promptVersion, String promptHash,
            UUID approvalId, UUID reportId, String reportHash);
    Optional<PromptVariant> findPromptAnalysisVariant(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision, UUID adoptionId);
    PromptVariant revokePromptAnalysisVariant(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision, UUID adoptionId);
    boolean isPromptAnalysisVariant(ActorContext actor, UUID workspaceId, UUID capabilityId, String capabilityVersion);

    record PromptVariant(UUID id, String version, UUID baseId, String baseVersion, UUID agentId,
            String agentVersion, UUID skillId, String skillVersion, UUID promptId, String promptVersion,
            String promptHash, UUID approvalId, UUID reportId, String reportHash, String contentHash, String status) { }
}
