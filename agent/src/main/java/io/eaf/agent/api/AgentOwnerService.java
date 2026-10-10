package io.eaf.agent.api;

import io.eaf.shared.ActorContext;
import java.util.Optional;
import java.util.UUID;

/** P31 固定 P15 只读分析 Agent 的窄派生入口。 */
public interface AgentOwnerService {
    AgentPromptVariant publishPromptAnalysisVariant(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision, UUID adoptionId, UUID promptId, String promptVersion,
            String promptHash, UUID approvalId, UUID reportId, String reportHash);
    Optional<AgentPromptVariant> findPromptAnalysisVariant(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision, UUID adoptionId);
    AgentPromptVariant revokePromptAnalysisVariant(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision, UUID adoptionId);

    record AgentPromptVariant(UUID id, String version, UUID baseId, String baseVersion,
            UUID promptId, String promptVersion, String promptHash, UUID approvalId, UUID reportId,
            String reportHash, String contentHash, String status) { }
}
