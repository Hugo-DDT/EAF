package io.eaf.evaluation.api;

import io.eaf.context.api.EnterpriseContext;
import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.UUID;

/** Learning 审核后交给 Evaluation 保存的固定基线/候选上下文对。 */
public record CandidateContextSnapshotCommand(ActorContext actor, UUID workspaceId, UUID candidateId,
                                               int candidateRevision, String targetType, UUID targetId,
                                               String baseVersion, UUID ownerId, String scope,
                                               String candidateContentHash, EnterpriseContext baselineContext,
                                               EnterpriseContext candidateContext,
                                               TeamPreparationSnapshotBinding teamPreparationBinding) {
    public CandidateContextSnapshotCommand(ActorContext actor, UUID workspaceId, UUID candidateId,
            int candidateRevision, String targetType, UUID targetId, String baseVersion, UUID ownerId,
            String scope, String candidateContentHash, EnterpriseContext baselineContext,
            EnterpriseContext candidateContext) {
        this(actor, workspaceId, candidateId, candidateRevision, targetType, targetId, baseVersion,
                ownerId, scope, candidateContentHash, baselineContext, candidateContext, null);
    }

    /** 精确 TEAM 基线和来源绑定；Evaluation 自己保存和复核这些字段。 */
    public record TeamPreparationSnapshotBinding(UUID improvementRunId, UUID qualityRunId, UUID cardId,
            int baseRevision, String baseMemoryVersion, long expectedCardVersion, String scenarioKey,
            Instant expiresAt, UUID sourceWorkItemId, String evidenceHash) { }
}
