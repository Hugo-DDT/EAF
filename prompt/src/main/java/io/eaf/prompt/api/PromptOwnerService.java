package io.eaf.prompt.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/** Prompt 派生目标和发布仍由 Prompt Owner 管理；普通 Catalog 继续只读已发布版本。 */
public interface PromptOwnerService {
    PromptTarget registerAnalysisTarget(ActorContext actor, UUID workspaceId, String idempotencyKey);
    PromptTarget getTarget(ActorContext actor, UUID workspaceId, UUID targetId);
    void assertCandidateAccessible(ActorContext actor, UUID workspaceId, UUID targetId,
            UUID candidateId, int candidateRevision, String baseHash);
    PromptCandidateSnapshot stageAnalysisCandidate(StageAnalysisCandidate command);
    PromptCandidateSnapshot requireEvaluationCandidate(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision);
    RenderedPrompt renderEvaluationCandidate(ActorContext actor, UUID workspaceId, UUID candidateId,
            int candidateRevision, String input);
    PromptRelease publishAnalysisCandidate(PublishAnalysisCandidate command);
    Optional<PromptRelease> findReleaseByOrigin(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision);
    PromptWithdrawal revokeAnalysisCandidate(RevokeAnalysisCandidate command);
    Optional<PromptWithdrawal> findWithdrawalByOrigin(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision);

    record PromptTarget(UUID id, UUID ownerId, UUID basePromptId, String baseVersion, String baseHash,
                        long rowVersion, String status, Instant updatedAt) { }
    record PromptCandidateSnapshot(UUID targetId, UUID candidateId, int candidateRevision,
            String baseHash, String systemTemplate, String userTemplate, String contentHash, String status) { }
    record StageAnalysisCandidate(ActorContext actor, UUID workspaceId, UUID targetId, long expectedTargetVersion,
            UUID candidateId, int candidateRevision, String instructionAppendix) { }
    record PromptRelease(UUID id, UUID targetId, UUID candidateId, int candidateRevision, UUID promptId,
            String promptVersion, String contentHash, UUID approvalId, UUID reportId, String reportHash,
            String status, Instant createdAt) { }
    record PublishAnalysisCandidate(ActorContext actor, UUID workspaceId, UUID targetId, long expectedTargetVersion,
            UUID candidateId, int candidateRevision, UUID approvalId, UUID reportId, String reportHash) { }
    record PromptWithdrawal(UUID id, UUID releaseId, UUID candidateId, int candidateRevision,
            UUID promptId, String promptVersion, String contentHash, String status, Instant createdAt) { }
    record RevokeAnalysisCandidate(ActorContext actor, UUID workspaceId, UUID candidateId,
            int candidateRevision) { }
}
