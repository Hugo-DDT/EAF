package io.eaf.evaluation.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.UUID;

/** P31 Prompt 对照复用固定 P22 P15 场景和评分；候选答案仍留在 Evaluation。 */
public interface PromptAnalysisEvaluationService {
    PromptEvaluationRun start(ActorContext actor, UUID workspaceId, UUID targetId, UUID candidateId,
            int candidateRevision, String split, Instant deadlineAt, String idempotencyKey);
    PromptEvaluationRun get(ActorContext actor, UUID workspaceId, UUID reportId);
    PromptEvaluationRun stop(ActorContext actor, UUID workspaceId, UUID reportId);
    PromptAnalysisReleaseEvidence releaseEvidence(ActorContext actor, UUID workspaceId, UUID candidateId,
            int candidateRevision, UUID devReportId, UUID heldOutReportId);

    record PromptEvaluationRun(UUID reportId, UUID candidateId, int candidateRevision, String split,
            String status, int plannedSamples, int completedSamples, int failedSamples, int notRunSamples,
            String datasetHash, String configurationHash, String modelMode, String reportHash) { }
    record PromptAnalysisReleaseEvidence(String reportKind, UUID reportId, UUID devReportId,
            UUID candidateId, int candidateRevision, UUID targetId, String baseHash, String candidateHash,
            String evidenceHash, String datasetHash, String configurationHash, int completedPairs,
            int improvedPairs, int samePairs, int regressedPairs, int unevaluablePairs, String modelMode,
            String usageStatus, String eligibility, String reasonCode, String reportHash, boolean current) { }
}
