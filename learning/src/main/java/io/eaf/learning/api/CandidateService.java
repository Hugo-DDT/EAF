package io.eaf.learning.api;

import com.fasterxml.jackson.databind.JsonNode;
import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Learning 持有候选修订和审核历史；目标资产读写仍必须通过所属模块 API。 */
public interface CandidateService {
    CandidateProposalResult proposeFromFeedback(ActorContext actor, UUID workspaceId, UUID feedbackId);
    /** 仅改进运行的服务端生成 Task 可创建 TEAM_EXPERIENCE_UPDATE 候选。 */
    CandidateSubmission proposeFromImprovementRun(ActorContext actor, UUID workspaceId, UUID improvementRunId);
    ImprovementRunSubmission createImprovementRun(ImprovementRunCommand command);
    PromptImprovementRunSubmission createPromptImprovementRun(PromptImprovementRunCommand command);
    PromptImprovementRun getPromptImprovementRun(ActorContext actor, UUID workspaceId, UUID runId);
    PromptImprovementRun stopPromptImprovementRun(ActorContext actor, UUID workspaceId, UUID runId, long expectedVersion);
    PromptAdoption adoptPromptImprovementRun(PromptAdoptionCommand command);
    PromptAdoption getPromptAdoption(ActorContext actor, UUID workspaceId, UUID candidateId);
    ImprovementRun getImprovementRun(ActorContext actor, UUID workspaceId, UUID improvementRunId);
    ImprovementRun stopImprovementRun(ActorContext actor, UUID workspaceId, UUID improvementRunId, long expectedVersion);
    CandidateSubmission propose(ManualCandidateCommand command);
    LearningCandidate get(ActorContext actor, UUID workspaceId, UUID candidateId);
    LearningCandidate revise(CandidateRevisionCommand command);
    LearningCandidate review(CandidateReviewCommand command);
    CandidateApproval approve(CandidateApprovalCommand command);
    CandidateRelease publish(CandidateReleaseCommand command);
    CandidateRelease getRelease(ActorContext actor, UUID workspaceId, UUID candidateId);
    CandidateWithdrawal withdrawRelease(CandidateWithdrawalCommand command);
    CandidateWithdrawal getWithdrawal(ActorContext actor, UUID workspaceId, UUID candidateId);
    // 只接收 USER Task 标识；Learning 自行核对来源、发布版本、评测批准与实际引用。
    CandidateIterationSubmission recordIteration(ActorContext actor, UUID workspaceId, UUID candidateId,
                                                 UUID followupTaskId);
    // 仅读取 Learning 保存的脱敏使用证据，不返回 Task 输入或输出正文。
    List<CandidateIteration> iterations(ActorContext actor, UUID workspaceId, UUID candidateId);

    /** 人工提出完整资产草稿；proposer、Owner 与 sourceType 均由服务端取得。 */
    record ManualCandidateCommand(ActorContext actor, UUID workspaceId, String targetType, UUID targetId,
                                 String baseVersion, JsonNode proposedContent, List<String> evidenceRefs,
                                 UUID sourceFeedbackId, String idempotencyKey) { }

    /** 更新候选必须创建新 revision，并用行版本避免并发覆盖。 */
    record CandidateRevisionCommand(ActorContext actor, UUID workspaceId, UUID candidateId,
                                    long expectedVersion, String targetType, UUID targetId,
                                    String baseVersion, JsonNode proposedContent,
                                    List<String> evidenceRefs) { }

    /** 接受审核需记录核验依据；审核只进入隔离评测阶段，不发布资产。 */
    record CandidateReviewCommand(ActorContext actor, UUID workspaceId, UUID candidateId,
                                  long expectedVersion, String decision, String reason,
                                  List<String> factEvidenceRefs) { }

    /** 只批准已审核且具有当前通过报告的精确 revision；拒绝决定不需要评测报告。 */
    record CandidateApprovalCommand(ActorContext actor, UUID workspaceId, UUID candidateId,
                                    long expectedVersion, String decision, String reason,
                                    UUID evaluationReportId) { }

    /** 发布调用方必须是 HUMAN Owner；目标模块仍自行复核本域权限、基线和内容摘要。 */
    record CandidateReleaseCommand(ActorContext actor, UUID workspaceId, UUID candidateId,
                                   long expectedVersion, UUID approvalId) { }

    /** 撤回只移除目标版本的新使用资格，不删除发布历史或已发生的外部副作用。 */
    record CandidateWithdrawalCommand(ActorContext actor, UUID workspaceId, UUID candidateId,
                                      long expectedVersion, String reasonRef) { }

    /** USER Task ID 是唯一输入；Learning 自行复核 Task、发布版本和实际引用来源。 */
    record CandidateIterationSubmission(CandidateIteration iteration, boolean created) { }

    record ImprovementRunCommand(ActorContext actor, UUID workspaceId, UUID cardId, int baseRevision,
            long expectedCardVersion, List<UUID> sourceFeedbackIds, String sharedCorrection,
            String datasetKey, String datasetVersion, Instant deadlineAt, String idempotencyKey) { }

    record ImprovementRunSubmission(ImprovementRun run, boolean created) { }

    record PromptImprovementRunCommand(ActorContext actor, UUID workspaceId, UUID targetId,
            long expectedTargetVersion, List<UUID> sourceFeedbackIds, String changeNote, String instructionAppendix,
            Instant deadlineAt, String idempotencyKey) { }
    record PromptImprovementRunSubmission(PromptImprovementRun run, LearningCandidate candidate, boolean created) { }
    record PromptImprovementRun(UUID id, UUID ownerId, UUID targetId, String baseHash, String changeNote, UUID candidateId,
            Integer candidateRevision, UUID devReportId, UUID heldOutReportId, String status, String reasonCode,
            long rowVersion, Instant createdAt, Instant updatedAt, Instant deadlineAt,
            List<UUID> sourceFeedbackIds) { }
    record PromptAdoptionCommand(ActorContext actor, UUID workspaceId, UUID candidateId, String idempotencyKey) { }
    record PromptAdoption(UUID id, UUID candidateId, int candidateRevision, UUID promptId, String promptVersion,
            String promptHash, UUID agentId, String agentVersion, String agentHash, UUID skillId,
            String skillVersion, String skillHash, UUID capabilityId, String capabilityVersion,
            String capabilityHash, UUID approvalId, UUID reportId, String reportHash, String status,
            Instant createdAt, Instant updatedAt) { }

    record ImprovementSourceRef(UUID feedbackId, UUID taskId, int attempt, String contextHash,
            int cardRevision, String memoryVersion, String contentHash) { }

    record ImprovementRun(UUID id, UUID workspaceId, UUID ownerId, UUID cardId, int baseRevision,
            String baseMemoryVersion, long expectedCardVersion, List<ImprovementSourceRef> sources,
            String sharedCorrection, String datasetKey, String datasetVersion, UUID qualityRunId,
            UUID generationTaskId, UUID candidateId, Integer candidateRevision, UUID devReportId,
            UUID heldOutReportId, String status, String stopReason, boolean stopRequested,
            long rowVersion, Instant createdAt, Instant updatedAt, Instant deadlineAt) { }
}
