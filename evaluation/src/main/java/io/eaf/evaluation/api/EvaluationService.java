package io.eaf.evaluation.api;

import io.eaf.shared.ActorContext;
import io.eaf.task.api.TaskAssetBinding;
import java.time.Instant;
import java.util.UUID;

public interface EvaluationService {
    EvaluationReport run(ActorContext actor, UUID workspaceId);
    EvaluationReport runP2(ActorContext actor, UUID workspaceId);
    //  固定评测必须复用受控 Task/Context/Runtime 链，不能绕过权限直接调用模型。
    EvaluationReport runP3(ActorContext actor, UUID workspaceId);
    //  真实质量入口必须以稳定键注册运行，并用一个质量运行 ID 汇总所有样本费用。
    P3QualityRunReport runP3Quality(ActorContext actor, UUID workspaceId, String idempotencyKey);
    // 质量报告只按 Evaluation Owner 的租户/Workspace 读取，答案与输入正文继续隔离。
    P3QualityRunReport getP3QualityRun(ActorContext actor, UUID workspaceId, UUID reportId);
    // 停止在样本边界生效；使用原幂等键再次启动可继续同一批稳定 Task。
    P3QualityRunReport stopP3QualityRun(ActorContext actor, UUID workspaceId, UUID reportId);
    //  固定集合同样复用真实 Task/Runtime，并把写入安全链路交给独立集成测试验证。
    EvaluationReport runP4(ActorContext actor, UUID workspaceId);

    // Learning 事实审核通过后冻结基线/候选上下文；此步骤不读取评测答案或改变正式资产。
    CandidateContextSnapshot captureCandidateContext(CandidateContextSnapshotCommand command);

    // 候选修订后保留旧快照作审计，但禁止新任务继续读取旧 revision。
    void invalidateCandidateContext(ActorContext actor, UUID workspaceId, UUID candidateId, int revision);

    // 用同一输入各运行一次；后续任务再以固定保留集扩展为成对评测及质量报告。
    CandidatePairRun runCandidatePair(ActorContext actor, UUID workspaceId, UUID snapshotId, String inputText);

    // 仅对固定保留集执行 20 用例、每例 3 次的基线/候选对照，并保留不可变样本证据。
    CandidateEvaluationReport runCandidateEvaluation(ActorContext actor, UUID workspaceId, UUID snapshotId);
    // 同一操作键重试会查询或恢复原运行；新操作键才会创建一份独立质量运行。
    CandidateEvaluationReport runCandidateEvaluation(ActorContext actor, UUID workspaceId, UUID snapshotId,
                                                      String idempotencyKey);

    // 停止只设置持久标志；当前样本收敛后保存为 STOPPED，可用原操作键恢复。
    CandidateEvaluationReport stopCandidateEvaluation(ActorContext actor, UUID workspaceId, UUID reportId);

    // 只允许 Evaluation 授权路径创建 USER 验收用途标记，业务请求不能自行指定质量运行 UUID。
    QualityRunRegistration registerQualityRun(ActorContext actor, UUID workspaceId, String purpose,
                                              String source, String idempotencyKey);

    /** 只登记固定的 TEAM 修订生成资产；普通用途注册不能伪造此质量范围。 */
    TeamImprovementGenerationBinding registerTeamImprovementGeneration(ActorContext actor, UUID workspaceId,
            UUID improvementRunId, String requestHash, String inputHash, Instant deadlineAt);

    /** 在独立事实审核通过后固定地启动 DEV 或保留集 prepare 对照。 */
    TeamPreparationRun startTeamPreparationEvaluation(ActorContext actor, UUID workspaceId, UUID candidateId,
            int candidateRevision, UUID snapshotId, UUID improvementRunId, UUID qualityRunId, String split,
            Instant deadlineAt);
    TeamPreparationRun getTeamPreparationRun(ActorContext actor, UUID workspaceId, UUID reportId);
    TeamPreparationRun stopTeamPreparationEvaluation(ActorContext actor, UUID workspaceId, UUID reportId,
            String reasonCode);
    TeamPreparationPair getTeamPreparationPair(ActorContext actor, UUID workspaceId, UUID reportId, String caseId);
    TeamPreparationReview reviewTeamPreparationPair(ActorContext actor, UUID workspaceId, UUID reportId,
            String caseId, TeamPreparationReviewCommand command, String idempotencyKey);
    TeamPreparationReleaseEvidence getTeamPreparationReleaseEvidence(ActorContext actor, UUID workspaceId,
            UUID candidateId, int candidateRevision);

    // 协作评测只能使用 Evaluation 域固定清单注册；调用方不能传入 Workflow 版本或来源。
    QualityRunRegistration registerCollaborationQualityRun(ActorContext actor, UUID workspaceId,
                                                            String idempotencyKey);
    // 读取当前 Workspace 的不可变分析范围，供评测界面展示固定对照定义。
    CollaborationEvaluationDefinition getCollaborationEvaluationDefinition(ActorContext actor, UUID workspaceId);

    // 模糊事实或引用语义由未启动该运行的人工评审者独立记录，不覆盖自动门结果。
    CandidateSampleReview reviewCandidateSample(CandidateSampleReviewCommand command);

    java.util.List<CandidateSampleReview> listCandidateSampleReviews(ActorContext actor, UUID workspaceId,
                                                                      UUID reportId);

    record TeamImprovementGenerationBinding(QualityRunRegistration registration, UUID agentId, String agentVersion,
            UUID capabilityId, String capabilityVersion, String capabilityHash, UUID skillId, String skillVersion,
            String skillHash, String taskKey) {
        public TaskAssetBinding assetBinding() {
            return new TaskAssetBinding(capabilityId, capabilityVersion, capabilityHash, skillId, skillVersion, skillHash);
        }
    }

    record TeamPreparationRun(UUID reportId, String status, String split, UUID candidateId,
            int candidateRevision, int plannedPairs, int completedPairs, int failedPairs,
            String manifestHash, String modelMode, Instant deadlineAt) { }

    record TeamPreparationReviewCommand(String verdict, String comment, java.util.List<String> factEvidenceRefs,
            UUID supersedesReviewId) { }

    record TeamPreparationPair(UUID reportId, String caseId, String split, String sharedBrief,
            String baselineOutput, String candidateOutput, String baselineHash, String candidateHash) { }

    record TeamPreparationReview(UUID id, String caseId, int revision, UUID reviewerId, String verdict,
            String comment, java.util.List<String> factEvidenceRefs, UUID supersedesReviewId, Instant createdAt) { }

    record TeamPreparationReleaseEvidence(String reportKind, UUID reportId, UUID candidateId, int candidateRevision,
            UUID cardId, int baseRevision, String baseMemoryVersion, String candidateContentHash,
            String evidenceHash, String datasetHash, String rubricVersion, String configurationHash,
            String baselineSnapshotHash, String candidateSnapshotHash, int totalPairs, int completedPairs,
            int betterPairs, int samePairs, int worsePairs, int unevaluablePairs, String modelMode,
            String eligibility, String reasonCode, String reportHash, boolean current) { }

    // 报告按租户和 Workspace 授权读取；不返回保留集原文或预期答案。
    CandidateEvaluationReport getCandidateEvaluation(ActorContext actor, UUID workspaceId, UUID reportId);

    // Learning 批准前复核报告所用快照仍可读取；不把历史 PASSED 当成当前授权证明。
    boolean isCandidateEvaluationCurrent(ActorContext actor, UUID workspaceId, UUID reportId);
}
// 本文件负责实现 EAF 的 EvaluationService.java 相关代码。
