package io.eaf.task.api;

import java.util.UUID;
import java.time.Instant;

/** Task 通过此端口验证服务端质量运行绑定，避免自身持有 Evaluation 表读权限。 */
public interface QualityRunSourceVerifier {
    boolean sourceMatches(UUID tenantId, UUID workspaceId, UUID qualityRunId, String source);

    // 协作运行只能启动清单固定的分析 Workflow 版本；普通用途登记不额外收窄既有行为。
    boolean workflowMatches(UUID tenantId, UUID workspaceId, UUID qualityRunId,
                            UUID workflowId, String workflowVersion);

    // 评测 reviewer 只能由协作清单登记的精确步骤执行，普通 EVALUATION 用途不能复用该连接。
    boolean workflowStepMatches(UUID tenantId, UUID workspaceId, UUID qualityRunId,
                                UUID workflowId, String workflowVersion, String stepId);

    /** 只有活动协作清单中的精确 reviewer 步骤可以授权隔离的评测 peer。 */
    boolean collaborationReviewerStepMatches(UUID tenantId, UUID workspaceId, UUID qualityRunId,
                                              UUID workflowId, String workflowVersion, String stepId);

    /** Task 仅能按 Evaluation 中登记的唯一样本、输入摘要、资产和稳定键创建。 */
    boolean scenarioSampleMatches(UUID tenantId, UUID workspaceId, UUID actorId, UUID qualityRunId,
                                  UUID sampleId, String taskKey, UUID agentId, String agentVersion,
                                  String inputHash, TaskAssetBinding assetBinding);

    /** Runtime 在每次检索/生成前调用，复核原样本 Task、期限、资产与合成 Knowledge 清单。 */
    boolean scenarioTaskMatches(UUID tenantId, UUID workspaceId, UUID actorId, UUID taskId, UUID rootTaskId,
                                UUID qualityRunId, int attempt, UUID agentId, String agentVersion,
                                String source, String runKind, String inputHash, TaskAssetBinding assetBinding);

    boolean isScenarioRun(UUID tenantId, UUID workspaceId, UUID qualityRunId);
    boolean isScenarioTask(UUID tenantId, UUID workspaceId, UUID taskId);

    /** 生成 Task 只能使用登记的单次输入、固定资产、稳定键和截止时间。 */
    boolean isTeamImprovementRun(UUID tenantId, UUID workspaceId, UUID qualityRunId);
    boolean isTeamPreparationRun(UUID tenantId, UUID workspaceId, UUID qualityRunId);
    boolean teamImprovementGenerationMatches(UUID tenantId, UUID workspaceId, UUID ownerId, UUID qualityRunId,
            UUID improvementRunId, String taskKey, UUID agentId, String agentVersion, String inputHash,
            TaskAssetBinding assetBinding, Instant deadlineAt);
    boolean bindTeamImprovementGenerationTask(UUID tenantId, UUID workspaceId, UUID ownerId, UUID qualityRunId,
            UUID improvementRunId, UUID taskId, int attempt, String taskKey, UUID agentId, String agentVersion,
            String inputHash, TaskAssetBinding assetBinding, Instant deadlineAt);
    boolean isTeamImprovementTask(UUID tenantId, UUID workspaceId, UUID taskId);
    boolean isTeamPreparationTask(UUID tenantId, UUID workspaceId, UUID taskId);

    /** 样本 Task 必须命中固定候选快照、分区、例子、侧别和资产。 */
    boolean teamPreparationSampleMatches(UUID tenantId, UUID workspaceId, UUID ownerId, UUID qualityRunId,
            UUID sampleId, UUID snapshotId, String taskKey, UUID agentId, String agentVersion, String inputHash,
            TaskAssetBinding assetBinding, Instant deadlineAt);
    boolean bindTeamPreparationSample(UUID tenantId, UUID workspaceId, UUID ownerId, UUID qualityRunId,
            UUID sampleId, UUID snapshotId, UUID taskId, int attempt, String taskKey, UUID agentId, String agentVersion,
            String inputHash, TaskAssetBinding assetBinding, Instant deadlineAt);
}
