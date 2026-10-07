package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface TaskService {
    TaskSnapshot create(CreateTaskCommand command);
    TaskSnapshot createExperienceDraft(CreateExperienceDraftCommand command);
    ExperienceDraftBinding experienceDraftBinding(ActorContext actor, UUID workspaceId, UUID taskId);
    void requireExperienceDraftTask(ActorContext actor, UUID workspaceId, UUID taskId);
    ServiceRequestSubmission confirmServiceRequest(ActorContext actor, UUID workspaceId, UUID sourceTaskId,
                                                   long expectedTaskVersion, String idempotencyKey);
    ServiceRequestSubmission serviceRequestSubmission(ActorContext actor, UUID workspaceId, UUID sourceTaskId);
    Optional<ServiceRequestSubmission> findServiceRequestSubmission(ActorContext actor, UUID workspaceId, UUID sourceTaskId);
    ServiceRequestSubmission requireServiceRequestSubmission(ActorContext actor, UUID workspaceId, UUID submissionId);
    void bindServiceRequestWorkflow(ActorContext actor, UUID workspaceId, UUID submissionId, UUID workflowInstanceId);
    void requireServiceRequestTaskCreation(ActorContext actor, UUID workspaceId, WorkflowTaskProvenance provenance,
                                          String toolName, String toolVersion, String argumentsJson);
    void requireServiceRequestHandlingTask(ActorContext actor, UUID workspaceId, UUID taskId);
    ServiceRequestWritePayload requireServiceRequestWritePayload(ActorContext actor, UUID workspaceId,
            UUID taskId, int attempt, UUID submissionId);
    ConversationSnapshot createConversation(CreateConversationCommand command);
    ConversationSnapshot getConversation(ActorContext actor, UUID workspaceId, UUID conversationId);
    ConversationPage listConversations(ActorContext actor, UUID workspaceId, String status,
                                       Instant cursorUpdatedAt, UUID cursorId, int pageSize);
    ConversationSnapshot updateConversation(ActorContext actor, UUID workspaceId, UUID conversationId,
                                            long expectedVersion, String title, String status);
    ConversationTurn createConversationTurn(CreateConversationTurnCommand command);
    ConversationTurnPage listConversationTurns(ActorContext actor, UUID workspaceId, UUID conversationId,
                                               Integer beforeTurnNo, int pageSize);
    ConversationBrief getConversationBrief(ActorContext actor, UUID workspaceId, UUID conversationId);
    ConversationBriefSave saveConversationBrief(SaveConversationBriefCommand command);
    ConversationRuntimeContext conversationRuntimeContext(UUID taskId);
    ConversationBinding conversationBinding(ActorContext actor, UUID workspaceId, UUID taskId);
    void requireConversationTaskAccess(ActorContext actor, UUID workspaceId, UUID taskId);
    boolean isConversationTaskVisible(ActorContext actor, UUID workspaceId, UUID taskId);
    void requireCurrentConversationFollowup(ActorContext actor, UUID workspaceId, UUID conversationId,
                                            UUID taskId, long expectedTaskVersion, int expectedBriefRevision);
    // Evaluation 专用入口绑定服务端质量运行 ID；普通业务 Task 创建命令无法设置该标记。
    TaskSnapshot createQualityRunTask(CreateQualityRunTaskCommand command);
    // Workflow 在固定实例来源前验证质量运行登记，避免内部调用把 EVALUATION 改标成 USER。
    void requireQualityRunSource(ActorContext actor, UUID workspaceId, UUID qualityRunId, String source);
    // 隔离 Workflow 还必须符合该质量运行在 Evaluation 清单中绑定的固定流程版本。
    void requireQualityRunWorkflow(ActorContext actor, UUID workspaceId, UUID qualityRunId, String source,
                                   UUID workflowId, String workflowVersion);
    // 仅 Evaluation 报告和受限 Runtime 可读取此 Task 的正文；通用 Task 投影会将正文隐藏。
    TaskSnapshot getScenarioEvaluationTask(ActorContext actor, UUID workspaceId, UUID taskId);
    /** Learning 内部在核对成功 attempt 后读取固定生成 Task；普通 Task 查询会隐藏其输入与结果。 */
    TaskSnapshot getTeamImprovementGenerationTask(ActorContext actor, UUID workspaceId, UUID taskId);
    boolean isTeamImprovementGenerationTask(UUID tenantId, UUID workspaceId, UUID taskId);
    TaskSnapshot cancelTeamImprovementGenerationTask(ActorContext actor, UUID workspaceId, UUID taskId,
                                                    long expectedVersion);
    ScenarioTaskTiming getScenarioEvaluationTiming(ActorContext actor, UUID workspaceId, UUID taskId, int attempt);
    void requireScenarioEvaluationTask(ActorContext actor, UUID workspaceId, UUID taskId);
    boolean isScenarioEvaluationTask(UUID tenantId, UUID workspaceId, UUID taskId);
    boolean isTeamPreparationEvaluationTask(UUID tenantId, UUID workspaceId, UUID taskId);
    void cancelScenarioEvaluationTask(UUID tenantId, UUID workspaceId, UUID ownerId, UUID taskId);
    // Execution 每次发送和恢复评测 reviewer 前，都复核 Task 的来源、工具和 Workflow 步骤关联。
    void requireEvaluationReviewerTask(ActorContext actor, UUID workspaceId, UUID taskId);
    // Workflow 用实例与步骤组成的稳定来源键恢复同一个业务 Task。
    TaskSnapshot createWorkflowTask(CreateWorkflowTaskCommand command);
    // Workflow 按稳定步骤键取消；未建 Task 时写入 tombstone，阻止迟到的创建。
    WorkflowTaskCancellation cancelWorkflowTask(UUID tenantId, UUID workspaceId, UUID actorId, String dispatchKey);
    java.util.Optional<TaskSnapshot> findByIdempotencyKey(ActorContext actor, UUID workspaceId, String idempotencyKey);
    // Workflow 等长流程可在首个 Task 前创建独立预算范围，不伪造运行中的 Task。
    BudgetScopeReference createBudgetScope(CreateBudgetScopeCommand command);
    // 子任务由 Task 域从父任务继承身份、来源、入口协议与根预算。
    TaskSnapshot createChild(CreateChildTaskCommand command);
    // 固定工具任务只记录不可变 Tool 快照，实际调用仍经过 Runtime 和 Execution。
    TaskSnapshot createToolExecution(CreateToolExecutionCommand command);
    TaskSnapshot get(ActorContext actor, UUID workspaceId, UUID taskId);
    // 列表过滤与单项读取共用 Workspace 和委托快照校验，游标只影响排序位置。
    TaskPage list(ActorContext actor, UUID workspaceId, UUID rootTaskId, Set<TaskStatus> statuses,
                  Instant statusUpdatedAfter, TaskPageCursor cursor, int pageSize);
    TaskSnapshot cancel(ActorContext actor, UUID workspaceId, UUID taskId, long expectedVersion);
    // Execution 按原 remoteTaskId 收到取消确认后，Task 域提交最终取消事实。
    void confirmRemoteCancellation(UUID tenantId, UUID workspaceId, UUID taskId, int attempt, UUID operationId);
    TaskSnapshot retry(ActorContext actor, UUID workspaceId, UUID taskId, long expectedVersion, String idempotencyKey);
    TaskSnapshot resume(ActorContext actor, UUID workspaceId, UUID taskId, long expectedVersion, String idempotencyKey);
    java.util.Optional<TaskWorkItem> claimOne();
    java.util.Optional<TaskWorkItem> claim(UUID taskId);
    // Execution 的持久轮询调度只唤醒与原 operationId 绑定的 WAITING_REMOTE Task。
    RemoteTaskWakeStatus wakeRemote(UUID tenantId, UUID workspaceId, UUID taskId, int attempt, UUID operationId);
    boolean renewLease(TaskWorkItem workItem);
    boolean isExecutionAuthorized(TaskWorkItem workItem);
    // Execution 将真实当前身份传回 Task 域，由 Task 域核对状态、委托快照、期限与 Workspace 授权。
    TaskExecutionCheck checkExecution(ActorContext actor, UUID workspaceId, UUID taskId, int attempt);
    // Execution 与 Task 取消共用 Task 行锁，确保“写入意图已提交”与“取消成功”只有一个先发生。
    void markExternalEffect(UUID taskId, int attempt, UUID operationId, String status);
    // reservationKey 把一次逻辑外部调用绑定为根预算中的单次预留和结算。
    BudgetReservation reserveModel(UUID taskId, int attempt, String reservationKey);
    BudgetReservation reserveTool(UUID taskId, int attempt, String reservationKey);
    // 每次远端 GetTask 轮询仍消耗同一根 Task 的步骤与工具调用预算。
    BudgetReservation reserveRemotePoll(UUID taskId, int attempt, UUID operationId, String reservationKey);
    void settleModel(UUID taskId, int attempt, String reservationKey, Integer inputTokens, Integer outputTokens);
    void recordToolExecution(UUID taskId, int attempt);
    TaskEvidence evidence(UUID tenantId, UUID workspaceId, UUID taskId);
    void complete(TaskWorkItem workItem, TaskRunner.RunOutcome outcome);
    List<TaskAttemptRecovery> recoverOnStartup();

    record CreateConversationCommand(ActorContext actor, UUID workspaceId, String mode, String title,
                                     String customerId, UUID capabilityId, String capabilityVersion,
                                     String capabilityHash, UUID agentId, String agentVersion,
                                     UUID skillId, String skillVersion, String skillHash,
                                     String idempotencyKey) { }

    record CreateExperienceDraftCommand(CreateTaskCommand task, UUID sourceTaskId, UUID sourceFeedbackId,
                                        UUID sourceConversationId) { }
    record ExperienceDraftBinding(UUID taskId, UUID ownerId, UUID sourceTaskId, UUID sourceFeedbackId,
                                  UUID sourceConversationId, String inputHash, Instant createdAt) { }

    record ConversationSnapshot(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId, String mode,
                                String title, String customerId, UUID capabilityId, String capabilityVersion,
                                String capabilityHash, UUID agentId, String agentVersion, UUID skillId,
                                String skillVersion, String skillHash, String status, int currentBriefRevision,
                                long version, int lastTurnNo, UUID activeTaskId, String activeTaskStatus,
                                Instant createdAt, Instant updatedAt) { }

    record ConversationPage(List<ConversationSnapshot> items, Instant nextUpdatedAt, UUID nextId) {
        public ConversationPage { items = items == null ? List.of() : List.copyOf(items); }
    }

    record CreateConversationTurnCommand(ActorContext actor, UUID workspaceId, UUID conversationId, UUID turnId,
                                         String input, String idempotencyKey, int expectedBriefRevision,
                                         int expectedHistoryThroughTurnNo, String contextSnapshotJson,
                                         List<UUID> includedTaskIds) {
        public CreateConversationTurnCommand {
            includedTaskIds = includedTaskIds == null ? List.of() : List.copyOf(includedTaskIds);
        }
    }

    record ConversationTurn(UUID id, UUID conversationId, UUID taskId, int turnNo, String input,
                            int briefRevision, int historyThroughTurnNo, String status,
                            long taskVersion, Instant createdAt) { }

    record ConversationTurnPage(List<ConversationTurn> items, int nextBeforeTurnNo) {
        public ConversationTurnPage { items = items == null ? List.of() : List.copyOf(items); }
    }

    record ConversationBrief(UUID conversationId, int revision, String content, UUID sourceTaskId,
                             List<UUID> sourceTurnIds, UUID confirmedBy, Instant createdAt) {
        public ConversationBrief {
            sourceTurnIds = sourceTurnIds == null ? List.of() : List.copyOf(sourceTurnIds);
        }
    }

    record SaveConversationBriefCommand(ActorContext actor, UUID workspaceId, UUID conversationId,
                                        int expectedRevision, String content, UUID suggestionTaskId,
                                        List<UUID> sourceTurnIds, String idempotencyKey) {
        public SaveConversationBriefCommand {
            sourceTurnIds = sourceTurnIds == null ? List.of() : List.copyOf(sourceTurnIds);
        }
    }

    record ConversationBriefSave(ConversationBrief result, int currentRevision, boolean changed) { }

    record ConversationRuntimeContext(UUID conversationId, UUID turnId, String mode, String customerId,
                                      int briefRevision, String contextSnapshotJson,
                                      List<UUID> includedTaskIds) {
        public ConversationRuntimeContext {
            includedTaskIds = includedTaskIds == null ? List.of() : List.copyOf(includedTaskIds);
        }
    }

    record ConversationBinding(UUID conversationId, String mode, String customerId, int briefRevision,
                               int currentBriefRevision, int turnNo, String status) { }

    record ScenarioTaskTiming(Long queueMillis, Long executionMillis) { }
}
// 本文件负责实现 EAF 的 TaskService.java 相关代码。
