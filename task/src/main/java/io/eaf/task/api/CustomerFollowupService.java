package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/** Task 域内的团队客户跟进业务记录；不把它建模为可运行的模型 Task。 */
public interface CustomerFollowupService {
    CardSnapshot create(CreateCardCommand command);
    CardSnapshot attachCreationWorkflow(ActorContext actor, UUID workspaceId, UUID followupId, UUID workflowId);
    FollowupPage list(ActorContext actor, UUID workspaceId, ListQuery query);
    FollowupDetail get(ActorContext actor, UUID workspaceId, UUID followupId);
    ResultPage results(ActorContext actor, UUID workspaceId, UUID followupId, Integer beforeResultNo, int limit);
    CardSnapshot update(UpdateCardCommand command);
    ResultReceipt appendResult(ResultCommand command);
    CardSnapshot recordCreationReceipt(ActorContext actor, UUID workspaceId, UUID followupId,
                                       UUID workflowId, UUID operationId, String externalId);
    /** 从 Execution 所持有的固定工具 Task provenance 关联已核验的原创建回执； 无业务卡时为空操作。 */
    void recordCreationReceiptForTask(ActorContext actor, UUID workspaceId, UUID taskId,
                                      UUID operationId, String externalId);
    SyncAttempt startSync(SyncCommand command);
    SyncAttempt attachSyncWorkflow(ActorContext actor, UUID workspaceId, UUID syncAttemptId, UUID workflowId);
    List<SyncAttempt> syncAttempts(ActorContext actor, UUID workspaceId, UUID followupId);
    void closeSyncAdmission(ActorContext actor, UUID workspaceId, UUID syncAttemptId, String terminalState);
    void requireSyncTaskCreation(ActorContext actor, UUID workspaceId, WorkflowTaskProvenance provenance,
                                 String toolName, String toolArgumentsJson);
    SyncWritePayload requireSyncWritePayload(ActorContext actor, UUID workspaceId, UUID taskId, int attempt,
                                             UUID followupId, UUID resultId);
    List<Assignee> eligibleAssignees(ActorContext actor, UUID workspaceId, String customerId, int limit);
    List<AnalysisResult> selectForAnalysis(ActorContext actor, UUID workspaceId, String customerId,
                                           List<UUID> resultIds);

    record CreateCardCommand(ActorContext actor, UUID workspaceId, String customerId, UUID sourceConversationId,
                             UUID sourceTaskId, long sourceTaskVersion, int sourceBriefRevision,
                             UUID assigneeId, String summary, Instant dueAt, String requestKey) { }
    record ListQuery(String scope, String customerId, UUID assigneeId, String status, Boolean overdue,
                     Instant cursorUpdatedAt, UUID cursorId, int limit) { }
    record UpdateCardCommand(ActorContext actor, UUID workspaceId, UUID followupId, long expectedVersion,
                             UUID assigneeId, boolean assigneeProvided, Instant dueAt, boolean dueAtProvided) { }
    record ResultCommand(ActorContext actor, UUID workspaceId, UUID followupId, long expectedVersion,
                         String outcomeCode, String summary, String nextAction, Instant nextContactAt,
                         String disposition, UUID correctsResultId, String requestKey) { }
    record SyncCommand(ActorContext actor, UUID workspaceId, UUID followupId, UUID resultId, String requestKey) { }
    record CardSnapshot(UUID id, UUID tenantId, UUID workspaceId, String customerId, UUID creatorId,
                        UUID assigneeId, String summary, Instant dueAt, String businessStatus,
                        UUID sourceConversationId, UUID sourceTaskId, long sourceTaskVersion,
                        int sourceBriefRevision, UUID creationWorkflowId, int lastResultNo,
                        long rowVersion, Instant createdAt, Instant updatedAt) { }
    record FollowupPage(List<CardSnapshot> items, Instant nextUpdatedAt, UUID nextId) {
        public FollowupPage { items = items == null ? List.of() : List.copyOf(items); }
    }
    record FollowupDetail(CardSnapshot card, Set<String> allowedActions, List<ResultSnapshot> recentResults,
                          Integer nextBeforeResultNo) {
        public FollowupDetail {
            allowedActions = allowedActions == null ? Set.of() : Set.copyOf(allowedActions);
            recentResults = recentResults == null ? List.of() : List.copyOf(recentResults);
        }
    }
    record ResultSnapshot(UUID id, UUID followupId, int resultNo, UUID recordedBy, String outcomeCode,
                          String summary, String nextAction, Instant nextContactAt, String disposition,
                          UUID correctsResultId, Instant createdAt, String syncStatus) { }
    record ResultPage(List<ResultSnapshot> items, Integer nextBeforeResultNo) {
        public ResultPage { items = items == null ? List.of() : List.copyOf(items); }
    }
    record ResultReceipt(CardSnapshot card, ResultSnapshot result, boolean created) { }
    record Assignee(UUID id, String displayName) { }
    record AnalysisResult(ResultSnapshot result, String customerId, UUID creatorId, String syncStatus) { }
    record SyncAttempt(UUID id, UUID followupId, UUID resultId, int attemptNo, UUID submittedBy,
                       UUID workflowInstanceId, boolean admissionOpen, String terminalState, Instant createdAt) { }
    record SyncWritePayload(UUID followupId, UUID resultId, UUID syncAttemptId, String customerId,
                            String externalId, int resultNo, UUID recordedBy, String outcomeCode,
                            String summary, String nextAction, Instant nextContactAt, String disposition,
                            UUID submittedBy) { }
}
