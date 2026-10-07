package io.eaf.workflow.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

// Workflow API 负责版本治理、启动快照与继续运行时的当前性检查。
public interface WorkflowService {
    WorkflowDefinition create(CreateWorkflowCommand command);
    WorkflowDefinition addVersion(ActorContext actor, UUID workspaceId, UUID workflowId, CreateWorkflowVersionCommand command);
    List<WorkflowDefinition> list(ActorContext actor, UUID workspaceId);
    WorkflowDefinition get(ActorContext actor, UUID workspaceId, UUID workflowId, String version);
    WorkflowDefinition publish(ActorContext actor, UUID workspaceId, UUID workflowId, String version, long expectedVersion);
    WorkflowDefinition withdraw(ActorContext actor, UUID workspaceId, UUID workflowId, String version, long expectedVersion);
    WorkflowInstance createInstance(CreateWorkflowInstanceCommand command);
    ServiceRequestBatchSnapshot.Created createServiceRequestBatch(CreateServiceRequestBatchCommand command);
    ServiceRequestBatchSnapshot getServiceRequestBatch(ActorContext actor, UUID workspaceId, UUID batchId);
    ServiceRequestBatchSnapshot.Detail getServiceRequestBatchItem(ActorContext actor, UUID workspaceId,
            UUID batchId, String itemKey);
    ServiceRequestBatchSnapshot cancelServiceRequestBatch(ActorContext actor, UUID workspaceId,
            UUID batchId, long expectedVersion);
    ServiceRequestBatchSnapshot.Item cancelServiceRequestBatchItem(ActorContext actor, UUID workspaceId,
            UUID batchId, String itemKey, long expectedVersion);
    void requireP21KnowledgeBranch(ActorContext actor, UUID workspaceId, UUID taskId);
    /** 仅供已在 Task 域锁定同步尝试的结果登记使用；通用 Workflow 请求不能选择该保留定义。 */
    WorkflowInstance createCustomerFollowupResultWorkflow(ActorContext actor, UUID workspaceId,
            UUID followupId, UUID resultId, String idempotencyKey);
    WorkflowInstance createServiceRequestRegistrationWorkflow(ActorContext actor, UUID workspaceId,
            UUID submissionId, String idempotencyKey);
    ServiceRequestHandlingSnapshot createServiceRequestHandlingWorkflow(ActorContext actor, UUID workspaceId,
            UUID submissionId, UUID assigneeId, String sharedBrief, String idempotencyKey, Instant deadlineAt);
    ServiceRequestHandlingSnapshot createServiceRequestHandlingWorkflow(ActorContext actor, UUID workspaceId,
            UUID submissionId, UUID assigneeId, String sharedBrief, String idempotencyKey, Instant deadlineAt,
            String scenarioKey, List<TeamExperienceRef> teamExperienceRefs);
    TeamExperienceSource requireTeamExperienceSource(ActorContext actor, UUID workspaceId, UUID workItemId);
    TeamExperienceTaskSelection requireTeamExperienceSelectionForTask(ActorContext actor, UUID workspaceId, UUID taskId);
    ServiceRequestHandlingSnapshot getServiceRequestHandling(ActorContext actor, UUID workspaceId, UUID instanceId);
    HumanWorkItemPage listHumanWorkItems(ActorContext actor, UUID workspaceId, String relation, String status,
            Instant cursorCreatedAt, UUID cursorId, int pageSize);
    HumanWorkItem getHumanWorkItem(ActorContext actor, UUID workspaceId, UUID workItemId);
    HumanWorkItem reassignHumanWorkItem(ActorContext actor, UUID workspaceId, UUID workItemId,
            long expectedVersion, UUID assigneeId);
    HumanWorkItem completeHumanWorkItem(ActorContext actor, UUID workspaceId, UUID workItemId,
            long expectedVersion, String idempotencyKey, String outcome, String summary, String nextAction);
    // 调用方在重新检查源资料新鲜度前，先恢复相同身份/内容的原实例。
    Optional<WorkflowInstance> findInstanceByIdempotencyKey(CreateWorkflowInstanceCommand command);
    // 按当前 HUMAN 身份与键读取原快照；调用方需核对本业务请求字段后才能重放。
    Optional<WorkflowInstance> lookupInstanceByIdempotencyKey(ActorContext actor, UUID workspaceId,
                                                               UUID workflowId, String workflowVersion,
                                                               String idempotencyKey);
    // 质量用途只通过 Evaluation 内部入口绑定；业务启动 DTO 无法伪造该关联。
    WorkflowInstance createQualityRunInstance(CreateQualityRunWorkflowCommand command);
    WorkflowInstance getInstance(ActorContext actor, UUID workspaceId, UUID instanceId);
    WorkflowSourcePage listForSourceTask(ActorContext actor, UUID workspaceId, UUID sourceTaskId,
                                         Instant cursorCreatedAt, UUID cursorInstanceId, int pageSize);
    // 运维摘要由 Workflow 自己过滤租户、Workspace、状态和稳定游标，不返回流程输入或结果。
    WorkflowOperationsPage listOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                          Instant createdAfter, WorkflowOperationsCursor cursor, int pageSize);
    WorkflowInstance cancelInstance(ActorContext actor, UUID workspaceId, UUID instanceId, long expectedVersion);
    WorkflowInstance requireRunnable(ActorContext actor, UUID workspaceId, UUID instanceId);

    record TeamExperienceSource(UUID workItemId, UUID instanceId, long workItemVersion, UUID completedBy,
            Instant completedAt, String outcome, String resultHash, String sourceType) { }
    record TeamExperienceTaskSelection(String scenarioKey, List<TeamExperienceRef> refs, UUID assigneeId) {
        public TeamExperienceTaskSelection { refs = refs == null ? List.of() : List.copyOf(refs); }
    }
    record ParallelBranch(String role, String stepId, String dispatchKey, String inputJson, String inputHash,
            UUID capabilityId, String capabilityVersion, UUID childTaskId, String status, String errorCode) { }
}
