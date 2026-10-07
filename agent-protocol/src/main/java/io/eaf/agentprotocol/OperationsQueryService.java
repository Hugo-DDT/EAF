package io.eaf.agentprotocol;

import io.eaf.approval.api.ApprovalService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.execution.api.ExecutionOperationsCursor;
import io.eaf.execution.api.ExecutionOperationsItem;
import io.eaf.execution.api.ExecutionService;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.learning.api.FeedbackService;
import io.eaf.memory.api.MemoryService;
import io.eaf.task.api.TaskPageCursor;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import io.eaf.workflow.api.WorkflowOperationsCursor;
import io.eaf.workflow.api.WorkflowOperationsItem;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.usage.api.UsageOperationsCursor;
import io.eaf.usage.api.UsageRecorder;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Service;

/** 汇总授权入口与所属域查询；不直接读取 Task 或其它业务域的数据表。 */
@Service
public class OperationsQueryService {
    private final TaskService tasks;
    private final WorkflowService workflows;
    private final ExecutionService executions;
    private final ApprovalService approvals;
    private final KnowledgeService knowledge;
    private final MemoryService memories;
    private final FeedbackService feedbacks;
    private final UsageRecorder usage;
    private final WorkspaceAuthorization workspaces;

    public OperationsQueryService(TaskService tasks, WorkflowService workflows, ExecutionService executions,
                                  ApprovalService approvals, KnowledgeService knowledge, MemoryService memories,
                                  FeedbackService feedbacks, UsageRecorder usage,
                                  WorkspaceAuthorization workspaces) {
        this.tasks = tasks;
        this.workflows = workflows;
        this.executions = executions;
        this.approvals = approvals;
        this.knowledge = knowledge;
        this.memories = memories;
        this.feedbacks = feedbacks;
        this.usage = usage;
        this.workspaces = workspaces;
    }

    public OperationsController.TaskOperationsPage tasks(ActorContext actor, UUID workspaceId,
                                                         Set<TaskStatus> statuses, Instant updatedAfter,
                                                         Instant cursorUpdatedAt, UUID cursorTaskId,
                                                         int pageSize) {
        requireOperator(actor, workspaceId);
        if ((cursorUpdatedAt == null) != (cursorTaskId == null))
            throw EafException.invalid("Task 运维分页游标必须同时提供时间和 ID。");

        // 运维权限不替代 Task 域的 task:read；缺少该域授权时返回隐藏页而不泄漏数量。
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "task:read"))
            return new OperationsController.TaskOperationsPage(false, List.of(), null, null);

        var page = tasks.list(actor, workspaceId, null, statuses, updatedAfter,
                cursorUpdatedAt == null ? null : new TaskPageCursor(cursorUpdatedAt, cursorTaskId), pageSize);
        var items = page.items().stream().map(task -> new OperationsController.TaskOperationsItem(
                task.id(), task.rootTaskId(), task.parentTaskId(), task.status().name(), task.attempt(),
                task.errorCode(), task.externalEffectStatus(), task.externalEffectPending(), task.entryProtocol(),
                task.runKind(), task.createdAt(), task.updatedAt())).toList();
        return new OperationsController.TaskOperationsPage(true, items, page.totalSize(), page.nextCursor());
    }

    public OperationsController.WorkflowOperationsResponse workflows(ActorContext actor, UUID workspaceId,
                                                                      Set<String> statuses, Instant createdAfter,
                                                                      Instant cursorCreatedAt, UUID cursorInstanceId,
                                                                      int pageSize) {
        requireOperator(actor, workspaceId);
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "workflow:read"))
            return new OperationsController.WorkflowOperationsResponse(false, List.of(), null, null);
        if ((cursorCreatedAt == null) != (cursorInstanceId == null))
            throw EafException.invalid("Workflow 运维分页游标必须同时提供时间和 ID。");
        var page = workflows.listOperations(actor, workspaceId, statuses, createdAfter,
                cursorCreatedAt == null ? null : new WorkflowOperationsCursor(cursorCreatedAt, cursorInstanceId), pageSize);
        return new OperationsController.WorkflowOperationsResponse(true, page.items(), page.totalSize(), page.nextCursor());
    }

    public OperationsController.ExecutionOperationsResponse executions(ActorContext actor, UUID workspaceId,
                                                                         Set<String> statuses, Instant createdAfter,
                                                                         Instant cursorCreatedAt, UUID cursorExecutionId,
                                                                         int pageSize) {
        requireOperator(actor, workspaceId);
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "execution:read"))
            return new OperationsController.ExecutionOperationsResponse(false, List.of(), null, null);
        if ((cursorCreatedAt == null) != (cursorExecutionId == null))
            throw EafException.invalid("Execution 运维分页游标必须同时提供时间和 ID。");
        var page = executions.listOperations(actor, workspaceId, statuses, createdAfter,
                cursorCreatedAt == null ? null : new ExecutionOperationsCursor(cursorCreatedAt, cursorExecutionId), pageSize);
        return new OperationsController.ExecutionOperationsResponse(true, page.items(), page.totalSize(), page.nextCursor());
    }

    public OperationsController.UsageOperationsResponse usage(ActorContext actor, UUID workspaceId,
                                                              Set<String> statuses, Set<String> costStatuses,
                                                              Instant startedAfter, Instant cursorStartedAt,
                                                              UUID cursorUsageId, int pageSize) {
        requireOperator(actor, workspaceId);
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "usage:read"))
            return new OperationsController.UsageOperationsResponse(false, List.of(), null, null);
        if ((cursorStartedAt == null) != (cursorUsageId == null))
            throw EafException.invalid("Usage 运维分页游标必须同时提供时间和 ID。");
        var page = usage.listOperations(actor, workspaceId, statuses, costStatuses, startedAfter,
                cursorStartedAt == null ? null : new UsageOperationsCursor(cursorStartedAt, cursorUsageId), pageSize);
        return new OperationsController.UsageOperationsResponse(true, page.items(), page.totalSize(), page.nextCursor());
    }

    public OperationsController.OutboxOperationsPage outboxes(ActorContext actor, UUID workspaceId, String owner,
                                                               Set<String> statuses, Instant createdAfter,
                                                               Instant cursorCreatedAt, UUID cursorEventId,
                                                               int pageSize) {
        requireOperator(actor, workspaceId);
        if ((cursorCreatedAt == null) != (cursorEventId == null))
            throw EafException.invalid("Outbox 运维分页游标必须同时提供时间和 ID。");
        return switch (owner) {
            case "approval" -> approvalOutbox(actor, workspaceId, statuses, createdAfter, cursorCreatedAt, cursorEventId, pageSize);
            case "execution" -> executionOutbox(actor, workspaceId, statuses, createdAfter, cursorCreatedAt, cursorEventId, pageSize);
            case "knowledge" -> knowledgeOutbox(actor, workspaceId, statuses, createdAfter, cursorCreatedAt, cursorEventId, pageSize);
            case "memory" -> memoryOutbox(actor, workspaceId, statuses, createdAfter, cursorCreatedAt, cursorEventId, pageSize);
            case "learning" -> learningOutbox(actor, workspaceId, statuses, createdAfter, cursorCreatedAt, cursorEventId, pageSize);
            default -> throw EafException.invalid("Outbox Owner 必须是 approval、execution、knowledge、memory 或 learning。");
        };
    }

    private OperationsController.OutboxOperationsPage approvalOutbox(ActorContext actor, UUID workspaceId,
                                                                      Set<String> statuses, Instant after,
                                                                      Instant cursorAt, UUID cursorId, int size) {
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "approval:read"))
            return hiddenOutbox("approval");
        var page = approvals.listOutboxOperations(actor, workspaceId, statuses, after, cursorAt, cursorId, size);
        var items = page.items().stream().map(item -> new OperationsController.OutboxOperationsItem(
                item.eventId(), "Approval", item.approvalId(), null, null, item.eventType(), item.status(),
                item.attempts(), null, item.nextAttemptAt(), item.createdAt())).toList();
        return outboxPage("approval", "OWNER_MANAGED", false, items, page.totalSize(), page.nextCreatedAt(), page.nextEventId());
    }

    private OperationsController.OutboxOperationsPage executionOutbox(ActorContext actor, UUID workspaceId,
                                                                       Set<String> statuses, Instant after,
                                                                       Instant cursorAt, UUID cursorId, int size) {
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "execution:read"))
            return hiddenOutbox("execution");
        var page = executions.listOutboxOperations(actor, workspaceId, statuses, after, cursorAt, cursorId, size);
        var items = page.items().stream().map(item -> new OperationsController.OutboxOperationsItem(
                item.eventId(), "Execution", item.executionId(), null, null, item.eventType(), item.status(),
                item.attempts(), null, item.nextAttemptAt(), item.createdAt())).toList();
        return outboxPage("execution", "OWNER_MANAGED", false, items, page.totalSize(), page.nextCreatedAt(), page.nextEventId());
    }

    private OperationsController.OutboxOperationsPage knowledgeOutbox(ActorContext actor, UUID workspaceId,
                                                                       Set<String> statuses, Instant after,
                                                                       Instant cursorAt, UUID cursorId, int size) {
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "knowledge:read"))
            return hiddenOutbox("knowledge");
        var page = knowledge.listOutboxOperations(actor, workspaceId, statuses, after, cursorAt, cursorId, size);
        var items = page.items().stream().map(item -> new OperationsController.OutboxOperationsItem(
                item.eventId(), "KnowledgeDocument", item.documentId(), Integer.toString(item.assetVersion()), item.action(),
                item.eventType(), item.status(), item.attempts(), item.errorCode(), item.nextAttemptAt(), item.createdAt())).toList();
        return outboxPage("knowledge", "OWNER_MANAGED", false, items, page.totalSize(), page.nextCreatedAt(), page.nextEventId());
    }

    private OperationsController.OutboxOperationsPage memoryOutbox(ActorContext actor, UUID workspaceId,
                                                                    Set<String> statuses, Instant after,
                                                                    Instant cursorAt, UUID cursorId, int size) {
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "memory:read"))
            return hiddenOutbox("memory");
        var page = memories.listOutboxOperations(actor, workspaceId, statuses, after, cursorAt, cursorId, size);
        var items = page.items().stream().map(item -> new OperationsController.OutboxOperationsItem(
                item.eventId(), "Memory", item.memoryId(), item.memoryVersion(), item.action(), item.eventType(),
                item.status(), item.attempts(), item.errorCode(), item.nextAttemptAt(), item.createdAt())).toList();
        return outboxPage("memory", "OWNER_MANAGED", false, items, page.totalSize(), page.nextCreatedAt(), page.nextEventId());
    }

    private OperationsController.OutboxOperationsPage learningOutbox(ActorContext actor, UUID workspaceId,
                                                                      Set<String> statuses, Instant after,
                                                                      Instant cursorAt, UUID cursorId, int size) {
        if (!workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "learning:read")
                || !workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "task:read"))
            return hiddenOutbox("learning");
        var page = feedbacks.listOutboxOperations(actor, workspaceId, statuses, after, cursorAt, cursorId, size);
        var items = page.items().stream().map(item -> new OperationsController.OutboxOperationsItem(
                item.eventId(), "Task", item.taskId(), null, null, item.eventType(), item.status(),
                0, null, null, item.createdAt())).toList();
        return outboxPage("learning", page.consumerState(), page.pendingIsFailure(), items, page.totalSize(),
                page.nextCreatedAt(), page.nextEventId());
    }

    private OperationsController.OutboxOperationsPage hiddenOutbox(String owner) {
        return new OperationsController.OutboxOperationsPage(false, owner, null, false, List.of(), null, null);
    }

    private OperationsController.OutboxOperationsPage outboxPage(String owner, String deliveryState,
                                                                  boolean pendingIsFailure,
                                                                  List<OperationsController.OutboxOperationsItem> items,
                                                                  long totalSize, Instant nextAt, UUID nextId) {
        var next = nextAt == null ? null : new OperationsController.OutboxCursor(nextAt, nextId);
        return new OperationsController.OutboxOperationsPage(true, owner, deliveryState,
                pendingIsFailure, items, totalSize, next);
    }

    private void requireOperator(ActorContext actor, UUID workspaceId) {
        // 运维查询仅允许本人操作的 HUMAN Workspace 管理员，Agent 委托不得扩大值班数据范围。
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("运维查询仅允许本人操作的 HUMAN 管理员。");
        workspaces.require(actor, workspaceId, "workspace:members:manage");
    }
}
