package io.eaf.agentprotocol;

import io.eaf.shared.ActorContext;
import io.eaf.execution.api.ExecutionOperationsCursor;
import io.eaf.execution.api.ExecutionOperationsItem;
import io.eaf.task.api.TaskStatus;
import io.eaf.workspace.api.WorkspaceGateReceipt;
import io.eaf.workspace.api.WorkspaceOperationalControl;
import io.eaf.workflow.api.WorkflowOperationsCursor;
import io.eaf.workflow.api.WorkflowOperationsItem;
import io.eaf.usage.api.UsageOperationsCursor;
import io.eaf.usage.api.UsageOperationsItem;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 为获准的 Workspace 管理员提供不含业务正文的运行队列查询。 */
@RestController
@RequestMapping("/api/v1/workspaces/{workspaceId}/operations")
public class OperationsController {
    private final OperationsQueryService operations;
    private final WorkspaceOperationalControl operationalControl;

    public OperationsController(OperationsQueryService operations, WorkspaceOperationalControl operationalControl) {
        this.operations = operations;
        this.operationalControl = operationalControl;
    }

    @PutMapping("/gates/task-admission")
    WorkspaceGateReceipt taskAdmission(@PathVariable UUID workspaceId,
                                       @RequestHeader(value = "Idempotency-Key", required = false) String requestKey,
                                       @RequestBody OperationalGateBody body,
                                       Authentication authentication) {
        return operationalControl.stopTaskAdmission(ApiSupport.actor(authentication), workspaceId,
                requestKey, body.enabled(), body.reason());
    }

    @PutMapping("/gates/business-outbound")
    WorkspaceGateReceipt businessOutbound(@PathVariable UUID workspaceId,
                                          @RequestHeader(value = "Idempotency-Key", required = false) String requestKey,
                                          @RequestBody OperationalGateBody body,
                                          Authentication authentication) {
        return operationalControl.stopBusinessOutbound(ApiSupport.actor(authentication), workspaceId,
                requestKey, body.enabled(), body.reason());
    }

    /** 停止或重新开放 Workspace 运维闸门；原因只用于受控审计留存，不进入响应。 */
    public record OperationalGateBody(boolean enabled, String reason) { }

    @GetMapping("/tasks")
    TaskOperationsPage tasks(@PathVariable UUID workspaceId,
                             @RequestParam(required = false) Set<TaskStatus> status,
                             @RequestParam(required = false) Instant updatedAfter,
                             @RequestParam(required = false) Instant cursorUpdatedAt,
                             @RequestParam(required = false) UUID cursorTaskId,
                             @RequestParam(defaultValue = "50") int limit,
                             Authentication authentication) {
        return operations.tasks(ApiSupport.actor(authentication), workspaceId, status, updatedAfter,
                cursorUpdatedAt, cursorTaskId, limit);
    }

    @GetMapping("/workflows")
    WorkflowOperationsResponse workflows(@PathVariable UUID workspaceId,
                                           @RequestParam(required = false) Set<String> status,
                                           @RequestParam(required = false) Instant createdAfter,
                                           @RequestParam(required = false) Instant cursorCreatedAt,
                                           @RequestParam(required = false) UUID cursorInstanceId,
                                           @RequestParam(defaultValue = "50") int limit,
                                           Authentication authentication) {
        return operations.workflows(ApiSupport.actor(authentication), workspaceId, status, createdAfter,
                cursorCreatedAt, cursorInstanceId, limit);
    }

    @GetMapping("/executions")
    ExecutionOperationsResponse executions(@PathVariable UUID workspaceId,
                                             @RequestParam(required = false) Set<String> status,
                                             @RequestParam(required = false) Instant createdAfter,
                                             @RequestParam(required = false) Instant cursorCreatedAt,
                                             @RequestParam(required = false) UUID cursorExecutionId,
                                             @RequestParam(defaultValue = "50") int limit,
                                             Authentication authentication) {
        return operations.executions(ApiSupport.actor(authentication), workspaceId, status, createdAfter,
                cursorCreatedAt, cursorExecutionId, limit);
    }

    @GetMapping("/usage")
    UsageOperationsResponse usage(@PathVariable UUID workspaceId,
                                  @RequestParam(required = false) Set<String> status,
                                  @RequestParam(required = false) Set<String> costStatus,
                                  @RequestParam(required = false) Instant startedAfter,
                                  @RequestParam(required = false) Instant cursorStartedAt,
                                  @RequestParam(required = false) UUID cursorUsageId,
                                  @RequestParam(defaultValue = "50") int limit,
                                  Authentication authentication) {
        return operations.usage(ApiSupport.actor(authentication), workspaceId, status, costStatus, startedAfter,
                cursorStartedAt, cursorUsageId, limit);
    }

    @GetMapping("/outboxes/{owner}")
    OutboxOperationsPage outboxes(@PathVariable UUID workspaceId, @PathVariable String owner,
                                  @RequestParam(required = false) Set<String> status,
                                  @RequestParam(required = false) Instant createdAfter,
                                  @RequestParam(required = false) Instant cursorCreatedAt,
                                  @RequestParam(required = false) UUID cursorEventId,
                                  @RequestParam(defaultValue = "50") int limit,
                                  Authentication authentication) {
        return operations.outboxes(ApiSupport.actor(authentication), workspaceId, owner, status, createdAfter,
                cursorCreatedAt, cursorEventId, limit);
    }

    /** 运维摘要只返回稳定关联和处置所需状态，不返回 Task 输入、模型结果或错误详情。 */
    public record TaskOperationsPage(boolean visible, java.util.List<TaskOperationsItem> items,
                                     Long totalSize, io.eaf.task.api.TaskPageCursor nextCursor) { }

    public record TaskOperationsItem(UUID taskId, UUID rootTaskId, UUID parentTaskId, String status,
                                     int attempt, String errorCode, String externalEffectStatus,
                                     boolean externalEffectPending, String entryProtocol, String runKind,
                                     Instant createdAt, Instant updatedAt) { }

    public record WorkflowOperationsResponse(boolean visible, java.util.List<WorkflowOperationsItem> items,
                                             Long totalSize, WorkflowOperationsCursor nextCursor) { }

    public record ExecutionOperationsResponse(boolean visible, java.util.List<ExecutionOperationsItem> items,
                                              Long totalSize, ExecutionOperationsCursor nextCursor) { }

    public record UsageOperationsResponse(boolean visible, java.util.List<UsageOperationsItem> items,
                                          Long totalSize, UsageOperationsCursor nextCursor) { }

    public record OutboxOperationsPage(boolean visible, String owner, String deliveryState,
                                       boolean pendingIsFailure, java.util.List<OutboxOperationsItem> items,
                                       Long totalSize, OutboxCursor nextCursor) { }

    public record OutboxOperationsItem(UUID eventId, String resourceType, UUID resourceId,
                                       String resourceVersion, String action, String eventType,
                                       String status, int attempts, String errorCode,
                                       Instant nextAttemptAt, Instant createdAt) { }

    public record OutboxCursor(Instant createdAt, UUID eventId) { }
}
