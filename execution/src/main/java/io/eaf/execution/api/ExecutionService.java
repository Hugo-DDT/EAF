package io.eaf.execution.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface ExecutionService {
    ExecutionSnapshot submit(ExecutionCommand command);
    ExecutionSnapshot get(ActorContext actor, UUID workspaceId, UUID executionId);
    /** 供已授权的业务关联读取某个固定 Workflow Task 的当前 Execution 投影。 */
    Optional<ExecutionSnapshot> findForTask(ActorContext actor, UUID workspaceId, UUID taskId);
    /** 仅读取本人固定 register Task 的 Execution 回执，不要求额外授予 execution:read。 */
    Optional<ExecutionSnapshot> findServiceRequestRegistration(ActorContext actor, UUID workspaceId,
            UUID taskId, int attempt, UUID submissionId);
    // 运维摘要由 Execution 自己检查动作权限并按状态/创建时间分页，不读取 Task 或 Outbox 表。
    ExecutionOperationsPage listOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                           Instant createdAfter, ExecutionOperationsCursor cursor, int pageSize);
    // Execution 自己检查 execution:read 并返回有限 outbox 状态，不读取 Audit 或 Task 表。
    ExecutionOutboxPage listOutboxOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                             Instant createdAfter, Instant cursorCreatedAt,
                                             UUID cursorEventId, int pageSize);
    /** 只把 FAILED 的原事件重新置为 PENDING，保留 eventId、payload 和累计尝试次数。 */
    ExecutionOutboxReplayReceipt replayOutbox(ActorContext actor, UUID workspaceId, UUID eventId,
                                              String requestKey, String reason);
    ExecutionSnapshot resume(ActorContext actor, UUID workspaceId, UUID executionId);
    ExecutionSnapshot verify(ActorContext actor, UUID workspaceId, UUID executionId);
    /** 以可审计请求键触发既有 operationId 的只读核验，不生成新的外部写入意图。 */
    ExecutionVerificationReceipt verifyOperational(ActorContext actor, UUID workspaceId, UUID executionId,
                                                   String requestKey, String reason);
    // 重新校验身份与资源后仅轮询已有的 A2A Task，不会重新发送 SendMessage。
    ExecutionSnapshot pollRemote(ActorContext actor, UUID workspaceId, UUID executionId);
    // CancelTask 只向原 remoteTaskId 发送取消；只有 peer 明确返回 CANCELED 才结束本地 Task。
    ExecutionSnapshot cancelRemote(ActorContext actor, UUID workspaceId, UUID executionId);
    Optional<ExecutionSnapshot> pending(UUID tenantId, UUID workspaceId, UUID taskId, int attempt);
    // 持久调度器只把到期的 WAITING_REMOTE Task 放回队列；远端 GetTask 仍由 Runtime/Execution 执行。
    void wakeDueRemoteTasks();
    // 只取消对应 Task 上尚未提交的审批等待；提交中或结果未知的写入继续核验。
    void cancelAwaitingApproval(UUID tenantId, UUID workspaceId, UUID taskId);
    default void recoverOnStartup() { }
}
// 本文件负责实现 EAF 的 ExecutionService.java 相关代码。
