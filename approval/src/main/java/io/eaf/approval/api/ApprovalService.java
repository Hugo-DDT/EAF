package io.eaf.approval.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public interface ApprovalService {
    ApprovalSnapshot request(ApprovalCreateCommand command);
    ApprovalSnapshot get(ActorContext actor, UUID workspaceId, UUID approvalId);
    ApprovalPendingPage listPending(ActorContext actor, UUID workspaceId, Instant cursorCreatedAt,
                                    UUID cursorId, int pageSize);
    ApprovalActionablePage listActionablePending(ActorContext actor, UUID workspaceId,
            Instant cursorCreatedAt, UUID cursorId, int pageSize);
    // Outbox 详情只读本域行并执行 approval:read 校验，不返回审批绑定正文或 payload。
    ApprovalOutboxPage listOutboxOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                            Instant createdAfter, Instant cursorCreatedAt,
                                            UUID cursorEventId, int pageSize);
    ApprovalSnapshot decide(ApprovalDecisionCommand command);
    // Execution 在 Task 已取消后关闭仍待处理的绑定审批；已作出的决定保持不可覆盖。
    boolean cancelPending(UUID tenantId, UUID workspaceId, UUID taskId, UUID executionId);
}
