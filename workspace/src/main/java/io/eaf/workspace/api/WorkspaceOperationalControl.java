package io.eaf.workspace.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.UUID;

/** Workspace 持有暂停新任务入口的状态，并为管理命令提供稳定回执。 */
public interface WorkspaceOperationalControl {
    WorkspaceGateReceipt stopTaskAdmission(ActorContext actor, UUID workspaceId,
                                           String requestKey, boolean enabled, String reason);

    WorkspaceGateReceipt stopBusinessOutbound(ActorContext actor, UUID workspaceId,
                                              String requestKey, boolean enabled, String reason);

    /** 在调用方事务中对 Workspace 行加共享锁，使停止命令与新任务提交按数据库顺序生效。 */
    boolean taskAdmissionOpen(UUID tenantId, UUID workspaceId);

    /** 业务 Connector 新调用受此闸门约束，已存在 Execution 的核验查询单独放行。 */
    boolean businessOutboundOpen(UUID tenantId, UUID workspaceId);
}
