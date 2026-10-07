package io.eaf.usage.api;

import io.eaf.shared.ActorContext;
import java.time.Instant;
import java.util.UUID;
import java.util.Set;

public interface UsageRecorder {
    void record(UsageRecord record);

    void markFailed(UUID taskId, UUID runId, int callNo, String errorCode);

    // 账单事实按稳定调用键幂等补记；冲突金额或币种必须拒绝覆盖。
    void recordBilledCost(String callKey, java.math.BigDecimal amount, String currency, String billingSource);

    // 出站前按精确费率上界原子占额；未知价格、重复在途键或超限均拒绝。
    SpendReservation reserveSpend(ReserveSpendCommand command);

    // 明确未发出请求时释放预留；响应未知时禁止调用此方法。
    void releaseSpend(String callKey);

    // 人工停止、授权撤销或安全拒绝会冻结该 Scope 后续出站并保存原因。
    void stopSpendScope(UUID tenantId, UUID workspaceId, String scopeType, UUID scopeId, String reason);

    // Evaluation 通过 Usage 所有者读取精确 Task 的逐调用证据，不直接查询 Usage 表。
    java.util.List<UsageRecord> findForTask(UUID tenantId, UUID workspaceId, UUID taskId);

    // 无 Task 的索引作业和工作流调用仍由 Usage 域按原作用域提供记录。
    java.util.List<UsageRecord> findForScope(UUID tenantId, UUID workspaceId, String scopeType, UUID scopeId);

    // 运维调用页由 Usage Owner 强制检查 usage:read，并仅返回有限计量字段。
    UsageOperationsPage listOperations(ActorContext actor, UUID workspaceId, Set<String> statuses,
                                       Set<String> costStatuses, Instant startedAfter,
                                       UsageOperationsCursor cursor, int pageSize);
}
// 本文件负责实现 EAF 的 UsageRecorder.java 相关代码。
