package io.eaf.context.api;

import java.util.UUID;

/** Runtime 注入的任务归属；该结构不会由 Context REST 请求体反序列化。 */
public record ContextTaskScope(UUID tenantId, UUID workspaceId, UUID actorId, UUID taskId,
                               // 质量运行 UUID 仅替换费用汇总 Scope，Task/Runtime 追溯仍沿原根任务 ID。
                               UUID rootTaskId, UUID runId, String source, UUID qualityRunId) {
    public ContextTaskScope(UUID tenantId, UUID workspaceId, UUID actorId, UUID taskId,
                            UUID rootTaskId, UUID runId, String source) {
        this(tenantId, workspaceId, actorId, taskId, rootTaskId, runId, source, null);
    }
}
// Context 只转递由 TaskWorkItem 派生的 ID，不允许 HTTP 调用者指定预算归属。
