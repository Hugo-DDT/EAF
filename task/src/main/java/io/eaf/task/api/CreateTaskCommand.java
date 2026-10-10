package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import io.eaf.model.api.ModelProfileRef;
import java.util.UUID;

/** Task 命令携带可信入口协议标记，普通请求 DTO 不得自行指定该字段。 */
public record CreateTaskCommand(ActorContext actor, UUID workspaceId, UUID agentId,
                                String agentVersion, String input, String businessEntityType,
                                String businessEntityId, String idempotencyKey, String traceId, String source,
                                TaskAssetBinding assetBinding, String entryProtocol, ModelProfileRef modelProfileRef) {
    public CreateTaskCommand(ActorContext actor, UUID workspaceId, UUID agentId, String agentVersion, String input,
                             String businessEntityType, String businessEntityId, String idempotencyKey, String traceId,
                             String source, TaskAssetBinding assetBinding, String entryProtocol) {
        this(actor, workspaceId, agentId, agentVersion, input, businessEntityType, businessEntityId,
                idempotencyKey, traceId, source, assetBinding, entryProtocol, null);
    }
    public CreateTaskCommand(ActorContext actor, UUID workspaceId, UUID agentId, String agentVersion, String input,
                             String businessEntityType, String businessEntityId, String idempotencyKey, String traceId) {
        this(actor, workspaceId, agentId, agentVersion, input, businessEntityType, businessEntityId,
                idempotencyKey, traceId, "USER", null, "REST", null);
    }
    public CreateTaskCommand(ActorContext actor, UUID workspaceId, UUID agentId, String agentVersion, String input,
                             String businessEntityType, String businessEntityId, String idempotencyKey, String traceId, String source) {
        this(actor, workspaceId, agentId, agentVersion, input, businessEntityType, businessEntityId,
                idempotencyKey, traceId, source, null, "REST", null);
    }
    // 旧领域调用保持 REST 默认值；协议应用入口显式绑定服务端识别的入口类型。
    public CreateTaskCommand(ActorContext actor, UUID workspaceId, UUID agentId, String agentVersion, String input,
                             String businessEntityType, String businessEntityId, String idempotencyKey, String traceId,
                             String source, TaskAssetBinding assetBinding) {
        this(actor, workspaceId, agentId, agentVersion, input, businessEntityType, businessEntityId,
                idempotencyKey, traceId, source, assetBinding, "REST", null);
    }
}
// 本文件负责实现 EAF 的 CreateTaskCommand.java 相关代码。
