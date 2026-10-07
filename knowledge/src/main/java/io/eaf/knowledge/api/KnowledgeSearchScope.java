package io.eaf.knowledge.api;

import java.util.UUID;

/** Knowledge 检索的服务端用途与任务归属；不包含可由调用者控制的授权布尔值。 */
public record KnowledgeSearchScope(UUID tenantId, UUID workspaceId, UUID actorId, UUID taskId, UUID runId,
                                   String source, String scopeType, UUID scopeId, int callNo) { }
// 对外协议只接收 query；此归属由 Context Runtime 路径在服务端构造。
