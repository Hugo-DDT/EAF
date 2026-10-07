package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

// 固定工具命令只携带已发布 Tool 标识和参数，不接受运行类、Connector 实现或来源字段。
public record CreateToolExecutionCommand(ActorContext actor, UUID workspaceId, UUID parentTaskId,
                                         String creationKey, String toolName, String toolVersion,
                                         String argumentsJson, String traceId) { }
