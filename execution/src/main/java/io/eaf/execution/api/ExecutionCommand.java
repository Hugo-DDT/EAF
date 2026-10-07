package io.eaf.execution.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

public record ExecutionCommand(ActorContext actor, UUID workspaceId, UUID taskId, int attempt,
                               UUID agentId, String agentVersion, String toolName, String toolVersion,
                               String argumentsJson, String idempotencyKey, String traceId) { }
// 本文件负责实现 EAF 的 ExecutionCommand.java 相关代码。
