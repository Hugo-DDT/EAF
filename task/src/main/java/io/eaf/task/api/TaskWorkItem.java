package io.eaf.task.api;

import io.eaf.model.api.ModelProfileSelection;
import java.time.Instant;
import java.util.UUID;

// 异步载荷固定原始委托摘要；执行时 Identity 仍须重查当前状态与授权。
public record TaskWorkItem(UUID id, UUID tenantId, UUID workspaceId, UUID actorId,
                           UUID agentId, String agentVersion, UUID promptId, String promptVersion,
                           UUID modelProfileId, String inputText, String businessEntityType,
                           String businessEntityId, String traceId, int attempt, long rowVersion,
                           Instant activeDeadline, Instant deadline, String source, TaskAssetBinding assetBinding,
                           UUID principalId, UUID delegationId, String authorizationHash,
                           UUID rootTaskId, UUID parentTaskId, String entryProtocol, String runKind,
                           String toolName, String toolVersion, String toolBindingRef, String toolArgumentsJson,
                           UUID leaseOwnerId, long leaseFence, UUID qualityRunId,
                           ModelProfileSelection modelSelection) { }
// 本文件负责实现 EAF 的 TaskWorkItem.java 相关代码。
