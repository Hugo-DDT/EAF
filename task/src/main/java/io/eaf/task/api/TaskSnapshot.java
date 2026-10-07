package io.eaf.task.api;

import java.time.Instant;
import java.util.UUID;

/**
 * Task 对外快照；外部效果状态用于区分已提交、待核验与确定失败，避免上层将未知写入误判为可重试。
 */
public record TaskSnapshot(UUID id, UUID tenantId, UUID workspaceId, UUID actorId, UUID agentId,
                           String agentVersion, String promptVersion, TaskStatus status, int attempt,
                           long version, String traceId, String inputText, String resultJson,
                           String errorCode, String errorDetail, String source,
                           Instant createdAt, Instant updatedAt, TaskAssetBinding assetBinding,
                           UUID rootTaskId, UUID parentTaskId, String entryProtocol, String runKind,
                           String externalEffectStatus, boolean externalEffectPending) { }
// 本文件负责实现 EAF 的 TaskSnapshot.java 相关代码。
