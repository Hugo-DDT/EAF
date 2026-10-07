package io.eaf.execution.api;

import java.time.Instant;
import java.util.UUID;

public record ExecutionSnapshot(UUID id, UUID tenantId, UUID workspaceId, UUID taskId, int attempt,
                                String toolName, String toolVersion, String status, String argumentsJson,
                                String resultJson, String policyVersion, String errorCode, String errorDetail,
                                Instant createdAt, Instant endedAt, UUID operationId, UUID connectorId,
                                String connectorVersion, String previewJson, String previewHash, UUID approvalId,
                                String verificationJson, long version) {
    public ExecutionSnapshot(UUID id, UUID tenantId, UUID workspaceId, UUID taskId, int attempt,
                             String toolName, String toolVersion, String status, String argumentsJson,
                             String resultJson, String policyVersion, String errorCode, String errorDetail,
                             Instant createdAt, Instant endedAt) {
        this(id, tenantId, workspaceId, taskId, attempt, toolName, toolVersion, status, argumentsJson,
                resultJson, policyVersion, errorCode, errorDetail, createdAt, endedAt, id, null, null,
                null, null, null, null, 1);
    }
}
// 本文件负责实现 EAF 的 ExecutionSnapshot.java 相关代码。
