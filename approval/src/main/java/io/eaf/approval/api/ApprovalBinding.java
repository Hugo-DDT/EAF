package io.eaf.approval.api;

import java.time.Instant;
import java.util.UUID;

/** 审批绑定的不可变快照；任何高风险字段变化都必须生成新的审批请求。 */
public record ApprovalBinding(UUID tenantId, UUID workspaceId, UUID requesterId, UUID agentId,
                              String agentVersion, UUID executionId, String operationId,
                              String toolName, String toolVersion, UUID connectorId,
                              String connectorVersion, String argumentsJson, String argumentsHash,
                              String policyVersion, String previewJson, String previewHash,
                              Instant expiresAt) { }
