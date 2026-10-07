package io.eaf.audit.api;

import java.util.UUID;

public record AuditFact(String factKey, UUID tenantId, UUID workspaceId, UUID actorId,
                        UUID taskId, String action, String result, String payloadJson, String traceId) { }
// 本文件负责实现 EAF 的 AuditFact.java 相关代码。
