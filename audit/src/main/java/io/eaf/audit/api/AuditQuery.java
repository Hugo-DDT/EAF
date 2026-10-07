package io.eaf.audit.api;

import java.util.List;
import java.util.UUID;

public interface AuditQuery {
    List<AuditEvent> byTask(UUID tenantId, UUID taskId);

    record AuditEvent(UUID id, String action, String result, String payloadJson, String traceId, java.time.Instant occurredAt) { }
}
// 本文件负责实现 EAF 的 AuditQuery.java 相关代码。
