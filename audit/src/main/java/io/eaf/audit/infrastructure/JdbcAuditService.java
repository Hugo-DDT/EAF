package io.eaf.audit.infrastructure;

import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.audit.api.AuditQuery;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JdbcAuditService implements AuditPort, AuditQuery {
    private final JdbcTemplate jdbc;

    public JdbcAuditService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    public void append(AuditFact fact) {
        jdbc.update("insert into audit.audit_event(id, fact_key, tenant_id, workspace_id, actor_id, task_id, action, result, payload_json, trace_id) values (gen_random_uuid(), ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?) on conflict (fact_key) do nothing",
                fact.factKey(), fact.tenantId(), fact.workspaceId(), fact.actorId(), fact.taskId(), fact.action(), fact.result(), fact.payloadJson() == null ? "{}" : fact.payloadJson(), fact.traceId());
    }

    @Override
    public List<AuditEvent> byTask(UUID tenantId, UUID taskId) {
        return jdbc.query("select id, action, result, payload_json::text, trace_id, occurred_at from audit.audit_event where tenant_id = ? and task_id = ? order by occurred_at, id",
                (rs, row) -> new AuditEvent(rs.getObject("id", UUID.class), rs.getString("action"), rs.getString("result"), rs.getString("payload_json"), rs.getString("trace_id"), rs.getTimestamp("occurred_at").toInstant()), tenantId, taskId);
    }
}
// 本文件负责实现 EAF 的 JdbcAuditService.java 相关代码。
