package io.eaf.execution.infrastructure;

import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** Execution 本地 outbox 投递器；重复投递由 Audit 的 factKey 幂等约束收敛。 */
@Component
@ConditionalOnProperty(name = "eaf.execution.outbox-publisher-enabled", havingValue = "true", matchIfMissing = true)
public class ExecutionOutboxPublisher {
    private final JdbcTemplate jdbc;
    private final AuditPort audit;

    public ExecutionOutboxPublisher(JdbcTemplate jdbc, AuditPort audit) { this.jdbc = jdbc; this.audit = audit; }

    @Scheduled(fixedDelayString = "${eaf.execution.outbox-poll-delay:1000}")
    @Transactional
    public void publish() {
        // 失败事件最多自动重试六次；人工重置为 PENDING 后只获一次调度，不清零累计 attempts。
        var rows = jdbc.query("select event_id, tenant_id, workspace_id, execution_id, event_type, payload_json::text, payload_json->>'actorId' actor_id, payload_json->>'taskId' task_id, payload_json->>'traceId' trace_id from execution.outbox where (status = 'PENDING' or (status = 'FAILED' and attempts < 6)) and next_attempt_at <= now() order by created_at for update skip locked limit 20",
                (rs, row) -> new Row(rs.getObject("event_id", UUID.class), rs.getObject("tenant_id", UUID.class),
                        rs.getObject("workspace_id", UUID.class), rs.getObject("execution_id", UUID.class),
                        rs.getString("event_type"), rs.getString("payload_json"), uuid(rs.getString("actor_id")),
                        uuid(rs.getString("task_id")), rs.getString("trace_id")));
        for (var row : rows) {
            try {
                // Audit 通过 factKey 去重；投递后进程中断时，重放只会追加同一条事实。
                audit.append(new AuditFact("execution-outbox:" + row.eventId(), row.tenantId(), row.workspaceId(), row.actorId(),
                        row.taskId(), row.eventType(), "DELIVERED", row.payload(), row.traceId()));
                jdbc.update("update execution.outbox set status = 'SENT', lease_until = null where event_id = ?", row.eventId());
            } catch (RuntimeException failure) {
                jdbc.update("update execution.outbox set status = 'FAILED', attempts = attempts + 1, next_attempt_at = now() + interval '5 seconds' where event_id = ?", row.eventId());
            }
        }
    }

    private static UUID uuid(String value) { return value == null ? null : UUID.fromString(value); }

    private record Row(UUID eventId, UUID tenantId, UUID workspaceId, UUID executionId, String eventType,
                       String payload, UUID actorId, UUID taskId, String traceId) { }
}
