package io.eaf.knowledge.infrastructure;

import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Knowledge 只投递本域发布事实；Audit 的唯一 factKey 是重复消费记录。 */
@Component
@ConditionalOnProperty(name = "eaf.knowledge.outbox-publisher-enabled", havingValue = "true", matchIfMissing = true)
public class KnowledgeOutboxPublisher {
    private static final int MAX_ATTEMPTS = 6;
    private final JdbcTemplate jdbc;
    private final AuditPort audit;
    private final TransactionTemplate transactions;

    public KnowledgeOutboxPublisher(JdbcTemplate jdbc, AuditPort audit, PlatformTransactionManager transactionManager) {
        this.jdbc = jdbc;
        this.audit = audit;
        this.transactions = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${eaf.knowledge.outbox-poll-delay:1000}")
    public void publish() {
        var ids = jdbc.query("select event_id from knowledge.outbox where (status = 'PENDING' "
                        + "or (status = 'FAILED' and attempt_count < ?)) and (next_attempt_at is null or next_attempt_at <= now()) "
                        + "order by created_at limit 20",
                (rs, row) -> rs.getObject("event_id", UUID.class), MAX_ATTEMPTS);
        ids.forEach(this::publishOne);
    }

    private void publishOne(UUID eventId) {
        try {
            transactions.executeWithoutResult(status -> {
                var rows = jdbc.query("select (payload->>'tenantId')::uuid as tenant_id, "
                                + "(payload->>'workspaceId')::uuid as workspace_id, event_type, payload::text, "
                                + "payload->>'candidateId' as candidate_id, payload->>'candidateRevision' as candidate_revision "
                                + "from knowledge.outbox where event_id = ? and (status = 'PENDING' "
                                + "or (status = 'FAILED' and attempt_count < ?)) "
                                + "and (next_attempt_at is null or next_attempt_at <= now()) for update",
                        (rs, row) -> new Event(rs.getObject("tenant_id", UUID.class),
                                rs.getObject("workspace_id", UUID.class), rs.getString("event_type"),
                                rs.getString("payload"), rs.getString("candidate_id"), rs.getString("candidate_revision")),
                        eventId, MAX_ATTEMPTS);
                if (rows.isEmpty()) return;
                var event = rows.getFirst();
                var traceId = event.candidateId() == null ? "knowledge-release:" + eventId
                        : "learning-candidate:" + event.candidateId() + ":" + event.candidateRevision();
                // 资产发布没有业务 Task/Execution；不伪造关联，按候选来源保留追踪键。
                audit.append(new AuditFact("knowledge-outbox:" + eventId, event.tenantId(), event.workspaceId(),
                        null, null, event.eventType(), "DELIVERED", event.payload(), traceId));
                jdbc.update("update knowledge.outbox set status = 'DELIVERED', next_attempt_at = null, last_error = null where event_id = ?",
                        eventId);
            });
        } catch (RuntimeException failure) {
            recordFailure(eventId, failure);
        }
    }

    private void recordFailure(UUID eventId, RuntimeException failure) {
        try {
            transactions.executeWithoutResult(status -> jdbc.update("update knowledge.outbox set status = 'FAILED', "
                            + "attempt_count = attempt_count + 1, last_error = ?, next_attempt_at = case attempt_count "
                            + "when 0 then now() + interval '1 second' when 1 then now() + interval '5 seconds' "
                            + "when 2 then now() + interval '30 seconds' when 3 then now() + interval '120 seconds' "
                            + "when 4 then now() + interval '300 seconds' else null end "
                            + "where event_id = ? and (status = 'PENDING' or (status = 'FAILED' and attempt_count < ?))",
                    failure.getClass().getSimpleName(), eventId, MAX_ATTEMPTS));
        } catch (RuntimeException ignored) {
            // 数据库自身暂不可用时保留原 outbox；下次轮询仍按原始幂等键处理。
        }
    }

    private record Event(UUID tenantId, UUID workspaceId, String eventType, String payload,
                         String candidateId, String candidateRevision) { }
}
