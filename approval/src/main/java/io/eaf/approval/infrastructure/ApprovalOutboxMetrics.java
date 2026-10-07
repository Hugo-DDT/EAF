package io.eaf.approval.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Approval 域仅公开本域 Outbox 的积压量和最早事件年龄。 */
@Component
public class ApprovalOutboxMetrics implements MeterBinder {
    private final JdbcTemplate jdbc;

    public ApprovalOutboxMetrics(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    public void bindTo(MeterRegistry registry) {
        for (var state : List.of("PENDING", "FAILED")) {
            registry.gauge("eaf.outbox.events", List.of(Tag.of("domain", "approval"), Tag.of("state", state)), this,
                    metrics -> metrics.count(state));
            registry.gauge("eaf.outbox.oldest.seconds", List.of(Tag.of("domain", "approval"), Tag.of("state", state)), this,
                    metrics -> metrics.oldestSeconds(state));
        }
    }

    private double count(String state) {
        return jdbc.queryForObject("select count(*) from approval.outbox where status = ?", Long.class, state);
    }

    private double oldestSeconds(String state) {
        return jdbc.queryForObject("select coalesce(extract(epoch from now() - min(created_at)), 0) from approval.outbox where status = ?",
                Double.class, state);
    }
}
