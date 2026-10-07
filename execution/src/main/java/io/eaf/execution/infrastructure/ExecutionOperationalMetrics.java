package io.eaf.execution.infrastructure;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Execution 域只发布本域 UNKNOWN 与 Outbox 聚合数量及年龄。 */
@Component
public class ExecutionOperationalMetrics implements MeterBinder {
    private final JdbcTemplate jdbc;

    public ExecutionOperationalMetrics(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("eaf.execution.unknown", this, ExecutionOperationalMetrics::unknownCount)
                .description("等待外部事实核验的 Execution 数量").register(registry);
        Gauge.builder("eaf.execution.unknown.oldest.seconds", this, ExecutionOperationalMetrics::oldestUnknownSeconds)
                .description("最早待核验 Execution 的记录年龄").register(registry);
        for (var state : List.of("PENDING", "FAILED")) {
            registry.gauge("eaf.outbox.events", List.of(Tag.of("domain", "execution"), Tag.of("state", state)),
                    this, metrics -> metrics.outboxCount(state));
            registry.gauge("eaf.outbox.oldest.seconds", List.of(Tag.of("domain", "execution"), Tag.of("state", state)),
                    this, metrics -> metrics.oldestOutboxSeconds(state));
        }
    }

    private double unknownCount() {
        return jdbc.queryForObject("select count(*) from execution.execution where status = 'UNKNOWN'", Long.class);
    }

    private double oldestUnknownSeconds() {
        return jdbc.queryForObject("select coalesce(extract(epoch from now() - min(created_at)), 0) from execution.execution where status = 'UNKNOWN'",
                Double.class);
    }

    private double outboxCount(String state) {
        return jdbc.queryForObject("select count(*) from execution.outbox where status = ?", Long.class, state);
    }

    private double oldestOutboxSeconds(String state) {
        return jdbc.queryForObject("select coalesce(extract(epoch from now() - min(created_at)), 0) from execution.outbox where status = ?",
                Double.class, state);
    }
}
