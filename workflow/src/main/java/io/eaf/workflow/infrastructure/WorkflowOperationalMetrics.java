package io.eaf.workflow.infrastructure;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Workflow 域只发布本域待处理实例的数量与年龄，不读取 Task 或 Execution 表。 */
@Component
public class WorkflowOperationalMetrics implements MeterBinder {
    private final JdbcTemplate jdbc;

    public WorkflowOperationalMetrics(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("eaf.workflow.waiting.instances", this, WorkflowOperationalMetrics::waitingCount)
                .description("等待子任务、人工处理或外部推进的 Workflow 实例数")
                .register(registry);
        Gauge.builder("eaf.workflow.waiting.oldest.seconds", this, WorkflowOperationalMetrics::oldestWaitingSeconds)
                .description("最早仍在等待的 Workflow 实例年龄")
                .register(registry);
    }

    private double waitingCount() {
        return jdbc.queryForObject("select count(*) from workflow.instance where status in ('WAITING_CHILD', 'WAITING_HUMAN')", Long.class);
    }

    private double oldestWaitingSeconds() {
        return jdbc.queryForObject("select coalesce(extract(epoch from now() - min(created_at)), 0) from workflow.instance where status in ('WAITING_CHILD', 'WAITING_HUMAN')",
                Double.class);
    }
}
