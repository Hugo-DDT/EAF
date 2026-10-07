package io.eaf.usage.infrastructure;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Usage 域只发布本域未知费用与已停止预算范围的聚合数量。 */
@Component
public class UsageOperationalMetrics implements MeterBinder {
    private final JdbcTemplate jdbc;

    public UsageOperationalMetrics(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("eaf.usage.unknown.cost", this, UsageOperationalMetrics::unknownCostCount)
                .description("仍无法核实价格或用量的 Usage 记录数").register(registry);
        Gauge.builder("eaf.usage.budget.stopped", this, UsageOperationalMetrics::stoppedBudgetCount)
                .description("已停止的持久预算范围数").register(registry);
    }

    private double unknownCostCount() {
        return jdbc.queryForObject("select count(*) from usage.model_usage where cost_status in ('UNKNOWN_USAGE','UNKNOWN_PRICE')", Long.class);
    }

    private double stoppedBudgetCount() {
        return jdbc.queryForObject("select count(*) from usage.spend_scope where status = 'STOPPED'", Long.class);
    }
}
