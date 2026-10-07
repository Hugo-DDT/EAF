package io.eaf.task.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Task 域只发布状态数量与等待年龄，不暴露任务内容或标识。 */
@Component
public class TaskOperationalMetrics implements MeterBinder {
    private static final List<String> STATES = List.of("QUEUED", "RUNNING", "WAITING_APPROVAL",
            "WAITING_VERIFICATION", "WAITING_REMOTE", "CANCELLING_REMOTE", "SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED");
    private static final List<String> WAITING_STATES = List.of("WAITING_APPROVAL", "WAITING_VERIFICATION", "WAITING_REMOTE");
    private final JdbcTemplate jdbc;
    private final AtomicInteger activeExecutions = new AtomicInteger();
    private Counter rejectedFull;
    private Counter rejectedLockBusy;
    private Counter rejectedWorkspaceFull;
    private final Map<String, Counter> claimAttempts = new ConcurrentHashMap<>();
    private volatile MeterRegistry registry;
    private Counter recoveryProcessed;

    public TaskOperationalMetrics(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    public void bindTo(MeterRegistry registry) {
        this.registry = registry;
        for (var state : STATES) {
            registry.gauge("eaf.task.state", java.util.List.of(io.micrometer.core.instrument.Tag.of("state", state)),
                    this, metrics -> metrics.stateCount(state));
        }
        for (var state : WAITING_STATES) {
            registry.gauge("eaf.task.waiting.oldest.seconds", java.util.List.of(io.micrometer.core.instrument.Tag.of("state", state)),
                    this, metrics -> metrics.oldestWaitingSeconds(state));
        }
        registry.gauge("eaf.task.execution.active", activeExecutions);
        registry.gauge("eaf.task.queue.oldest.age", this, metrics -> metrics.oldestQueuedSeconds());
        rejectedFull = Counter.builder("eaf.task.admission.rejected").tag("reason", "queue_full").register(registry);
        rejectedLockBusy = Counter.builder("eaf.task.admission.rejected").tag("reason", "lock_busy").register(registry);
        rejectedWorkspaceFull = Counter.builder("eaf.task.admission.rejected").tag("reason", "workspace_full").register(registry);
        recoveryProcessed = Counter.builder("eaf.task.recovery.processed").register(registry);
    }

    void executionStarted() { activeExecutions.incrementAndGet(); }
    void executionStopped() { activeExecutions.decrementAndGet(); }
    void admissionRejected(String reason) {
        var counter = switch (reason) {
            case "lock_busy" -> rejectedLockBusy;
            case "workspace_full" -> rejectedWorkspaceFull;
            default -> rejectedFull;
        };
        if (counter != null) counter.increment();
    }

    void claimAttempt(String result) {
        if (!List.of("claimed", "root_busy", "task_busy", "paused", "empty").contains(result)) return;
        var meterRegistry = registry;
        if (meterRegistry != null) claimAttempts.computeIfAbsent(result, key -> Counter.builder("eaf.task.claim.attempts")
                .tag("result", key).register(meterRegistry)).increment();
    }

    void recoveryProcessed(int count) {
        var counter = recoveryProcessed;
        if (counter != null && count > 0) counter.increment(count);
    }

    private double stateCount(String state) {
        return jdbc.queryForObject("select count(*) from task.task where status = ?", Long.class, state);
    }

    private double oldestWaitingSeconds(String state) {
        return jdbc.queryForObject("select coalesce(extract(epoch from now() - min(created_at)), 0) from task.task where status = ?",
                Double.class, state);
    }

    private double oldestQueuedSeconds() {
        return jdbc.queryForObject("select coalesce(extract(epoch from now() - min(updated_at)), 0) from task.task where status = 'QUEUED'",
                Double.class);
    }
}
