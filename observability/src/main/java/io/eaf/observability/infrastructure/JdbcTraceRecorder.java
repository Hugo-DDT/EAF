package io.eaf.observability.infrastructure;

import io.eaf.observability.api.TraceObservation;
import io.eaf.observability.api.TraceRecorder;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JdbcTraceRecorder implements TraceRecorder {
    private final JdbcTemplate jdbc;
    private final MeterRegistry metrics;
    public JdbcTraceRecorder(JdbcTemplate jdbc, MeterRegistry metrics) { this.jdbc = jdbc; this.metrics = metrics; }
    @Override public void record(TraceObservation o) {
        var phase = boundedPhase(o.phase());
        var outcome = boundedOutcome(o.outcome());
        var inserted = jdbc.update("insert into observability.trace_observation(id, trace_id, task_id, run_id, phase, elapsed_ms, outcome, error_code, occurred_at) values (gen_random_uuid(), ?, ?, ?, ?, ?, ?, ?, ?) on conflict do nothing",
                o.traceId(), o.taskId(), o.runId(), o.phase(), Math.max(0, o.elapsedMs()), o.outcome(), o.errorCode(), java.sql.Timestamp.from(o.occurredAt()));
        // 只在 Trace 幂等记录首次落库后计量，恢复重放不会重复放大次数。
        if (inserted == 1) {
            metrics.counter("eaf.runtime.operations", "phase", phase, "outcome", outcome).increment();
            metrics.timer("eaf.runtime.operation.duration", "phase", phase, "outcome", outcome)
                    .record(Math.max(0, o.elapsedMs()), TimeUnit.MILLISECONDS);
        }
    }

    private static String boundedPhase(String value) {
        if (value == null) return "other";
        if ("runtime".equals(value)) return "runtime";
        if (value.startsWith("model-")) return "model";
        if (value.startsWith("tool-")) return "tool";
        if (value.startsWith("budget-")) return "budget";
        return "other";
    }

    private static String boundedOutcome(String value) {
        if (value == null) return "other";
        return switch (value) {
            case "SUCCEEDED" -> "succeeded";
            case "FAILED", "VERIFICATION_FAILED" -> "failed";
            case "TIMED_OUT" -> "timed_out";
            case "UNKNOWN" -> "unknown";
            case "DENIED" -> "denied";
            case "AWAITING_APPROVAL", "AWAITING_REMOTE", "VERIFYING" -> "waiting";
            default -> "other";
        };
    }
}
// 本文件负责实现 EAF 的 JdbcTraceRecorder.java 相关代码。
