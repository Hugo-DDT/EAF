package io.eaf.bootstrap;

import io.eaf.task.api.TaskExecutionService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.management.ManagementFactory;
import java.util.UUID;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

/** 独立节点 runner：通过真实 TaskExecutionService 驱动队列，受父测试文件闸门同步启动。 */
public final class P20TaskWorkerRunner {
    private P20TaskWorkerRunner() { }

    public static void main(String[] args) throws Exception {
        var app = new SpringApplicationBuilder(EafApplication.class);
        if (java.util.Arrays.stream(args).anyMatch(arg -> "--p20.worker.slow-model=true".equals(arg)))
            app.sources(P20SlowModelConfiguration.class);
        try (var context = app.run(args)) {
            var environment = context.getEnvironment();
            var runId = UUID.fromString(environment.getRequiredProperty("p20.worker.run-id"));
            var startFile = Path.of(environment.getRequiredProperty("p20.worker.start-file"));
            var expectedCount = environment.getProperty("p20.worker.expected-count", Integer.class, 0);
            while (!Files.exists(startFile)) Thread.sleep(25);
            var execution = context.getBean(TaskExecutionService.class);
            var jdbc = context.getBean(JdbcTemplate.class);
            var claims = 0;
            var cpuStart = processCpuNanos();
            var deadline = System.nanoTime() + java.time.Duration.ofMinutes(3).toNanos();
            while (System.nanoTime() < deadline) {
                if (execution.dispatchNext()) claims++;
                var pending = jdbc.queryForObject("select count(*) from task.task where trace_id = ? "
                                + "and status in ('QUEUED','RUNNING')", Long.class, runId.toString());
                var total = jdbc.queryForObject("select count(*) from task.task where trace_id = ?", Long.class, runId.toString());
                if (total != null && total >= expectedCount && (pending == null || pending == 0)) {
                    System.out.println("P20_WORKER_CLAIMS=" + claims);
                    System.out.println("P20_WORKER_CPU_NANOS=" + Math.max(0, processCpuNanos() - cpuStart));
                    var runtime = Runtime.getRuntime();
                    System.out.println("P20_WORKER_HEAP_USED_BYTES=" + (runtime.totalMemory() - runtime.freeMemory()));
                    return;
                }
                Thread.sleep(20);
            }
            throw new IllegalStateException("worker 排空超过 3 分钟；claims=" + claims);
        }
    }

    private static long processCpuNanos() {
        var bean = ManagementFactory.getOperatingSystemMXBean();
        return bean instanceof com.sun.management.OperatingSystemMXBean process ? process.getProcessCpuTime() : 0;
    }
}
