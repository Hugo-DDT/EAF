package io.eaf.bootstrap;

import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskExecutionService;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskStatus;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(classes = EafApplication.class, properties = {
        "eaf.security.mode=local", "eaf.task.dispatcher-enabled=false", "eaf.task.max-concurrent=2",
        "eaf.task.max-queued=64", "eaf.task.max-queued-per-workspace=32", "eaf.task.fair-dispatch-enabled=true",
        "eaf.task.claim-scan-limit=32", "eaf.task.evaluation-slot-wait=PT5S", "eaf.task.shutdown-grace=PT2S",
        "eaf.workflow.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.execution.remote-poller-enabled=false", "eaf.model.mode=deterministic",
        "eaf.model.quota-mode=postgres", "eaf.model.shared-max-concurrent=1",
        "eaf.model.shared-lease-duration=PT20S", "eaf.model.shared-heartbeat-interval=PT5S"
})
@TestMethodOrder(OrderAnnotation.class)
class P20MultiInstanceTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN,
            Set.of("task:create", "task:read", "task:cancel"));

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
    }

    @Autowired TaskService tasks;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;

    @Test
    @Order(1)
    void postgresProviderSlotIsSharedAcrossTwoJvmProcesses() throws Exception {
        var runId = UUID.randomUUID();
        var root = Path.of("target", "p20", runId.toString());
        Files.createDirectories(root);
        var releaseFile = root.resolve("quota.release");
        var holder = startQuotaWorker("hold", releaseFile, root.resolve("holder.log"));
        Worker contender = null;
        try {
            waitForMarker(holder.log(), "P20_QUOTA_HELD=", holder.process(), Duration.ofSeconds(90));
            contender = startQuotaWorker("check", releaseFile, root.resolve("contender.log"));
            assertThat(contender.process().waitFor(90, TimeUnit.SECONDS)).isTrue();
            assertThat(contender.process().exitValue()).isZero();
            assertThat(Files.readString(contender.log(), StandardCharsets.UTF_8)).contains("P20_QUOTA_FULL=true");
            assertThat(jdbc.queryForObject("select count(*) from model.outbound_slot where state = 'HELD'", Long.class)).isEqualTo(1L);
        } finally {
            Files.createFile(releaseFile);
            if (!holder.process().waitFor(30, TimeUnit.SECONDS)) stop(holder.process());
            if (contender != null) stop(contender.process());
        }
        assertThat(Files.readString(holder.log(), StandardCharsets.UTF_8)).contains("P20_QUOTA_RELEASED=true");
        assertThat(jdbc.queryForObject("select count(*) from model.outbound_slot where state = 'FREE'", Long.class)).isEqualTo(1L);
    }

    @Test
    @Order(2)
    void workspaceQueueCapAndRotationLetSparseWorkspaceClaimAfterBusyWorkspace() {
        var sparseWorkspace = Ids.WORKSPACE_A2;
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, sparseWorkspace, Ids.ALICE, "task:create");
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, sparseWorkspace, Ids.ALICE, "task:cancel");

        var busy = new ArrayList<TaskSnapshot>();
        for (var index = 0; index < 32; index++) busy.add(create(Ids.WORKSPACE_A, "p20-fair-busy-" + index, "fair-busy"));
        var sparse = create(sparseWorkspace, "p20-fair-sparse", "fair-sparse");
        assertThatThrownBy(() -> create(Ids.WORKSPACE_A, "p20-fair-overflow", "fair-overflow"))
                .isInstanceOf(EafException.class)
                .satisfies(failure -> assertThat(((EafException) failure).code()).isEqualTo("TASK_CAPACITY_EXCEEDED"));
        assertThat(create(Ids.WORKSPACE_A, "p20-fair-busy-0", "fair-busy").id()).isEqualTo(busy.getFirst().id());
        assertThat(jdbc.queryForObject("select count(*) from task.task where workspace_id = ? and status = 'QUEUED'",
                Long.class, Ids.WORKSPACE_A)).isEqualTo(32L);

        var first = tasks.claimOne().orElseThrow();
        var second = tasks.claimOne().orElseThrow();
        assertThat(first.workspaceId()).isEqualTo(Ids.WORKSPACE_A);
        assertThat(second.workspaceId()).isEqualTo(sparseWorkspace);
        assertThat(sparse.id()).isEqualTo(second.id());
        cancelAll(busy, sparseWorkspace, sparse);
    }

    @Test
    @Order(3)
    void lockedRootBudgetIsSkippedForNextTaskInTheWorkspace() throws Exception {
        var workspace = Ids.WORKSPACE_A2;
        var runId = UUID.randomUUID();
        var blocked = create(workspace, "p20-root-lock-blocked-" + runId, runId.toString());
        var runnable = create(workspace, "p20-root-lock-runnable-" + runId, runId.toString());
        jdbc.update("update task.dispatch_scope set last_turn = 0, candidate_created_at = null, candidate_id = null "
                + "where tenant_id = ? and workspace_id = ?", Ids.TENANT_A, workspace);

        var locked = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var failure = new AtomicReference<Throwable>();
        var locker = Thread.ofPlatform().name("p20-root-budget-lock").start(() -> {
            try {
                new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
                    jdbc.queryForObject("select scope_id from task.budget_scope where root_task_id = ? for update",
                            UUID.class, blocked.id());
                    locked.countDown();
                    try {
                        if (!release.await(20, TimeUnit.SECONDS)) throw new IllegalStateException("test lock release timed out");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                });
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        });

        try {
            assertThat(locked.await(10, TimeUnit.SECONDS)).isTrue();
            var work = tasks.claimOne().orElseThrow();
            assertThat(work.id()).isEqualTo(runnable.id());
            var claimed = tasks.get(ALICE, workspace, work.id());
            assertThat(claimed.status()).isEqualTo(TaskStatus.RUNNING);
        } finally {
            release.countDown();
            locker.join(TimeUnit.SECONDS.toMillis(10));
            cancelAllAt(List.of(blocked, runnable), workspace);
        }
        assertThat(locker.isAlive()).isFalse();
        assertThat(failure.get()).isNull();
    }

    @Test
    @Order(4)
    void twoIndependentJvmsDispatchAndFinishDifferentPersistedTasks() throws Exception {
        var runId = UUID.randomUUID();
        var snapshots = IntStream.range(0, 32)
                .mapToObj(index -> create(Ids.WORKSPACE_A, "p20-node-" + runId + "-" + index, runId.toString()))
                .toList();
        var root = Path.of("target", "p20", runId.toString());
        Files.createDirectories(root);
        var startFile = root.resolve("start.ready");
        var workers = List.of(startWorker(runId, startFile, root.resolve("node-a.log")),
                startWorker(runId, startFile, root.resolve("node-b.log")));
        try {
            for (var worker : workers)
                waitForMarker(worker.log(), "Started P20TaskWorkerRunner", worker.process(), Duration.ofSeconds(90));
            Files.createFile(startFile);
            for (var worker : workers) assertThat(worker.process().waitFor(180, TimeUnit.SECONDS)).isTrue();
            var firstOutput = Files.readString(workers.get(0).log(), StandardCharsets.UTF_8);
            var secondOutput = Files.readString(workers.get(1).log(), StandardCharsets.UTF_8);
            assertThat(workers).allSatisfy(worker -> assertThat(worker.process().exitValue()).isZero());
            var firstClaims = workerClaims(firstOutput);
            var secondClaims = workerClaims(secondOutput);
            assertThat(firstClaims).isPositive();
            assertThat(secondClaims).isPositive();
            assertThat(firstClaims + secondClaims).isEqualTo(snapshots.size());
            assertThat(snapshots).allSatisfy(task -> assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status())
                    .isEqualTo(TaskStatus.SUCCEEDED));
            assertThat(jdbc.queryForObject("select count(*) from task.task_attempt a join task.task t on t.id = a.task_id "
                    + "where t.trace_id = ? and a.attempt = 1 and a.status = 'SUCCEEDED'", Long.class, runId.toString()))
                    .isEqualTo((long) snapshots.size());
        } finally {
            Files.writeString(startFile, "go", java.nio.file.StandardOpenOption.CREATE,
                    java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
            for (var worker : workers) stop(worker.process());
        }
    }

    @Test
    @Order(5)
    void offlineMaintenanceResizesOnlyAnIdleSharedProviderPool() throws Exception {
        var log = Path.of("target", "p20", UUID.randomUUID().toString(), "maintenance.log");
        Files.createDirectories(log.getParent());
        var java = Path.of(System.getProperty("java.home"), "bin", "java.exe");
        if (!Files.exists(java)) java = Path.of(System.getProperty("java.home"), "bin", "java");
        var classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var builder = new ProcessBuilder(java.toString(), "-Xmx512m", "-cp", classpath,
                EafApplication.class.getName(), "--eaf.p20.maintenance-command=model-pool-set",
                "--eaf.p20.expected-model-slots=1", "--eaf.p20.offline-confirmation=all-eaf-nodes-stopped-and-http-transports-closed",
                "--eaf.model.quota-mode=postgres", "--eaf.model.shared-max-concurrent=2",
                "--spring.flyway.locations=classpath:db/test-migration,classpath:db/migration");
        var environment = builder.environment();
        environment.put("EAF_DB_URL", postgres.getJdbcUrl());
        environment.put("EAF_DB_USERNAME", postgres.getUsername());
        environment.put("EAF_DB_PASSWORD", postgres.getPassword());
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        var process = builder.start();
        assertThat(process.waitFor(60, TimeUnit.SECONDS)).as("offline Model maintenance process should exit").isTrue();
        assertThat(process.exitValue()).as(Files.readString(log, StandardCharsets.UTF_8)).isZero();
        assertThat(Files.readString(log, StandardCharsets.UTF_8)).contains("\"sharedProviderSlots\":2");
        assertThat(jdbc.queryForObject("select count(*) from model.outbound_slot", Long.class)).isEqualTo(2L);
        assertThat(jdbc.queryForObject("select count(*) from model.outbound_slot where state <> 'FREE'", Long.class)).isZero();
    }

    private TaskSnapshot create(UUID workspaceId, String key, String trace) {
        return tasks.create(new CreateTaskCommand(ALICE, workspaceId, Ids.AGENT_RISK, "1.0.0",
                "合成任务 " + key, null, null, key, trace));
    }

    private void cancelAll(List<TaskSnapshot> busy, UUID sparseWorkspace, TaskSnapshot sparse) {
        cancelAllAt(busy, Ids.WORKSPACE_A);
        cancelAllAt(List.of(sparse), sparseWorkspace);
    }

    private void cancelAllAt(List<TaskSnapshot> tasksToCancel, UUID workspaceId) {
        for (var task : tasksToCancel) {
            var current = tasks.get(ALICE, workspaceId, task.id());
            if (!terminal(current.status())) tasks.cancel(ALICE, workspaceId, task.id(), current.version());
        }
    }

    private static boolean terminal(TaskStatus status) {
        return Set.of(TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.TIMED_OUT, TaskStatus.CANCELLED).contains(status);
    }

    private static Worker startWorker(UUID runId, Path startFile, Path logFile) throws Exception {
        var java = Path.of(System.getProperty("java.home"), "bin", "java.exe");
        if (!Files.exists(java)) java = Path.of(System.getProperty("java.home"), "bin", "java");
        var classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var builder = new ProcessBuilder(java.toString(), "-Xmx768m", "-cp", classpath,
                P20TaskWorkerRunner.class.getName(), "--p20.worker.run-id=" + runId,
                "--p20.worker.start-file=" + startFile.toAbsolutePath(),
                "--eaf.security.mode=local", "--eaf.task.dispatcher-enabled=false", "--eaf.task.max-concurrent=2",
                "--eaf.task.max-queued=64", "--eaf.task.max-queued-per-workspace=32", "--eaf.task.fair-dispatch-enabled=true",
                "--eaf.task.claim-scan-limit=32", "--eaf.task.shutdown-grace=PT2S",
                "--eaf.workflow.dispatcher-enabled=false", "--eaf.execution.outbox-publisher-enabled=false",
                "--eaf.execution.remote-poller-enabled=false", "--eaf.model.mode=deterministic",
                "--eaf.model.quota-mode=postgres", "--eaf.model.shared-max-concurrent=1",
                "--eaf.model.shared-lease-duration=PT20S", "--eaf.model.shared-heartbeat-interval=PT5S",
                "--server.port=0",
                "--spring.flyway.locations=classpath:db/test-migration,classpath:db/migration");
        var environment = builder.environment();
        environment.put("EAF_DB_URL", postgres.getJdbcUrl());
        environment.put("EAF_DB_USERNAME", postgres.getUsername());
        environment.put("EAF_DB_PASSWORD", postgres.getPassword());
        environment.put("EAF_TASK_MAX_QUEUED", "64");
        environment.put("EAF_TASK_MAX_QUEUED_PER_WORKSPACE", "32");
        environment.put("EAF_TASK_FAIR_DISPATCH_ENABLED", "true");
        environment.put("EAF_MODEL_QUOTA_MODE", "postgres");
        environment.put("EAF_MODEL_SHARED_MAX_CONCURRENT", "1");
        builder.redirectErrorStream(true).redirectOutput(logFile.toFile());
        return new Worker(builder.start(), logFile);
    }

    private static Worker startQuotaWorker(String action, Path releaseFile, Path logFile) throws Exception {
        var java = Path.of(System.getProperty("java.home"), "bin", "java.exe");
        if (!Files.exists(java)) java = Path.of(System.getProperty("java.home"), "bin", "java");
        var classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var builder = new ProcessBuilder(java.toString(), "-Xmx512m", "-cp", classpath,
                io.eaf.model.infrastructure.P20QuotaWorkerRunner.class.getName(),
                "--p20.quota.action=" + action, "--p20.quota.release-file=" + releaseFile.toAbsolutePath(),
                "--eaf.security.mode=local", "--eaf.task.dispatcher-enabled=false", "--eaf.task.max-concurrent=2",
                "--eaf.task.max-queued=64", "--eaf.task.max-queued-per-workspace=32", "--eaf.task.fair-dispatch-enabled=true",
                "--eaf.workflow.dispatcher-enabled=false", "--eaf.execution.outbox-publisher-enabled=false",
                "--eaf.execution.remote-poller-enabled=false", "--eaf.model.quota-mode=postgres",
                "--eaf.model.shared-max-concurrent=1", "--eaf.model.shared-lease-duration=PT20S",
                "--eaf.model.shared-heartbeat-interval=PT5S",
                "--server.port=0",
                "--spring.flyway.locations=classpath:db/test-migration,classpath:db/migration");
        var environment = builder.environment();
        environment.put("EAF_DB_URL", postgres.getJdbcUrl());
        environment.put("EAF_DB_USERNAME", postgres.getUsername());
        environment.put("EAF_DB_PASSWORD", postgres.getPassword());
        environment.put("EAF_TASK_MAX_QUEUED", "64");
        environment.put("EAF_TASK_MAX_QUEUED_PER_WORKSPACE", "32");
        environment.put("EAF_TASK_FAIR_DISPATCH_ENABLED", "true");
        environment.put("EAF_MODEL_QUOTA_MODE", "postgres");
        environment.put("EAF_MODEL_SHARED_MAX_CONCURRENT", "1");
        builder.redirectErrorStream(true).redirectOutput(logFile.toFile());
        return new Worker(builder.start(), logFile);
    }

    private static void waitForMarker(Path log, String marker, Process process, Duration timeout) throws Exception {
        var deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(log) && Files.readString(log, StandardCharsets.UTF_8).contains(marker)) return;
            if (!process.isAlive()) throw new AssertionError("Quota 子进程提前退出：" + Files.readString(log, StandardCharsets.UTF_8));
            Thread.sleep(50);
        }
        throw new AssertionError("Quota 子进程启动超时：" + (Files.exists(log) ? Files.readString(log, StandardCharsets.UTF_8) : "日志未创建"));
    }

    private static int workerClaims(String output) {
        var marker = output.lines().filter(line -> line.startsWith("P20_WORKER_CLAIMS=")).findFirst()
                .orElseThrow(() -> new AssertionError("Worker 缺少完成记录：" + output.substring(Math.max(0, output.length() - 2_000))));
        return Integer.parseInt(marker.substring("P20_WORKER_CLAIMS=".length()));
    }

    private static void stop(Process process) throws InterruptedException {
        if (process.isAlive()) process.destroy();
        if (process.isAlive() && !process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly();
    }

    private record Worker(Process process, Path log) { }
}
