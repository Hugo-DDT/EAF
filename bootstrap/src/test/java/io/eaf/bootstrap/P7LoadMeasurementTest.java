package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateChildTaskCommand;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskWorkItem;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/** 在隔离 PostgreSQL 上测量 Task admission/read、共享根预算争抢和 Hikari 等待，不调用外部 Provider。 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "server.address=127.0.0.1",
        "eaf.security.mode=local",
        "eaf.model.mode=deterministic",
        "eaf.task.dispatcher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false",
        "eaf.execution.remote-poller-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false",
        "eaf.memory.outbox-publisher-enabled=false",
        "eaf.outbound.enterprise.egress-policy-confirmed=false",
        "spring.datasource.hikari.maximum-pool-size=8",
        "spring.datasource.hikari.connection-timeout=10000"
})
class P7LoadMeasurementTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String WORKSPACE_ID = "10000000-0000-4000-8000-000000000001";
    private static final String AGENT_ID = "20000000-0000-4000-8000-000000000001";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eaf.security.mode", () -> "local");
    }

    @LocalServerPort
    int port;

    @Autowired
    ObjectMapper json;

    @Autowired
    TaskService tasks;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    HikariDataSource dataSource;

    @Test
    void measuresTaskAdmissionQueuedReadsAndRootBudgetContention() throws Exception {
        // 负载规模由脚本参数控制，限制上界避免一次误操作占满本机测试数据库。
        var sampleCount = Integer.getInteger("p7.load.samples", 120);
        var concurrency = Integer.getInteger("p7.load.concurrency", 12);
        assertThat(sampleCount).isBetween(1, 1000);
        assertThat(concurrency).isBetween(1, 64);

        var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        var apiRoot = "http://127.0.0.1:" + port + "/api/v1/workspaces/" + WORKSPACE_ID;
        var readiness = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                        + "/actuator/health/readiness")).timeout(Duration.ofSeconds(5)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(readiness.statusCode()).isEqualTo(200);
        assertThat(json.readTree(readiness.body()).path("status").asText()).isEqualTo("UP");

        var budgetBatch = measureRootBudgetContention(sampleCount, concurrency);
        reportQueueClaims(budgetBatch.claimBatch(), sampleCount, concurrency);
        reportBudgetContention(budgetBatch, sampleCount, concurrency);
        assertThat(budgetBatch.claimBatch().workItems()).hasSize(sampleCount);
        assertThat(budgetBatch.claimBatch().errorTypes()).isEmpty();
        var allowedReservations = budgetBatch.samples().stream().filter(BudgetSample::allowed).toList();
        assertThat(allowedReservations).hasSize(1);
        assertThat(budgetBatch.samples().stream().filter(sample -> !sample.allowed())
                .map(BudgetSample::errorType)).containsOnly("MODEL_CALL_LIMIT");
        assertThat(budgetBatch.samples()).hasSize(sampleCount);
        var rootBudget = jdbc.queryForMap("select model_calls, token_used, token_reserved from task.budget_scope where root_task_id = ?",
                budgetBatch.rootTaskId());
        assertThat(((Number) rootBudget.get("model_calls")).intValue()).isEqualTo(1);
        assertThat(((Number) rootBudget.get("token_used")).longValue()).isZero();
        assertThat(((Number) rootBudget.get("token_reserved")).longValue())
                .isEqualTo(allowedReservations.getFirst().tokenBudget());
        assertThat(jdbc.queryForObject("select count(*) from task.budget_reservation where root_task_id = ? and kind = 'MODEL'",
                Integer.class, budgetBatch.rootTaskId())).isEqualTo(1);

        var runId = UUID.randomUUID().toString();
        var createRequests = IntStream.range(0, sampleCount).mapToObj(index -> {
            var payload = """
                    {"agentId":"%s","agentVersion":"1.0.0","input":"P7-26 本地合成 admission 样本 %d"}
                    """.formatted(AGENT_ID, index);
            return HttpRequest.newBuilder(URI.create(apiRoot + "/tasks"))
                    .timeout(Duration.ofSeconds(30))
                    .header("Authorization", "Bearer eaf-local-alice")
                    .header("Idempotency-Key", "p7-26-" + runId + "-" + index)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();
        }).toList();

        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            var createBatch = sendTogether(client, workers, createRequests, concurrency);
            var createdIds = new ArrayList<String>();
            for (var sample : createBatch.samples()) {
                if (sample.statusCode() == 202) {
                    var response = json.readTree(sample.body());
                    createdIds.add(response.path("id").asText());
                    assertThat(response.path("status").asText()).isEqualTo("QUEUED");
                }
            }

            var readRequests = createdIds.stream().map(taskId -> HttpRequest.newBuilder(
                            URI.create(apiRoot + "/tasks/" + taskId))
                    .timeout(Duration.ofSeconds(30))
                    .header("Authorization", "Bearer eaf-local-alice")
                    .GET()
                    .build()).toList();
            var readBatch = sendTogether(client, workers, readRequests, concurrency);
            var poolPressureBatch = measureConnectionPoolPressure(client, readRequests, concurrency);
            var readableQueued = readBatch.samples().stream()
                    .filter(sample -> sample.statusCode() == 200)
                    .filter(sample -> {
                        try {
                            return "QUEUED".equals(json.readTree(sample.body()).path("status").asText());
                        } catch (Exception invalidJson) {
                            return false;
                        }
                    }).count();

            report("task-create", createBatch, sampleCount, concurrency);
            report("task-read", readBatch, createdIds.size(), concurrency);
            report("task-read-pool-pressure", poolPressureBatch, readRequests.size(), concurrency);
            System.out.printf(Locale.ROOT,
                    "P7_LOAD_RESULT created=%d queued_and_readable=%d provider_calls=0 external_writes=0%n",
                    createdIds.size(), readableQueued);

            assertThat(createBatch.samples()).hasSize(sampleCount);
            assertThat(createdIds).hasSize(sampleCount);
            assertThat(readBatch.samples()).hasSize(sampleCount);
            assertThat(readableQueued).isEqualTo((long) sampleCount);
            assertThat(poolPressureBatch.samples()).hasSize(sampleCount);
            assertThat(poolPressureBatch.samples()).allSatisfy(sample -> assertThat(sample.statusCode()).isEqualTo(200));
            assertThat(poolPressureBatch.maximumActiveConnections()).isEqualTo(dataSource.getMaximumPoolSize());
            if (concurrency > 1) assertThat(poolPressureBatch.maximumWaitingThreads()).isPositive();
        }
    }

    private Batch measureConnectionPoolPressure(HttpClient client, List<HttpRequest> requests, int concurrency)
            throws Exception {
        var heldConnections = new ArrayList<java.sql.Connection>();
        try {
            // 固定占住连接池除一个槽位外的连接，观察真实 HTTP 查询在 Hikari 等待队列中的行为。
            for (int index = 0; index < dataSource.getMaximumPoolSize() - 1; index++)
                heldConnections.add(dataSource.getConnection());
            try (var workers = Executors.newFixedThreadPool(concurrency)) {
                var batch = sendTogether(client, workers, requests, concurrency);
                System.out.printf(Locale.ROOT,
                        "P7_LOAD_POOL op=task-read-pool-pressure pool_max=%d peak_active=%d peak_waiting=%d held_connections=%d provider_calls=0 external_writes=0%n",
                        dataSource.getMaximumPoolSize(), batch.maximumActiveConnections(),
                        batch.maximumWaitingThreads(), heldConnections.size());
                return batch;
            }
        } finally {
            for (var connection : heldConnections) connection.close();
        }
    }

    private BudgetBatch measureRootBudgetContention(int sampleCount, int concurrency) throws Exception {
        var actor = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN,
                Set.of("task:create", "task:read", "task:resume", "approval:read", "execution:read", "tool:read"));
        var runId = UUID.randomUUID().toString();
        var root = tasks.create(new CreateTaskCommand(actor, Ids.WORKSPACE_A, Ids.AGENT_RISK, "1.0.0",
                "本地根预算争抢样本", null, null, "p7-26-budget-root-" + runId,
                "p7-26-budget-trace-" + runId, "USER"));
        // 先领取根 Task，再创建并领取共享根预算的子 Task，避免将排队请求误当成竞争样本。
        var rootWork = tasks.claimOne().orElseThrow();
        assertThat(rootWork.id()).isEqualTo(root.id());
        // 根预算也限制所有子 Task 的活跃时间；扩展夹具额度，避免它先于模型调用上限耗尽。
        var activeSlice = jdbc.queryForObject("select active_reserved_ms from task.task where id = ?", Long.class, root.id());
        assertThat(activeSlice).isNotNull().isPositive();
        var activeBudget = Math.multiplyExact(activeSlice, (long) sampleCount + 1);
        assertThat(jdbc.update("update task.budget_scope set max_active_ms = ? where root_task_id = ?",
                activeBudget, root.id())).isEqualTo(1);
        assertThat(jdbc.update("update task.budget_scope set max_model_calls = 1 where root_task_id = ?", root.id())).isEqualTo(1);

        var children = new ArrayList<TaskWorkItem>();
        var childIds = new ArrayList<UUID>();
        for (int index = 0; index < sampleCount; index++) {
            var child = tasks.createChild(new CreateChildTaskCommand(actor, Ids.WORKSPACE_A, root.id(),
                    "p7-26-budget-branch-" + index, "同根预算并发预留样本", "p7-26-budget-child-" + index));
            childIds.add(child.id());
        }
        var claimBatch = claimQueuedTasksTogether(sampleCount, concurrency);
        children.addAll(claimBatch.workItems());
        assertThat(children.stream().map(TaskWorkItem::id).toList()).containsExactlyInAnyOrderElementsOf(childIds);

        var ready = new CountDownLatch(Math.min(concurrency, sampleCount));
        var start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            var futures = children.stream().map(work -> CompletableFuture.supplyAsync(() -> {
                ready.countDown();
                try {
                    if (!start.await(30, TimeUnit.SECONDS))
                        return new BudgetSample(false, 0L, 0, "start barrier timeout");
                    var before = System.nanoTime();
                    var reservation = tasks.reserveModel(work.id(), work.attempt(), "p7-26-budget-reserve-" + work.id());
                    return new BudgetSample(reservation.allowed(), System.nanoTime() - before,
                            reservation.tokenBudget(), reservation.code() == null ? "" : reservation.code());
                } catch (Exception failure) {
                    return new BudgetSample(false, 0L, 0, failure.getClass().getSimpleName());
                }
            }, workers)).toList();
            assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
            var wallStart = System.nanoTime();
            start.countDown();
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            var wallElapsed = System.nanoTime() - wallStart;
            return new BudgetBatch(root.id(), futures.stream().map(CompletableFuture::join).toList(),
                    wallElapsed, claimBatch);
        }
    }

    private ClaimBatch claimQueuedTasksTogether(int requested, int concurrency) throws Exception {
        var workerCount = Math.min(concurrency, requested);
        var claimed = new ConcurrentLinkedQueue<TaskWorkItem>();
        var elapsedNanos = new ConcurrentLinkedQueue<Long>();
        var errorTypes = new ConcurrentLinkedQueue<String>();
        var emptyClaims = new AtomicInteger();
        var ready = new CountDownLatch(workerCount);
        var start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(workerCount)) {
            var futures = IntStream.range(0, workerCount).mapToObj(worker -> CompletableFuture.runAsync(() -> {
                ready.countDown();
                try {
                    if (!start.await(30, TimeUnit.SECONDS)) {
                        errorTypes.add("start barrier timeout");
                        return;
                    }
                    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                    // 多 Worker 可能同时读到同一首个候选；Task 复核租约后返回空时重试并单独计数。
                    while (claimed.size() < requested && System.nanoTime() < deadline) {
                        var before = System.nanoTime();
                        try {
                            var claim = tasks.claimOne();
                            elapsedNanos.add(System.nanoTime() - before);
                            if (claim.isPresent()) claimed.add(claim.get());
                            else {
                                emptyClaims.incrementAndGet();
                                Thread.sleep(1);
                            }
                        } catch (Exception failure) {
                            errorTypes.add(failure.getClass().getSimpleName());
                            return;
                        }
                    }
                    if (claimed.size() < requested) errorTypes.add("claim deadline exceeded");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    errorTypes.add("interrupted");
                }
            }, workers)).toList();
            assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
            var wallStart = System.nanoTime();
            start.countDown();
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            var wallElapsed = System.nanoTime() - wallStart;
            return new ClaimBatch(List.copyOf(claimed), List.copyOf(elapsedNanos),
                    emptyClaims.get(), List.copyOf(errorTypes), wallElapsed);
        }
    }

    private void reportQueueClaims(ClaimBatch batch, int requested, int concurrency) {
        var wallSeconds = batch.wallElapsedNanos() / 1_000_000_000.0;
        var throughput = wallSeconds == 0 ? 0 : batch.workItems().size() / wallSeconds;
        var sortedLatencies = batch.elapsedNanos().stream().sorted().toList();
        System.out.printf(Locale.ROOT,
                "P7_LOAD_METRIC op=task-queue-claim requested=%d concurrency=%d claimed=%d empty_claims=%d errors=%d wall_s=%.3f throughput_rps=%.2f p50_ms=%.2f p95_ms=%.2f provider_calls=0 external_writes=0%n",
                requested, concurrency, batch.workItems().size(), batch.emptyClaims(), batch.errorTypes().size(),
                wallSeconds, throughput, percentileMillis(sortedLatencies, 0.50),
                percentileMillis(sortedLatencies, 0.95));
        batch.errorTypes().stream().limit(5).forEach(error -> System.out.printf(Locale.ROOT,
                "P7_LOAD_ERROR op=task-queue-claim type=%s%n", error));
    }

    private void reportBudgetContention(BudgetBatch batch, int requested, int concurrency) {
        var latencies = batch.samples().stream().map(BudgetSample::elapsedNanos).sorted().toList();
        var allowed = batch.samples().stream().filter(BudgetSample::allowed).count();
        var denied = batch.samples().stream().filter(sample -> !sample.allowed()
                && "MODEL_CALL_LIMIT".equals(sample.errorType())).count();
        var errors = batch.samples().size() - allowed - denied;
        var wallSeconds = batch.wallElapsedNanos() / 1_000_000_000.0;
        var throughput = wallSeconds == 0 ? 0 : batch.samples().size() / wallSeconds;
        System.out.printf(Locale.ROOT,
                "P7_LOAD_METRIC op=root-budget-reserve requested=%d concurrency=%d allowed=%d denied=%d errors=%d wall_s=%.3f throughput_rps=%.2f p50_ms=%.2f p95_ms=%.2f provider_calls=0 external_writes=0%n",
                requested, concurrency, allowed, denied, errors, wallSeconds, throughput,
                percentileMillis(latencies, 0.50), percentileMillis(latencies, 0.95));
        batch.samples().stream().filter(sample -> !sample.errorType().isBlank()
                        && !"MODEL_CALL_LIMIT".equals(sample.errorType())).limit(5)
                .forEach(sample -> System.out.printf(Locale.ROOT,
                        "P7_LOAD_ERROR op=root-budget-reserve type=%s%n", sample.errorType()));
    }

    private Batch sendTogether(HttpClient client, ExecutorService workers,
                               List<HttpRequest> requests, int concurrency) throws Exception {
        if (requests.isEmpty()) return new Batch(List.of(), 0L, 0, 0);
        var keepSampling = new AtomicBoolean(true);
        var maximumActiveConnections = new AtomicInteger();
        var maximumWaitingThreads = new AtomicInteger();
        var sampler = Thread.ofPlatform().daemon().name("p7-load-pool-observer").start(() -> {
            while (keepSampling.get()) {
                var pool = dataSource.getHikariPoolMXBean();
                if (pool != null) {
                    maximumActiveConnections.accumulateAndGet(pool.getActiveConnections(), Math::max);
                    maximumWaitingThreads.accumulateAndGet(pool.getThreadsAwaitingConnection(), Math::max);
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
            }
        });
        // 屏障令工作线程同时开始，wall-clock throughput 使用整个批次的实测窗口。
        var ready = new CountDownLatch(Math.min(concurrency, requests.size()));
        var start = new CountDownLatch(1);
        var futures = requests.stream().map(request -> CompletableFuture.supplyAsync(() -> {
            ready.countDown();
            try {
                if (!start.await(30, TimeUnit.SECONDS)) return new Sample(0, 0L, "", "start barrier timeout");
                var before = System.nanoTime();
                var response = client.send(request, HttpResponse.BodyHandlers.ofString());
                return new Sample(response.statusCode(), System.nanoTime() - before,
                        response.body(), "");
            } catch (Exception failure) {
                return new Sample(0, 0L, "", failure.getClass().getSimpleName());
            }
        }, workers)).toList();
        try {
            assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
            var wallStart = System.nanoTime();
            start.countDown();
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            var wallElapsed = System.nanoTime() - wallStart;
            return new Batch(futures.stream().map(CompletableFuture::join).toList(), wallElapsed,
                    maximumActiveConnections.get(), maximumWaitingThreads.get());
        } finally {
            keepSampling.set(false);
            sampler.join(2_000);
        }
    }

    private void report(String operation, Batch batch, int requested, int concurrency) {
        var latencies = batch.samples().stream().map(Sample::elapsedNanos).sorted().toList();
        var success = batch.samples().stream().filter(sample -> sample.statusCode() >= 200
                && sample.statusCode() < 300).count();
        var errors = batch.samples().size() - success;
        var wallSeconds = batch.wallElapsedNanos() / 1_000_000_000.0;
        var throughput = wallSeconds == 0 ? 0 : batch.samples().size() / wallSeconds;
        System.out.printf(Locale.ROOT,
                "P7_LOAD_METRIC op=%s requested=%d concurrency=%d success=%d errors=%d wall_s=%.3f throughput_rps=%.2f p50_ms=%.2f p95_ms=%.2f pool_max=%d peak_active=%d peak_waiting=%d%n",
                operation, requested, concurrency, success, errors, wallSeconds, throughput,
                percentileMillis(latencies, 0.50), percentileMillis(latencies, 0.95),
                dataSource.getMaximumPoolSize(), batch.maximumActiveConnections(), batch.maximumWaitingThreads());
        batch.samples().stream().filter(sample -> sample.statusCode() == 0 || sample.statusCode() >= 300)
                .limit(5).forEach(sample -> System.out.printf(Locale.ROOT,
                        "P7_LOAD_ERROR op=%s status=%d type=%s%n", operation, sample.statusCode(), sample.errorType()));
    }

    private double percentileMillis(List<Long> sortedNanos, double percentile) {
        if (sortedNanos.isEmpty()) return 0;
        var rank = Math.max(1, (int) Math.ceil(percentile * sortedNanos.size()));
        return sortedNanos.get(rank - 1) / 1_000_000.0;
    }

    private record Sample(int statusCode, long elapsedNanos, String body, String errorType) { }
    private record Batch(List<Sample> samples, long wallElapsedNanos,
                         int maximumActiveConnections, int maximumWaitingThreads) { }
    private record BudgetSample(boolean allowed, long elapsedNanos, int tokenBudget, String errorType) { }
    private record ClaimBatch(List<TaskWorkItem> workItems, List<Long> elapsedNanos, int emptyClaims,
                              List<String> errorTypes, long wallElapsedNanos) { }
    private record BudgetBatch(UUID rootTaskId, List<BudgetSample> samples, long wallElapsedNanos,
                               ClaimBatch claimBatch) { }
}
