package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/** 显式启用的端到端合成容量场景；默认 Maven 测试不启动千级负载。 */
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EnabledIfSystemProperty(named = "p20.capacity.enabled", matches = "true")
@SpringBootTest(classes = EafApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "eaf.security.mode=local", "eaf.task.dispatcher-enabled=false", "eaf.task.max-concurrent=8",
                "eaf.task.max-queued=1024", "eaf.task.max-queued-per-workspace=256", "eaf.task.fair-dispatch-enabled=true",
                "eaf.task.claim-scan-limit=32", "eaf.task.evaluation-slot-wait=PT5S", "eaf.task.shutdown-grace=PT2S",
                "eaf.workflow.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
                "eaf.execution.remote-poller-enabled=false", "eaf.model.mode=deterministic",
                "eaf.model.quota-mode=postgres", "eaf.model.shared-max-concurrent=16",
                "eaf.model.shared-lease-duration=PT90S", "eaf.model.shared-heartbeat-interval=PT10S",
                "eaf.knowledge.outbox-publisher-enabled=false", "eaf.memory.outbox-publisher-enabled=false",
                "p20.worker.slow-model=true"
        })
class P20CapacityTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID TENANT = UUID.fromString("70000000-0000-4000-8000-000000000001");
    private static final UUID ALICE = UUID.fromString("80000000-0000-4000-8000-000000000001");
    private static final UUID AGENT = UUID.fromString("20000000-0000-4000-8000-000000000001");
    private static final UUID BASE_WORKSPACE = UUID.fromString("10000000-0000-4000-8000-000000000001");
    private static final String AUTH = "Bearer eaf-local-alice";
    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DRAIN_TIMEOUT = Duration.ofMinutes(4);

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

    @LocalServerPort int port;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper json;
    @Autowired DataSource dataSource;
    @Value("${p20.capacity.output-dir:target/p20-capacity}") String outputDirectory;

    @Test
    void executeConfiguredTwoNodeRestAndRuntimeCapacityScenario() throws Exception {
        var scenario = System.getProperty("p20.capacity.scenario", "Backlog");
        if (!List.of("Backlog", "Mixed").contains(scenario)) throw new IllegalArgumentException("Scenario must be Backlog or Mixed.");
        var clients = Integer.getInteger("p20.capacity.clients", 1_000);
        var workspaceCount = Integer.getInteger("p20.capacity.workspaces", 4);
        if (clients < 1 || clients > 1_024 || workspaceCount < 1 || workspaceCount > 4
                || (clients + workspaceCount - 1) / workspaceCount > 256)
            throw new IllegalArgumentException("容量参数须满足 1..1024 客户端、1..4 工作区且每区不超过 256 个排队任务。");
        var mixed = "Mixed".equals(scenario);
        var runId = UUID.randomUUID();
        var workspaces = prepareWorkspaces(workspaceCount, runId);
        var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        var peakTasks = new AtomicLong();
        var maxDbLockWaiters = new AtomicLong();
        var maxPoolActive = new AtomicInteger();
        var maxPoolWaiting = new AtomicInteger();
        var sampleFailures = new AtomicInteger();
        var sampler = Executors.newSingleThreadScheduledExecutor();
        var sampleFuture = sampler.scheduleAtFixedRate(() -> sampleDatabase(runId, peakTasks, maxDbLockWaiters,
                maxPoolActive, maxPoolWaiting, sampleFailures), 0, 50, TimeUnit.MILLISECONDS);
        var workers = mixed ? startWorkers(runId, clients) : List.<Worker>of();
        try {
            if (mixed) awaitWorkersReady(workers);
            var start = System.nanoTime();
            if (mixed) releaseWorkers(workers);
            var samples = mixed
                    ? submitAndPoll(client, runId, clients, workspaces, true)
                    : submitOnly(client, runId, clients, workspaces);
            if (!mixed) {
                var backlogPeak = currentNonterminal(runId);
                peakTasks.accumulateAndGet(backlogPeak, Math::max);
                assertThat(backlogPeak).as("Backlog 峰值应覆盖全部已接纳 Task").isGreaterThanOrEqualTo(clients);
                workers = startWorkers(runId, clients);
                awaitWorkersReady(workers);
                releaseWorkers(workers);
                samples = pollAll(client, samples);
            }
            for (var worker : workers) {
                var remaining = TimeUnit.NANOSECONDS.toMillis(start + DRAIN_TIMEOUT.toNanos() - System.nanoTime());
                if (remaining <= 0 || !worker.process().waitFor(remaining, TimeUnit.MILLISECONDS))
                    throw new IllegalStateException("容量检查的子节点超过 4 分钟未排空。");
                assertThat(worker.process().exitValue()).as(Files.readString(worker.log(), StandardCharsets.UTF_8)).isZero();
            }
            var totalElapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            var accepted = samples.size();
            assertThat(accepted).isEqualTo(clients);
            assertThat(currentNonterminal(runId)).isZero();
            assertThat(sampleFailures.get()).as("数据库容量采样必须可用").isZero();
            writeReport(runId, scenario, clients, workspaceCount, samples, workers, peakTasks.get(),
                    maxDbLockWaiters.get(), maxPoolActive.get(), maxPoolWaiting.get(), totalElapsedMs,
                    sampleFailures.get());
            System.out.printf("P20_CAPACITY_RESULT=%s;runId=%s;accepted=%d;peakQueuedRunning=%d;elapsedMs=%d%n",
                    scenario, runId, accepted, peakTasks.get(), totalElapsedMs);
            var failures = samples.stream().filter(sample -> "FAILED".equals(sample.finalStatus)).count();
            var timeouts = samples.stream().filter(sample -> "TIMED_OUT".equals(sample.finalStatus)).count();
            assertThat(failures).as("synthetic agent tasks should not fail").isZero();
            assertThat(timeouts).as("synthetic agent tasks should not time out").isZero();
            if (!mixed) assertThat(peakTasks.get()).isGreaterThanOrEqualTo(clients);
        } finally {
            sampleFuture.cancel(true);
            sampler.shutdownNow();
            for (var worker : workers) stop(worker.process());
        }
    }

    private List<UUID> prepareWorkspaces(int count, UUID runId) {
        var result = new ArrayList<UUID>();
        result.add(BASE_WORKSPACE);
        for (var index = 1; index < count; index++) {
            var workspace = UUID.randomUUID();
            jdbc.update("insert into workspace.workspace(id, tenant_id, name, status) values (?, ?, ?, 'ACTIVE')",
                    workspace, TENANT, "Synthetic " + runId + " " + index);
            jdbc.update("insert into workspace.workspace_profile(tenant_id, workspace_id, kind, owner_id, parent_workspace_id, created_by, creation_hash, created_at) "
                            + "values (?, ?, 'PROJECT', null, ?, ?, ?, now())",
                    TENANT, workspace, BASE_WORKSPACE, ALICE, "0".repeat(64));
            jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                            + "select tenant_id, ?, actor_id, action, status from workspace.\"grant\" "
                            + "where tenant_id = ? and workspace_id = ? and actor_id = ?",
                    workspace, TENANT, BASE_WORKSPACE, ALICE);
            jdbc.update("insert into workspace.operational_gate(tenant_id, workspace_id, gate_name, enabled, changed_at, row_version) "
                            + "select tenant_id, ?, gate_name, enabled, changed_at, row_version from workspace.operational_gate "
                            + "where tenant_id = ? and workspace_id = ?",
                    workspace, TENANT, BASE_WORKSPACE);
            jdbc.update("insert into agent.version(id, tenant_id, workspace_id, name, asset_version, prompt_id, prompt_version, "
                            + "model_profile_id, status, response_profile, retrieval_mode, evidence_policy, rag_enabled) "
                            + "select id, tenant_id, ?, name, asset_version, prompt_id, prompt_version, model_profile_id, status, "
                            + "response_profile, retrieval_mode, evidence_policy, rag_enabled from agent.version "
                            + "where id = ? and workspace_id = ? and asset_version = '1.0.0'",
                    workspace, AGENT, BASE_WORKSPACE);
            jdbc.update("insert into prompt.version(id, tenant_id, workspace_id, asset_version, system_template, user_template, status) "
                            + "select id, tenant_id, ?, asset_version, system_template, user_template, status from prompt.version "
                            + "where id = '21000000-0000-4000-8000-000000000001' and workspace_id = ? and asset_version = '1.0.0'",
                    workspace, BASE_WORKSPACE);
            result.add(workspace);
        }
        return List.copyOf(result);
    }

    private List<ClientSample> submitOnly(HttpClient client, UUID runId, int clients, List<UUID> workspaces) throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var gate = new CountDownLatch(1);
            var futures = new ArrayList<CompletableFuture<ClientSample>>(clients);
            for (var index = 0; index < clients; index++) {
                final int clientId = index;
                futures.add(CompletableFuture.supplyAsync(() -> {
                    await(gate);
                    return create(client, runId, clientId, workspaces.get(clientId % workspaces.size()));
                }, executor));
            }
            gate.countDown();
            return futures.stream().map(CompletableFuture::join).toList();
        }
    }

    private List<ClientSample> submitAndPoll(HttpClient client, UUID runId, int clients, List<UUID> workspaces,
                                             boolean cancelSome) throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var gate = new CountDownLatch(1);
            var futures = new ArrayList<CompletableFuture<ClientSample>>(clients);
            for (var index = 0; index < clients; index++) {
                final int clientId = index;
                futures.add(CompletableFuture.supplyAsync(() -> {
                    await(gate);
                    var sample = create(client, runId, clientId, workspaces.get(clientId % workspaces.size()));
                    if (cancelSome && clientId % 100 == 0) cancel(client, sample);
                    return waitForTerminal(client, sample);
                }, executor));
            }
            gate.countDown();
            return futures.stream().map(CompletableFuture::join).toList();
        }
    }

    private ClientSample create(HttpClient client, UUID runId, int clientId, UUID workspaceId) {
        var key = "p20-capacity-" + runId + "-" + clientId;
        var body = "{\"agentId\":\"" + AGENT + "\",\"agentVersion\":\"1.0.0\",\"input\":\"合成容量任务 "
                + clientId + "，用于独立任务处理。\"}";
        var start = System.nanoTime();
        for (var attempt = 0; attempt < 50 && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < 90; attempt++) {
            try {
                var response = client.send(HttpRequest.newBuilder(taskUri(workspaceId, null))
                                .timeout(CLIENT_TIMEOUT).header("Authorization", AUTH)
                                .header("Idempotency-Key", key).header("X-Trace-Id", runId.toString())
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 202) {
                    var task = json.readTree(response.body());
                    return new ClientSample(clientId, task.path("id").asText(), workspaceId,
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), attempt,
                            System.nanoTime(), task.path("status").asText());
                }
                if (response.statusCode() != 429)
                    throw new IllegalStateException("REST Task 创建失败：HTTP " + response.statusCode() + " " + response.body());
            } catch (java.io.IOException uncertain) {
                // 响应不明时只用同一个幂等键查询/重放，不创建新身份。
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("REST 客户端被中断。", interrupted);
            }
            // 有界指数退避并加抖动，避免排队准入争抢时持续以 10–50ms 原键重放。
            var backoffCapMs = Math.min(1_000L, 50L << Math.min(attempt, 5));
            try { Thread.sleep(java.util.concurrent.ThreadLocalRandom.current().nextLong(backoffCapMs / 2, backoffCapMs + 1)); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("REST 退避被中断。", interrupted);
            }
        }
        throw new IllegalStateException("REST 提交超过 90 秒或 50 次同键尝试，clientId=" + clientId);
    }

    private ClientSample waitForTerminal(HttpClient client, ClientSample sample) {
        var deadline = System.nanoTime() + DRAIN_TIMEOUT.toNanos();
        var terminalAt = -1L;
        var firstRunningMs = -1L;
        var queryCount = 0;
        var queryDurations = new ArrayList<Long>();
        var finalStatus = "UNKNOWN";
        var errorCode = "";
        var errorDetail = "";
        while (System.nanoTime() < deadline) {
            var before = System.nanoTime();
            try {
                var response = client.send(HttpRequest.newBuilder(taskUri(sample.workspaceId, sample.taskId))
                                .timeout(CLIENT_TIMEOUT).header("Authorization", AUTH).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200)
                    throw new IllegalStateException("REST Task 查询失败：HTTP " + response.statusCode());
                var snapshot = json.readTree(response.body());
                finalStatus = snapshot.path("status").asText("UNKNOWN");
                errorCode = snapshot.path("errorCode").asText("");
                errorDetail = snapshot.path("errorDetail").asText("");
                queryDurations.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before));
                queryCount++;
                if ("RUNNING".equals(finalStatus) && firstRunningMs < 0)
                    firstRunningMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sample.acceptedAtNanos);
                if (List.of("SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED").contains(finalStatus)) {
                    terminalAt = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sample.acceptedAtNanos);
                    break;
                }
            } catch (java.io.IOException failure) {
                throw new IllegalStateException("REST Task 查询网络失败。", failure);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("REST 查询被中断。", interrupted);
            }
            try { Thread.sleep(250); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("REST 查询轮询被中断。", interrupted);
            }
        }
        if (terminalAt < 0) throw new IllegalStateException("Task 未在 4 分钟内进入终态：" + sample.taskId);
        if (!errorCode.isBlank() && sample.clientId < 5)
            System.out.printf("P20_CAPACITY_FAILURE_SAMPLE=client:%d;code:%s;detail:%s%n", sample.clientId, errorCode, errorDetail);
        return sample.withObservation(firstRunningMs, terminalAt, finalStatus, errorCode, queryCount, queryDurations);
    }

    private List<ClientSample> pollAll(HttpClient client, List<ClientSample> samples) throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = samples.stream().map(sample -> CompletableFuture.supplyAsync(
                    () -> waitForTerminal(client, sample), executor)).toList();
            return futures.stream().map(CompletableFuture::join).toList();
        }
    }

    private void cancel(HttpClient client, ClientSample sample) {
        try {
            var response = client.send(HttpRequest.newBuilder(taskUri(sample.workspaceId, sample.taskId + "/cancel"))
                    .timeout(CLIENT_TIMEOUT).header("Authorization", AUTH).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"expectedVersion\":1}")).build(),
                    HttpResponse.BodyHandlers.ofString());
            sample.cancelHttpStatus = response.statusCode();
        } catch (Exception failure) {
            sample.cancelHttpStatus = -1;
        }
    }

    private URI taskUri(UUID workspaceId, String suffix) {
        var path = "/api/v1/workspaces/" + workspaceId + "/tasks" + (suffix == null ? "" : "/" + suffix);
        return URI.create("http://127.0.0.1:" + port + path);
    }

    private void sampleDatabase(UUID runId, AtomicLong peak, AtomicLong maxLocks,
                                AtomicInteger maxPool, AtomicInteger maxWaiting, AtomicInteger failures) {
        try {
            peak.accumulateAndGet(currentNonterminal(runId), Math::max);
            var locks = jdbc.queryForObject("select count(*) from pg_stat_activity where wait_event_type = 'Lock'", Long.class);
            if (locks != null) maxLocks.accumulateAndGet(locks, Math::max);
            if (dataSource instanceof HikariDataSource hikari && hikari.getHikariPoolMXBean() != null) {
                maxPool.accumulateAndGet(hikari.getHikariPoolMXBean().getActiveConnections(), Math::max);
                maxWaiting.accumulateAndGet(hikari.getHikariPoolMXBean().getThreadsAwaitingConnection(), Math::max);
            }
        } catch (RuntimeException unavailable) {
            failures.incrementAndGet();
        }
    }

    private long currentNonterminal(UUID runId) {
        var count = jdbc.queryForObject("select count(*) from task.task where trace_id = ? and status in ('QUEUED','RUNNING')",
                Long.class, runId.toString());
        return count == null ? 0 : count;
    }

    private List<Worker> startWorkers(UUID runId, int expectedCount) throws Exception {
        var root = Path.of("target", "p20-capacity", runId.toString());
        Files.createDirectories(root);
        var gate = root.resolve("start.ready");
        return List.of(startWorker(runId, expectedCount, gate, root.resolve("node-a.log")),
                startWorker(runId, expectedCount, gate, root.resolve("node-b.log")));
    }

    private Worker startWorker(UUID runId, int expectedCount, Path gate, Path log) throws Exception {
        var java = Path.of(System.getProperty("java.home"), "bin", "java.exe");
        if (!Files.exists(java)) java = Path.of(System.getProperty("java.home"), "bin", "java");
        var classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var builder = new ProcessBuilder(java.toString(), "-Xmx768m", "-cp", classpath,
                P20TaskWorkerRunner.class.getName(), "--p20.worker.run-id=" + runId,
                "--p20.worker.start-file=" + gate.toAbsolutePath(), "--p20.worker.expected-count=" + expectedCount,
                "--p20.worker.slow-model=true", "--p20.worker.model-delay=PT0.25S",
                "--server.port=0", "--eaf.security.mode=local", "--eaf.task.dispatcher-enabled=false",
                "--eaf.task.max-concurrent=8", "--eaf.task.max-queued=1024", "--eaf.task.max-queued-per-workspace=256",
                "--eaf.task.fair-dispatch-enabled=true", "--eaf.task.claim-scan-limit=32", "--eaf.task.shutdown-grace=PT2S",
                "--eaf.workflow.dispatcher-enabled=false", "--eaf.execution.outbox-publisher-enabled=false",
                "--eaf.execution.remote-poller-enabled=false", "--eaf.model.mode=deterministic",
                "--eaf.model.quota-mode=postgres", "--eaf.model.shared-max-concurrent=16",
                "--eaf.model.shared-lease-duration=PT90S", "--eaf.model.shared-heartbeat-interval=PT10S",
                "--spring.flyway.locations=classpath:db/test-migration,classpath:db/migration");
        var environment = builder.environment();
        environment.put("EAF_DB_URL", postgres.getJdbcUrl());
        environment.put("EAF_DB_USERNAME", postgres.getUsername());
        environment.put("EAF_DB_PASSWORD", postgres.getPassword());
        environment.put("EAF_TASK_MAX_QUEUED", "1024");
        environment.put("EAF_TASK_MAX_QUEUED_PER_WORKSPACE", "256");
        environment.put("EAF_TASK_FAIR_DISPATCH_ENABLED", "true");
        environment.put("EAF_MODEL_QUOTA_MODE", "postgres");
        environment.put("EAF_MODEL_SHARED_MAX_CONCURRENT", "16");
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        return new Worker(builder.start(), gate, log);
    }

    private void awaitWorkersReady(List<Worker> workers) throws Exception {
        var deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        for (var worker : workers) {
            while (System.nanoTime() < deadline) {
                if (Files.exists(worker.log()) && Files.readString(worker.log(), StandardCharsets.UTF_8).contains("Started P20TaskWorkerRunner")) break;
                if (!worker.process().isAlive()) throw new IllegalStateException("子节点启动失败：" + Files.readString(worker.log(), StandardCharsets.UTF_8));
                Thread.sleep(50);
            }
            if (!Files.readString(worker.log(), StandardCharsets.UTF_8).contains("Started P20TaskWorkerRunner"))
                throw new IllegalStateException("子节点启动超时。");
        }
    }

    private void releaseWorkers(List<Worker> workers) throws Exception {
        for (var worker : workers) Files.writeString(worker.gate(), "go", java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void writeReport(UUID runId, String scenario, int clients, int workspaces,
                             List<ClientSample> samples, List<Worker> workers, long peak,
                             long dbLocks, int poolActive, int poolWaiting, long elapsedMs,
                             int samplingFailures) throws Exception {
        var output = Path.of(outputDirectory).toAbsolutePath();
        Files.createDirectories(output);
        var nodeStats = new ArrayList<NodeStats>();
        for (var worker : workers) nodeStats.add(parseNode(worker,
                Files.readString(worker.log(), StandardCharsets.UTF_8)));
        var statusCounts = new HashMap<String, Long>();
        samples.forEach(sample -> statusCounts.merge(sample.finalStatus, 1L, Long::sum));
        var submitTimes = samples.stream().map(sample -> sample.submitMs).sorted().toList();
        var queueTimes = samples.stream().map(sample -> sample.firstRunningMs).filter(value -> value >= 0).sorted().toList();
        var terminalTimes = samples.stream().map(sample -> sample.terminalMs).filter(value -> value >= 0).sorted().toList();
        var queryTimes = samples.stream().flatMap(sample -> sample.queryDurations.stream()).sorted().toList();
        var report = new LinkedHashMap<String, Object>();
        report.put("runId", runId);
        report.put("scenario", scenario);
        report.put("createdAt", java.time.Instant.now().toString());
        report.put("environment", Map.of("os", System.getProperty("os.name"), "architecture", System.getProperty("os.arch"),
                "java", System.getProperty("java.version"), "availableProcessors", Runtime.getRuntime().availableProcessors(),
                "postgres", jdbc.queryForObject("select version()", String.class), "instances", 2,
                "workspaces", workspaces, "clients", clients, "model", "deterministic synthetic delay 250ms per call"));
        report.put("metrics", Map.ofEntries(Map.entry("accepted", samples.size()), Map.entry("peakQueuedRunning", peak),
                Map.entry("statuses", statusCounts), Map.entry("errorCodes", samples.stream().filter(s -> !s.errorCode.isBlank())
                        .collect(java.util.stream.Collectors.groupingBy(s -> s.errorCode, java.util.TreeMap::new,
                                java.util.stream.Collectors.counting()))), Map.entry("submitP50Ms", percentile(submitTimes, .50)),
                Map.entry("submissionRetries", samples.stream().mapToInt(sample -> sample.retries).sum()),
                Map.entry("submissionHttpAttempts", samples.size() + samples.stream().mapToInt(sample -> sample.retries).sum()),
                Map.entry("submitP95Ms", percentile(submitTimes, .95)), Map.entry("acceptedToRunningP50Ms", percentile(queueTimes, .50)),
                Map.entry("acceptedToRunningP95Ms", percentile(queueTimes, .95)), Map.entry("acceptedToTerminalP50Ms", percentile(terminalTimes, .50)),
                Map.entry("acceptedToTerminalP95Ms", percentile(terminalTimes, .95)), Map.entry("queryP50Ms", percentile(queryTimes, .50)),
                Map.entry("queryP95Ms", percentile(queryTimes, .95)), Map.entry("logicalQueries", samples.stream().mapToInt(s -> s.queryCount).sum()),
                Map.entry("cancelRequests", samples.stream().filter(s -> s.cancelHttpStatus != 0).count()),
                Map.entry("cancelHttp2xx", samples.stream().filter(s -> s.cancelHttpStatus >= 200 && s.cancelHttpStatus < 300).count()),
                Map.entry("elapsedMs", elapsedMs), Map.entry("acceptedPerSecond", roundedRate(samples.size(), elapsedMs)),
                Map.entry("terminalPerSecond", roundedRate(terminalTimes.size(), elapsedMs)), Map.entry("maxPostgresLockWaiters", dbLocks),
                Map.entry("maxParentHikariActive", poolActive), Map.entry("maxParentHikariWaiting", poolWaiting),
                Map.entry("samplingFailures", samplingFailures), Map.entry("cpuAndHeap", nodeStats)));
        report.put("evidenceBoundary", "synthetic REST -> PostgreSQL -> two TaskExecutionService JVMs -> delayed deterministic ModelGateway; no Provider HTTP, paid model, production load or business-quality claim");
        Files.writeString(output.resolve("p20-" + scenario.toLowerCase(java.util.Locale.ROOT) + "-" + runId + ".json"),
                json.writerWithDefaultPrettyPrinter().writeValueAsString(report), StandardCharsets.UTF_8);
        var csv = new StringBuilder("clientId,taskId,workspaceId,submissionMs,retries,firstRunningMs,terminalMs,status,errorCode,queries,cancelHttpStatus\n");
        samples.stream().sorted(Comparator.comparingInt(s -> s.clientId)).forEach(sample -> csv.append(sample.clientId).append(',')
                .append(sample.taskId).append(',').append(sample.workspaceId).append(',').append(sample.submitMs).append(',')
                .append(sample.retries).append(',').append(sample.firstRunningMs).append(',').append(sample.terminalMs).append(',')
                .append(sample.finalStatus).append(',').append(sample.errorCode).append(',').append(sample.queryCount).append(',')
                .append(sample.cancelHttpStatus).append('\n'));
        Files.writeString(output.resolve("p20-" + scenario.toLowerCase(java.util.Locale.ROOT) + "-" + runId + ".csv"), csv,
                StandardCharsets.UTF_8);
        Files.writeString(output.resolve("p20-" + scenario.toLowerCase(java.util.Locale.ROOT) + "-" + runId + ".md"),
                "# P20 " + scenario + " capacity run\n\n" + "Run ID: `" + runId + "`\n\nClients accepted: " + samples.size()
                        + "\n\nPeak QUEUED + RUNNING: " + peak + "\n\nElapsed: " + elapsedMs + " ms\n\n"
                        + "Model: deterministic synthetic gateway with 250 ms delay per call. No paid Provider or production evidence.\n",
                StandardCharsets.UTF_8);
    }

    private NodeStats parseNode(Worker worker, String log) {
        return new NodeStats(nodeValue(log, "P20_WORKER_CLAIMS="), nodeValue(log, "P20_WORKER_CPU_NANOS="),
                nodeValue(log, "P20_WORKER_HEAP_USED_BYTES="), worker.process().exitValue());
    }

    private static long nodeValue(String log, String key) {
        return log.lines().filter(line -> line.startsWith(key)).findFirst()
                .map(line -> Long.parseLong(line.substring(key.length()))).orElse(-1L);
    }

    private static long percentile(List<Long> sorted, double quantile) {
        if (sorted.isEmpty()) return -1;
        return sorted.get(Math.max(0, (int) Math.ceil(quantile * sorted.size()) - 1));
    }

    private static double roundedRate(long count, long elapsedMs) {
        return elapsedMs <= 0 ? 0 : Math.round(count * 1_000.0 / elapsedMs * 100.0) / 100.0;
    }

    private static void await(CountDownLatch gate) {
        try { gate.await(); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("合成客户端启动闸门被中断。", interrupted);
        }
    }

    private static void stop(Process process) throws InterruptedException {
        if (process.isAlive()) process.destroy();
        if (process.isAlive() && !process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly();
    }

    private record Worker(Process process, Path gate, Path log) { }
    private record NodeStats(long claims, long cpuNanos, long heapUsedBytes, int exitCode) { }

    private static final class ClientSample {
        final int clientId;
        final String taskId;
        final UUID workspaceId;
        final long submitMs;
        final int retries;
        final long acceptedAtNanos;
        final String initialStatus;
        long firstRunningMs = -1;
        long terminalMs = -1;
        String finalStatus;
        String errorCode = "";
        int queryCount;
        int cancelHttpStatus;
        List<Long> queryDurations = List.of();

        ClientSample(int clientId, String taskId, UUID workspaceId, long submitMs, int retries,
                     long acceptedAtNanos, String initialStatus) {
            this.clientId = clientId; this.taskId = taskId; this.workspaceId = workspaceId;
            this.submitMs = submitMs; this.retries = retries; this.acceptedAtNanos = acceptedAtNanos;
            this.initialStatus = initialStatus;
        }

        ClientSample withObservation(long runningMs, long terminalMs, String status, String errorCode, int queries, List<Long> durations) {
            firstRunningMs = runningMs; this.terminalMs = terminalMs; finalStatus = status;
            this.errorCode = errorCode;
            queryCount = queries; queryDurations = List.copyOf(durations);
            return this;
        }
    }
}
