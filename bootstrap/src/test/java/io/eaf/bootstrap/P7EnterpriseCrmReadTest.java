package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.eaf.agent.api.AgentCatalog;
import io.eaf.agentruntime.infrastructure.JdbcAgentRuntime;
import io.eaf.audit.api.AuditPort;
import io.eaf.capability.api.CapabilityService;
import io.eaf.connector.api.ConnectorService;
import io.eaf.context.api.ContextService;
import io.eaf.context.api.EvaluationContextSnapshotReader;
import io.eaf.execution.api.ExecutionCommand;
import io.eaf.execution.api.ExecutionService;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelResult;
import io.eaf.model.api.ModelToolCall;
import io.eaf.observability.api.TraceRecorder;
import io.eaf.prompt.api.PromptCatalog;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.CreateToolExecutionCommand;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskWorkItem;
import io.eaf.task.api.TaskStatus;
import io.eaf.tool.api.ToolCatalog;
import io.eaf.usage.api.UsageRecorder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false", "eaf.memory.outbox-publisher-enabled=false",
        "eaf.model.scenario=SUCCESS", "eaf.security.mode=local"})
//  验证版本化只读 Tool/客户授权/Evaluation 合成路径； 在同一夹具测量热客户并发读取。
class P7EnterpriseCrmReadTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String BINDING_REF = "p7-crm-read-contract.customer-read";
    private static final String CREDENTIAL = "p7-crm-contract-fixture-credential";
    private static final java.util.UUID CAPABILITY = java.util.UUID.fromString("54000000-0000-4000-8000-000000000001");
    private static final AtomicInteger REQUESTS = new AtomicInteger();
    private static final AtomicInteger IN_FLIGHT = new AtomicInteger();
    private static final AtomicInteger PEAK_IN_FLIGHT = new AtomicInteger();
    private static final AtomicInteger CRM_DELAY_MILLIS = new AtomicInteger();
    private static final CopyOnWriteArrayList<String> PATHS = new CopyOnWriteArrayList<>();
    private static final CopyOnWriteArrayList<String> AUTHORIZATION = new CopyOnWriteArrayList<>();

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));
    private static HttpServer crm;
    private static java.util.concurrent.ExecutorService crmWorkers;

    @BeforeAll
    static void startCrmFixture() throws Exception {
        crm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        crm.createContext("/customers/", P7EnterpriseCrmReadTest::readCustomer);
        crmWorkers = Executors.newFixedThreadPool(32);
        crm.setExecutor(crmWorkers);
        crm.start();
    }

    @AfterAll
    static void stopCrmFixture() {
        if (crm != null) crm.stop(0);
        if (crmWorkers != null) crmWorkers.shutdownNow();
    }

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.credentials.p7-crm-read.token", () -> CREDENTIAL);
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired CapabilityService capabilities;
    @Autowired ConnectorService connectors;
    @Autowired ExecutionService executions;
    @Autowired ObjectMapper json;
    @Autowired AgentCatalog agents;
    @Autowired PromptCatalog prompts;
    @Autowired AuditPort audit;
    @Autowired UsageRecorder usage;
    @Autowired TraceRecorder traces;
    @Autowired ToolCatalog toolCatalog;
    @Autowired ContextService contextService;
    @Autowired Clock clock;
    @Autowired EvaluationContextSnapshotReader evaluationContexts;

    @BeforeEach
    void prepareReadOnlyConnector() {
        REQUESTS.set(0);
        IN_FLIGHT.set(0);
        PEAK_IN_FLIGHT.set(0);
        CRM_DELAY_MILLIS.set(0);
        PATHS.clear();
        AUTHORIZATION.clear();
        jdbc.update("update connector.instance set status = 'ACTIVE', base_url = ? where tenant_id = ? and workspace_id = ? "
                        + "and provider = 'P7_CRM_READ_CONTRACT_FIXTURE'",
                "http://127.0.0.1:" + crm.getAddress().getPort(), Ids.TENANT_A, Ids.WORKSPACE_A);
        jdbc.update("delete from policy.customer_grant where tenant_id = ? and workspace_id = ? and actor_id = ?",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
    }

    @Test
    void customerResourceDenialStopsBeforeCrmForKnownAndUnknownIds() {
        // 不同存在状态的 ID 使用同一 Policy 拒绝，不向 CRM 探测客户是否存在。
        var known = runFixedRead(actor(), "customer-001", "p7-18-denied-known");
        var unknown = runFixedRead(actor(), "missing-001", "p7-18-denied-unknown");

        assertThat(known.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(unknown.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(known.errorCode()).isEqualTo("POLICY_DENIED");
        assertThat(unknown.errorCode()).isEqualTo(known.errorCode());
        assertThat(known.errorDetail()).isEqualTo(unknown.errorDetail());
        assertThat(REQUESTS).hasValue(0);
    }

    @Test
    void authorizedReadUsesTheVersionedContractAndProjectsOnlyToolFields() throws Exception {
        grantCustomer("customer-001");
        var profile = connectors.requireActive(Ids.TENANT_A, Ids.WORKSPACE_A, "P7_CRM_READ_CONTRACT_FIXTURE");
        var task = runFixedRead(actor(), "customer-001", "p7-18-authorized-read");
        var result = json.readTree(task.resultJson());
        var fields = new HashSet<String>();
        result.fieldNames().forEachRemaining(fields::add);

        assertThat(profile.allowedUses()).containsExactly("customer.read");
        assertThat(profile.permissions()).containsExactly("crm.customer.read");
        assertThat(task.status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(fields).containsExactlyInAnyOrder("customerId", "renewalStatus", "lastContactDate",
                "complaintSummary", "sourceId", "externalVersion");
        assertThat(result.path("customerId").asText()).isEqualTo("customer-001");
        assertThat(result.path("renewalStatus").asText()).isEqualTo("ACTIVE");
        assertThat(result.path("sourceId").asText()).isEqualTo("fixture-crm:customer-001");
        assertThat(result.path("externalVersion").asText()).isEqualTo("crm-v7:customer-001");
        assertThat(REQUESTS).hasValue(1);
        assertThat(PATHS).containsExactly("/customers/customer-001");
        assertThat(AUTHORIZATION).containsExactly("Bearer " + CREDENTIAL);
    }

    @Test
    void measuresConcurrentAuthorizedReadsForOneHotCustomer() throws Exception {
        var sampleCount = Integer.getInteger("p7.load.hot-customer-samples", 24);
        var concurrency = Integer.getInteger("p7.load.concurrency", 12);
        assertThat(sampleCount).isBetween(1, 120);
        assertThat(concurrency).isBetween(1, 16);
        grantCustomer("customer-001");
        // 固定本地读延迟使同一客户资源授权与 Connector 路径出现可观察的并发重叠。
        CRM_DELAY_MILLIS.set(25);

        var actor = actor();
        var runId = java.util.UUID.randomUUID().toString();
        var workItems = prepareHotCustomerWorkItems(actor, sampleCount, runId);
        var ready = new CountDownLatch(Math.min(concurrency, sampleCount));
        var start = new CountDownLatch(1);
        var results = new ConcurrentLinkedQueue<HotCustomerSample>();
        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            var futures = java.util.stream.IntStream.range(0, sampleCount).mapToObj(index ->
                    CompletableFuture.runAsync(() -> {
                        ready.countDown();
                        try {
                            if (!start.await(30, TimeUnit.SECONDS)) {
                                results.add(new HotCustomerSample(false, 0L, "start barrier timeout"));
                                return;
                            }
                            var before = System.nanoTime();
                            try {
                                var work = workItems.get(index);
                                tasks.complete(work, runtime.run(work));
                                var task = tasks.get(actor, Ids.WORKSPACE_A, work.id());
                                results.add(new HotCustomerSample(task.status() == TaskStatus.SUCCEEDED,
                                        System.nanoTime() - before, task.errorCode() == null ? "" : task.errorCode()));
                            } catch (Exception failure) {
                                results.add(new HotCustomerSample(false, System.nanoTime() - before,
                                        failure.getClass().getSimpleName()));
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            results.add(new HotCustomerSample(false, 0L, "interrupted"));
                        }
                    }, workers)).toList();
            assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
            var wallStarted = System.nanoTime();
            start.countDown();
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            var wallElapsed = System.nanoTime() - wallStarted;
            var successful = results.stream().filter(HotCustomerSample::succeeded).count();
            var latencies = results.stream().map(HotCustomerSample::elapsedNanos).filter(value -> value > 0)
                    .sorted().toList();
            var throughput = wallElapsed == 0 ? 0 : sampleCount / (wallElapsed / 1_000_000_000.0);
            System.out.printf(java.util.Locale.ROOT,
                    "P7_LOAD_HOT_CUSTOMER requested=%d concurrency=%d succeeded=%d errors=%d crm_requests=%d peak_in_flight=%d wall_s=%.3f throughput_rps=%.2f p50_ms=%.2f p95_ms=%.2f customer_scope=single-synthetic-grant provider_calls=0 external_writes=0%n",
                    sampleCount, concurrency, successful, results.size() - successful, REQUESTS.get(), PEAK_IN_FLIGHT.get(),
                    wallElapsed / 1_000_000_000.0, throughput, percentileMillis(latencies, 0.50),
                    percentileMillis(latencies, 0.95));
            results.stream().filter(sample -> !sample.succeeded()).limit(5).forEach(sample ->
                    System.out.printf("P7_LOAD_HOT_CUSTOMER_ERROR type=%s%n", sample.errorType()));

            assertThat(results).hasSize(sampleCount);
            assertThat(successful).isEqualTo((long) sampleCount);
            assertThat(REQUESTS).hasValue(sampleCount);
            assertThat(PATHS).hasSize(sampleCount).containsOnly("/customers/customer-001");
            assertThat(PEAK_IN_FLIGHT.get()).isBetween(1, concurrency);
        } finally {
            CRM_DELAY_MILLIS.set(0);
        }
    }

    @Test
    // Runtime 恢复工具批次时只把 Tool Schema 声明的 CRM 字段续交给模型。
    void agentRuntimeProjectsToolResultBeforeSendingItBackToModel() throws Exception {
        grantCustomer("customer-001");
        var secondRequest = new AtomicReference<io.eaf.model.api.ModelRequest>();
        var calls = new AtomicInteger();
        ModelGateway gateway = new ModelGateway() {
            @Override public ModelResult call(io.eaf.model.api.ModelRequest request) {
                if (calls.incrementAndGet() == 1)
                    return new ModelResult("local-fixture", "fixture-model", null, 10, 2, "KNOWN",
                            List.of(new ModelToolCall("p7-18-read-call", "crm.customer.query",
                                    "{\"customerId\":\"customer-001\"}")), "TOOL_CALLS");
                secondRequest.set(request);
                return new ModelResult("local-fixture", "fixture-model",
                        "{\"riskLevel\":\"LOW\",\"summary\":\"已读取客户记录。\",\"reasons\":[\"CRM 只读结果\"],\"uncertainties\":[]}",
                        10, 2, "KNOWN");
            }
            @Override public int callCount() { return calls.get(); }
        };
        var capability = capabilities.requirePublished(actor(), Ids.WORKSPACE_A, CAPABILITY, "1.3.0");
        var binding = new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash());
        var created = tasks.create(new CreateTaskCommand(actor(), Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.1.0",
                "通过 CRM 只读 Tool 获取客户事实。", null, null, "p7-18-agent-runtime-read", "trace-p7-18-agent-runtime-read",
                "USER", binding, "REST"));
        var work = tasks.claimOne().orElseThrow();
        var testRuntime = new JdbcAgentRuntime(jdbc, agents, prompts, gateway, audit, usage, traces, tasks,
                toolCatalog, executions, contextService, json, clock, evaluationContexts, capabilities);
        var outcome = testRuntime.run(work);
        tasks.complete(work, outcome);

        assertThat(outcome.status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(calls).hasValue(2);
        var toolMessage = secondRequest.get().messages().stream().filter(message -> "tool".equals(message.role()))
                .findFirst().orElseThrow();
        assertThat(toolMessage.content()).contains("\"sourceId\":\"fixture-crm:customer-001\"")
                .contains("\"externalVersion\":\"crm-v7:customer-001\"")
                .doesNotContain("contractVersion", "readAt");
        assertThat(REQUESTS).hasValue(1);
        assertThat(tasks.get(actor(), Ids.WORKSPACE_A, created.id()).status()).isEqualTo(TaskStatus.SUCCEEDED);
    }

    @Test
    void evaluationReadUsesSyntheticSourceAndNeverCallsCrm() throws Exception {
        grantCustomer("customer-001");
        var task = tasks.create(new CreateTaskCommand(actor(), Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.1.0",
                "评测读取 customer-001", null, null, "p7-18-evaluation-read", "trace-p7-18-evaluation-read", "EVALUATION"));
        var work = tasks.claim(task.id()).orElseThrow();
        var execution = executions.submit(new ExecutionCommand(actor(), Ids.WORKSPACE_A, task.id(), work.attempt(),
                Ids.AGENT_RISK, "2.1.0", "crm.customer.query", "1.1.0", "{\"customerId\":\"customer-001\"}",
                "p7-18-evaluation-read-execution", "trace-p7-18-evaluation-read"));
        var result = json.readTree(execution.resultJson());

        assertThat(execution.status()).isEqualTo("SUCCEEDED");
        assertThat(result.path("sourceId").asText()).isEqualTo("evaluation-sandbox:customer-001");
        assertThat(result.path("externalVersion").asText()).isEqualTo("evaluation-sandbox-v1");
        assertThat(REQUESTS).hasValue(0);
    }

    @Test
    void crmFailuresAreMappedToSanitizedStableErrors() {
        assertCrmFailure("customer-401", "CRM_CUSTOMER_UNAVAILABLE");
        assertCrmFailure("customer-403", "CRM_CUSTOMER_UNAVAILABLE");
        assertCrmFailure("customer-404", "CRM_CUSTOMER_UNAVAILABLE");
        assertCrmFailure("customer-409", "CRM_CONFLICT");
        assertCrmFailure("customer-429", "CRM_RATE_LIMITED");
        assertCrmFailure("customer-503", "CRM_UPSTREAM_FAILURE");
        assertCrmFailure("customer-wrong-contract", "INVALID_TOOL_RESULT");
        assertCrmFailure("customer-missing-version", "INVALID_TOOL_RESULT");
        assertThat(REQUESTS).hasValue(8);
    }

    private TaskSnapshot runFixedRead(ActorContext actor, String customerId, String key) {
        var capability = capabilities.requirePublished(actor, Ids.WORKSPACE_A, CAPABILITY, "1.3.0");
        var binding = new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash());
        var root = tasks.create(new CreateTaskCommand(actor, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.1.0",
                "读取 CRM 契约夹具客户事实。", null, null, key + "-root", "trace-" + key,
                "USER", binding, "REST"));
        var rootWork = tasks.claimOne().orElseThrow(() -> new IllegalStateException("CRM root Task 未进入可领取队列：" + root.status()));
        var child = tasks.createToolExecution(new CreateToolExecutionCommand(actor, Ids.WORKSPACE_A, root.id(), key,
                "crm.customer.query", "1.1.0", "{\"customerId\":\"" + customerId + "\"}", "trace-" + key));
        tasks.complete(rootWork, TaskRunner.RunOutcome.success("{}", false, null, null));
        var childWork = tasks.claimOne().orElseThrow(() -> new IllegalStateException("CRM child Task 未进入可领取队列：" + child.status()));
        tasks.complete(childWork, runtime.run(childWork));
        return tasks.get(actor, Ids.WORKSPACE_A, child.id());
    }

    private List<TaskWorkItem> prepareHotCustomerWorkItems(ActorContext actor, int sampleCount, String runId) {
        var workItems = new java.util.ArrayList<TaskWorkItem>();
        var capability = capabilities.requirePublished(actor, Ids.WORKSPACE_A, CAPABILITY, "1.3.0");
        var binding = new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash());
        for (int index = 0; index < sampleCount; index++) {
            var key = "p7-26-hot-customer-" + runId + "-" + index;
            var root = tasks.create(new CreateTaskCommand(actor, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.1.0",
                    "读取本地合成 CRM 客户 customer-001。", null, null, key + "-root", "trace-" + key,
                    "USER", binding, "REST"));
            var rootWork = tasks.claimOne().orElseThrow(
                    () -> new IllegalStateException("热客户根 Task 未进入 USER 队列：" + root.status()));
            if (!root.id().equals(rootWork.id()))
                throw new IllegalStateException("热客户夹具发现未预期的 USER 队列任务。");
            var child = tasks.createToolExecution(new CreateToolExecutionCommand(actor, Ids.WORKSPACE_A, root.id(), key,
                    "crm.customer.query", "1.1.0", "{\"customerId\":\"customer-001\"}", "trace-" + key));
            tasks.complete(rootWork, TaskRunner.RunOutcome.success("{}", false, null, null));
            var childWork = tasks.claimOne().orElseThrow(
                    () -> new IllegalStateException("热客户 Tool Task 未进入 USER 队列：" + child.status()));
            if (!child.id().equals(childWork.id()))
                throw new IllegalStateException("热客户夹具发现未预期的 USER 队列 Tool Task。");
            workItems.add(childWork);
        }
        return List.copyOf(workItems);
    }

    private void assertCrmFailure(String customerId, String expectedCode) {
        assertThatThrownBy(() -> connectors.readCustomer(Ids.TENANT_A, Ids.WORKSPACE_A, BINDING_REF,
                customerId, Instant.now().plusSeconds(5)))
                .isInstanceOfSatisfying(EafException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(expectedCode);
                    assertThat(failure.getMessage()).doesNotContain("fixture-secret");
                });
    }

    private double percentileMillis(List<Long> sortedNanos, double percentile) {
        if (sortedNanos.isEmpty()) return 0;
        var rank = Math.max(1, (int) Math.ceil(percentile * sortedNanos.size()));
        return sortedNanos.get(rank - 1) / 1_000_000.0;
    }

    private void grantCustomer(String customerId) {
        jdbc.update("insert into policy.customer_grant(tenant_id, workspace_id, actor_id, customer_id, status) "
                        + "values (?, ?, ?, ?, 'ACTIVE') on conflict (tenant_id, workspace_id, actor_id, customer_id) "
                        + "do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE, customerId);
    }

    private ActorContext actor() {
        return new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of("task:create", "task:read",
                "agent:read", "capability:read", "skill:read", "tool:read", "prompt:read", "context:read",
                "knowledge:read", "memory:read", "crm:customer:read"));
    }

    private static void readCustomer(HttpExchange exchange) throws IOException {
        REQUESTS.incrementAndGet();
        var inFlight = IN_FLIGHT.incrementAndGet();
        PEAK_IN_FLIGHT.accumulateAndGet(inFlight, Math::max);
        try {
            PATHS.add(exchange.getRequestURI().getPath());
            AUTHORIZATION.add(exchange.getRequestHeaders().getFirst("Authorization"));
            var delay = CRM_DELAY_MILLIS.get();
            if (delay > 0) Thread.sleep(delay);
            var customerId = exchange.getRequestURI().getPath().substring("/customers/".length());
            var status = switch (customerId) {
                case "customer-401" -> 401;
                case "customer-403" -> 403;
                case "customer-404" -> 404;
                case "customer-409" -> 409;
                case "customer-429" -> 429;
                case "customer-503" -> 503;
                default -> 200;
            };
            String contractVersion = "customer-wrong-contract".equals(customerId) ? "EAF-CRM-READ-V0" : "EAF-CRM-READ-V1";
            String externalVersion = "customer-missing-version".equals(customerId) ? null : "crm-v7:" + customerId;
            var bodyText = status == 200
                    ? customerBody(customerId, contractVersion, externalVersion)
                    : "{\"error\":\"fixture-secret-internal-detail\"}";
            var body = bodyText.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("本地 CRM 夹具请求被中断。", interrupted);
        } finally {
            IN_FLIGHT.decrementAndGet();
            exchange.close();
        }
    }

    private record HotCustomerSample(boolean succeeded, long elapsedNanos, String errorType) { }

    private static String customerBody(String customerId, String contractVersion, String externalVersion) {
        var versionField = externalVersion == null ? "" : ",\"externalVersion\":\"" + externalVersion + "\"";
        return "{\"contractVersion\":\"" + contractVersion + "\",\"customerId\":\"" + customerId
                + "\",\"renewalStatus\":\"ACTIVE\",\"lastContactDate\":\"2026-09-30\","
                + "\"complaintSummary\":\"synthetic-fixture-only\",\"sourceId\":\"fixture-crm:"
                + customerId + "\"" + versionField + "}";
    }
}
