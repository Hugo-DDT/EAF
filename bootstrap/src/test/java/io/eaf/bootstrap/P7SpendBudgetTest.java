package io.eaf.bootstrap;

import io.eaf.model.api.ModelBillingProfile;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelRequest;
import io.eaf.model.api.ModelResult;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.task.api.CreateChildTaskCommand;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.usage.api.ReserveSpendCommand;
import io.eaf.usage.api.UsageRecorder;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(properties = {
        "eaf.task.dispatcher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false",
        "eaf.execution.remote-poller-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false",
        "eaf.memory.outbox-publisher-enabled=false",
        "eaf.model.live.fee-cap=0.10",
        "eaf.model.live.fee-currency=USD",
        "eaf.model.live.stop-condition=CAP_REACHED,UNKNOWN_COST,SAFETY_VIOLATION,HUMAN_STOP"
})
// 用本地 PostgreSQL 验证实际 Runtime 出站闸门和金额 Scope 的并发守恒。
class P7SpendBudgetTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID TENANT = UUID.fromString("70000000-0000-4000-8000-000000000001");
    private static final UUID WORKSPACE = UUID.fromString("10000000-0000-4000-8000-000000000001");
    private static final UUID ALICE = UUID.fromString("80000000-0000-4000-8000-000000000001");
    private static final UUID AGENT = UUID.fromString("20000000-0000-4000-8000-000000000001");
    private static final ActorContext ACTOR = new ActorContext(ALICE, TENANT, ActorType.HUMAN, Set.of("task:create", "task:read", "task:cancel"));
    private static final AtomicInteger PROVIDER_CALLS = new AtomicInteger();
    private static final AtomicReference<String> PROFILE_MODEL = new AtomicReference<>("p7-unpriced");

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class FixtureModelConfiguration {
        @Bean
        @Primary
        ModelGateway billingFixtureModel() {
            return new ModelGateway() {
                @Override public ModelResult call(ModelRequest request) {
                    PROVIDER_CALLS.incrementAndGet();
                    return new ModelResult("response-provider", "response-selected-model",
                            "{\"riskLevel\":\"LOW\",\"summary\":\"fixture\",\"reasons\":[\"fixture\"],\"uncertainties\":[]}",
                            100, 20, "KNOWN");
                }
                @Override public int callCount() { return PROVIDER_CALLS.get(); }
                @Override public ModelBillingProfile billingProfile() {
                    return new ModelBillingProfile("fixture-provider", PROFILE_MODEL.get());
                }
            };
        }
    }

    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired UsageRecorder usage;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void resetFixture() {
        PROFILE_MODEL.set("p7-unpriced");
        PROVIDER_CALLS.set(0);
    }

    @Test
    void unknownPriceStopsBeforeProviderEgress() {
        // 缺价必须在模型调用前拒绝，并把确定的零 Token 结算回 Task 预算。
        var task = createTask("p7-no-price-" + UUID.randomUUID());
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(task.id());

        var outcome = runtime.run(work);
        tasks.complete(work, outcome);

        assertThat(outcome.errorCode()).isEqualTo("UNKNOWN_PRICE");
        assertThat(PROVIDER_CALLS).hasValue(0);
        assertThat(jdbc.queryForObject("select token_used from task.budget_scope where root_task_id = ?", Long.class, task.id())).isZero();
        assertThat(jdbc.queryForObject("select stop_reason from usage.spend_scope where scope_type = 'TASK' and scope_id = ?", String.class, task.id()))
                .isEqualTo("UNKNOWN_PRICE");
    }

    @Test
    void pricedModelCallSettlesAgainstTheReservedRateSnapshot() {
        // 真实调用结果按出站前固定的费率与上界核算，模型返回不能替换价格身份。
        PROFILE_MODEL.set("p7-priced-runtime");
        addPrice(PROFILE_MODEL.get(), "10", "10");
        var task = createTask("p7-priced-" + UUID.randomUUID());
        var work = tasks.claimOne().orElseThrow();
        var outcome = runtime.run(work);
        tasks.complete(work, outcome);

        assertThat(outcome.status().name()).isEqualTo("SUCCEEDED");
        assertThat(PROVIDER_CALLS).hasValue(1);
        assertThat(jdbc.queryForObject("select settled_amount from usage.spend_reservation where scope_id = ?", BigDecimal.class, task.id()))
                .isEqualByComparingTo("0.00120000");
        var scope = jdbc.queryForMap("select spent_amount, reserved_amount from usage.spend_scope where scope_id = ?", task.id());
        assertThat((BigDecimal) scope.get("spent_amount")).isEqualByComparingTo("0.00120000");
        assertThat((BigDecimal) scope.get("reserved_amount")).isEqualByComparingTo("0");
        var recorded = usage.findForTask(TENANT, WORKSPACE, task.id()).getFirst();
        assertThat(recorded.provider()).isEqualTo("fixture-provider");
        assertThat(recorded.model()).isEqualTo(PROFILE_MODEL.get());
        assertThat(recorded.scopeType()).isEqualTo("TASK");
        assertThat(recorded.scopeId()).isEqualTo(task.rootTaskId());
    }

    @Test
    void childTaskCannotResetRootSpendScope() {
        // 两个 Task 共用 Task 域返回的 rootTaskId，Usage 只建立并累计一个持久金额 Scope。
        PROFILE_MODEL.set("p7-child-budget");
        addPrice(PROFILE_MODEL.get(), "10", "10");
        var root = createTask("p7-child-root-" + UUID.randomUUID());
        var rootWork = tasks.claimOne().orElseThrow();
        var child = tasks.createChild(new CreateChildTaskCommand(ACTOR, WORKSPACE, root.id(), "child-budget", "子任务合成输入。", "trace-child-budget"));
        assertThat(child.rootTaskId()).isEqualTo(root.id());
        var rootOutcome = runtime.run(rootWork);
        tasks.complete(rootWork, rootOutcome);
        var childWork = tasks.claimOne().orElseThrow();
        assertThat(childWork.id()).isEqualTo(child.id());
        var childOutcome = runtime.run(childWork);
        tasks.complete(childWork, childOutcome);

        assertThat(rootOutcome.status().name()).isEqualTo("SUCCEEDED");
        assertThat(childOutcome.status().name()).isEqualTo("SUCCEEDED");
        assertThat(jdbc.queryForObject("select count(*) from usage.spend_scope where scope_type = 'TASK' and scope_id = ?",
                Integer.class, root.id())).isOne();
        var amounts = jdbc.queryForMap("select spent_amount, reserved_amount from usage.spend_scope where scope_type = 'TASK' and scope_id = ?",
                root.id());
        assertThat((BigDecimal) amounts.get("spent_amount")).isEqualByComparingTo("0.00240000");
        assertThat((BigDecimal) amounts.get("reserved_amount")).isEqualByComparingTo("0");
        assertThat(usage.findForTask(TENANT, WORKSPACE, child.id()).getFirst().scopeId()).isEqualTo(root.id());
        assertThat(PROVIDER_CALLS).hasValue(2);
    }

    @Test
    void concurrentReservationsCannotExceedOneLastQuota() throws Exception {
        // Scope 行锁串行化最后一笔余额，失败方停止 Scope 且不能发生超额预留。
        PROFILE_MODEL.set("p7-race-price");
        addPrice(PROFILE_MODEL.get(), "10", "10");
        var scopeId = UUID.randomUUID();
        var first = command(scopeId, "p7-race-a-" + scopeId, new BigDecimal("0.0015"));
        var second = command(scopeId, "p7-race-b-" + scopeId, new BigDecimal("0.0015"));
        var start = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            var a = pool.submit(() -> { start.await(); return usage.reserveSpend(first); });
            var b = pool.submit(() -> { start.await(); return usage.reserveSpend(second); });
            start.countDown();
            var results = List.of(a.get(), b.get());
            assertThat(results.stream().filter(result -> result.allowed()).count()).isEqualTo(1);
            assertThat(results.stream().filter(result -> !result.allowed()).findFirst().orElseThrow().code()).isEqualTo("COST_CAP_REACHED");
        } finally {
            pool.shutdownNow();
        }
        var amounts = jdbc.queryForMap("select spent_amount, reserved_amount, status from usage.spend_scope where scope_id = ?", scopeId);
        assertThat((BigDecimal) amounts.get("spent_amount")).isEqualByComparingTo("0");
        assertThat((BigDecimal) amounts.get("reserved_amount")).isEqualByComparingTo("0.00100000");
        assertThat(amounts.get("status")).isEqualTo("STOPPED");
    }

    @Test
    void replayKeepsUnknownInFlightReservationAndCannotEgressAgain() {
        // 模拟 Provider 响应丢失后的进程重启：稳定调用键不退款，也不允许第二次出站。
        PROFILE_MODEL.set("p7-replay-price");
        addPrice(PROFILE_MODEL.get(), "10", "10");
        var scopeId = UUID.randomUUID();
        var original = command(scopeId, "p7-lost-response-" + scopeId, new BigDecimal("0.10"));
        assertThat(usage.reserveSpend(original).allowed()).isTrue();
        var recovered = usage.reserveSpend(original);
        assertThat(recovered.allowed()).isFalse();
        assertThat(recovered.code()).isEqualTo("SPEND_CALL_REPLAY");
        assertThat(jdbc.queryForObject("select reserved_amount from usage.spend_scope where scope_id = ?", BigDecimal.class, scopeId))
                .isEqualByComparingTo("0.00100000");
        assertThat(jdbc.queryForObject("select state from usage.spend_reservation where call_key = ?", String.class, original.callKey()))
                .isEqualTo("RESERVED");
    }

    @Test
    void independentProcessRestartKeepsReservationAndRejectsReplay() throws Exception {
        // 两个独立 EAF JVM 先后访问同一 PostgreSQL scope，验证进程退出不释放未知中的费用预留。
        PROFILE_MODEL.set("p7-process-recovery");
        addPrice(PROFILE_MODEL.get(), "10", "10");
        var scopeId = UUID.randomUUID();
        var callKey = "p7-process-recovery-" + scopeId;

        var first = runSpendProcess(scopeId, callKey);
        assertThat(first.exitCode()).as(first.output()).isZero();
        assertThat(first.output()).contains("P7_SPEND_RESERVATION=ALLOWED");
        assertThat(jdbc.queryForObject("select reserved_amount from usage.spend_scope where scope_id = ?",
                BigDecimal.class, scopeId)).isEqualByComparingTo("0.00100000");

        var restarted = runSpendProcess(scopeId, callKey);
        assertThat(restarted.exitCode()).as(restarted.output()).isZero();
        assertThat(restarted.output()).contains("P7_SPEND_RESERVATION=SPEND_CALL_REPLAY");
        assertThat(jdbc.queryForObject("select reserved_amount from usage.spend_scope where scope_id = ?",
                BigDecimal.class, scopeId)).isEqualByComparingTo("0.00100000");
        assertThat(jdbc.queryForObject("select state from usage.spend_reservation where call_key = ?",
                String.class, callKey)).isEqualTo("RESERVED");
    }

    private io.eaf.task.api.TaskSnapshot createTask(String key) {
        return tasks.create(new CreateTaskCommand(ACTOR, WORKSPACE, AGENT, "2.0.0", "分析合成测试输入。", null, null,
                key, "trace-" + key, "USER"));
    }

    private ReserveSpendCommand command(UUID scopeId, String key, BigDecimal cap) {
        return new ReserveSpendCommand(TENANT, WORKSPACE, "TASK", scopeId, key, "fixture-provider",
                PROFILE_MODEL.get(), "CHAT", 100, cap, "USD");
    }

    private void addPrice(String model, String inputRate, String outputRate) {
        jdbc.update("insert into usage.price_schedule(provider, model, call_type, price_version, source, source_version, currency, billing_unit, input_price_per_million, output_price_per_million, effective_at) "
                        + "values ('fixture-provider', ?, 'CHAT', 'v1', 'local-fixture', 'p7-fixture-1', 'USD', 'TOKEN_MILLION', ?, ?, ?)",
                model, new BigDecimal(inputRate), new BigDecimal(outputRate), java.sql.Timestamp.from(Instant.parse("2026-09-01T00:00:00Z")));
    }

    private ProcessResult runSpendProcess(UUID scopeId, String callKey) throws Exception {
        var classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var executable = System.getProperty("os.name", "").toLowerCase().contains("win") ? "java.exe" : "java";
        var java = Path.of(System.getProperty("java.home"), "bin", executable).toString();
        var process = new ProcessBuilder(java, "-cp", classpath, P7SpendBudgetRunner.class.getName(),
                "--server.port=0", "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--eaf.security.mode=local", "--eaf.task.dispatcher-enabled=false",
                "--eaf.workflow.dispatcher-enabled=false", "--eaf.execution.outbox-publisher-enabled=false",
                "--eaf.execution.remote-poller-enabled=false", "--eaf.knowledge.outbox-publisher-enabled=false",
                "--eaf.memory.outbox-publisher-enabled=false", "--p7.spend.scope-id=" + scopeId,
                "--p7.spend.call-key=" + callKey, "--p7.spend.model=" + PROFILE_MODEL.get())
                .redirectErrorStream(true).start();
        var output = CompletableFuture.supplyAsync(() -> readOutput(process));
        if (!process.waitFor(60, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("Usage 独立 JVM 未在截止时间内退出。");
        }
        return new ProcessResult(process.exitValue(), output.get(5, TimeUnit.SECONDS));
    }

    private static String readOutput(Process process) {
        try { return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8); }
        catch (java.io.IOException ignored) { return ""; }
    }

    private record ProcessResult(int exitCode, String output) { }
}
