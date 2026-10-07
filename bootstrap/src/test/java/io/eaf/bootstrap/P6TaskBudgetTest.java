package io.eaf.bootstrap;

import com.sun.net.httpserver.HttpServer;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateChildTaskCommand;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.CreateToolExecutionCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
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
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false"})
// 本组 PostgreSQL 集成测试验证父子幂等、根预算并发守恒和固定工具任务的 Execution 治理边界。
class P6TaskBudgetTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN,
            Set.of("task:create", "task:read", "task:resume", "approval:read", "execution:read", "tool:read"));

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));
    static HttpServer crm;
    static int crmPort;
    static final AtomicInteger writeCount = new AtomicInteger();

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
        registry.add("eaf.credentials.test-crm.token", () -> "p6-test-credential");
    }

    @BeforeEach
    void startCrm() throws Exception {
        if (crm != null) crm.stop(0);
        writeCount.set(0);
        crm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        crm.createContext("/", exchange -> {
            if (!"Bearer p6-test-credential".equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
            }
            if ("GET".equals(exchange.getRequestMethod()) && "/customers/customer-001".equals(exchange.getRequestURI().getPath())) {
                var body = "{\"customerId\":\"customer-001\",\"renewalStatus\":\"ACTIVE\",\"lastContactDate\":\"2026-09-20\",\"complaintSummary\":\"none\",\"sourceId\":\"crm-test-001\"}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                try (var output = exchange.getResponseBody()) { output.write(body); }
                return;
            }
            if ("POST".equals(exchange.getRequestMethod())) writeCount.incrementAndGet();
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        crm.start();
        crmPort = crm.getAddress().getPort();
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'",
                "http://127.0.0.1:" + crmPort, Ids.TENANT_A, Ids.WORKSPACE_A);
    }

    @AfterAll
    static void stopCrm() { if (crm != null) crm.stop(0); }

    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired JdbcTemplate jdbc;
    private final List<UUID> testRoots = new ArrayList<>();

    @AfterEach
    void cleanupTasks() {
        for (var rootId : testRoots) {
            jdbc.update("update task.task set status = 'CANCELLED', active_reserved_ms = 0, active_budget_reservation_key = null where root_task_id = ? and status in ('QUEUED', 'RUNNING', 'WAITING_APPROVAL', 'WAITING_VERIFICATION')", rootId);
            jdbc.update("update task.task_attempt a set status = 'CANCELLED' from task.task t where a.task_id = t.id and t.root_task_id = ? and a.status in ('QUEUED', 'RUNNING')", rootId);
            jdbc.update("update task.budget_scope set active_reserved_ms = 0 where root_task_id = ?", rootId);
        }
    }

    @Test
    void childTaskCreationIsStableAndUnknownUsageIsChargedOnceToRoot() {
        var root = createRoot("budget-unknown");
        var rootWork = tasks.claimOne().orElseThrow();
        assertThat(rootWork.id()).isEqualTo(root.id());
        var reservation = tasks.reserveModel(root.id(), 1, "model:unknown:1");
        assertThat(reservation.allowed()).isTrue();
        assertThat(reservation.tokenBudget()).isEqualTo(8_000);
        tasks.settleModel(root.id(), 1, "model:unknown:1", null, null);
        tasks.settleModel(root.id(), 1, "model:unknown:1", null, null);

        var command = new CreateChildTaskCommand(ALICE, Ids.WORKSPACE_A, root.id(), "analysis-1", "analyze the allowed evidence", "trace-child-1");
        var child = tasks.createChild(command);
        var repeated = tasks.createChild(command);
        assertThat(repeated.id()).isEqualTo(child.id());
        assertThat(child.rootTaskId()).isEqualTo(root.id());
        assertThat(child.parentTaskId()).isEqualTo(root.id());
        assertThat(child.source()).isEqualTo("USER");
        assertThat(child.entryProtocol()).isEqualTo("REST");
        assertThatThrownBy(() -> tasks.createChild(new CreateChildTaskCommand(ALICE, Ids.WORKSPACE_A, root.id(),
                "analysis-1", "changed input", "trace-child-1"))).isInstanceOf(EafException.class);

        var budget = jdbc.queryForMap("select model_calls, token_used, token_reserved, steps_used from task.budget_scope where root_task_id = ?", root.id());
        assertThat(budget.get("model_calls")).isEqualTo(1);
        assertThat(budget.get("token_used")).isEqualTo(8_000L);
        assertThat(budget.get("token_reserved")).isEqualTo(0L);
        assertThat(budget.get("steps_used")).isEqualTo(1);
        assertThat(jdbc.queryForObject("select settled_amount from task.budget_reservation where root_task_id = ? and reservation_key = 'model:unknown:1'", Long.class, root.id()))
                .isEqualTo(8_000L);
    }

    @Test
    void concurrentChildrenCannotReservePastTheRootModelLimit() throws Exception {
        var root = createRoot("budget-race");
        jdbc.update("update task.budget_scope set max_active_ms = 180000, max_model_calls = 1 where root_task_id = ?", root.id());
        var rootWork = tasks.claimOne().orElseThrow();
        var childA = tasks.createChild(new CreateChildTaskCommand(ALICE, Ids.WORKSPACE_A, root.id(), "branch-a", "branch A", "trace-a"));
        var childB = tasks.createChild(new CreateChildTaskCommand(ALICE, Ids.WORKSPACE_A, root.id(), "branch-b", "branch B", "trace-b"));
        var workA = tasks.claimOne().orElseThrow();
        var workB = tasks.claimOne().orElseThrow();
        assertThat(Set.of(workA.id(), workB.id())).containsExactlyInAnyOrder(childA.id(), childB.id());

        var start = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        try {
            var a = pool.submit(() -> { start.await(); return tasks.reserveModel(childA.id(), 1, "parallel:model:a"); });
            var b = pool.submit(() -> { start.await(); return tasks.reserveModel(childB.id(), 1, "parallel:model:b"); });
            start.countDown();
            var results = List.of(a.get(), b.get());
            assertThat(results.stream().filter(result -> result.allowed()).count()).isEqualTo(1);
            assertThat(jdbc.queryForObject("select model_calls from task.budget_scope where root_task_id = ?", Integer.class, root.id())).isEqualTo(1);
            assertThat(jdbc.queryForObject("select token_reserved from task.budget_scope where root_task_id = ?", Long.class, root.id())).isEqualTo(8_000L);
        } finally {
            pool.shutdownNow();
        }
        assertThat(rootWork.id()).isEqualTo(root.id());
    }

    @Test
    void fixedToolTaskUsesExecutionApprovalAndEvaluationCannotCreateWriteChild() {
        pointCrm();
        var root = createRoot("fixed-tool-parent");
        var rootWork = tasks.claimOne().orElseThrow();
        var toolTask = tasks.createToolExecution(new CreateToolExecutionCommand(ALICE, Ids.WORKSPACE_A, root.id(),
                "write-followup-1", "crm.followup.create", "1.0.0",
                "{\"customerId\":\"customer-001\",\"summary\":\" fixed task\"}", "trace-fixed-tool"));
        var parentOutcome = runtime.run(rootWork);
        tasks.complete(rootWork, parentOutcome);
        assertThat(parentOutcome.status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(parentOutcome.modelCalled()).isTrue();

        var toolWork = tasks.claimOne().orElseThrow();
        assertThat(toolWork.id()).isEqualTo(toolTask.id());
        assertThat(toolWork.runKind()).isEqualTo("TOOL_EXECUTION");
        var outcome = runtime.run(toolWork);
        tasks.complete(toolWork, outcome);
        assertThat(outcome.status()).isEqualTo(TaskStatus.WAITING_APPROVAL);
        assertThat(outcome.modelCalled()).isFalse();
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, toolTask.id()).status()).isEqualTo(TaskStatus.WAITING_APPROVAL);
        assertThat(jdbc.queryForObject("select count(*) from execution.execution where task_id = ? and status = 'AWAITING_APPROVAL'", Integer.class, toolTask.id())).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from usage.model_usage where task_id = ?", Integer.class, root.id())).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from usage.model_usage where task_id = ?", Integer.class, toolTask.id())).isZero();
        var sharedBudget = jdbc.queryForMap("select model_calls, tool_calls, steps_used from task.budget_scope where root_task_id = ?", root.id());
        assertThat(sharedBudget.get("model_calls")).isEqualTo(1);
        assertThat(sharedBudget.get("tool_calls")).isEqualTo(1);
        assertThat(sharedBudget.get("steps_used")).isEqualTo(2);
        assertThat(writeCount.get()).isZero();

        var evaluation = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.0.0",
                "evaluation parent", null, null, key("evaluation-parent"), "trace-evaluation", "EVALUATION"));
        testRoots.add(evaluation.id());
        assertThat(tasks.claim(evaluation.id())).isPresent();
        assertThatThrownBy(() -> tasks.createToolExecution(new CreateToolExecutionCommand(ALICE, Ids.WORKSPACE_A, evaluation.id(),
                "evaluation-write", "crm.followup.create", "1.0.0",
                "{\"customerId\":\"customer-001\",\"summary\":\"must be denied\"}", "trace-evaluation-write")))
                .isInstanceOf(EafException.class);
        assertThat(writeCount.get()).isZero();
    }

    private io.eaf.task.api.TaskSnapshot createRoot(String suffix) {
        var root = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.0.0",
                "root " + suffix, null, null, key(suffix), "trace-" + suffix, "USER"));
        testRoots.add(root.id());
        return root;
    }

    private static String key(String value) { return value + "-" + UUID.randomUUID(); }

    private void pointCrm() {
        // 固定写入预算测试显式授予 Workspace 写动作，避免把客户 grant 当成 Workspace 授权。
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'",
                "http://127.0.0.1:" + crmPort, Ids.TENANT_A, Ids.WORKSPACE_A);
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'crm:followup:create', 'ACTIVE') "
                        + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
    }
}
