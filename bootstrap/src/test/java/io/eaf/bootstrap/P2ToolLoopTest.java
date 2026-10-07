package io.eaf.bootstrap;

import com.sun.net.httpserver.HttpServer;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.evaluation.api.EvaluationService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

@Testcontainers
@SpringBootTest(properties = "eaf.task.dispatcher-enabled=false")
class P2ToolLoopTest {
    // 复用的固定数据库镜像，保证历史集成测试仍验证完整迁移链。
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID TENANT = UUID.fromString("70000000-0000-4000-8000-000000000001");
    private static final UUID WORKSPACE = UUID.fromString("10000000-0000-4000-8000-000000000001");
    private static final UUID ACTOR = UUID.fromString("80000000-0000-4000-8000-000000000001");
    private static final UUID AGENT = UUID.fromString("20000000-0000-4000-8000-000000000001");
    private static final AtomicInteger CRM_CALLS = new AtomicInteger();

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));
    static HttpServer crm;

    @BeforeAll
    static void startCrm() throws Exception {
        crm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        crm.createContext("/customers/customer-001", exchange -> {
            CRM_CALLS.incrementAndGet();
            // 测试 CRM 拒绝匿名请求，确认凭据只能由固定 Connector 注入。
            if (!"Bearer p2-test-credential".equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
            }
            var body = "{\"customerId\":\"customer-001\",\"renewalStatus\":\"ACTIVE\",\"lastContactDate\":\"2026-09-20\",\"complaintSummary\":\"none\",\"sourceId\":\"crm-test-001\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        crm.createContext("/customers/customer-002", exchange -> {
            CRM_CALLS.incrementAndGet();
            var status = "Bearer p2-test-credential".equals(exchange.getRequestHeaders().getFirst("Authorization")) ? 403 : 401;
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        crm.start();
    }

    @AfterAll
    static void stopCrm() { if (crm != null) crm.stop(0); }

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.credentials.test-crm.token", () -> "p2-test-credential");
    }

    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired RuntimeQuery runtimeQuery;
    @Autowired EvaluationService evaluations;
    @Autowired DataSource dataSource;

    @Test
    void executesReadOnlyToolLoopAndRecordsPerCallEvidence() {
        pointCrm(new JdbcTemplate(dataSource));
        var jdbc = new JdbcTemplate(dataSource);
        var before = CRM_CALLS.get();
        var task = run("customer-001", "p2-allowed");

        assertThat(task.status().name()).isEqualTo("SUCCEEDED");
        assertThat(task.resultJson()).contains("\"riskLevel\": \"LOW\"");
        assertThat(CRM_CALLS.get()).isEqualTo(before + 1);
        assertThat(jdbc.queryForObject("select count(*) from execution.execution where task_id = ? and status = 'SUCCEEDED'", Integer.class, task.id())).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from usage.model_usage where task_id = ? and source = 'USER'", Integer.class, task.id())).isEqualTo(2);
        assertThat(jdbc.queryForObject("select tool_calls from task.task where id = ?", Integer.class, task.id())).isEqualTo(1);
        assertThat(jdbc.queryForObject("select tool_executions from task.task where id = ?", Integer.class, task.id())).isEqualTo(1);
        var toolResult = runtimeQuery.steps(TENANT, task.id()).stream().filter(step -> "TOOL_RESULT".equals(step.type())).findFirst().orElseThrow();
        assertThat(toolResult.callNo()).isEqualTo(1);
        assertThat(toolResult.executionId()).isNotNull();
    }

    @Test
    void policyDenialPrecedesConnectorAndWrites() {
        var before = CRM_CALLS.get();
        var task = run("customer-002", "p2-denied");

        assertThat(task.status().name()).isEqualTo("FAILED");
        assertThat(task.errorCode()).isEqualTo("POLICY_DENIED");
        assertThat(CRM_CALLS.get()).isEqualTo(before);
        assertThat(new JdbcTemplate(dataSource).queryForObject("select count(*) from execution.execution where task_id = ?", Integer.class, task.id())).isEqualTo(1);
    }

    @Test
    void runsP2EvaluationThroughTheGovernedTaskPath() {
        pointCrm(new JdbcTemplate(dataSource));
        var before = CRM_CALLS.get();
        var report = evaluations.runP2(new ActorContext(ACTOR, TENANT, ActorType.HUMAN,
                Set.of("task:create", "task:read", "tool:read", "evaluation:run")), WORKSPACE);
        var jdbc = new JdbcTemplate(dataSource);
        var failedSamples = jdbc.query("select case_id, sample_no, observed_risk, error_code from evaluation.eval_result "
                        + "where run_id = ? and not passed order by case_id, sample_no",
                (rs, row) -> "%s#%s risk=%s error=%s".formatted(rs.getString("case_id"), rs.getInt("sample_no"),
                        rs.getString("observed_risk"), rs.getString("error_code")), report.id());

        assertThat(report.datasetVersion()).isEqualTo("p2-v1");
        // 质量门失败时直接列出问题用例，便于区分工具执行故障与答案回归。
        assertThat(report.status()).as("评测失败样本：%s", failedSamples).isEqualTo("PASSED");
        assertThat(report.passed()).isEqualTo(9);
        assertThat(report.total()).isEqualTo(9);
        // 评测读取必须走合成数据源，客户授权仍由 Policy 检查且不能触达 CRM。
        assertThat(CRM_CALLS.get()).isEqualTo(before);
        assertThat(jdbc.queryForObject("select count(*) from task.task where source = 'EVALUATION' and trace_id = ?", Integer.class,
                "evaluation-" + report.id())).isEqualTo(9);
        assertThat(jdbc.queryForObject("select count(*) from usage.model_usage where source = 'EVALUATION' and task_id in (select task_id from evaluation.eval_result where run_id = ?)", Integer.class,
                report.id())).isEqualTo(12);
    }

    private void pointCrm(JdbcTemplate jdbc) {
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'",
                "http://127.0.0.1:" + crm.getAddress().getPort(), TENANT, WORKSPACE);
    }

    private io.eaf.task.api.TaskSnapshot run(String customerId, String key) {
        var actor = new ActorContext(ACTOR, TENANT, ActorType.HUMAN,
                Set.of("task:create", "task:read", "tool:read"));
        var created = tasks.create(new CreateTaskCommand(actor, WORKSPACE, AGENT, "2.0.0",
                "请分析 customerId=" + customerId + " 的状态。", null, null, key, "trace-" + key, "USER"));
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(created.id());
        tasks.complete(work, runtime.run(work));
        return tasks.get(actor, WORKSPACE, created.id());
    }
}
// 本文件负责实现 EAF 的 ToolLoopTest.java 相关代码。
