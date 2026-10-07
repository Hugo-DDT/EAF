package io.eaf.bootstrap;

import javax.sql.DataSource;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.http.MediaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import java.util.UUID;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(OrderAnnotation.class)
class EafApplicationTest {

    // 所有 Spring 集成测试使用同一固定 pgvector 镜像，使 V40 迁移可在真实扩展上校验。
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.datasource.hikari.connection-timeout", () -> "1000");
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired MockMvc mvc;
    @Autowired DataSource dataSource;
    @Autowired ObjectMapper mapper;
    @Autowired AuditPort audit;

    @Test
    @Order(1)
    void startsMigratesAndExposesOnlySanitizedHealth() throws Exception {
        var jdbc = new JdbcTemplate(dataSource);
        assertThat(jdbc.queryForObject("select note from eaf_meta.p0_probe where id = 1", String.class))
                .isEqualTo("migration-ran");
        assertThat(jdbc.queryForObject("select count(*) from eaf_meta.flyway_schema_history where success and version = '1'", Integer.class))
                .isEqualTo(1);

        mvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString(postgres.getJdbcUrl()))))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString(postgres.getPassword()))));
        mvc.perform(get("/does-not-exist")).andExpect(status().isForbidden());
    }

    @Test
    @Order(2)
    void authenticatesScopesAndRunsAnIdempotentTask() throws Exception {
        mvc.perform(get("/api/v1/me"))
                .andExpect(status().isUnauthorized());
        mvc.perform(get("/api/v1/me").header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tenantId").value("70000000-0000-4000-8000-000000000001"))
                .andExpect(jsonPath("$.actions", org.hamcrest.Matchers.hasItem("task:create")));
        mvc.perform(get("/api/v1/workspaces/10000000-0000-4000-8000-000000000003/agents")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isNotFound());

        var request = """
                {"agentId":"20000000-0000-4000-8000-000000000001","agentVersion":"1.0.0","input":"客户续约材料摘要"}
                """;
        var created = mvc.perform(post("/api/v1/workspaces/10000000-0000-4000-8000-000000000001/tasks")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p1-test-1")
                        .header("X-Trace-Id", "trace-p1-test-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request))
                .andExpect(status().isAccepted())
                .andReturn();
        var location = created.getResponse().getHeader("Location");
        assertThat(location).isNotBlank();

        String finalStatus = null;
        for (var i = 0; i < 40; i++) {
            var body = mvc.perform(get(location).header("Authorization", "Bearer eaf-local-alice"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
            finalStatus = mapper.readTree(body).path("status").asText();
            if ("SUCCEEDED".equals(finalStatus)) break;
            Thread.sleep(100);
        }
        assertThat(finalStatus).isEqualTo("SUCCEEDED");
        var jdbc = new JdbcTemplate(dataSource);
        var taskId = UUID.fromString(location.substring(location.lastIndexOf('/') + 1));
        assertThat(jdbc.queryForObject("select count(*) from agent_runtime.run where task_id = ? and source = 'USER' and status = 'SUCCEEDED'", Integer.class, taskId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from usage.model_usage where task_id = ? and source = 'USER' and status = 'SUCCEEDED' and input_tokens = 120 and output_tokens = 32 and cost_status = 'UNKNOWN_PRICE' and estimated_cost is null", Integer.class, taskId)).isEqualTo(1);
        // 成功模型调用与任务运行各有独立阶段，分别核验可避免把新增埋点误计为重复执行。
        assertThat(jdbc.queryForObject("select count(*) from observability.trace_observation where task_id = ? and trace_id = ? and phase = 'model-1' and outcome = 'SUCCEEDED'", Integer.class, taskId, "trace-p1-test-1")).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from observability.trace_observation where task_id = ? and trace_id = ? and phase = 'runtime' and outcome = 'SUCCEEDED'", Integer.class, taskId, "trace-p1-test-1")).isEqualTo(1);
        mvc.perform(get(location).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(jsonPath("$.result.riskLevel").value("LOW"))
                .andExpect(jsonPath("$.steps.length()").value(3));
        mvc.perform(get(location + "/replay").header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.originalStatus").value("SUCCEEDED"))
                .andExpect(jsonPath("$.replayStatus").value("SUCCEEDED"))
                .andExpect(jsonPath("$.modelCalled").value(false))
                .andExpect(jsonPath("$.matched").value(true));
        mvc.perform(get(location + "/audit-events").header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(4));

        var repeated = mvc.perform(post("/api/v1/workspaces/10000000-0000-4000-8000-000000000001/tasks")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p1-test-1")
                        .contentType(MediaType.APPLICATION_JSON).content(request))
                .andExpect(status().isAccepted()).andReturn();
        assertThat(repeated.getResponse().getHeader("Location")).isEqualTo(location);
        mvc.perform(post("/api/v1/workspaces/10000000-0000-4000-8000-000000000001/tasks")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p1-test-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(request.replace("续约", "冲突")))
                .andExpect(status().isConflict());

        var evaluation = mvc.perform(post("/api/v1/workspaces/10000000-0000-4000-8000-000000000001/evaluations/p1")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.datasetVersion").value("p1-v1"))
                .andExpect(jsonPath("$.passed").value(18))
                .andExpect(jsonPath("$.total").value(18))
                .andExpect(jsonPath("$.status").value("PASSED"))
                .andExpect(jsonPath("$.sampleCount").value(3))
                .andExpect(jsonPath("$.modelCalls").value(18))
                .andExpect(jsonPath("$.knownUsageSamples").value(18))
                .andExpect(jsonPath("$.failedCases").isEmpty())
                .andReturn();
        var evaluationId = UUID.fromString(mapper.readTree(evaluation.getResponse().getContentAsString()).path("id").asText());
        assertThat(jdbc.queryForObject("select count(*) from evaluation.eval_result where run_id = ? and passed", Integer.class, evaluationId)).isEqualTo(18);
        var evaluationTaskIds = jdbc.queryForList("select task_id from evaluation.eval_result where run_id = ?", UUID.class, evaluationId);
        assertThat(evaluationTaskIds).hasSize(18);
        for (var evaluationTaskId : evaluationTaskIds) {
            assertThat(jdbc.queryForObject("select source from agent_runtime.run where task_id = ?", String.class, evaluationTaskId)).isEqualTo("EVALUATION");
            assertThat(jdbc.queryForObject("select source from usage.model_usage where task_id = ?", String.class, evaluationTaskId)).isEqualTo("EVALUATION");
        }
    }

    @Test
    @Order(3)
    void coversBothTenantsAndBothWorkspaceSlots() throws Exception {
        //  在 Alice 的两个 Workspace 发布 RAG Agent； 在主 Workspace 新增版本化 Agent。
                mvc.perform(get("/api/v1/workspaces/10000000-0000-4000-8000-000000000001/agents")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(27));
        mvc.perform(get("/api/v1/workspaces/10000000-0000-4000-8000-000000000002/agents")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(5));
        mvc.perform(get("/api/v1/workspaces/10000000-0000-4000-8000-000000000003/agents")
                        .header("Authorization", "Bearer eaf-local-carol"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(3));
        mvc.perform(get("/api/v1/workspaces/10000000-0000-4000-8000-000000000004/agents")
                        .header("Authorization", "Bearer eaf-local-carol"))
                .andExpect(status().isNotFound());
        mvc.perform(get("/api/v1/workspaces/10000000-0000-4000-8000-000000000003/agents")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isNotFound());

        var tenantBTask = mvc.perform(post("/api/v1/workspaces/10000000-0000-4000-8000-000000000003/tasks")
                        .header("Authorization", "Bearer eaf-local-carol")
                        .header("Idempotency-Key", "p1-matrix-tenant-b")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"agentId\":\"20000000-0000-4000-8000-000000000002\",\"agentVersion\":\"1.0.0\",\"input\":\"Tenant B 矩阵详情\"}"))
                .andExpect(status().isAccepted()).andReturn();
        var tenantBLocation = tenantBTask.getResponse().getHeader("Location");
        String tenantBStatus = null;
        for (var i = 0; i < 40; i++) {
            var body = mvc.perform(get(tenantBLocation).header("Authorization", "Bearer eaf-local-carol"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
            tenantBStatus = mapper.readTree(body).path("status").asText();
            if ("SUCCEEDED".equals(tenantBStatus)) break;
            Thread.sleep(100);
        }
        assertThat(tenantBStatus).isEqualTo("SUCCEEDED");
        mvc.perform(get(tenantBLocation).header("Authorization", "Bearer eaf-local-carol"))
                .andExpect(jsonPath("$.steps.length()").value(3));
        mvc.perform(get(tenantBLocation).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isNotFound());
        mvc.perform(get(tenantBLocation + "/replay").header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isNotFound());
        mvc.perform(post("/api/v1/workspaces/10000000-0000-4000-8000-000000000003/evaluations/p1")
                        .header("Authorization", "Bearer eaf-local-carol"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.passed").value(18))
                .andExpect(jsonPath("$.total").value(18))
                .andExpect(jsonPath("$.sampleCount").value(3));
    }

    @Test
    @Order(4)
    void auditRuntimeRoleCannotMutateHistory() {
        var jdbc = new JdbcTemplate(dataSource);
        var fact = new AuditFact("p1-audit-idempotency", UUID.fromString("70000000-0000-4000-8000-000000000001"),
                UUID.fromString("10000000-0000-4000-8000-000000000001"), null, null, "P1_AUDIT_TEST", "ACCEPTED", "{}", "trace-p1-audit");
        audit.append(fact);
        audit.append(fact);
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class, fact.factKey())).isEqualTo(1);
        assertThat(jdbc.queryForObject("select tableowner from pg_tables where schemaname = 'audit' and tablename = 'audit_event'", String.class))
                .isEqualTo("eaf_audit_owner");
        assertThat(jdbc.queryForObject("select has_table_privilege('eaf_audit_runtime', 'audit.audit_event', 'SELECT')", Boolean.class)).isTrue();
        assertThat(jdbc.queryForObject("select has_table_privilege('eaf_audit_runtime', 'audit.audit_event', 'INSERT')", Boolean.class)).isTrue();
        assertThat(jdbc.queryForObject("select has_table_privilege('eaf_audit_runtime', 'audit.audit_event', 'UPDATE')", Boolean.class)).isFalse();
        assertThat(jdbc.queryForObject("select has_table_privilege('eaf_audit_runtime', 'audit.audit_event', 'DELETE')", Boolean.class)).isFalse();

        assertThat(jdbc.execute((ConnectionCallback<Boolean>) connection -> {
            try (var statement = connection.createStatement()) {
                statement.execute("set role eaf_audit_runtime");
                try {
                    statement.executeUpdate("update audit.audit_event set result = 'TAMPERED' where false");
                    return false;
                } catch (SQLException expected) {
                    return true;
                } finally {
                    statement.execute("reset role");
                }
            }
        })).isTrue();
    }

    @Test
    @Order(5)
    void publishedAgentAndPromptContentCannotBeMutated() {
        var jdbc = new JdbcTemplate(dataSource);
        assertThatThrownBy(() -> jdbc.update("update prompt.version set user_template = ? where id = ? and workspace_id = ? and asset_version = '1.0.0'",
                "tampered", UUID.fromString("21000000-0000-4000-8000-000000000001"), UUID.fromString("10000000-0000-4000-8000-000000000001")))
                .hasMessageContaining("published prompt content is immutable");
        assertThatThrownBy(() -> jdbc.update("update agent.version set prompt_version = ? where id = ? and workspace_id = ? and asset_version = '1.0.0'",
                "9.9.9", UUID.fromString("20000000-0000-4000-8000-000000000001"), UUID.fromString("10000000-0000-4000-8000-000000000001")))
                .hasMessageContaining("published agent content is immutable");
    }

    @Test
    @Order(6)
    void missingInvalidAndUnreachableDatabaseConfigurationPreventStartup() {
        assertThatThrownBy(() -> startWithDatabase("", "", ""))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> startWithDatabase(postgres.getJdbcUrl(), postgres.getUsername(), "wrong-password"))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> startWithDatabase("jdbc:postgresql://127.0.0.1:1/eaf", "eaf", "not-a-secret"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    @Order(7)
    void databaseOutageChangesReadinessButNotLiveness() throws Exception {
        postgres.getDockerClient().stopContainerCmd(postgres.getContainerId()).exec();
        try {
            mvc.perform(get("/actuator/health/readiness")).andExpect(status().isServiceUnavailable());
            mvc.perform(get("/actuator/health/liveness")).andExpect(status().isOk());
        } finally {
            postgres.getDockerClient().startContainerCmd(postgres.getContainerId()).exec();
        }
    }

    private void startWithDatabase(String url, String username, String password) {
        new SpringApplicationBuilder(EafApplication.class)
                .web(WebApplicationType.SERVLET)
                .properties(
                        "EAF_DB_URL=" + url,
                        "EAF_DB_USERNAME=" + username,
                        "EAF_DB_PASSWORD=" + password,
                        "spring.datasource.hikari.connection-timeout=1000")
                .run("--server.port=0");
    }
}
// 本文件负责实现 EAF 的 EafApplicationTest.java 相关代码。
