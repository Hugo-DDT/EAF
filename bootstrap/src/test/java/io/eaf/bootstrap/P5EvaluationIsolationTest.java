package io.eaf.bootstrap;

import io.eaf.execution.api.ExecutionCommand;
import io.eaf.execution.api.ExecutionService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskService;
import java.util.Set;
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
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false"})
// 验证 EVALUATION 来源从数据库传播到 Policy 和 Connector，确保评测不碰真实连接或外部写入。
class P5EvaluationIsolationTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired TaskService tasks;
    @Autowired ExecutionService executions;
    @Autowired JdbcTemplate jdbc;

    @Test
    void evaluationReadsSyntheticDataAndCannotSubmitWrites() {
        jdbc.update("update connector.instance set base_url = 'http://127.0.0.1:1' where tenant_id = ? and workspace_id = ?",
                Ids.TENANT_A, Ids.WORKSPACE_A);

        var readTask = evaluationTask("candidate comparison reads synthetic customer data", "p5-eval-read");
        var readWork = tasks.claim(readTask.id()).orElseThrow();
        var read = executions.submit(new ExecutionCommand(ALICE, Ids.WORKSPACE_A, readTask.id(), readWork.attempt(),
                Ids.AGENT_RISK, "2.0.0", "crm.customer.query", "1.0.0", "{\"customerId\":\"customer-001\"}",
                "p5-eval-read-execution", "trace-p5-eval-read"));
        assertThat(read.status()).isEqualTo("SUCCEEDED");
        assertThat(read.resultJson()).contains("evaluation-sandbox:customer-001");

        var writeTask = evaluationTask("candidate comparison must not create a followup", "p5-eval-write");
        var writeWork = tasks.claim(writeTask.id()).orElseThrow();
        var denied = executions.submit(new ExecutionCommand(ALICE, Ids.WORKSPACE_A, writeTask.id(), writeWork.attempt(),
                Ids.AGENT_RISK, "2.0.0", "crm.followup.create", "1.0.0",
                "{\"customerId\":\"customer-001\",\"summary\":\"test\"}",
                "p5-eval-write-execution", "trace-p5-eval-write"));
        assertThat(denied.status()).isEqualTo("DENIED");
        assertThat(denied.errorDetail()).isEqualTo("EVALUATION_WRITE_DENIED");
        assertThat(jdbc.queryForObject("select count(*) from approval.request where task_id = ?", Integer.class, writeTask.id())).isZero();
        assertThat(jdbc.queryForObject("select count(*) from execution.execution where task_id = ? and status in ('AWAITING_APPROVAL','SUCCEEDED','UNKNOWN')",
                Integer.class, writeTask.id())).isZero();
    }

    private io.eaf.task.api.TaskSnapshot evaluationTask(String input, String key) {
        var task = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.0.0", input,
                null, null, key, "trace-" + key, "EVALUATION"));
        return task;
    }
}
