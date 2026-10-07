package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.model.api.ModelBillingProfile;
import io.eaf.model.api.TypedDecisionGateway;
import io.eaf.model.api.TypedDecisionRequest;
import io.eaf.model.api.TypedDecisionResult;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.usage.api.UsageRecorder;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
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
@SpringBootTest(properties = {
        "eaf.task.dispatcher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.remote-poller-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false",
        "eaf.memory.outbox-publisher-enabled=false"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class P8JevRuntimeTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID TENANT = UUID.fromString("70000000-0000-4000-8000-000000000001");
    private static final UUID WORKSPACE = UUID.fromString("10000000-0000-4000-8000-000000000001");
    private static final UUID ACTOR = UUID.fromString("80000000-0000-4000-8000-000000000001");
    private static final UUID AGENT = UUID.fromString("20000000-0000-4000-8000-000000000001");
    private static final DecisionFixture DECISIONS = new DecisionFixture();

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
        registry.add("eaf.model.live.fee-cap", () -> "1.00");
        registry.add("eaf.model.live.fee-currency", () -> "USD");
    }

    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired RuntimeQuery runtimeQuery;
    @Autowired UsageRecorder usage;
    @Autowired DataSource dataSource;
    @Autowired ObjectMapper json;

    @BeforeEach
    void resetFixture() { DECISIONS.reset(); }

    @Test
    void customerRiskTaskUsesTypedChoiceForRiskAndRecordsDistributionAndUsage() throws Exception {
        var task = run("合成客户摘要：客户出现重大投诉，续约中断。", "p8-high-risk");
        var result = json.readTree(task.resultJson());
        var decisionUsage = usage.findForTask(TENANT, WORKSPACE, task.id()).stream()
                .filter(record -> "DECISION".equals(record.callType())).toList();

        assertThat(task.status().name()).isEqualTo("SUCCEEDED");
        assertThat(result.path("riskLevel").asText()).isEqualTo("HIGH");
        assertThat(result.path("riskDecision").path("provider").asText()).isEqualTo("typesafe");
        assertThat(result.path("riskDecision").path("choice").asText()).isEqualTo("HIGH");
        assertThat(result.path("riskDecision").path("probabilities").path("HIGH").asDouble()).isEqualTo(0.8);
        assertThat(decisionUsage).hasSize(1);
        assertThat(decisionUsage.getFirst().model()).isEqualTo("jev-1.13.0");
        assertThat(decisionUsage.getFirst().inputTokens()).isEqualTo(12);
        assertThat(decisionUsage.getFirst().estimatedCost()).isNotNull();
        assertThat(runtimeQuery.steps(TENANT, task.id()).stream().map(step -> step.type()))
                .contains("DECISION_REQUESTED", "DECISION_RESPONSE");
        assertThat(DECISIONS.callCount()).isEqualTo(1);
    }

    @Test
    void insufficientSyntheticInformationIsReturnedAsUnknown() throws Exception {
        var task = run("合成客户摘要：近期联系和续约状态缺失，现有记录相互冲突。", "p8-unknown");
        var result = json.readTree(task.resultJson());

        assertThat(task.status().name()).isEqualTo("SUCCEEDED");
        assertThat(result.path("riskLevel").asText()).isEqualTo("UNKNOWN");
        assertThat(result.path("riskDecision").path("choice").asText()).isEqualTo("UNKNOWN");
        assertThat(DECISIONS.callCount()).isEqualTo(1);
    }

    @Test
    void exhaustedTaskTokenBudgetBlocksJevBeforeProviderCall() {
        var task = create("合成客户摘要：出现重大投诉。", "p8-budget");
        var jdbc = new JdbcTemplate(dataSource);
        jdbc.update("update task.budget_scope set token_used = max_tokens where root_task_id = ?", task.id());

        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(task.id());
        tasks.complete(work, runtime.run(work));
        var result = tasks.get(actor(), WORKSPACE, task.id());

        assertThat(result.status().name()).isEqualTo("FAILED");
        assertThat(result.errorCode()).isEqualTo("BUDGET_EXCEEDED");
        assertThat(DECISIONS.callCount()).isZero();
        assertThat(usage.findForTask(TENANT, WORKSPACE, task.id())).isEmpty();
    }

    @Test
    void taskAuthorizationIsCheckedBeforeTypedDecision() {
        var unauthorized = new ActorContext(UUID.fromString("80000000-0000-4000-8000-000000000099"),
                TENANT, ActorType.HUMAN, Set.of());
        var command = new CreateTaskCommand(unauthorized, WORKSPACE, AGENT, "1.0.0", "合成客户摘要",
                null, null, "p8-unauthorized", "trace-p8-unauthorized", "USER");

        assertThatThrownBy(() -> tasks.create(command)).isInstanceOf(EafException.class);
        assertThat(DECISIONS.callCount()).isZero();
    }

    private io.eaf.task.api.TaskSnapshot run(String input, String key) {
        var created = create(input, key);
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(created.id());
        tasks.complete(work, runtime.run(work));
        return tasks.get(actor(), WORKSPACE, created.id());
    }

    private io.eaf.task.api.TaskSnapshot create(String input, String key) {
        return tasks.create(new CreateTaskCommand(actor(), WORKSPACE, AGENT, "1.0.0", input,
                null, null, key, "trace-" + key, "USER"));
    }

    private ActorContext actor() {
        return new ActorContext(ACTOR, TENANT, ActorType.HUMAN, Set.of("task:create", "task:read"));
    }

    @TestConfiguration
    static class FixtureConfiguration {
        @Bean
        @Primary
        TypedDecisionGateway p8DecisionFixture() { return DECISIONS; }
    }

    private static final class DecisionFixture implements TypedDecisionGateway {
        private final AtomicInteger calls = new AtomicInteger();

        @Override public boolean enabled() { return true; }
        @Override public boolean external() { return true; }
        @Override public ModelBillingProfile billingProfile() {
            return new ModelBillingProfile("typesafe", "jev-1.13.0", "DECISION");
        }
        @Override public TypedDecisionResult decide(TypedDecisionRequest request) {
            calls.incrementAndGet();
            var choice = request.state().contains("缺失") || request.state().contains("冲突") ? "UNKNOWN" : "HIGH";
            var probabilities = new LinkedHashMap<String, BigDecimal>();
            probabilities.put("LOW", new BigDecimal("0.05"));
            probabilities.put("MEDIUM", new BigDecimal("0.10"));
            probabilities.put("HIGH", new BigDecimal(choice.equals("HIGH") ? "0.80" : "0.05"));
            probabilities.put("UNKNOWN", new BigDecimal(choice.equals("UNKNOWN") ? "0.80" : "0.05"));
            return new TypedDecisionResult("typesafe", "jev-1.13.0", choice, probabilities, 12, 2, "KNOWN");
        }
        @Override public int callCount() { return calls.get(); }
        void reset() { calls.set(0); }
    }
}
