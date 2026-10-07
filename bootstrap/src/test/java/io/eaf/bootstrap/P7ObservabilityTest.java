package io.eaf.bootstrap;

import io.eaf.observability.api.TraceObservation;
import io.eaf.observability.api.TraceRecorder;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.usage.api.ReserveSpendCommand;
import io.eaf.usage.api.UsageRecorder;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.authentication;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 使用 PostgreSQL 检查指标来源、低基数标签及 Prometheus SERVICE 身份边界。 */
@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false", "eaf.memory.outbox-publisher-enabled=false",
        "eaf.security.mode=disabled"})
@AutoConfigureMockMvc
class P7ObservabilityTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired TraceRecorder traces;
    @Autowired UsageRecorder usage;

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Test
    void scrapeUsesBoundedMetricsAndRequiresServicePrincipal() throws Exception {
        var taskId = UUID.randomUUID();
        var runId = UUID.randomUUID();
        var eventId = UUID.randomUUID();
        traces.record(new TraceObservation("p7-observe-" + UUID.randomUUID(), taskId, runId,
                "model-1", 37, "SUCCEEDED", "UPSTREAM_FAILURE", Instant.now()));
        var refusal = usage.reserveSpend(new ReserveSpendCommand(Ids.TENANT_A, UUID.randomUUID(), "TASK", UUID.randomUUID(),
                "p7-observe-budget-" + UUID.randomUUID(), "fixture", "fixture", "CHAT", 1, BigDecimal.ONE, "USD"));
        assertThat(refusal.allowed()).isFalse();
        // 注入单条失败事件，确认 Outbox 指标只发布域、状态与数量，不公开事件 ID 或 Payload。
        jdbc.update("insert into execution.outbox(event_id, tenant_id, workspace_id, execution_id, event_type, aggregate_version, payload_json, attempts, status, created_at) "
                        + "values (?, ?, ?, ?, 'test.failure.v1', 1, '{}'::jsonb, 1, 'FAILED', now() - interval '2 minutes')",
                eventId, Ids.TENANT_A, UUID.randomUUID(), UUID.randomUUID());

        mvc.perform(get("/actuator/prometheus")).andExpect(status().isUnauthorized());
        mvc.perform(get("/actuator/prometheus").with(authentication(serviceAuthentication(ActorType.HUMAN))))
                .andExpect(status().isForbidden());

        var scrape = mvc.perform(get("/actuator/prometheus").with(authentication(serviceAuthentication(ActorType.SERVICE))))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        assertThat(scrape).contains("eaf_runtime_operations_total{outcome=\"succeeded\",phase=\"model\"} 1.0")
                .contains("eaf_runtime_operation_duration_seconds_count{outcome=\"succeeded\",phase=\"model\"} 1")
                .contains("eaf_execution_unknown ")
                .contains("eaf_workflow_waiting_instances ")
                .contains("eaf_usage_unknown_cost ")
                .contains("eaf_usage_budget_denials_total 1.0")
                .contains("eaf_outbox_events{domain=\"execution\",state=\"FAILED\"} 1.0")
                .contains("eaf_outbox_events{domain=\"approval\",state=\"PENDING\"}")
                .doesNotContain(taskId.toString(), runId.toString(), eventId.toString(), "UPSTREAM_FAILURE");
        mvc.perform(get("/actuator/metrics").with(authentication(serviceAuthentication(ActorType.SERVICE))))
                .andExpect(status().isForbidden());
        assertThat(jdbc.queryForObject("select count(*) from observability.trace_observation where task_id = ?", Long.class, taskId))
                .isEqualTo(1);
    }

    private static UsernamePasswordAuthenticationToken serviceAuthentication(ActorType type) {
        var actor = new ActorContext(UUID.randomUUID(), Ids.TENANT_A, type, Set.of());
        return new UsernamePasswordAuthenticationToken(actor, null, List.of());
    }
}
