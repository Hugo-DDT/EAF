package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import io.eaf.agentprotocol.A2aProtocolMapping;
import io.eaf.agentprotocol.TaskResponse;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.a2aproject.sdk.spec.TaskState;
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
// PostgreSQL 验证远端登记、可见能力 Card 与固定 A2A 1.0 DTO 映射。
class P6A2aContractTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID WORKSPACE = Ids.WORKSPACE_A;
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired A2aProtocolMapping mapping;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper json;

    @Test
    void registrationCardAndMessageTaskMappingsKeepA2aDescriptiveAndScoped() throws Exception {
        var reviewer = mapping.requireRegisteredReviewer(ALICE, WORKSPACE);
        assertThat(reviewer.registration().ownerId()).isEqualTo(Ids.ALICE);
        assertThat(reviewer.registration().capabilityVersion()).isEqualTo("1.0.0");
        assertThat(reviewer.registration().peerSkillId()).isEqualTo("agent.risk.review");
        assertThat(reviewer.registration().allowedInputFields()).containsExactly("customerId", "riskSummary");
        assertThat(reviewer.registration().allowedOutputFields()).containsExactly("riskLevel", "rationale", "citations");
        assertThat(reviewer.registration().delegationActions()).containsExactly("agent:risk-review");
        assertThat(reviewer.registration().resourceScope()).isEqualTo("OWNER_GRANTED_CUSTOMER");
        assertThat(reviewer.connector().allowedUses()).contains("a2a.send", "a2a.get");

        jdbc.update("update connector.instance set status = 'DISABLED' where id = ?", reviewer.connector().id());
        try {
            assertThatThrownBy(() -> mapping.requireRegisteredReviewer(ALICE, WORKSPACE))
                    .isInstanceOf(EafException.class);
        } finally {
            jdbc.update("update connector.instance set status = 'ACTIVE' where id = ?", reviewer.connector().id());
        }

        var card = mapping.card(ALICE, WORKSPACE);
        // 已发布的面向用户版本会出现在发现信息中；固定的结果写入 Capability 保持内部可用。
        assertThat(card.skills()).extracting("id").containsExactlyInAnyOrder(
                "customer-risk-followup@1.0.0", "customer-risk-followup@1.3.0", "customer-risk-followup@1.4.0",
                "customer-risk-followup@1.5.0", "knowledge-question-answer@1.0.0", "customer-followup-draft@1.0.0",
                "conversational-knowledge-answer@1.0.0", "conversational-knowledge-answer@1.1.0",
                "conversational-customer-assistant@1.0.0", "conversational-customer-assistant@1.1.0",
                "conversational-customer-assistant@1.2.0", "experience-draft-assistant@1.0.0");
        assertThat(card.skills()).extracting("id").doesNotContain("p12-customer-result-sync@1.0.0");
        assertThat(card.capabilities().streaming()).isFalse();
        assertThat(card.capabilities().pushNotifications()).isFalse();
        var cardJson = new Gson().toJsonTree(card);
        assertThat(cardJson.toString()).contains("JSONRPC", "1.0", "customer-risk-followup@1.0.0")
                .doesNotContain("54000000-0000-4000-8000-000000000001", "tenantId", "workspaceId");
        assertThat(mapping.publicCard().skills()).isEmpty();

        var parsed = mapping.parseUserMessage(json.readTree(""
                + "{\"messageId\":\"m-1\",\"role\":\"ROLE_USER\",\"parts\":[{\"text\":\"Review the approved summary\"}]}") );
        assertThat(parsed.messageId()).isEqualTo("m-1");
        assertThat(parsed.text()).isEqualTo("Review the approved summary");
        for (var invalid : List.of(
                "{\"messageId\":\"m-2\",\"role\":\"ROLE_AGENT\",\"parts\":[{\"text\":\"hi\"}]}",
                "{\"messageId\":\"m-3\",\"role\":\"ROLE_USER\",\"parts\":[{\"url\":\"http://example.invalid/file\"}]}",
                "{\"messageId\":\"m-4\",\"role\":\"ROLE_USER\",\"parts\":[{\"text\":\"hi\"}],\"metadata\":{\"execute\":true}}")) {
            assertThatThrownBy(() -> mapping.parseUserMessage(json.readTree(invalid)))
                    .isInstanceOf(EafException.class);
        }

        var expectedStates = Map.of("QUEUED", TaskState.TASK_STATE_SUBMITTED,
                "RUNNING", TaskState.TASK_STATE_WORKING,
                "WAITING_APPROVAL", TaskState.TASK_STATE_INPUT_REQUIRED,
                "WAITING_VERIFICATION", TaskState.TASK_STATE_WORKING,
                "SUCCEEDED", TaskState.TASK_STATE_COMPLETED,
                "FAILED", TaskState.TASK_STATE_FAILED,
                "TIMED_OUT", TaskState.TASK_STATE_FAILED,
                "CANCELLED", TaskState.TASK_STATE_CANCELED);
        for (var entry : expectedStates.entrySet()) {
            var id = UUID.randomUUID();
            var task = mapping.task(new TaskResponse(id, entry.getKey(), 1, 1, "trace", null, null, null,
                    "input", json.readTree("{\"riskLevel\":\"LOW\"}"), null, null, "USER", Instant.now(),
                    Instant.now(), null, id, null, "A2A", "AGENT", List.of()));
            assertThat(task.id()).isEqualTo(id.toString());
            assertThat(task.contextId()).isEqualTo(id.toString());
            assertThat(task.status().state()).isEqualTo(entry.getValue());
            assertThat(task.artifacts()).hasSize(1);
        }

        jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'agent:read'",
                Ids.TENANT_A, WORKSPACE, Ids.ALICE);
        try {
            assertThatThrownBy(() -> mapping.requireRegisteredReviewer(ALICE, WORKSPACE))
                    .isInstanceOf(EafException.class);
        } finally {
            jdbc.update("update workspace.\"grant\" set status = 'ACTIVE' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'agent:read'",
                    Ids.TENANT_A, WORKSPACE, Ids.ALICE);
        }

        jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'capability:read'",
                Ids.TENANT_A, WORKSPACE, Ids.ALICE);
        try {
            assertThatThrownBy(() -> mapping.card(ALICE, WORKSPACE)).isInstanceOf(EafException.class);
            assertThatThrownBy(() -> mapping.requireRegisteredReviewer(ALICE, WORKSPACE)).isInstanceOf(EafException.class);
        } finally {
            jdbc.update("update workspace.\"grant\" set status = 'ACTIVE' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'capability:read'",
                    Ids.TENANT_A, WORKSPACE, Ids.ALICE);
        }
    }
}
