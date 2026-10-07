package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.model.api.ModelGateway;
import io.eaf.shared.ActorContext;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false", "eaf.memory.outbox-publisher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false", "eaf.model.mode=deterministic",
        "eaf.model.jev.mode=deterministic", "eaf.model.jev.evidence-mode=deterministic"})
@AutoConfigureMockMvc
class P12CustomerFollowupTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String WORKSPACE = "10000000-0000-4000-8000-000000000001";
    private static final String AUTH = "Bearer eaf-local-alice";
    private static final UUID TENANT_ID = UUID.fromString("70000000-0000-4000-8000-000000000001");
    private static final UUID ALICE_ID = UUID.fromString("80000000-0000-4000-8000-000000000001");

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;
    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired JdbcTemplate jdbc;
    @Autowired ModelGateway model;

    @Test
    void selectedTeamResultIsSnapshottedAndRevocationStopsTheNextRun() throws Exception {
        assertThat(jdbc.queryForObject("select count(*) from capability.version where capability_id = ? and asset_version = '1.2.0' and status = 'PUBLISHED'",
                Integer.class, UUID.fromString("54000000-0000-4000-8000-00000000000d"))).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from workflow.version where workflow_id = ? and asset_version = '1.0.0' and status = 'PUBLISHED'",
                Integer.class, UUID.fromString("58000000-0000-4000-8000-00000000000d"))).isEqualTo(1);
        assertThat(jdbc.queryForObject("select content_hash from workflow.version where workflow_id = ? and asset_version = '1.0.0'",
                String.class, UUID.fromString("58000000-0000-4000-8000-00000000000d")))
                .isEqualTo("2cbf7f71cf9e13bda38ac598418e9bc43ae596910305bd1f5f6ce372931c2ae4");
        assertThat(jdbc.queryForObject("select content_hash from workflow.capability_dependency where workflow_id = ? and workflow_version = '1.0.0'",
                String.class, UUID.fromString("58000000-0000-4000-8000-00000000000d")))
                .isEqualTo("7809485e7d5de0344e26ec7c1d9887b6f8b13eacbce9ac267904d67dfeb700dd");
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values "
                        + "(?, ?, ?, 'customer-followup:read', 'ACTIVE'), (?, ?, ?, 'customer-followup:write', 'ACTIVE'), "
                        + "(?, ?, ?, 'crm:followup:result', 'ACTIVE'), (?, ?, ?, 'crm:customer:read', 'ACTIVE') on conflict do nothing",
                TENANT_ID, UUID.fromString(WORKSPACE), ALICE_ID,
                TENANT_ID, UUID.fromString(WORKSPACE), ALICE_ID,
                TENANT_ID, UUID.fromString(WORKSPACE), ALICE_ID,
                TENANT_ID, UUID.fromString(WORKSPACE), ALICE_ID);
        jdbc.update("insert into policy.customer_grant(tenant_id, workspace_id, actor_id, customer_id, status) values (?, ?, ?, ?, 'ACTIVE') "
                        + "on conflict (tenant_id, workspace_id, actor_id, customer_id) do update set status = 'ACTIVE'",
                TENANT_ID, UUID.fromString(WORKSPACE), ALICE_ID, "synthetic-customer-001");

        var conversation = createConversation();
        mvc.perform(get(path("/conversations/" + conversation)).header("Authorization", AUTH))
                .andExpect(status().isOk()).andExpect(jsonPath("$.capabilityVersion").value("1.2.0"));
        var sourceTurn = send(conversation, Map.of("input", "请分析合成客户的续约沟通，并指出待核实事项。"), "p12-source-turn");
        complete(sourceTurn.path("taskId").asText());
        var sourceTaskId = UUID.fromString(sourceTurn.path("taskId").asText());
        var sourceTaskVersion = tasks.get(actor(), UUID.fromString(WORKSPACE), sourceTaskId).version();
        var resultId = UUID.randomUUID();
        var followupId = UUID.randomUUID();
        jdbc.update("insert into task.customer_followup(id, tenant_id, workspace_id, customer_id, creator_id, assignee_id, summary, "
                        + "business_status, source_conversation_id, source_task_id, source_task_version, source_brief_revision, last_result_no) "
                        + "values (?, ?, ?, ?, ?, ?, ?, 'IN_PROGRESS', ?, ?, ?, 0, 1)",
                followupId, TENANT_ID, UUID.fromString(WORKSPACE), "synthetic-customer-001", ALICE_ID, ALICE_ID,
                "经确认的团队共享续约跟进摘要。", UUID.fromString(conversation), sourceTaskId, sourceTaskVersion);
        jdbc.update("insert into task.customer_followup_result(id, tenant_id, workspace_id, followup_id, result_no, recorded_by, "
                        + "outcome_code, summary, next_action, disposition) values (?, ?, ?, ?, 1, ?, 'CONTACTED', ?, null, 'CONTINUE')",
                resultId, TENANT_ID, UUID.fromString(WORKSPACE), followupId, ALICE_ID,
                "成员已联系客户，客户要求下周确认续约时间。");

        var withResult = send(conversation, Map.of("input", "结合团队处理进度继续分析下一步。",
                "followupResultIds", List.of(resultId.toString())), "p12-result-turn");
        complete(withResult.path("taskId").asText());
        var completed = task(withResult.path("taskId").asText());
        assertThat(completed.path("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(completed.path("result").path("followupUsage").path("included").get(0).path("summary").asText())
                .isEqualTo("成员已联系客户，客户要求下周确认续约时间。");
        assertThat(completed.path("result").path("followupUsage").path("included").get(0).path("syncStatus").asText())
                .isEqualTo("NOT_REQUESTED");

        var revokedTurn = send(conversation, Map.of("input", "请根据团队的新进展再评估一次。",
                "followupResultIds", List.of(resultId.toString())), "p12-revoked-result-turn");
        jdbc.update("update policy.customer_grant set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and actor_id = ? and customer_id = ?",
                TENANT_ID, UUID.fromString(WORKSPACE), ALICE_ID, "synthetic-customer-001");
        var callsBeforeRun = model.callCount();
        complete(revokedTurn.path("taskId").asText());
        var rejected = task(revokedTurn.path("taskId").asText());
        assertThat(rejected.path("status").asText()).isEqualTo("FAILED");
        assertThat(rejected.path("errorCode").asText()).isEqualTo("CONVERSATION_CONTEXT_UNAVAILABLE");
        assertThat(model.callCount()).isEqualTo(callsBeforeRun);
    }

    private ActorContext actor() {
        return new ActorContext(ALICE_ID, TENANT_ID, io.eaf.shared.ActorType.HUMAN, java.util.Set.of());
    }

    private String createConversation() throws Exception {
        var body = Map.of("mode", "CUSTOMER_ASSISTANT", "title", "团队结果复盘", "customerId", "synthetic-customer-001");
        var response = mvc.perform(post(path("/conversations")).header("Authorization", AUTH)
                        .header("Idempotency-Key", "p12-session").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(body)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        return json.readTree(response).path("id").asText();
    }

    private JsonNode send(String conversation, Map<String, Object> body, String key) throws Exception {
        return json.readTree(mvc.perform(post(path("/conversations/" + conversation + "/turns"))
                        .header("Authorization", AUTH).header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(body)))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
    }

    private void complete(String taskId) {
        var item = tasks.claimOne().orElseThrow();
        assertThat(item.id()).isEqualTo(UUID.fromString(taskId));
        tasks.complete(item, runtime.run(item));
    }

    private JsonNode task(String id) throws Exception {
        return json.readTree(mvc.perform(get(path("/tasks/" + id)).header("Authorization", AUTH))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
    }

    private String path(String path) { return "/api/v1/workspaces/" + WORKSPACE + path; }
}
