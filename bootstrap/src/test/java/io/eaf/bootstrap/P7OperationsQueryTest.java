package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.usage.api.UsageRecord;
import io.eaf.usage.api.UsageRecorder;
import io.eaf.workflow.api.CreateWorkflowInstanceCommand;
import io.eaf.workflow.api.WorkflowService;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 使用隔离数据库验证运维 Task 列表的管理员边界、分页和字段最小化。 */
@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false", "eaf.knowledge.outbox-publisher-enabled=false",
        "eaf.memory.outbox-publisher-enabled=false", "eaf.security.mode=disabled"})
@AutoConfigureMockMvc
class P7OperationsQueryTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @Autowired private MockMvc mvc;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private ObjectMapper json;
    @Autowired private WorkflowService workflows;
    @Autowired private UsageRecorder usage;

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @BeforeEach
    void prepareOperatorWithoutTaskRead() {
        jdbc.update("update \"identity\".subject set status = 'ACTIVE' where id in (?, ?)", Ids.ALICE, Ids.BOB);
        jdbc.update("insert into organization.member(tenant_id, subject_id, status) values (?, ?, 'ACTIVE') "
                        + "on conflict (tenant_id, subject_id) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.BOB);
        jdbc.update("delete from workspace.\"grant\" where workspace_id = ? and actor_id = ?",
                Ids.WORKSPACE_A, Ids.BOB);
        grant(Ids.BOB, "workspace:members:manage");
    }

    @Test
    void rejectsOrdinaryAndCrossTenantQueriesAndHidesUnprivilegedTaskDomain() throws Exception {
        mvc.perform(get(tasksUrl()).with(authentication(principalAuthentication(Ids.ALICE))))
                .andExpect(status().isForbidden());
        mvc.perform(get("/api/v1/workspaces/" + Ids.WORKSPACE_B + "/operations/tasks")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isNotFound());

        mvc.perform(get(tasksUrl()).with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.visible").value(false))
                .andExpect(jsonPath("$.items.length()").value(0))
                .andExpect(jsonPath("$.totalSize").value(org.hamcrest.Matchers.nullValue()));
        mvc.perform(get("/api/v1/workspaces/" + Ids.WORKSPACE_A + "/operations/workflows")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk()).andExpect(jsonPath("$.visible").value(false))
                .andExpect(jsonPath("$.totalSize").value(org.hamcrest.Matchers.nullValue()));
        mvc.perform(get("/api/v1/workspaces/" + Ids.WORKSPACE_A + "/operations/usage")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk()).andExpect(jsonPath("$.visible").value(false))
                .andExpect(jsonPath("$.items.length()").value(0));
        grant(Ids.BOB, "learning:read");
        mvc.perform(outboxRequest("learning", Ids.BOB, "PENDING"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.visible").value(false))
                .andExpect(jsonPath("$.deliveryState").value(org.hamcrest.Matchers.nullValue()));
    }

    @Test
    void pagesSafeTaskSummariesByStatusWithoutReturningBusinessText() throws Exception {
        var firstTask = createTask("customer narrative alpha must stay private");
        var secondTask = createTask("customer narrative beta must stay private");
        grant(Ids.BOB, "task:read");

        var firstPage = mvc.perform(get(tasksUrl()).param("status", "QUEUED").param("limit", "1")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.visible").value(true))
                .andExpect(jsonPath("$.totalSize").value(2))
                .andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.items[0].status").value("QUEUED"))
                .andExpect(jsonPath("$.items[0].externalEffectStatus").value("NONE"))
                .andReturn().getResponse().getContentAsString();
        assertThat(firstPage).doesNotContain("customer narrative", "\"input\"", "\"result\"", "errorDetail");
        var cursor = json.readTree(firstPage).path("nextCursor");
        assertThat(cursor.isObject()).isTrue();
        var firstPageTaskId = UUID.fromString(json.readTree(firstPage).path("items").get(0).path("taskId").asText());
        assertThat(Set.of(firstTask, secondTask)).contains(firstPageTaskId);

        mvc.perform(get(tasksUrl()).param("status", "QUEUED").param("limit", "1")
                        .param("cursorUpdatedAt", cursor.path("updatedAt").asText())
                        .param("cursorTaskId", cursor.path("taskId").asText())
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.visible").value(true))
                .andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.items[0].taskId").value(
                        firstPageTaskId.equals(firstTask) ? secondTask.toString() : firstTask.toString()));
    }

    @Test
    void pagesWorkflowAndExecutionOwnerSummariesWithoutTheirPayloads() throws Exception {
        var workflowId = UUID.fromString("58000000-0000-4000-8000-000000000001");
        var workflowDraft = workflows.get(new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of()),
                Ids.WORKSPACE_A, workflowId, "1.0.0");
        if ("DRAFT".equals(workflowDraft.status()))
            workflowDraft = workflows.publish(new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of()),
                    Ids.WORKSPACE_A, workflowId, "1.0.0", workflowDraft.rowVersion());
        var workflow = workflows.createInstance(new CreateWorkflowInstanceCommand(
                new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of()), Ids.WORKSPACE_A,
                workflowId, workflowDraft.version(), "{\"customerId\":\"private-workflow-payload\"}",
                "p7-operations-workflow-" + UUID.randomUUID(), "USER"));

        var executionId = UUID.randomUUID();
        jdbc.update("insert into execution.execution(id, tenant_id, workspace_id, actor_id, task_id, attempt, agent_id, agent_version, tool_name, tool_version, arguments_json, request_hash, idempotency_key, status, policy_version, operation_id, error_code, error_detail, created_at, ended_at) "
                        + "values (?, ?, ?, ?, ?, 1, ?, '1.0.0', 'crm.followup.create', '1.0.0', ?::jsonb, ?, ?, 'UNKNOWN', 'P7-TEST', ?, 'WRITE_RESULT_UNKNOWN', 'private execution detail', now(), now())",
                executionId, Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE, UUID.randomUUID(), Ids.AGENT_RISK,
                "{\"summary\":\"private-execution-payload\"}", "a".repeat(64), "p7-operations-execution-" + executionId,
                UUID.randomUUID());
        grant(Ids.BOB, "workflow:read");
        grant(Ids.BOB, "execution:read");
        grant(Ids.BOB, "usage:read");

        var workflowPage = mvc.perform(get("/api/v1/workspaces/" + Ids.WORKSPACE_A + "/operations/workflows")
                        .param("status", "QUEUED").with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk()).andExpect(jsonPath("$.visible").value(true))
                .andExpect(jsonPath("$.items[0].instanceId").value(workflow.id().toString()))
                .andReturn().getResponse().getContentAsString();
        assertThat(workflowPage).doesNotContain("private-workflow-payload", "inputJson", "resultJson", "definitionSnapshot");

        var executionPage = mvc.perform(get("/api/v1/workspaces/" + Ids.WORKSPACE_A + "/operations/executions")
                        .param("status", "UNKNOWN").with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk()).andExpect(jsonPath("$.visible").value(true))
                .andExpect(jsonPath("$.items[0].executionId").value(executionId.toString()))
                .andExpect(jsonPath("$.items[0].errorCode").value("WRITE_RESULT_UNKNOWN"))
                .andReturn().getResponse().getContentAsString();
        assertThat(executionPage).doesNotContain("private-execution-payload", "private execution detail", "argumentsJson");

        var usageId = UUID.randomUUID();
        var startedAt = Instant.now();
        usage.record(new UsageRecord(Ids.TENANT_A, Ids.WORKSPACE_A, UUID.randomUUID(), UUID.randomUUID(), "USER",
                "fixture-provider", "fixture-model", null, null, "UNKNOWN", 0, "UNKNOWN", "USAGE_PENDING",
                startedAt, null, 1, "p7-operations-usage-" + usageId,
                "CHAT", "TASK", UUID.randomUUID()));
        var usagePage = mvc.perform(get("/api/v1/workspaces/" + Ids.WORKSPACE_A + "/operations/usage")
                        .param("status", "UNKNOWN").param("costStatus", "UNKNOWN_PRICE")
                        .with(authentication(principalAuthentication(Ids.BOB))))
                .andExpect(status().isOk()).andExpect(jsonPath("$.visible").value(true))
                .andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.items[0].costStatus").value("UNKNOWN_PRICE"))
                .andReturn().getResponse().getContentAsString();
        assertThat(usagePage).doesNotContain("p7-operations-usage-", "callKey", "priceVersion");
    }

    @Test
    void queriesOutboxesThroughOwnersAndMarksLearningPendingAsNotConfigured() throws Exception {
        for (var action : List.of("approval:read", "execution:read", "knowledge:read", "memory:read", "learning:read", "task:read"))
            grant(Ids.BOB, action);
        var approvalEvent = UUID.randomUUID();
        jdbc.update("insert into approval.outbox(event_id, tenant_id, workspace_id, approval_id, event_type, payload_json, status, created_at) "
                        + "values (?, ?, ?, ?, 'eaf.approval.decided.v1', '{\"private\":\"approval payload\"}'::jsonb, 'PENDING', now())",
                approvalEvent, Ids.TENANT_A, Ids.WORKSPACE_A, UUID.randomUUID());
        var executionEvent = UUID.randomUUID();
        jdbc.update("insert into execution.outbox(event_id, tenant_id, workspace_id, execution_id, event_type, aggregate_version, payload_json, status, created_at) "
                        + "values (?, ?, ?, ?, 'eaf.execution.uncertain.v1', 1, '{\"private\":\"execution payload\"}'::jsonb, 'FAILED', now())",
                executionEvent, Ids.TENANT_A, Ids.WORKSPACE_A, UUID.randomUUID());
        var feedbackId = UUID.randomUUID();
        var feedbackTask = UUID.randomUUID();
        jdbc.update("insert into learning.feedback(id, tenant_id, workspace_id, task_id, task_attempt, task_status, actor_id, source_type, correction, evidence, source_snapshot, idempotency_key, request_hash, created_at) "
                        + "values (?, ?, ?, ?, 1, 'SUCCEEDED', ?, 'USER', 'private correction', 'private evidence', '{}'::jsonb, ?, ?, now())",
                feedbackId, Ids.TENANT_A, Ids.WORKSPACE_A, feedbackTask, Ids.ALICE,
                "p7-operations-feedback-" + feedbackId, "b".repeat(64));
        jdbc.update("insert into learning.outbox(event_id, event_type, payload, status, created_at) "
                        + "values (?, 'eaf.feedback.recorded.v1', ?::jsonb, 'PENDING', now())",
                feedbackId, "{\"feedbackId\":\"" + feedbackId + "\"}");

        var approval = mvc.perform(outboxRequest("approval", Ids.BOB, "PENDING"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.visible").value(true))
                .andExpect(jsonPath("$.items[0].eventId").value(approvalEvent.toString()))
                .andReturn().getResponse().getContentAsString();
        assertThat(approval).doesNotContain("approval payload");
        var execution = mvc.perform(outboxRequest("execution", Ids.BOB, "FAILED"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items[0].eventId").value(executionEvent.toString()))
                .andReturn().getResponse().getContentAsString();
        assertThat(execution).doesNotContain("execution payload");

        for (var owner : List.of("knowledge", "memory"))
            mvc.perform(outboxRequest(owner, Ids.BOB, "PENDING"))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.visible").value(true));

        var learning = mvc.perform(outboxRequest("learning", Ids.BOB, "PENDING"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.visible").value(true))
                .andExpect(jsonPath("$.deliveryState").value("NOT_CONFIGURED"))
                .andExpect(jsonPath("$.pendingIsFailure").value(false))
                .andExpect(jsonPath("$.items[0].eventId").value(feedbackId.toString()))
                .andExpect(jsonPath("$.items[0].resourceId").value(feedbackTask.toString()))
                .andReturn().getResponse().getContentAsString();
        assertThat(learning).doesNotContain("private correction", "private evidence", "feedbackId");
    }

    private UUID createTask(String input) throws Exception {
        var body = "{\"agentId\":\"" + Ids.AGENT_RISK + "\",\"agentVersion\":\"1.1.0\",\"input\":\""
                + input + "\"}";
        var response = mvc.perform(post("/api/v1/workspaces/" + Ids.WORKSPACE_A + "/tasks")
                        .header("Idempotency-Key", "p7-operations-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON).content(body)
                        .with(authentication(principalAuthentication(Ids.ALICE))))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString();
        return UUID.fromString(json.readTree(response).path("id").asText());
    }

    private void grant(UUID subjectId, String action) {
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) "
                        + "do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, subjectId, action);
    }

    private static String tasksUrl() {
        return "/api/v1/workspaces/" + Ids.WORKSPACE_A + "/operations/tasks";
    }

    private static org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder outboxRequest(
            String owner, UUID operator, String status) {
        return get("/api/v1/workspaces/" + Ids.WORKSPACE_A + "/operations/outboxes/" + owner)
                .param("status", status).with(authentication(principalAuthentication(operator)));
    }

    private static UsernamePasswordAuthenticationToken principalAuthentication(UUID subjectId) {
        return new UsernamePasswordAuthenticationToken(new ActorContext(subjectId, Ids.TENANT_A,
                ActorType.HUMAN, Set.of()), null, List.of());
    }
}
