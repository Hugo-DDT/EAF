package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.Ids;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.infrastructure.WorkflowDispatcher;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false"})
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
// 验收 REST 入口只传递可信 Actor 与白名单业务字段，并保持跨 Workspace 隐藏。
class P6WorkflowTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-000000000001");
    private static final String ALICE_TOKEN = "Bearer eaf-local-alice";

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;
    @Autowired JdbcTemplate jdbc;
    @Autowired WorkflowDispatcher dispatcher;
    @Autowired WorkflowService workflows;

    @Test
    void restPublishesStartsQueriesAndCancelsWithoutExposingWorkflowInternals() throws Exception {
        // 复用已发布 Capability 的固定图定义作为示例；Owner 和当前依赖由服务端校验。
        var alice = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());
        var seed = workflows.get(alice, Ids.WORKSPACE_A, WORKFLOW_ID, "1.0.0");
        var create = json.createObjectNode().put("name", "p6-10-rest-demo-" + UUID.randomUUID())
                .put("description", "REST acceptance")
                .put("version", "1.0.0")
                .put("inputSchema", seed.inputSchema())
                .put("outputSchema", seed.outputSchema())
                .put("entryStepId", seed.entryStepId());
        create.set("steps", json.valueToTree(seed.steps()));
        var createdWorkflow = mvc.perform(post("/api/v1/workspaces/{workspaceId}/workflows", Ids.WORKSPACE_A)
                        .header("Authorization", ALICE_TOKEN).contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(create)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("DRAFT"))
                .andReturn();
        var workflowId = UUID.fromString(json.readTree(createdWorkflow.getResponse().getContentAsString()).path("id").asText());

        // 发布权限和版本 CAS 仍由 Workflow 域校验，REST 客户端只提交预期行版本。
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/workflows/{workflowId}/versions/1.0.0/publish?expectedVersion=1",
                        Ids.WORKSPACE_A, workflowId)
                        .header("Authorization", ALICE_TOKEN))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PUBLISHED"));

        var path = "/api/v1/workspaces/%s/workflows/%s/instances".formatted(Ids.WORKSPACE_A, workflowId);
        mvc.perform(post(path).header("Authorization", ALICE_TOKEN).header("Idempotency-Key", "p6-10-rest-demo")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"version\":\"1.0.0\",\"input\":{\"customerId\":\"C-100\"},\"source\":\"EVALUATION\"}"))
                .andExpect(status().isBadRequest());

        var created = mvc.perform(post(path).header("Authorization", ALICE_TOKEN).header("Idempotency-Key", "p6-10-rest-demo")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"version\":\"1.0.0\",\"input\":{\"customerId\":\"C-100\"}}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andExpect(jsonPath("$.deadlineAt").isNotEmpty())
                .andExpect(jsonPath("$.inputJson").doesNotExist())
                .andExpect(jsonPath("$.resultJson").doesNotExist())
                .andExpect(jsonPath("$.definitionSnapshot").doesNotExist())
                .andReturn();
        var instanceId = UUID.fromString(json.readTree(created.getResponse().getContentAsString()).path("id").asText());

        mvc.perform(post(path).header("Authorization", ALICE_TOKEN).header("Idempotency-Key", "p6-10-rest-demo")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"version\":\"1.0.0\",\"input\":{\"customerId\":\"C-100\"}}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").value(instanceId.toString()));

        var readPath = "/api/v1/workspaces/%s/workflows/%s/instances/%s".formatted(
                Ids.WORKSPACE_A, workflowId, instanceId);
        mvc.perform(get(readPath).header("Authorization", ALICE_TOKEN))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("QUEUED"));
        // Workflow 读取按 Workspace 授权；同 Workspace 获得读取权的成员可读取实例。
        var oldBobReadGrant = jdbc.queryForList("select status from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'workflow:read'",
                String.class, Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB);
        try {
            grant(Ids.WORKSPACE_A, Ids.BOB, "workflow:read");
            mvc.perform(get(readPath).header("Authorization", "Bearer eaf-local-bob"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.id").value(instanceId.toString()));
        } finally {
            restoreGrant(Ids.WORKSPACE_A, Ids.BOB, "workflow:read", oldBobReadGrant);
        }

        var oldReadGrant = jdbc.queryForList("select status from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'workflow:read'",
                String.class, Ids.TENANT_A, Ids.WORKSPACE_A2, Ids.ALICE);
        var oldWriteGrant = jdbc.queryForList("select status from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'workflow:write'",
                String.class, Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        try {
            grant(Ids.WORKSPACE_A2, "workflow:read");
            mvc.perform(get("/api/v1/workspaces/{workspaceId}/workflows/{workflowId}/instances/{instanceId}",
                            Ids.WORKSPACE_A2, workflowId, instanceId)
                            .header("Authorization", ALICE_TOKEN))
                    .andExpect(status().isNotFound());

            grant(Ids.WORKSPACE_A, "workflow:write");
            var cancelling = mvc.perform(post(readPath + "/cancel").header("Authorization", ALICE_TOKEN)
                            .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":1}"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("CANCELLING"))
                    .andReturn();
            dispatcher.dispatchOne();
            mvc.perform(get(readPath).header("Authorization", ALICE_TOKEN))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("CANCELLED"));
            System.out.printf("P6_10_REST_EVIDENCE workflowId=%s workflowVersion=1.0.0 instanceId=%s startStatus=QUEUED cancelStatus=%s finalStatus=CANCELLED childSideEffects=0%n",
                    workflowId, instanceId, json.readTree(cancelling.getResponse().getContentAsString()).path("status").asText());
        } finally {
            restoreGrant(Ids.WORKSPACE_A2, "workflow:read", oldReadGrant);
            restoreGrant(Ids.WORKSPACE_A, "workflow:write", oldWriteGrant);
        }
    }

    // 测试仅临时授予入口所需动作，结束后恢复原授权状态。
    private void grant(UUID workspaceId, String action) {
        grant(workspaceId, Ids.ALICE, action);
    }

    // 测试只临时增加被测主体的单项 Workspace 权限，不改变其他主体的既有授权。
    private void grant(UUID workspaceId, UUID actorId, String action) {
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, workspaceId, actorId, action);
    }

    private void restoreGrant(UUID workspaceId, String action, List<String> previous) {
        restoreGrant(workspaceId, Ids.ALICE, action, previous);
    }

    private void restoreGrant(UUID workspaceId, UUID actorId, String action, List<String> previous) {
        if (previous.isEmpty())
            jdbc.update("delete from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and actor_id = ? and action = ?",
                    Ids.TENANT_A, workspaceId, actorId, action);
        else
            jdbc.update("update workspace.\"grant\" set status = ? where tenant_id = ? and workspace_id = ? and actor_id = ? and action = ?",
                    previous.getFirst(), Ids.TENANT_A, workspaceId, actorId, action);
    }
}
