package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.context.api.ContextQuery;
import io.eaf.context.api.ContextService;
import io.eaf.memory.api.CreateMemoryCommand;
import io.eaf.memory.api.MemoryService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false"})
@AutoConfigureMockMvc
// 隔离 PostgreSQL 验证混合 Context 的实体边界、预算、历史快照与撤权恢复。
class P5ContextMemoryTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID SEED_ID = UUID.fromString("56000000-0000-4000-8000-000000000001");
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());
    private static final ActorContext BOB = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN, Set.of());

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

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;
    @Autowired JdbcTemplate jdbc;
    @Autowired ContextService contexts;
    @Autowired MemoryService memories;
    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired RuntimeQuery runtimeQuery;

    @Test
    void contextMixesAuthorizedSourcesAndReusesMemorySnapshotAfterAccessChanges() throws Exception {
        var seed = memories.get(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0");
        var published = memories.publish(ALICE, Ids.WORKSPACE_A, SEED_ID, seed.version(), seed.rowVersion());
        assertThat(published.businessEntityType()).isEqualTo("Customer");
        assertThat(published.businessEntityId()).isEqualTo("customer-001");

        var knowledge = createPublishedDocument("p5-context-knowledge", "上下文验证", "可读知识资料：柠檬客户的正式续约条款。 ");
        var customerA = contexts.query(ALICE, Ids.WORKSPACE_A,
                new ContextQuery("柠檬客户续约", 5, 2_000, "Customer", "customer-001"));
        assertThat(customerA.items()).extracting(item -> item.sourceType()).contains("KNOWLEDGE", "MEMORY");
        assertThat(customerA.items().stream().mapToInt(item -> item.estimatedTokens()).sum())
                .isLessThanOrEqualTo(customerA.tokenBudget());
        assertThat(customerA.items().get(0).sourceType()).isEqualTo("KNOWLEDGE");
        assertThat(customerA.items().get(0).documentId()).isEqualTo(knowledge.id());
        assertThat(customerA.items().stream().filter(item -> "MEMORY".equals(item.sourceType())).findFirst().orElseThrow().citationId())
                .isEqualTo("mem-2");

        var customerB = contexts.query(ALICE, Ids.WORKSPACE_A,
                new ContextQuery("柠檬客户续约", 5, 2_000, "Customer", "customer-002"));
        assertThat(customerB.items()).extracting(item -> item.sourceType()).contains("KNOWLEDGE").doesNotContain("MEMORY");

        // 高置信度的个人记忆仍受 Owner 隔离；confidence 不参与授权判断。
        var privateMemory = memories.create(new CreateMemoryCommand(ALICE, Ids.WORKSPACE_A, "private-customer-b", "1.0.0",
                "PREFERENCE", "PERSONAL", "仅所有者可见的客户偏好", 1.0, Instant.now().plusSeconds(600),
                "manual:private-customer-b", List.of("test:private-customer-b"), "Customer", "customer-002"));
        memories.publish(ALICE, Ids.WORKSPACE_A, privateMemory.id(), privateMemory.version(), privateMemory.rowVersion());
        grant(Ids.BOB, "context:read");
        grant(Ids.BOB, "memory:read");
        var bobContext = contexts.query(BOB, Ids.WORKSPACE_A,
                new ContextQuery("客户偏好", 5, 2_000, "Customer", "customer-002"));
        assertThat(bobContext.items()).noneMatch(item -> privateMemory.id().equals(item.memoryId()));

        // 缺少 memory:read 时跳过记忆检索，但保留仍有权限的 Knowledge。
        revoke(ALICE.actorId(), "memory:read");
        var memoryDenied = contexts.query(ALICE, Ids.WORKSPACE_A,
                new ContextQuery("柠檬客户续约", 5, 2_000, "Customer", "customer-001"));
        assertThat(memoryDenied.items()).extracting(item -> item.sourceType()).contains("KNOWLEDGE").doesNotContain("MEMORY");
        grant(ALICE.actorId(), "memory:read");

        var task = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A,
                UUID.fromString("20000000-0000-4000-8000-000000000001"), "3.0.0", "请总结客户续约信息。",
                "Customer", "customer-001", "p5-context-memory-task", "trace-p5-context-memory", "USER"));
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(task.id());
        // Runtime 返回后由外层 Task 调度器完成 Task；完成态 run 不能被旧 work item 重新领取。
        tasks.complete(work, runtime.run(work));
        var saved = runtimeQuery.steps(Ids.TENANT_A, task.id()).stream()
                .filter(step -> "CONTEXT_SNAPSHOT".equals(step.type())).findFirst().orElseThrow();
        var snapshot = json.readTree(saved.content());
        assertThat(saved.content()).startsWith("{").doesNotContain("用户任务（决定本次输出）");
        assertThat(snapshot.path("items").toString()).contains("MEMORY", SEED_ID.toString(), "1.0.0");
        var modelResponses = countModelResponses(task.id());
        assertThat(modelResponses).isGreaterThan(0);

        // Task 已结束，旧 work item 不能重新启动模型；结果读取与回放仍需复核原 Context 快照。
        revoke(ALICE.actorId(), "memory:read");
        var deniedResume = runtime.run(work);
        assertThat(deniedResume.errorCode()).isEqualTo("TASK_NOT_RUNNING");
        assertThat(countModelResponses(task.id())).isEqualTo(modelResponses);
        grant(ALICE.actorId(), "memory:read");
        memories.revoke(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0", published.rowVersion());
        var revokedResume = runtime.run(work);
        assertThat(revokedResume.errorCode()).isEqualTo("TASK_NOT_RUNNING");
        assertThat(countModelResponses(task.id())).isEqualTo(modelResponses);
        assertThat(runtimeQuery.canExposeResult(ALICE, Ids.WORKSPACE_A, task.id())).isFalse();
        assertThat(runtimeQuery.replay(ALICE, Ids.WORKSPACE_A, task.id()).errorCode()).isEqualTo("REPLAY_CONTEXT_UNAVAILABLE");
        var redactedHistory = runtimeQuery.steps(ALICE, Ids.WORKSPACE_A, task.id());
        // 一个 attempt 的任一 Context 来源撤回后，模型答复和结构化结果也必须一起脱敏。
        assertThat(redactedHistory).anyMatch(step -> "MODEL_RESPONSE".equals(step.type()) && step.content() == null);
        assertThat(redactedHistory).anyMatch(step -> "STRUCTURED_RESULT".equals(step.type()) && step.content() == null);
        assertThat(redactedHistory).allMatch(step -> step.content() == null);
        var path = "/api/v1/workspaces/%s/tasks/%s".formatted(Ids.WORKSPACE_A, task.id());
        mvc.perform(get(path).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.result").value(org.hamcrest.Matchers.nullValue()));
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/tasks", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-context-memory-task")
                        .header("X-Trace-Id", "trace-p5-context-memory")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"agentId":"20000000-0000-4000-8000-000000000001","agentVersion":"3.0.0","input":"请总结客户续约信息。","businessEntity":{"type":"Customer","id":"customer-001"}}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.id").value(task.id().toString()))
                .andExpect(jsonPath("$.result").value(org.hamcrest.Matchers.nullValue()));
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/tasks", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p6-forged-task-source")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"agentId":"20000000-0000-4000-8000-000000000001","agentVersion":"3.0.0","input":"伪造来源","source":"EVALUATION"}
                                """))
                .andExpect(status().isBadRequest());
        revoke(ALICE.actorId(), "audit:read");
        mvc.perform(get(path + "/audit-events").header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isForbidden());
        grant(ALICE.actorId(), "audit:read");

        // FAILED 是正常 Task 状态，详情接口仍返回可诊断错误，而不伪造成功结果。
        var failedTask = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A,
                UUID.fromString("20000000-0000-4000-8000-000000000001"), "3.0.0", "失败详情 REST 回归",
                null, null, "p6-failed-task-details", "trace-p6-failed-task-details"));
        var failedWork = tasks.claimOne().orElseThrow();
        assertThat(failedWork.id()).isEqualTo(failedTask.id());
        tasks.complete(failedWork, TaskRunner.RunOutcome.failed("DETERMINISTIC_TEST_FAILURE", "合成失败详情"));
        mvc.perform(get("/api/v1/workspaces/{workspaceId}/tasks/{taskId}", Ids.WORKSPACE_A, failedTask.id())
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("FAILED"))
                .andExpect(jsonPath("$.errorCode").value("DETERMINISTIC_TEST_FAILURE"))
                .andExpect(jsonPath("$.errorDetail").value("合成失败详情"));
    }

    private PublishedDocument createPublishedDocument(String key, String title, String content) throws Exception {
        var document = mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("title", title, "sourceRef", "manual://" + key,
                                "content", content, "metadata", Map.of()))))
                .andExpect(status().isCreated()).andReturn();
        var id = UUID.fromString(json.readTree(document.getResponse().getContentAsString()).path("id").asText());
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents/{documentId}/chunks", Ids.WORKSPACE_A, id)
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk());
        var build = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents/{documentId}/index-builds", Ids.WORKSPACE_A, id)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", key + "-build"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents/{documentId}/publish?expectedVersion=1&buildId={buildId}",
                        Ids.WORKSPACE_A, id, build.path("id").asText())
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", key + "-publish"))
                .andExpect(status().isOk());
        return new PublishedDocument(id);
    }

    private void grant(UUID actorId, String action) {
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, ?, 'ACTIVE') "
                        + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, actorId, action);
    }

    private void revoke(UUID actorId, String action) {
        jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = ? and status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, actorId, action);
    }

    private int countModelResponses(UUID taskId) {
        return jdbc.queryForObject("select count(*) from agent_runtime.step where task_id = ? and type = 'MODEL_RESPONSE'",
                Integer.class, taskId);
    }

    private record PublishedDocument(UUID id) { }
}
