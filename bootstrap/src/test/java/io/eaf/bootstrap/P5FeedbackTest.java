package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Hashing;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import java.sql.Timestamp;
import java.time.Instant;
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
// 在隔离 PostgreSQL 中验证反馈来源不可伪造、幂等隔离和评测任务拒绝。
class P5FeedbackTest {
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

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;
    @Autowired JdbcTemplate jdbc;
    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;

    @Test
    void recordsOnlyVisibleUserTaskFeedbackWithImmutableProvenance() throws Exception {
        var documentId = createPublishedDocument("p5-feedback-source", "反馈来源知识", "正式知识：纠正反馈必须保留任务使用的来源版本。");
        var userTask = completeTask("p5-feedback-user", "USER", "正式知识：纠正反馈必须保留任务使用的来源版本。");
        var unknownExecution = insertUnknownExecution(userTask);

        var forgedSource = """
                {"correction":"客户事实应优先由授权 CRM 核验。","evidence":"执行结果为 UNKNOWN。","executionId":"%s","sourceType":"EVALUATION"}
                """.formatted(unknownExecution);
        mvc.perform(post(feedbackPath(Ids.WORKSPACE_A, userTask))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-feedback-forged-source")
                        .contentType(MediaType.APPLICATION_JSON).content(forgedSource))
                .andExpect(status().isBadRequest());
        // Learning 从已认证 Task 推导反馈来源，正常 DTO 不包含 sourceType。
        var body = """
                {"correction":"客户事实应优先由授权 CRM 核验。","evidence":"执行结果为 UNKNOWN，不能据此声称已成功。","executionId":"%s"}
                """.formatted(unknownExecution);
        var first = json.readTree(mvc.perform(post(feedbackPath(Ids.WORKSPACE_A, userTask))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-feedback-one")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        assertThat(first.path("sourceType").asText()).isEqualTo("USER");
        assertThat(first.path("source").path("execution").path("status").asText()).isEqualTo("UNKNOWN");
        assertThat(first.path("source").path("execution").path("verified").asBoolean()).isFalse();
        var contextSources = first.path("source").path("contextSources");
        assertThat(contextSources.toString()).contains(documentId.toString(), "contentHash");
        assertThat(first.toString()).doesNotContain("正式知识：纠正反馈必须保留任务使用的来源版本。");

        var repeated = json.readTree(mvc.perform(post(feedbackPath(Ids.WORKSPACE_A, userTask))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-feedback-one")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(repeated.path("id").asText()).isEqualTo(first.path("id").asText());
        mvc.perform(post(feedbackPath(Ids.WORKSPACE_A, userTask))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-feedback-one")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"correction\":\"不同纠正\",\"evidence\":\"另一份证据\"}"))
                .andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("IDEMPOTENCY_CONFLICT"));

        mvc.perform(post(feedbackPath(Ids.WORKSPACE_A, userTask))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-feedback-two")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"correction\":\"保留相反意见以供审核。\",\"evidence\":\"提交人提出了相反结论。\"}"))
                .andExpect(status().isCreated());
        var listed = json.readTree(mvc.perform(get(feedbackPath(Ids.WORKSPACE_A, userTask))
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(listed).hasSize(2);
        assertThat(jdbc.queryForObject("select count(*) from learning.outbox where event_type = 'eaf.feedback.recorded.v1'", Integer.class))
                .isEqualTo(2);

        var evaluationTask = completeTask("p5-feedback-eval", "EVALUATION", "评测来源不可生成学习反馈。");
        mvc.perform(post(feedbackPath(Ids.WORKSPACE_A, evaluationTask))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-feedback-forged-evaluation")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"correction\":\"伪造评测来源\",\"evidence\":\"评测来源 Task 不可产生反馈\"}"))
                .andExpect(status().isForbidden());
        assertThat(jdbc.queryForObject("select count(*) from learning.feedback where task_id = ?", Integer.class, evaluationTask)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from learning.outbox", Integer.class)).isEqualTo(2);

        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'feedback:create', 'ACTIVE') on conflict do nothing",
                Ids.TENANT_A, Ids.WORKSPACE_A2, Ids.ALICE);
        mvc.perform(post(feedbackPath(Ids.WORKSPACE_A2, userTask))
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p5-feedback-cross-workspace")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"correction\":\"跨空间反馈\",\"evidence\":\"不应读取其他空间任务\"}"))
                .andExpect(status().isNotFound());
        assertThat(jdbc.queryForObject("select count(*) from learning.feedback where idempotency_key = 'p5-feedback-cross-workspace'", Integer.class))
                .isZero();
    }

    private UUID completeTask(String key, String source, String input) {
        var task = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "3.0.0", input,
                null, null, key, "trace-" + key, source));
        var work = "USER".equals(source) ? tasks.claimOne().orElseThrow() : tasks.claim(task.id()).orElseThrow();
        tasks.complete(work, runtime.run(work));
        return task.id();
    }

    private UUID createPublishedDocument(String key, String title, String content) throws Exception {
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
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", key + "-build"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents/{documentId}/publish", Ids.WORKSPACE_A, id)
                        .param("expectedVersion", "1").param("buildId", build.path("id").asText())
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", key + "-publish"))
                .andExpect(status().isOk());
        return id;
    }

    private UUID insertUnknownExecution(UUID taskId) {
        var id = UUID.randomUUID();
        jdbc.update("insert into execution.execution(id, tenant_id, workspace_id, actor_id, task_id, attempt, agent_id, agent_version, tool_name, tool_version, arguments_json, request_hash, idempotency_key, status, operation_id, created_at, ended_at) values (?, ?, ?, ?, ?, 1, ?, '1.0.0', 'test.read', '1.0.0', '{}'::jsonb, ?, ?, 'UNKNOWN', ?, ?, ?)",
                id, Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE, taskId, Ids.AGENT_RISK, Hashing.sha256("{}"),
                "p5-unknown-" + id, id, Timestamp.from(Instant.now()), Timestamp.from(Instant.now()));
        return id;
    }

    private String feedbackPath(UUID workspaceId, UUID taskId) {
        return "/api/v1/workspaces/" + workspaceId + "/tasks/" + taskId + "/feedback";
    }
}
