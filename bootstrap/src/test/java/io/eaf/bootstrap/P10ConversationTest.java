package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.capability.api.CapabilityService;
import io.eaf.model.api.ModelGateway;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false", "eaf.memory.outbox-publisher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false", "eaf.model.mode=deterministic",
        "eaf.model.jev.mode=deterministic", "eaf.model.jev.evidence-mode=deterministic"})
@AutoConfigureMockMvc
class P10ConversationTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String WORKSPACE = "10000000-0000-4000-8000-000000000001";
    private static final UUID TENANT = UUID.fromString("70000000-0000-4000-8000-000000000001");
    private static final UUID ALICE = UUID.fromString("80000000-0000-4000-8000-000000000001");
    private static final UUID CUSTOMER_CAPABILITY = UUID.fromString("54000000-0000-4000-8000-00000000000d");

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
    @Autowired CapabilityService capabilities;
    @Autowired JdbcTemplate jdbc;
    @Autowired ModelGateway model;

    @Test
    void conversationsRewriteClarifyConfirmBriefCompareAndRecoverFollowup() throws Exception {
        publishKnowledge();
        var qa = createConversation("KNOWLEDGE_QA", null, "连续知识问答");
        var first = send(qa, "标准续约产品编号是什么？", "qa-turn-1");
        mvc.perform(post(path(qa, "/turns" )).header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "qa-turn-1").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("input", "标准续约产品编号是什么？"))))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.turnId").value(first.path("turnId").asText()));
        var firstCalls = model.callCount();
        complete(first.path("taskId").asText());
        var firstTaskState = task(first.path("taskId").asText());
        assertThat(model.callCount()).as(firstTaskState.toPrettyString()).isEqualTo(firstCalls + 1);

        // 同一会话中明确的单一指代走一次整理和一次回答。
        var followup = send(qa, "它的续约规则是什么？", "qa-turn-2");
        complete(followup.path("taskId").asText());
        assertThat(model.callCount()).isEqualTo(firstCalls + 3);
        mvc.perform(get(path(qa, "/turns?limit=50")).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(2));

        // 同键不同输入必须冲突；其他 HUMAN 不能读取私人会话或会话 Task。
        mvc.perform(post(path(qa, "/turns")).header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "qa-turn-2").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"input\":\"不同的问题\"}"))
                .andExpect(status().isConflict());
        mvc.perform(get(path(qa, "")).header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isIn(403, 404));
        mvc.perform(get("/api/v1/workspaces/" + WORKSPACE + "/tasks/" + followup.path("taskId").asText())
                        .header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isIn(403, 404));

        var ambiguity = createConversation("KNOWLEDGE_QA", null, "不明确指代");
        var ambiguousHistory = send(ambiguity, "比较标准产品与加急产品的编号。", "amb-turn-1");
        complete(ambiguousHistory.path("taskId").asText());
        var unclear = send(ambiguity, "它的延期规则是什么？", "amb-turn-2");
        complete(unclear.path("taskId").asText());
        mvc.perform(get("/api/v1/workspaces/" + WORKSPACE + "/tasks/" + unclear.path("taskId").asText())
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.result.answerStatus").value("INSUFFICIENT"))
                .andExpect(jsonPath("$.result.clarificationQuestion").isNotEmpty())
                .andExpect(jsonPath("$.result.citations.length()").value(0));
        assertThat(jdbc.queryForObject("select count(*) from agent_runtime.step s join agent_runtime.run r on r.id = s.run_id "
                + "where r.task_id = ? and s.type in ('RETRIEVAL_CANDIDATES','EVIDENCE_REQUESTED')", Integer.class,
                UUID.fromString(unclear.path("taskId").asText()))).isZero();

        // 简报内容未变化的请求键恢复原结果；有变化则使用新的不可变修订。
        var manual = saveBrief(qa, 0, "续约规则主题", null, "qa-brief-1");
        assertThat(manual.path("result").path("revision").asInt()).isEqualTo(1);
        var next = saveBrief(qa, 1, "编号与续约规则主题", null, "qa-brief-2");
        assertThat(next.path("result").path("revision").asInt()).isEqualTo(2);
        var replay = saveBrief(qa, 0, "续约规则主题", null, "qa-brief-1");
        assertThat(replay.path("result").path("revision").asInt()).isEqualTo(1);
        assertThat(replay.path("currentRevision").asInt()).isEqualTo(2);

        // 回归旧会话绑定：新建入口固定；此处用已发布快照验证旧跟进入口仍可用。
        var customer = createLegacyConversation("CUSTOMER_ASSISTANT", "synthetic-customer-001", "客户演示");
        mvc.perform(get(path(customer, "")).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.capabilityVersion").value("1.0.0"));
        var analysis1 = send(customer, "客户确认今年计划续约，最近联系记录尚待核对。", "customer-turn-1");
        complete(analysis1.path("taskId").asText());
        var result1 = taskResult(analysis1.path("taskId").asText());
        assertThat(result1.path("followupDraft").path("customerId").asText()).isEqualTo("synthetic-customer-001");
        assertThat(result1.path("briefSuggestion").path("baseRevision").asInt()).isZero();
        var savedBrief = saveBrief(customer, 0, result1.path("briefSuggestion").path("content").asText(),
                analysis1.path("taskId").asText(), "customer-brief-1");
        assertThat(savedBrief.path("result").path("revision").asInt()).isEqualTo(1);

        var analysis2 = send(customer, "客户要求在年底前再次联系并确认审批进度。", "customer-turn-2");
        complete(analysis2.path("taskId").asText());
        mvc.perform(get(path(customer, "/analysis-comparison")).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.available").value(true));

        var sourceTask = task(analysis2.path("taskId").asText());
        var followupBody = json.writeValueAsString(Map.of("sourceTaskId", analysis2.path("taskId").asText(),
                "expectedTaskVersion", sourceTask.path("version").asLong(), "expectedBriefRevision", 1,
                "summary", result1.path("followupDraft").path("summary").asText()));
        var followupPath = path(customer, "/followups");
        var createdWorkflow = json.readTree(mvc.perform(post(followupPath).header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "customer-followup-1").contentType(MediaType.APPLICATION_JSON).content(followupBody))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        var legacyFollowupPath = "/api/v1/workspaces/" + WORKSPACE + "/tasks/" + sourceTask.path("id").asText() + "/followups";
        var legacyFollowupBody = json.writeValueAsString(Map.of("expectedVersion", sourceTask.path("version").asLong(),
                "summary", result1.path("followupDraft").path("summary").asText()));
        var legacyWorkflow = json.readTree(mvc.perform(post(legacyFollowupPath).header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "customer-followup-legacy-1").contentType(MediaType.APPLICATION_JSON)
                        .content(legacyFollowupBody))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        saveBrief(customer, 1, "用户确认的较新客户资料。", null, "customer-brief-2");
        var replayedWorkflow = json.readTree(mvc.perform(post(followupPath).header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "customer-followup-1").contentType(MediaType.APPLICATION_JSON).content(followupBody))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        assertThat(replayedWorkflow.path("instanceId").asText()).isEqualTo(createdWorkflow.path("instanceId").asText());
        var replayedLegacyWorkflow = json.readTree(mvc.perform(post(legacyFollowupPath).header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "customer-followup-legacy-1").contentType(MediaType.APPLICATION_JSON)
                        .content(legacyFollowupBody))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        assertThat(replayedLegacyWorkflow.path("instanceId").asText()).isEqualTo(legacyWorkflow.path("instanceId").asText());
        mvc.perform(post(followupPath).header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "customer-followup-stale").contentType(MediaType.APPLICATION_JSON).content(followupBody))
                .andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("FOLLOWUP_SOURCE_STALE"));
    }

    private void publishKnowledge() throws Exception {
        var document = json.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents")
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p10-knowledge")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of(
                                "title", "合成续约规则", "sourceRef", "demo://p10/renewal",
                                "content", "# 产品续约\n\n标准续约产品编号为 SKU-CLOUD-RENEW-12。标准续约应在到期前三十天联系客户；加急产品需由团队核对当前规则。",
                                "metadata", Map.of("classification", "synthetic-demo")))))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var documentId = document.path("id").asText();
        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId + "/chunks")
                        .param("chunkingVersion", "p9-structure-1").header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk());
        var build = json.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId + "/index-builds")
                        .param("assetVersion", "1").param("chunkingVersion", "p9-structure-1")
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p10-knowledge-build"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(build.path("status").asText()).isEqualTo("READY");
        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId
                        + "/publish?expectedVersion=1&buildId=" + build.path("id").asText())
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p10-knowledge-publish"))
                .andExpect(status().isOk());
    }

    private String createConversation(String mode, String customerId, String title) throws Exception {
        var request = new java.util.LinkedHashMap<String, Object>();
        request.put("mode", mode); request.put("title", title); request.put("customerId", customerId);
        var response = mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/conversations")
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p10-session-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(request)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        return json.readTree(response).path("id").asText();
    }

    private String createLegacyConversation(String mode, String customerId, String title) {
        var actor = new ActorContext(ALICE, TENANT, ActorType.HUMAN, Set.of("capability:read", "task:create"));
        var capability = capabilities.requirePublished(actor, UUID.fromString(WORKSPACE), CUSTOMER_CAPABILITY, "1.0.0");
        return tasks.createConversation(new TaskService.CreateConversationCommand(actor, UUID.fromString(WORKSPACE), mode,
                title, customerId, capability.id(), capability.version(), capability.contentHash(), capability.agentId(),
                capability.agentVersion(), capability.skillId(), capability.skillVersion(), capability.skillContentHash(),
                "p10-legacy-session-" + UUID.randomUUID())).id().toString();
    }

    private JsonNode send(String conversationId, String input, String key) throws Exception {
        return json.readTree(mvc.perform(post(path(conversationId, "/turns"))
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of("input", input))))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
    }

    private void complete(String taskId) {
        var item = tasks.claimOne().orElseThrow();
        assertThat(item.id()).isEqualTo(UUID.fromString(taskId));
        tasks.complete(item, runtime.run(item));
    }

    private JsonNode task(String taskId) throws Exception {
        return json.readTree(mvc.perform(get("/api/v1/workspaces/" + WORKSPACE + "/tasks/" + taskId)
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
    }

    private JsonNode taskResult(String taskId) throws Exception { return task(taskId).path("result"); }

    private JsonNode saveBrief(String conversationId, int revision, String content, String suggestionTaskId,
                               String key) throws Exception {
        var request = new java.util.LinkedHashMap<String, Object>();
        request.put("expectedRevision", revision); request.put("content", content);
        request.put("suggestionTaskId", suggestionTaskId == null ? null : UUID.fromString(suggestionTaskId));
        return json.readTree(mvc.perform(put(path(conversationId, "/brief"))
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(request)))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
    }

    private String path(String conversationId, String suffix) {
        return "/api/v1/workspaces/" + WORKSPACE + "/conversations/" + conversationId + suffix;
    }
}
