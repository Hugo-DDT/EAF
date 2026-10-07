package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.model.api.ModelGateway;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import java.util.LinkedHashMap;
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
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false", "eaf.memory.outbox-publisher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false", "eaf.model.mode=deterministic",
        "eaf.model.jev.mode=deterministic", "eaf.model.jev.evidence-mode=deterministic"})
@AutoConfigureMockMvc
class P11ExperienceCardTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String WORKSPACE = "10000000-0000-4000-8000-000000000001";
    private static final String AUTH = "Bearer eaf-local-alice";

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
    @Autowired ModelGateway model;
    @Autowired JdbcTemplate jdbc;
    private final Map<String, String> qaTaskIds = new LinkedHashMap<>();

    @Test
    void feedbackCanBecomePrivateDraftThenConfirmedExperienceUsedOnlyInMatchingContexts() throws Exception {
        var firstDraftCard = createCard(Map.of("title", "分页草稿 A", "content", "先说明范围。", "type", "PREFERENCE",
                "applicability", "GENERAL"), "p11-page-card-a");
        var secondDraftCard = createCard(Map.of("title", "分页草稿 B", "content", "再说明依据。", "type", "PREFERENCE",
                "applicability", "GENERAL"), "p11-page-card-b");
        var firstPage = json.readTree(mvc.perform(get(cardPath()).param("limit", "1").header("Authorization", AUTH))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(firstPage.path("items").size()).isEqualTo(1);
        assertThat(firstPage.path("nextCursorUpdatedAt").asText()).isNotBlank();
        assertThat(firstPage.path("nextCursorId").asText()).isNotBlank();
        var secondPage = json.readTree(mvc.perform(get(cardPath()).param("limit", "1")
                        .param("cursorUpdatedAt", firstPage.path("nextCursorUpdatedAt").asText())
                        .param("cursorId", firstPage.path("nextCursorId").asText()).header("Authorization", AUTH))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(secondPage.path("items").size()).isEqualTo(1);
        assertThat(secondPage.path("items").get(0).path("id").asText())
                .isNotEqualTo(firstPage.path("items").get(0).path("id").asText());

        var beforePublish = createConversation("KNOWLEDGE_QA", null, "草稿不进入上下文");
        var draftTurn = send(beforePublish, "标准续约产品编号是什么？", "p11-draft-not-used");
        complete(draftTurn.path("taskId").asText());
        assertThat(task(draftTurn.path("taskId").asText()).path("result").path("experienceUsage").path("included").size())
                .isZero();

        var qa = createConversation("KNOWLEDGE_QA", null, "反馈与经验");
        var turn = send(qa, "标准续约产品编号是什么？", "p11-source-turn");
        complete(turn.path("taskId").asText());
        var sourceTaskId = turn.path("taskId").asText();
        assertThat(task(sourceTaskId).path("result").path("answerStatus").asText()).isEqualTo("INSUFFICIENT");

        var feedbackBody = Map.of("correction", "先列适用范围，再给产品编号。",
                "evidence", "合成手册说明标准续约和加急产品规则不同。");
        var feedback = json.readTree(mvc.perform(post(taskPath(sourceTaskId) + "/feedback")
                        .header("Authorization", AUTH).header("Idempotency-Key", "p11-feedback-1")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(feedbackBody)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var callsBeforeDraft = model.callCount();
        var draftBody = Map.of("feedbackId", feedback.path("id").asText(), "draftText", "用短句表达，避免把待核实事项写成结论。");
        var draftPath = taskPath(sourceTaskId) + "/experience-drafts";
        var draft = json.readTree(mvc.perform(post(draftPath).header("Authorization", AUTH)
                        .header("Idempotency-Key", "p11-draft-1").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(draftBody)))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        var replay = json.readTree(mvc.perform(post(draftPath).header("Authorization", AUTH)
                        .header("Idempotency-Key", "p11-draft-1").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(draftBody)))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        assertThat(replay.path("id").asText()).isEqualTo(draft.path("id").asText());
        complete(draft.path("id").asText());
        var completedDraft = task(draft.path("id").asText());
        assertThat(completedDraft.path("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(completedDraft.path("result").path("title").asText()).isNotBlank();
        assertThat(model.callCount()).isEqualTo(callsBeforeDraft + 1);
        assertThat(completedDraft.path("input").asText()).contains(feedbackBody.get("correction"), feedbackBody.get("evidence"))
                .doesNotContain("已依据本轮授权知识片段整理");
        mvc.perform(get(taskPath(draft.path("id").asText())).header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isIn(403, 404));
        mvc.perform(post(taskPath(draft.path("id").asText()) + "/retry").header("Authorization", AUTH)
                        .header("Idempotency-Key", "p11-draft-retry")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":" + completedDraft.path("version").asLong() + "}"))
                .andExpect(status().isConflict()).andExpect(jsonPath("$.code").value("EXPERIENCE_DRAFT_RETRY_UNSUPPORTED"));

        var cardBody = Map.of("title", completedDraft.path("result").path("title").asText(),
                "content", completedDraft.path("result").path("content").asText(), "type", "PROCEDURAL",
                "applicability", "GENERAL", "sourceTaskId", sourceTaskId,
                "sourceFeedbackId", feedback.path("id").asText(), "draftTaskId", draft.path("id").asText());
        var card = json.readTree(mvc.perform(post(cardPath()).header("Authorization", AUTH)
                        .header("Idempotency-Key", "p11-card-create").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(cardBody)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        assertThat(card.path("revision").asInt()).isEqualTo(1);
        assertThat(card.path("replayed").asBoolean()).isFalse();
        var cardReplay = json.readTree(mvc.perform(post(cardPath()).header("Authorization", AUTH)
                        .header("Idempotency-Key", "p11-card-create").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(cardBody)))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(cardReplay.path("cardId").asText()).isEqualTo(card.path("cardId").asText());
        assertThat(cardReplay.path("replayed").asBoolean()).isTrue();
        assertThat(getCard(card.path("cardId").asText()).path("status").asText()).isEqualTo("DRAFT");
        var changedCreate = new LinkedHashMap<>(cardBody);
        changedCreate.put("title", "同键不同内容");
        mvc.perform(post(cardPath()).header("Authorization", AUTH).header("Idempotency-Key", "p11-card-create")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(changedCreate)))
                .andExpect(status().isConflict());
        mvc.perform(get(cardPath() + "/" + card.path("cardId").asText()).header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isIn(403, 404));
        var wrongSource = new LinkedHashMap<>(cardBody);
        wrongSource.put("sourceTaskId", draft.path("id").asText());
        mvc.perform(post(cardPath()).header("Authorization", AUTH).header("Idempotency-Key", "p11-card-wrong-source")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(wrongSource)))
                .andExpect(status().isNotFound());
        publishCard(card.path("cardId").asText(), 1, 1);

        var noKnowledge = createConversation("KNOWLEDGE_QA", null, "经验不作为知识证据");
        var noKnowledgeTurn = send(noKnowledge, "请回答标准续约编号。", "p11-no-knowledge");
        complete(noKnowledgeTurn.path("taskId").asText());
        var insufficient = task(noKnowledgeTurn.path("taskId").asText()).path("result");
        assertThat(insufficient.path("answerStatus").asText()).isEqualTo("INSUFFICIENT");
        assertThat(insufficient.path("citations").size()).isZero();
        assertThat(insufficient.path("experienceUsage").path("included").findValuesAsText("cardId"))
                .contains(card.path("cardId").asText());

        publishKnowledge();
        var qaWithExperience = createConversation("KNOWLEDGE_QA", null, "启用通用经验");
        var qaTurn = send(qaWithExperience, "标准续约产品编号是什么？", "p11-qa-with-experience");
        complete(qaTurn.path("taskId").asText());
        var answer = task(qaTurn.path("taskId").asText()).path("result");
        assertThat(answer.path("answerStatus").asText()).isEqualTo("ANSWERED");
        assertThat(answer.path("citations").size()).isEqualTo(1);
        assertThat(answer.path("experienceUsage").path("included").findValuesAsText("cardId"))
                .contains(card.path("cardId").asText());
        assertThat(answer.path("experienceUsage").path("included").get(0).path("revision").asInt()).isEqualTo(1);

        var currentV1 = getCard(card.path("cardId").asText());
        var v2 = json.readTree(mvc.perform(post(cardPath() + "/" + card.path("cardId").asText() + "/versions")
                        .header("Authorization", AUTH).header("Idempotency-Key", "p11-card-save-v2")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of(
                                "expectedVersion", currentV1.path("version").asLong(), "title", "先核对范围与缺口",
                                "content", "先列出适用范围；资料不足时把待核实事项独立呈现，不猜测结论。", "type", "PROCEDURAL"))))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(v2.path("revision").asInt()).isEqualTo(2);
        var activeV1WithDraft = getCard(card.path("cardId").asText());
        assertThat(activeV1WithDraft.path("latestRevision").asInt()).isEqualTo(2);
        assertThat(activeV1WithDraft.path("activeRevision").asInt()).isEqualTo(1);
        assertThat(activeV1WithDraft.path("latest").path("status").asText()).isEqualTo("DRAFT");
        var v1DuringDraft = queryQa("p11-v1-remains-active", "核对续约资料范围与待核实项。");
        assertThat(v1DuringDraft.path("experienceUsage").path("included").get(0).path("revision").asInt()).isEqualTo(1);

        mvc.perform(post(cardPath() + "/" + card.path("cardId").asText() + "/versions/2/publish")
                        .header("Authorization", AUTH).header("Idempotency-Key", "p11-card-publish-v2")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of(
                                "expectedVersion", v2.path("cardVersion").asLong()))))
                .andExpect(status().isOk());
        var v2AfterPublish = queryQa("p11-v2-active", "核对续约资料范围与待核实项。");
        assertThat(v2AfterPublish.path("experienceUsage").path("included").findValuesAsText("cardId"))
                .contains(card.path("cardId").asText());
        assertThat(v2AfterPublish.path("experienceUsage").path("included").findValuesAsText("revision"))
                .contains("2");

        var customerCard = json.readTree(mvc.perform(post(cardPath()).header("Authorization", AUTH)
                        .header("Idempotency-Key", "p11-customer-card")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of(
                                "title", "客户 A 联系偏好", "content", "联系前先核实当前合同阶段。", "type", "PREFERENCE",
                                "applicability", "CUSTOMER", "customerId", "synthetic-customer-001"))))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        publishCard(customerCard.path("cardId").asText(), 1, 1);
        var customerA = analyzeCustomer("synthetic-customer-001", "p11-customer-a");
        assertThat(customerA.path("experienceUsage").path("included").findValuesAsText("cardId"))
                .contains(customerCard.path("cardId").asText());
        var customerB = analyzeCustomer("synthetic-customer-002", "p11-customer-b");
        assertThat(customerB.path("experienceUsage").path("included").findValuesAsText("cardId"))
                .doesNotContain(customerCard.path("cardId").asText());

        for (int i = 1; i <= 4; i++) {
            var budgetCard = createCard(Map.of("title", "预算经验 " + i, "content", "核验信息边界。".repeat(114),
                    "type", "PREFERENCE", "applicability", "GENERAL"), "p11-budget-card-" + i);
            publishCard(budgetCard.path("cardId").asText(), 1, 1);
        }
        var budgetResult = queryQa("p11-budget-selection-a", "说明当前续约资料能支持的结论。");
        var budgetSnapshot = contextSnapshot(findTurnTask("p11-budget-selection-a"));
        var memoryTokens = 0;
        var hasKnowledge = false;
        for (var item : budgetSnapshot.path("items")) {
            if ("MEMORY".equals(item.path("sourceType").asText())) memoryTokens += item.path("estimatedTokens").asInt();
            if ("KNOWLEDGE".equals(item.path("sourceType").asText())) hasKnowledge = true;
        }
        assertThat(budgetResult.path("experienceUsage").path("included").size()).isLessThanOrEqualTo(3);
        assertThat(budgetResult.path("experienceUsage").path("omittedByLimit").asInt()).isPositive();
        assertThat(budgetResult.path("experienceUsage").path("omittedByBudget").asInt()).isPositive();
        assertThat(memoryTokens).isLessThanOrEqualTo(600);
        assertThat(hasKnowledge).isTrue();
        var repeatedBudgetResult = queryQa("p11-budget-selection-b", "说明当前续约资料能支持的结论。");
        assertThat(repeatedBudgetResult.path("experienceUsage").path("included").findValuesAsText("cardId"))
                .containsExactlyElementsOf(budgetResult.path("experienceUsage").path("included").findValuesAsText("cardId"));

        mvc.perform(post(cardPath()).header("Authorization", AUTH).header("Idempotency-Key", "p11-expired-card-rejected")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of(
                                "title", "过期卡", "content", "不应保存已过期内容。", "type", "PREFERENCE",
                                "applicability", "GENERAL", "expiresAt", "2020-01-01T00:00:00Z"))))
                .andExpect(status().isBadRequest());

        var followupConversation = createConversation("CUSTOMER_ASSISTANT", "synthetic-customer-001", "确认跟进");
        var followupTurn = send(followupConversation, "请制定一次续约提醒的待确认摘要。", "p11-followup-turn");
        complete(followupTurn.path("taskId").asText());
        var followupSource = task(followupTurn.path("taskId").asText());
        var followupSummary = followupSource.path("result").path("followupDraft").path("summary").asText();
        assertThat(followupSummary).isNotBlank();
        var followup = json.readTree(mvc.perform(post(taskPath(followupTurn.path("taskId").asText()) + "/followups")
                        .header("Authorization", AUTH).header("Idempotency-Key", "p11-followup-confirm")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of(
                                "expectedVersion", followupSource.path("version").asLong(), "summary", followupSummary))))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        assertThat(followup.path("workflowVersion").asText()).isEqualTo("1.2.0");

        var currentCard = getCard(card.path("cardId").asText());
        var activeRevision = currentCard.path("activeRevision").asInt();
        mvc.perform(post(cardPath() + "/" + card.path("cardId").asText() + "/revoke")
                        .header("Authorization", AUTH).header("Idempotency-Key", "p11-card-revoke")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of(
                                "expectedVersion", currentCard.path("version").asLong(), "expectedActiveRevision", activeRevision))))
                .andExpect(status().isOk());
        var afterRevoke = analyzeCustomer("synthetic-customer-001", "p11-customer-a-after-revoke");
        assertThat(afterRevoke.path("experienceUsage").path("included").findValuesAsText("cardId"))
                .doesNotContain(card.path("cardId").asText());
        assertThat(jdbc.queryForObject("select count(*) from task.experience_draft_binding where task_id = ?",
                Integer.class, UUID.fromString(draft.path("id").asText()))).isEqualTo(1);
    }

    private JsonNode analyzeCustomer(String customerId, String key) throws Exception {
        var conversation = createConversation("CUSTOMER_ASSISTANT", customerId, key);
        var turn = send(conversation, "请根据已确认资料分析续约沟通风险。", key + "-turn");
        complete(turn.path("taskId").asText());
        return task(turn.path("taskId").asText()).path("result");
    }

    private JsonNode queryQa(String key, String input) throws Exception {
        var conversation = createConversation("KNOWLEDGE_QA", null, key);
        var turn = send(conversation, input, key + "-turn");
        qaTaskIds.put(key, turn.path("taskId").asText());
        complete(turn.path("taskId").asText());
        return task(turn.path("taskId").asText()).path("result");
    }

    private String findTurnTask(String conversationKey) throws Exception {
        return java.util.Objects.requireNonNull(qaTaskIds.get(conversationKey), "QA Task was not created: " + conversationKey);
    }

    private JsonNode contextSnapshot(String taskId) throws Exception {
        var content = jdbc.queryForObject("select content from agent_runtime.step where task_id = ? and type = 'CONTEXT_SNAPSHOT' order by step_no limit 1",
                String.class, UUID.fromString(taskId));
        return json.readTree(content);
    }

    private void publishKnowledge() throws Exception {
        var document = json.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents")
                        .header("Authorization", AUTH).header("Idempotency-Key", "p11-knowledge")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of(
                                "title", "合成续约规则", "sourceRef", "demo://p11/renewal",
                                "content", "# 产品续约\n\n标准续约产品编号为 SKU-CLOUD-RENEW-12。标准续约应在到期前三十天联系客户。",
                                "metadata", Map.of("classification", "synthetic-demo")))))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var id = document.path("id").asText();
        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + id + "/chunks")
                        .param("chunkingVersion", "p9-structure-1").header("Authorization", AUTH))
                .andExpect(status().isOk());
        var build = json.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + id + "/index-builds")
                        .param("assetVersion", "1").param("chunkingVersion", "p9-structure-1")
                        .header("Authorization", AUTH).header("Idempotency-Key", "p11-knowledge-build"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + id
                        + "/publish?expectedVersion=1&buildId=" + build.path("id").asText())
                        .header("Authorization", AUTH).header("Idempotency-Key", "p11-knowledge-publish"))
                .andExpect(status().isOk());
    }

    private String createConversation(String mode, String customerId, String title) throws Exception {
        var body = new LinkedHashMap<String, Object>(); body.put("mode", mode); body.put("title", title); body.put("customerId", customerId);
        return json.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/conversations")
                        .header("Authorization", AUTH).header("Idempotency-Key", "p11-session-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(body)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).path("id").asText();
    }

    private JsonNode send(String conversationId, String input, String key) throws Exception {
        return json.readTree(mvc.perform(post(conversationPath(conversationId) + "/turns")
                        .header("Authorization", AUTH).header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of("input", input))))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
    }

    private void complete(String taskId) {
        var item = tasks.claimOne().orElseThrow();
        assertThat(item.id()).isEqualTo(UUID.fromString(taskId));
        tasks.complete(item, runtime.run(item));
    }

    private JsonNode task(String taskId) throws Exception {
        return json.readTree(mvc.perform(get(taskPath(taskId)).header("Authorization", AUTH))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
    }

    private JsonNode getCard(String cardId) throws Exception {
        return json.readTree(mvc.perform(get(cardPath() + "/" + cardId).header("Authorization", AUTH))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
    }

    private JsonNode createCard(Map<String, Object> body, String key) throws Exception {
        return json.readTree(mvc.perform(post(cardPath()).header("Authorization", AUTH).header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(body)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
    }

    private void publishCard(String cardId, long version, int revision) throws Exception {
        mvc.perform(post(cardPath() + "/" + cardId + "/versions/" + revision + "/publish")
                        .header("Authorization", AUTH).header("Idempotency-Key", "p11-publish-" + cardId)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of("expectedVersion", version))))
                .andExpect(status().isOk());
    }

    private String cardPath() { return "/api/v1/workspaces/" + WORKSPACE + "/experience-cards"; }
    private String taskPath(String taskId) { return "/api/v1/workspaces/" + WORKSPACE + "/tasks/" + taskId; }
    private String conversationPath(String id) { return "/api/v1/workspaces/" + WORKSPACE + "/conversations/" + id; }
}
