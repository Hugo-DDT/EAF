package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelFailure;
import io.eaf.model.api.ModelRequest;
import io.eaf.model.api.ModelResult;
import io.eaf.model.api.ModelToolCall;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Hashing;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
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
class P3PgvectorTest {
    // 只接受真实 pgvector 扩展；固定 amd64 digest 让本地与 CI 的类型行为一致。
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String WORKSPACE = "10000000-0000-4000-8000-000000000001";
    private static final UUID TENANT = UUID.fromString("70000000-0000-4000-8000-000000000001");
    private static final UUID ACTOR = UUID.fromString("80000000-0000-4000-8000-000000000001");

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
    @Autowired ObjectMapper mapper;
    @Autowired DataSource dataSource;
    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired RuntimeQuery runtimeQuery;
    @MockitoSpyBean ModelGateway modelGateway;

    @Test
    void realExtensionStoresAndOrdersConfiguredVectors() throws Exception {
        var document = mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-pgvector-document")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(Map.of("title", "向量规范", "sourceRef", "manual://vector",
                                "content", "a".repeat(900), "metadata", Map.of("category", "policy")))))
                .andExpect(status().isCreated()).andReturn();
        var documentId = mapper.readTree(document.getResponse().getContentAsString()).path("id").asText();
        var chunks = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId + "/chunks")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(chunks).hasSize(2);

        var jdbc = new JdbcTemplate(dataSource);
        var signature = Hashing.sha256("deterministic|p3-test-embedding-8|UNKNOWN|8|p3-plain-1|COSINE");
        insertVector(jdbc, UUID.fromString(documentId), chunks.get(0), signature, "[1,0,0,0,0,0,0,0]");
        insertVector(jdbc, UUID.fromString(documentId), chunks.get(1), signature, "[0.8,0.6,0,0,0,0,0,0]");

        assertThat(jdbc.queryForObject("select extname from pg_extension where extname = 'vector'", String.class)).isEqualTo("vector");
        var ordered = jdbc.queryForList("select chunk_id::text from knowledge.embedding where tenant_id = ? and workspace_id = ? and document_id = ? and configuration_signature = ? order by embedding <=> ?::public.vector",
                String.class, TENANT, UUID.fromString(WORKSPACE), UUID.fromString(documentId), signature, "[1,0,0,0,0,0,0,0]");
        assertThat(ordered).containsExactly(chunks.get(0).path("id").asText(), chunks.get(1).path("id").asText());
        // 使用新的合法签名隔离唯一约束，确保失败原因确实来自 pgvector 维度校验。
        assertThatThrownBy(() -> insertVector(jdbc, UUID.fromString(documentId), chunks.get(0),
                        Hashing.sha256("wrong-dimension"), "[1,0]"))
                .isInstanceOf(DataAccessException.class);
    }

    @Test
    void buildsIndexInBoundedBatchesAndReplaysSameBuild() throws Exception {
        var document = mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-pgvector-build-document")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(Map.of("title", "索引作业", "sourceRef", "manual://index",
                                "content", "b".repeat(900), "metadata", Map.of("category", "policy")))))
                .andExpect(status().isCreated()).andReturn();
        var documentId = mapper.readTree(document.getResponse().getContentAsString()).path("id").asText();
        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId + "/chunks")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk());

        // 作业响应必须反映完整批次校验；重复构建键只能返回同一 build，不能再次产生向量。
        var first = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId + "/index-builds")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-pgvector-index-build"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(first.path("status").asText()).isEqualTo("READY");
        assertThat(first.path("totalChunks").asInt()).isEqualTo(2);
        assertThat(first.path("completedChunks").asInt()).isEqualTo(2);
        assertThat(first.path("embeddingCalls").asInt()).isEqualTo(1);

        var repeated = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId + "/index-builds")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-pgvector-index-build"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(repeated.path("id").asText()).isEqualTo(first.path("id").asText());
        assertThat(new JdbcTemplate(dataSource).queryForObject("select count(*) from knowledge.embedding where build_id = ?", Integer.class,
                UUID.fromString(first.path("id").asText()))).isEqualTo(2);

        var published = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId
                        + "/publish?expectedVersion=1&buildId=" + first.path("id").asText())
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-publish"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(published.path("action").asText()).isEqualTo("PUBLISHED");
        assertThat(published.path("documentRowVersion").asLong()).isEqualTo(2);
        var repeatedPublish = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId
                        + "/publish?expectedVersion=1&buildId=" + first.path("id").asText())
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-publish"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(repeatedPublish.path("eventId").asText()).isEqualTo(published.path("eventId").asText());

        var revoked = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId
                        + "/revoke?expectedVersion=2")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-revoke"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(revoked.path("action").asText()).isEqualTo("REVOKED");
        assertThat(revoked.path("documentRowVersion").asLong()).isEqualTo(3);
        var jdbc = new JdbcTemplate(dataSource);
        assertThat(jdbc.queryForObject("select status from knowledge.document where id = ?", String.class, UUID.fromString(documentId))).isEqualTo("REVOKED");
        assertThat(jdbc.queryForObject("select count(*) from knowledge.publication_event where document_id = ?", Integer.class, UUID.fromString(documentId))).isEqualTo(2);
    }

    @Test
    void rejectsIndexBuildBeforeChunksExist() throws Exception {
        var document = mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-pgvector-no-chunks-document")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(Map.of("title", "未切块", "sourceRef", "manual://empty-index",
                                "content", "尚未切块的正文", "metadata", Map.of()))))
                .andExpect(status().isCreated()).andReturn();
        var documentId = mapper.readTree(document.getResponse().getContentAsString()).path("id").asText();
        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId + "/index-builds")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-pgvector-no-chunks-build"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("INDEX_BUILD_NO_CHUNKS"));
    }

    @Test
    void searchFiltersDocumentPermissionAndRevocationInsideRecall() throws Exception {
        var visible = createPublishedDocument("p3-search-visible", "可见规范", "允许访问的正式规范");
        var hidden = createPublishedDocument("p3-search-hidden", "受限规范", "同空间但已撤销读取授权的规范");
        var jdbc = new JdbcTemplate(dataSource);
        // 只撤销文档级读取权，保留 Workspace 权限，验证过滤发生在 pgvector 召回 SQL 内。
        jdbc.update("update knowledge.document_permission set status = 'REVOKED' where document_id = ? and actor_id = ? and action = 'knowledge:read'",
                hidden.documentId(), ACTOR);

        var first = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/search")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"query\":\"规范\",\"topK\":10}"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(first.path("hits").toString()).contains(visible.documentId().toString()).doesNotContain(hidden.documentId().toString());

        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + visible.documentId()
                        + "/revoke?expectedVersion=2")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-search-visible-revoke"))
                .andExpect(status().isOk());
        var afterRevoke = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/search")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"query\":\"规范\",\"topK\":10}"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(afterRevoke.path("hits").toString()).doesNotContain(visible.documentId().toString(), hidden.documentId().toString());
    }

    @Test
    void rebuildsPublishedDocumentIntoSeparateBuildBeforeSwitchingRecall() throws Exception {
        var content = "配置切换验证文本：旧索引继续服务，新索引完成后才切换。";
        var document = createPublishedDocument("p3-rebuild", "重建规范", content);
        var oldSearch = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/search")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(Map.of("query", content, "topK", 10))))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(oldSearch.path("hits").toString()).contains(document.buildId().toString());

        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + document.documentId() + "/chunks?chunkingVersion=p3-plain-2")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk());
        var rebuilt = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + document.documentId()
                        + "/index-builds?chunkingVersion=p3-plain-2")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-rebuild-build-v2"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        var newBuild = UUID.fromString(rebuilt.path("id").asText());
        assertThat(rebuilt.path("status").asText()).isEqualTo("READY");
        assertThat(newBuild).isNotEqualTo(document.buildId());

        var beforeSwitch = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/search")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(Map.of("query", content, "topK", 10))))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(beforeSwitch.path("hits").toString()).contains(document.buildId().toString()).doesNotContain(newBuild.toString());

        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + document.documentId()
                        + "/publish?expectedVersion=2&buildId=" + newBuild)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p3-rebuild-switch"))
                .andExpect(status().isOk());
        var afterSwitch = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/search")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON).content(mapper.writeValueAsString(Map.of("query", content, "topK", 10))))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(afterSwitch.path("hits").toString()).contains(newBuild.toString()).doesNotContain(document.buildId().toString());
    }

    // 固定评测通过持久 Task/Runtime 验证 RAG 输出形状，不把评测答案写入知识库。
    @Test
    void runsFixedP3EvaluationThroughPersistentTasksAndReportsCitationShape() throws Exception {
        //  的本地固定样本验证共享运行 Scope、幂等恢复和答案隔离，不宣称真实 Provider 质量。
        // 多次评测共用价格 fixture 时按唯一键幂等插入，避免用例顺序影响数据库状态。
        new JdbcTemplate(dataSource).update("insert into usage.price_schedule(provider, model, call_type, price_version, source, source_version, currency, billing_unit, input_price_per_million, output_price_per_million, effective_at) values ('deterministic', 'p1-test', 'CHAT', 'p7-09-fixture-v1', 'local-fixture', 'p7-09-test', 'USD', 'TOKEN_MILLION', 1, 1, '2026-01-01T00:00:00Z') on conflict do nothing");
        var first = mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/evaluations/p3")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p7-09-p3-run-replay"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        var report = mapper.readTree(first);
        var reportId = UUID.fromString(report.path("id").asText());
        assertThat(report.path("manifest").path("datasetVersion").asText()).isEqualTo("p3-v2");
        assertThat(report.path("manifest").path("agentVersion").asText()).isEqualTo("3.1.0");
        assertThat(report.path("manifest").path("promptVersion").asText()).isEqualTo("3.1.0");
        assertThat(report.path("totalSamples").asInt()).isEqualTo(60);
        assertThat(report.path("completedSamples").asInt()).isEqualTo(60);
        assertThat(report.path("passedSamples").asInt()).isEqualTo(60);
        assertThat(report.path("status").asText()).isEqualTo("PASSED");
        assertThat(report.path("modelCalls").asInt()).isEqualTo(60);
        assertThat(report.path("estimatedCost").decimalValue()).isEqualByComparingTo("0.00912000");
        assertThat(report.path("samples")).hasSize(60);
        assertThat(first).doesNotContain("inputText", "expectedRisk");

        var jdbc = new JdbcTemplate(dataSource);
        assertThat(jdbc.queryForObject("select count(*) from evaluation.dataset_case where dataset_version = 'p3-v1'", Integer.class)).isEqualTo(20);
        assertThat(jdbc.queryForObject("select count(*) from evaluation.dataset_case where dataset_version = 'p3-v2'", Integer.class)).isEqualTo(20);
        assertThat(jdbc.queryForObject("select count(*) from task.task where quality_run_id = ? and source = 'EVALUATION'",
                Integer.class, reportId)).isEqualTo(60);
        assertThat(jdbc.queryForObject("select count(*) from evaluation.eval_result where run_id = ?",
                Integer.class, reportId)).isEqualTo(60);
        assertThat(jdbc.queryForObject("select count(*) from usage.model_usage where scope_type = 'EVALUATION' and scope_id = ?",
                Integer.class, reportId)).isEqualTo(60);
        assertThat(jdbc.queryForObject("select count(*) from usage.model_usage u join task.task t on t.id = u.task_id where t.quality_run_id = ? and (u.scope_type <> 'EVALUATION' or u.scope_id is distinct from ?)",
                Integer.class, reportId, reportId)).isZero();
        var evaluationActor = new ActorContext(ACTOR, TENANT, ActorType.HUMAN, Set.of("evaluation:run", "task:read"));
        var firstSampleTask = UUID.fromString(report.path("samples").get(0).path("taskId").asText());
        assertThat(runtimeQuery.contextSources(evaluationActor, UUID.fromString(WORKSPACE), firstSampleTask)).isEmpty();
        assertThat(jdbc.queryForObject("select count(*) from agent_runtime.step s join agent_runtime.run r on r.id = s.run_id join task.task t on t.id = r.task_id where t.quality_run_id = ? and s.type in ('TOOL_CALL', 'TOOL_RESULT')",
                Integer.class, reportId)).isZero();

        var repeated = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/evaluations/p3")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p7-09-p3-run-replay"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(repeated.path("id").asText()).isEqualTo(reportId.toString());
        assertThat(jdbc.queryForObject("select count(*) from task.task where quality_run_id = ?",
                Integer.class, reportId)).isEqualTo(60);

        // evaluation:run 不能隐式读取报告；只有单独获得 evaluation:read 后才返回报告。
        jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where workspace_id = ? and actor_id = ? and action = 'evaluation:read'",
                UUID.fromString(WORKSPACE), ACTOR);
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get(
                        "/api/v1/workspaces/" + WORKSPACE + "/evaluations/p3/" + reportId)
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isForbidden());
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'evaluation:read', 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                TENANT, UUID.fromString(WORKSPACE), ACTOR);
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get(
                        "/api/v1/workspaces/" + WORKSPACE + "/evaluations/p3/" + reportId)
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.id").value(reportId.toString()));
    }

    @Test
    void stopsQualityRunWhenSafetySampleFails() throws Exception {
        var jdbc = new JdbcTemplate(dataSource);
        // 价格 fixture 为样本调用提供可核验用量；冲突安全插入避免与同类正例重复写入。
        jdbc.update("insert into usage.price_schedule(provider, model, call_type, price_version, source, source_version, currency, billing_unit, input_price_per_million, output_price_per_million, effective_at) values ('deterministic', 'p1-test', 'CHAT', 'p7-09-fixture-v1', 'local-fixture', 'p7-09-test', 'USD', 'TOKEN_MILLION', 1, 1, '2026-01-01T00:00:00Z') on conflict do nothing");
        // 故意让安全样本的固定答案与确定性模型结果冲突，验证硬门槛不被总体成功率抵消。
        var changed = jdbc.update("update evaluation.dataset_case set expected_risk = 'HIGH' where dataset_version = 'p3-v2' and case_id = 'boundary-01' and expected_risk = 'MEDIUM'");
        assertThat(changed).isEqualTo(1);

        String response;
        try {
            response = mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/evaluations/p3")
                            .header("Authorization", "Bearer eaf-local-alice")
                            .header("Idempotency-Key", "p7-09-p3-safety-gate-negative"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        } finally {
            // 用例只在本次独立评测运行中制造一条错误预期，避免污染后续数据库测试。
            jdbc.update("update evaluation.dataset_case set expected_risk = 'MEDIUM' where dataset_version = 'p3-v2' and case_id = 'boundary-01' and expected_risk = 'HIGH'");
        }

        var report = mapper.readTree(response);
        assertThat(report.path("totalSamples").asInt()).isEqualTo(60);
        assertThat(report.path("completedSamples").asInt()).isEqualTo(1);
        assertThat(report.path("passedSamples").asInt()).isZero();
        assertThat(report.path("safetyViolations").asInt()).isZero();
        assertThat(report.path("status").asText()).isEqualTo("FAILED");
        assertThat(report.path("failureReason").asText()).isEqualTo("SAFETY_CASE_FAILED");
        assertThat(report.path("samples").findValuesAsText("caseId").stream()
                .filter("boundary-01"::equals).count()).isEqualTo(1);
    }

    @Test
    void keepsProviderDependencyFailureDistinctFromSafetyScoring() throws Exception {
        var jdbc = new JdbcTemplate(dataSource);
        // Provider 尚不可用时仍记录首个失败样本并停止，不把缺失模型答案伪装成安全评分结果。
        jdbc.update("insert into usage.price_schedule(provider, model, call_type, price_version, source, source_version, currency, billing_unit, input_price_per_million, output_price_per_million, effective_at) values ('deterministic', 'p1-test', 'CHAT', 'p7-09-fixture-v1', 'local-fixture', 'p7-09-test', 'USD', 'TOKEN_MILLION', 1, 1, '2026-01-01T00:00:00Z') on conflict do nothing");
        doThrow(new ModelFailure("DEPENDENCY_UNAVAILABLE", "本地 Provider 配置不可用。", false, false))
                .when(modelGateway).call(any(ModelRequest.class));

        var response = mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/evaluations/p3")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p7-09-p3-provider-dependency-failure"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        var report = mapper.readTree(response);

        assertThat(report.path("status").asText()).isEqualTo("FAILED");
        assertThat(report.path("failureReason").asText()).isEqualTo("DEPENDENCY_UNAVAILABLE");
        assertThat(report.path("costStatus").asText()).isEqualTo("UNKNOWN_PRICE");
        assertThat(report.path("completedSamples").asInt()).isEqualTo(1);
        assertThat(report.path("failedSamples").asInt()).isEqualTo(1);
        // 模型调用次数按已记录的 Gateway 尝试统计；失败原因与用量计价状态各自保留。
        assertThat(report.path("modelCalls").asInt()).isEqualTo(1);
        assertThat(report.path("safetyViolations").asInt()).isZero();
        assertThat(report.path("samples").get(0).path("errorCode").asText()).isEqualTo("DEPENDENCY_UNAVAILABLE");
        verify(modelGateway).call(any(ModelRequest.class));
    }

    // 旧失败运行的幂等键不能跨到新数据集，避免借版本升级绕过终态。
    @Test
    void doesNotReuseFailedP3V1RegistrationForNewDatasetVersion() throws Exception {
        var jdbc = new JdbcTemplate(dataSource);
        var oldRunId = UUID.randomUUID();
        var idempotencyKey = "p7-09-p3-v1-terminal-key";
        var now = Timestamp.from(Instant.now());
        var requestHash = Hashing.sha256(String.join("\u001f", "RAG_HELD_OUT", "EVALUATION",
                TENANT.toString(), WORKSPACE, ACTOR.toString()));
        jdbc.update("insert into evaluation.quality_run_registration(id, tenant_id, workspace_id, owner_id, idempotency_key, purpose, source, request_hash, status, created_at) values (?, ?, ?, ?, ?, 'RAG_HELD_OUT', 'EVALUATION', ?, 'FAILED', ?)",
                oldRunId, TENANT, UUID.fromString(WORKSPACE), ACTOR, idempotencyKey, requestHash, now);
        jdbc.update("insert into evaluation.eval_run(id, tenant_id, workspace_id, dataset_version, status, passed, total, started_at, started_by, configuration_hash, manifest_snapshot, failure_reason) values (?, ?, ?, 'p3-v1', 'FAILED', 0, 60, ?, ?, ?, '{}'::jsonb, 'SAFETY_CASE_FAILED')",
                oldRunId, TENANT, UUID.fromString(WORKSPACE), now, ACTOR, Hashing.sha256("terminal-p3-v1"));

        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/evaluations/p3")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", idempotencyKey))
                .andExpect(status().isConflict());

        assertThat(jdbc.queryForObject("select dataset_version from evaluation.eval_run where id = ?", String.class, oldRunId))
                .isEqualTo("p3-v1");
        assertThat(jdbc.queryForObject("select status from evaluation.eval_run where id = ?", String.class, oldRunId))
                .isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("select count(*) from task.task where quality_run_id = ?", Integer.class, oldRunId)).isZero();
    }

    @Test
    void failsClosedWhenHeldOutModelSuggestsBusinessTool() throws Exception {
        var jdbc = new JdbcTemplate(dataSource);
        // 保留集 Task 不声明任何工具；模拟模型忽略该限制仍请求 CRM 写工具。
        jdbc.update("insert into usage.price_schedule(provider, model, call_type, price_version, source, source_version, currency, billing_unit, input_price_per_million, output_price_per_million, effective_at) values ('deterministic', 'p1-test', 'CHAT', 'p7-09-fixture-v1', 'local-fixture', 'p7-09-test', 'USD', 'TOKEN_MILLION', 1, 1, '2026-01-01T00:00:00Z') on conflict do nothing");
        doReturn(new ModelResult("deterministic", "p1-test", null, 11, 7, "KNOWN",
                List.of(new ModelToolCall("p7-09-forbidden-tool-call", "crm.followup.create", "{}")), "TOOL_CALLS"))
                .when(modelGateway).call(any(ModelRequest.class));

        var response = mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/evaluations/p3")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p7-09-p3-forbidden-tool-call"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        var report = mapper.readTree(response);
        var jdbcRunId = UUID.fromString(report.path("id").asText());
        var taskId = UUID.fromString(report.path("samples").get(0).path("taskId").asText());

        assertThat(report.path("status").asText()).isEqualTo("FAILED");
        assertThat(report.path("failureReason").asText()).isEqualTo("SAFETY_VIOLATION");
        assertThat(report.path("safetyViolations").asInt()).isEqualTo(1);
        assertThat(report.path("completedSamples").asInt()).isEqualTo(1);
        verify(modelGateway).call(argThat(request -> request.tools().isEmpty()));
        assertThat(jdbc.queryForObject("select count(*) from agent_runtime.step s join agent_runtime.run r on r.id = s.run_id where r.task_id = ? and s.type = 'TOOL_CALL'",
                Integer.class, taskId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from agent_runtime.step s join agent_runtime.run r on r.id = s.run_id where r.task_id = ? and s.type = 'TOOL_RESULT'",
                Integer.class, taskId)).isZero();
        // 未授权提议不得穿过 Execution 边界创建业务副作用记录。
        assertThat(jdbc.queryForObject("select count(*) from execution.execution where task_id = ?",
                Integer.class, taskId)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from task.task where quality_run_id = ? and source = 'EVALUATION'",
                Integer.class, jdbcRunId)).isEqualTo(1);
    }

    @Test
    void resumesStoppedQualityRunWithOriginalSampleAndUsageKeys() throws Exception {
        var jdbc = new JdbcTemplate(dataSource);
        jdbc.update("insert into usage.price_schedule(provider, model, call_type, price_version, source, source_version, currency, billing_unit, input_price_per_million, output_price_per_million, effective_at) values ('deterministic', 'p1-test', 'CHAT', 'p7-09-fixture-v1', 'local-fixture', 'p7-09-test', 'USD', 'TOKEN_MILLION', 1, 1, '2026-01-01T00:00:00Z') on conflict do nothing");
        // 暂缓首个样本结果写入，为并发 stop 请求制造确定窗口；只影响本测试数据库。
        jdbc.execute("create or replace function evaluation.p7_test_delay_first_p3_result() returns trigger language plpgsql as $$ begin if new.case_id = 'boundary-01' and new.sample_no = 1 then perform pg_sleep(4); end if; return new; end; $$");
        jdbc.execute("create trigger p7_test_delay_first_p3_result before insert on evaluation.eval_result for each row execute function evaluation.p7_test_delay_first_p3_result()");

        var idempotencyKey = "p7-09-p3-stop-resume-local";
        var running = CompletableFuture.supplyAsync(() -> {
            try {
                return mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/evaluations/p3")
                                .header("Authorization", "Bearer eaf-local-alice")
                                .header("Idempotency-Key", idempotencyKey))
                        .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
            } catch (Exception failure) {
                throw new IllegalStateException(failure);
            }
        });

        UUID runId = null;
        try {
            var registrationDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (runId == null && System.nanoTime() < registrationDeadline && !running.isDone()) {
                runId = jdbc.query("select id from evaluation.quality_run_registration where tenant_id = ? and workspace_id = ? and idempotency_key = ?",
                        rs -> rs.next() ? rs.getObject("id", UUID.class) : null,
                        TENANT, UUID.fromString(WORKSPACE), idempotencyKey);
                if (runId == null) Thread.sleep(10);
            }
            assertThat(runId).as("质量运行应先持久化注册，随后才执行样本").isNotNull();

            var firstTaskDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            var firstTaskIds = List.<UUID>of();
            while (firstTaskIds.isEmpty() && System.nanoTime() < firstTaskDeadline && !running.isDone()) {
                firstTaskIds = jdbc.query("select id from task.task where quality_run_id = ? and source = 'EVALUATION'",
                        (rs, row) -> rs.getObject("id", UUID.class), runId);
                if (firstTaskIds.isEmpty()) Thread.sleep(10);
            }
            assertThat(firstTaskIds).hasSize(1);
            assertThat(jdbc.queryForObject("select count(*) from evaluation.eval_result where run_id = ?", Integer.class, runId))
                    .as("首个样本仍在受控延迟窗口内")
                    .isZero();

            var stopResponse = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE
                                    + "/evaluations/p3/" + runId + "/stop")
                            .header("Authorization", "Bearer eaf-local-alice"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            assertThat(stopResponse.path("status").asText()).isIn("STOP_REQUESTED", "STOPPED");

            var stopped = mapper.readTree(running.get(20, TimeUnit.SECONDS));
            assertThat(stopped.path("id").asText()).isEqualTo(runId.toString());
            assertThat(stopped.path("status").asText()).isEqualTo("STOPPED");
            assertThat(stopped.path("completedSamples").asInt()).isEqualTo(1);
            assertThat(stopped.path("samples")).hasSize(1);
            var savedTaskId = stopped.path("samples").get(0).path("taskId").asText();
            assertThat(UUID.fromString(savedTaskId)).isEqualTo(firstTaskIds.getFirst());
            assertThat(jdbc.queryForObject("select count(*) from usage.model_usage where scope_type = 'EVALUATION' and scope_id = ?",
                    Integer.class, runId)).isEqualTo(1);

            // 同一运行键恢复只补齐缺失样本，已完成 Task 与 Usage 不会被重复创建或计费。
            var resumed = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/evaluations/p3")
                            .header("Authorization", "Bearer eaf-local-alice")
                            .header("Idempotency-Key", idempotencyKey))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            assertThat(resumed.path("id").asText()).isEqualTo(runId.toString());
            assertThat(resumed.path("status").asText()).isEqualTo("PASSED");
            assertThat(resumed.path("completedSamples").asInt()).isEqualTo(60);
            assertThat(resumed.path("samples").findValuesAsText("taskId")).contains(savedTaskId);
            assertThat(jdbc.queryForObject("select count(*) from task.task where quality_run_id = ? and source = 'EVALUATION'",
                    Integer.class, runId)).isEqualTo(60);
            assertThat(jdbc.queryForObject("select count(*) from usage.model_usage where scope_type = 'EVALUATION' and scope_id = ?",
                    Integer.class, runId)).isEqualTo(60);
        } finally {
            if (runId != null && !running.isDone()) {
                try {
                    mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/evaluations/p3/" + runId + "/stop")
                            .header("Authorization", "Bearer eaf-local-alice"));
                    running.get(20, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    running.cancel(true);
                }
            }
            jdbc.execute("drop trigger if exists p7_test_delay_first_p3_result on evaluation.eval_result");
            jdbc.execute("drop function if exists evaluation.p7_test_delay_first_p3_result()");
        }
    }

    @Test
    void contextKeepsCitationsAndDropsWholeChunksWhenBudgetRunsOut() throws Exception {
        var document = createPublishedDocument("p3-context", "上下文规范", "这是本次实际提供给模型的正式知识片段。");
        var context = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/context/queries")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"query\":\"规范\",\"topK\":5,\"tokenBudget\":100}"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(context.path("status").asText()).isEqualTo("READY");
        assertThat(context.path("items").toString()).contains(document.documentId().toString(), "kb-");

        var exhausted = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/context/queries")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"query\":\"规范\",\"topK\":5,\"tokenBudget\":1}"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(exhausted.path("status").asText()).isEqualTo("BUDGET_EXHAUSTED");
        assertThat(exhausted.path("items")).isEmpty();
        assertThat(exhausted.path("omittedCount").asInt()).isGreaterThan(0);
    }

    @Test
    void p9StructuredHybridQaAndConfirmedFollowupUseFixedProfiles() throws Exception {
        var content = "# 续约服务\n\n## 标准续约产品\n\n标准续约产品编号为 SKU-CLOUD-RENEW-12。续约团队应在合同到期日前三十天联系客户。\n\n```text\n# 围栏中的标题不是结构标题\n```";
        var createdDocument = mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p9-structured-document")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(Map.of("title", "续约产品规则",
                                "sourceRef", "demo://p9/test-renewal", "content", content,
                                "metadata", Map.of("classification", "synthetic-demo")))))
                .andExpect(status().isCreated()).andReturn();
        var documentId = UUID.fromString(mapper.readTree(createdDocument.getResponse().getContentAsString()).path("id").asText());
        var chunks = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId + "/chunks")
                        .param("chunkingVersion", "p9-structure-1")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(chunks.size()).isGreaterThan(0);
        assertThat(chunks.get(0).path("headingPath").toString()).isEqualTo("[\"续约服务\",\"标准续约产品\"]");
        assertThat(chunks.get(0).path("offsetUnit").asText()).isEqualTo("UNICODE_CODE_POINT");

        var build = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId + "/index-builds")
                        .param("assetVersion", "1").param("chunkingVersion", "p9-structure-1")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p9-structured-build"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(build.path("status").asText()).isEqualTo("READY");
        assertThat(new JdbcTemplate(dataSource).queryForObject(
                "select count(*) from knowledge.lexical_entry where build_id = ?", Integer.class,
                UUID.fromString(build.path("id").asText()))).isEqualTo(chunks.size());
        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId
                        + "/publish?expectedVersion=1&buildId=" + build.path("id").asText())
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p9-structured-publish"))
                .andExpect(status().isOk());

        var hybrid = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/search")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"query\":\"SKU-CLOUD-RENEW-12\",\"topK\":5,\"mode\":\"HYBRID\"}"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        var lexicalHit = java.util.stream.StreamSupport.stream(hybrid.path("hits").spliterator(), false)
                .filter(hit -> hit.path("documentId").asText().equals(documentId.toString())).findFirst().orElseThrow();
        assertThat(lexicalHit.path("channels").toString()).contains("LEXICAL");
        assertThat(lexicalHit.path("lexicalRank").asInt()).isEqualTo(1);
        assertThat(lexicalHit.path("headingPath").toString()).isEqualTo("[\"续约服务\",\"标准续约产品\"]");
        var listed = mapper.readTree(mvc.perform(get("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents?limit=20")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(listed.path("items").toString()).contains(documentId.toString());

        var qaCreated = mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/tasks")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p9-qa-task")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"agentId\":\"20000000-0000-4000-8000-000000000009\",\"agentVersion\":\"1.0.0\",\"input\":\"标准续约产品编号是什么？\"}"))
                .andExpect(status().isAccepted()).andReturn();
        var qaId = UUID.fromString(mapper.readTree(qaCreated.getResponse().getContentAsString()).path("id").asText());
        var qaWork = tasks.claimOne().orElseThrow();
        assertThat(qaWork.id()).isEqualTo(qaId);
        tasks.complete(qaWork, runtime.run(qaWork));
        mvc.perform(get("/api/v1/workspaces/" + WORKSPACE + "/tasks/" + qaId)
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCEEDED"))
                .andExpect(jsonPath("$.result.answerStatus").value("ANSWERED"))
                .andExpect(jsonPath("$.result.citations[0]").value("kb-1"));
        mvc.perform(get("/api/v1/workspaces/" + WORKSPACE + "/tasks/" + qaId + "/sources")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].content").value(org.hamcrest.Matchers.containsString("SKU-CLOUD-RENEW-12")));
        mvc.perform(get("/api/v1/workspaces/" + WORKSPACE + "/tasks?limit=20")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[*].id").value(org.hamcrest.Matchers.hasItem(qaId.toString())));

        var followupCreated = mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/tasks")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p9-followup-task")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"agentId\":\"20000000-0000-4000-8000-00000000000a\",\"agentVersion\":\"1.0.0\","
                                + "\"input\":\"客户 ID: synthetic-customer-001\\n客户材料：\\n客户续约中断，最近发生重大投诉，需核实联系记录。\","
                                + "\"businessEntity\":{\"type\":\"CUSTOMER\",\"id\":\"synthetic-customer-001\"}}"))
                .andExpect(status().isAccepted()).andReturn();
        var followupId = UUID.fromString(mapper.readTree(followupCreated.getResponse().getContentAsString()).path("id").asText());
        var followupWork = tasks.claimOne().orElseThrow();
        assertThat(followupWork.id()).isEqualTo(followupId);
        tasks.complete(followupWork, runtime.run(followupWork));
        var followupTask = mapper.readTree(mvc.perform(get("/api/v1/workspaces/" + WORKSPACE + "/tasks/" + followupId)
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(followupTask.path("result").path("followupDraft").path("customerId").asText())
                .isEqualTo("synthetic-customer-001");
        var confirmation = "{\"expectedVersion\":" + followupTask.path("version").asLong()
                + ",\"summary\":\"建议联系客户核实续约及投诉处理情况。\"}";
        var confirmationPath = "/api/v1/workspaces/" + WORKSPACE + "/tasks/" + followupId + "/followups";
        var firstFollowup = mapper.readTree(mvc.perform(post(confirmationPath)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p9-followup-confirm")
                        .contentType(MediaType.APPLICATION_JSON).content(confirmation))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        var replayedFollowup = mapper.readTree(mvc.perform(post(confirmationPath)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p9-followup-confirm")
                        .contentType(MediaType.APPLICATION_JSON).content(confirmation))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        assertThat(replayedFollowup.path("instanceId").asText()).isEqualTo(firstFollowup.path("instanceId").asText());
        mvc.perform(post(confirmationPath)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", "p9-followup-confirm")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(confirmation.replace("建议联系客户核实续约及投诉处理情况", "未经核实直接承诺解决")))
                .andExpect(status().isConflict());
        mvc.perform(get(confirmationPath + "?limit=20").header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].instanceId").value(firstFollowup.path("instanceId").asText()));

        var approvalId = UUID.randomUUID();
        var approvalCreatedAt = Instant.now();
        var approvalExpiry = approvalCreatedAt.plusSeconds(300);
        new JdbcTemplate(dataSource).update("insert into approval.request(id, tenant_id, workspace_id, task_id, execution_id, binding_hash, binding_json, state, expires_at, row_version, created_at) values (?, ?, ?, ?, ?, ?, '{}'::jsonb, 'PENDING', ?, 1, ?)",
                approvalId, TENANT, UUID.fromString(WORKSPACE), followupId, UUID.randomUUID(), "p9-pending-approval-test",
                Timestamp.from(approvalExpiry), Timestamp.from(approvalCreatedAt));
        var pendingApprovals = mapper.readTree(mvc.perform(get("/api/v1/workspaces/" + WORKSPACE + "/approvals?limit=20")
                        .header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items[0].id").value(approvalId.toString()))
                .andExpect(jsonPath("$.items[0].taskId").value(followupId.toString()))
                .andReturn().getResponse().getContentAsString());
        assertThat(pendingApprovals.path("items").get(0).has("binding")).isFalse();
    }

    @Test
    void ragRuntimeStoresContextSnapshotAndAcceptsOnlyItsCitations() throws Exception {
        var content = "P3正式知识：客户明确续约时，风险等级按低风险处理。";
        var document = createPublishedDocument("p3-runtime-rag", "RAG 运行规范", content);
        var actor = new ActorContext(ACTOR, TENANT, ActorType.HUMAN, Set.of("task:create", "task:read"));
        var created = tasks.create(new CreateTaskCommand(actor, UUID.fromString(WORKSPACE),
                UUID.fromString("20000000-0000-4000-8000-000000000001"), "3.0.0", content,
                null, null, "p3-runtime-rag-task", "trace-p3-runtime-rag", "USER"));
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(created.id());
        tasks.complete(work, runtime.run(work));

        var snapshot = tasks.get(actor, UUID.fromString(WORKSPACE), created.id());
        assertThat(snapshot.status().name()).isEqualTo("SUCCEEDED");
        var result = mapper.readTree(snapshot.resultJson());
        assertThat(result.path("citations").toString()).isEqualTo("[\"kb-1\"]");
        var contextStep = runtime instanceof io.eaf.agentruntime.api.RuntimeQuery query
                ? query.steps(TENANT, created.id()).stream().filter(step -> "CONTEXT_SNAPSHOT".equals(step.type())).findFirst().orElseThrow()
                : null;
        assertThat(contextStep).isNotNull();
        assertThat(contextStep.content()).contains(document.documentId().toString(), "kb-1");

        // 撤回后新请求不再使用旧片段，历史正文和回放结果也按当前读取授权收敛。
        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + document.documentId()
                        + "/revoke?expectedVersion=2")
                .header("Authorization", "Bearer eaf-local-alice")
                .header("Idempotency-Key", "p3-runtime-rag-revoke"))
                .andExpect(status().isOk());
        assertThat(runtimeQuery.canExposeResult(actor, UUID.fromString(WORKSPACE), created.id())).isFalse();
        var redacted = runtimeQuery.steps(actor, UUID.fromString(WORKSPACE), created.id()).stream()
                .filter(step -> "CONTEXT_SNAPSHOT".equals(step.type())).findFirst().orElseThrow();
        assertThat(redacted.content()).isNull();
        assertThat(runtimeQuery.replay(actor, UUID.fromString(WORKSPACE), created.id()).errorCode())
                .isEqualTo("REPLAY_CONTEXT_UNAVAILABLE");
    }

    private PublishedDocument createPublishedDocument(String key, String title, String content) throws Exception {
        var document = mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(Map.of("title", title, "sourceRef", "manual://search/" + key,
                                "content", content, "metadata", Map.of()))))
                .andExpect(status().isCreated()).andReturn();
        var documentId = UUID.fromString(mapper.readTree(document.getResponse().getContentAsString()).path("id").asText());
        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId + "/chunks")
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk());
        var build = mapper.readTree(mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId + "/index-builds")
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", key + "-build"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        var buildId = UUID.fromString(build.path("id").asText());
        mvc.perform(post("/api/v1/workspaces/" + WORKSPACE + "/knowledge/documents/" + documentId
                        + "/publish?expectedVersion=1&buildId=" + buildId)
                        .header("Authorization", "Bearer eaf-local-alice")
                        .header("Idempotency-Key", key + "-publish"))
                .andExpect(status().isOk());
        return new PublishedDocument(documentId, buildId);
    }

    private record PublishedDocument(UUID documentId, UUID buildId) { }

    private void insertVector(JdbcTemplate jdbc, UUID documentId, JsonNode chunk, String signature, String vector) {
        jdbc.update("insert into knowledge.embedding(id, tenant_id, workspace_id, document_id, asset_version, chunk_id, provider, model, model_revision, dimension, chunking_version, distance_metric, configuration_signature, embedding, created_at) values (?, ?, ?, ?, 1, ?, 'deterministic', 'p3-test-embedding-8', 'UNKNOWN', 8, 'p3-plain-1', 'COSINE', ?, ?::public.vector, ?)",
                UUID.randomUUID(), TENANT, UUID.fromString(WORKSPACE), documentId, UUID.fromString(chunk.path("id").asText()), signature, vector, Timestamp.from(Instant.now()));
    }
}
// 本测试使用真实 pgvector 验证扩展、固定维度、配置签名和精确余弦距离排序，不把向量直接暴露为业务 API。
