package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.context.api.ContextSourceRef;
import io.eaf.context.api.EvaluationContextSnapshotReader;
import io.eaf.context.api.EnterpriseContext;
import io.eaf.evaluation.api.EvaluationService;
import io.eaf.evaluation.api.CandidateSampleReviewCommand;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.learning.api.Feedback;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.usage.api.UsageRecorder;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false"})
@AutoConfigureMockMvc
// 验证规则提议、人工候选、独立事实审核、修订失效和候选/正式资产隔离。
class P5CandidateLifecycleTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
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
    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired EvaluationService evaluations;
    @Autowired EvaluationContextSnapshotReader evaluationContexts;
    @Autowired KnowledgeService knowledge;
    @Autowired UsageRecorder usage;

    @Test
    void proposesBothAssetTypesWithoutPublishingAndInvalidatesReviewsOnRevision() throws Exception {
        var knowledgeId = createPublishedDocument("p5-candidate-knowledge", "候选知识基线", "已发布基线：候选不能进入普通检索。");
        var task = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "3.0.0",
                "已发布基线：候选不能进入普通检索。", null, null, "p5-candidate-task", "trace-p5-candidate", "USER"));
        var work = tasks.claimOne().orElseThrow();
        tasks.complete(work, runtime.run(work));
        var feedback = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/tasks/{taskId}/feedback", Ids.WORKSPACE_A, task.id())
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p5-candidate-feedback")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"correction\":\"待审核补充：候选发布之前必须完成隔离评测。\",\"evidence\":\"提交人说明该规则来自内部复核。\"}"))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());

        var rulePath = "/api/v1/workspaces/" + Ids.WORKSPACE_A + "/learning-candidates/from-feedback/" + feedback.path("id").asText();
        var generated = json.readTree(mvc.perform(post(rulePath).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        assertThat(generated.path("candidates")).hasSize(1);
        var knowledgeCandidate = generated.path("candidates").get(0);
        assertThat(knowledgeCandidate.path("status").asText()).isEqualTo("PROPOSED");
        assertThat(knowledgeCandidate.path("targetType").asText()).isEqualTo("KNOWLEDGE_UPDATE");
        assertThat(knowledgeCandidate.path("targetId").asText()).isEqualTo(knowledgeId.toString());
        assertThat(knowledgeCandidate.path("evidenceGap").asBoolean()).isTrue();
        assertThat(knowledgeCandidate.path("proposedContent").asText()).contains("待审核补充");

        mvc.perform(post(rulePath).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.sources[0].outcome").value("REPLAYED"));
        assertThat(jdbc.queryForObject("select count(*) from learning.candidate where source_feedback_id = ?", Integer.class,
                UUID.fromString(feedback.path("id").asText()))).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from learning.candidate_source where outcome = 'CREATED'", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from knowledge.document_version where document_id = ?", Integer.class, knowledgeId)).isEqualTo(1);

        var feedbackId = UUID.fromString(feedback.path("id").asText());
        var originalSnapshot = jdbc.queryForObject("select source_snapshot::text from learning.feedback where id = ?", String.class, feedbackId);
        var source = json.readValue(originalSnapshot, Feedback.Source.class);
        var batchRefs = java.util.stream.IntStream.range(0, 11)
                .mapToObj(index -> new ContextSourceRef("batch-" + index, "KNOWLEDGE", UUID.randomUUID(), 1,
                        null, null, null, null, "hash-" + index)).toList();
        var expanded = new Feedback.Source(source.agentId(), source.agentVersion(), source.promptVersion(),
                source.assetBinding(), batchRefs, source.execution());
        jdbc.update("update learning.feedback set source_snapshot = ?::jsonb where id = ?", json.writeValueAsString(expanded), feedbackId);
        var limitedBatch = json.readTree(mvc.perform(post(rulePath).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(limitedBatch.path("sources")).hasSize(11);
        assertThat(limitedBatch.path("sources").findValuesAsText("reasonCode")).contains("BATCH_LIMIT");
        assertThat(jdbc.queryForObject("select count(*) from learning.candidate_source where feedback_id = ?", Integer.class, feedbackId))
                .isEqualTo(12); // 一个原目标加十个处理目标，最后一个来源记录批次限额。
        jdbc.update("update learning.feedback set source_snapshot = ?::jsonb where id = ?", originalSnapshot, feedbackId);

        var memoryBody = """
                {"targetType":"MEMORY_UPSERT","targetId":null,"baseVersion":null,
                 "proposedContent":{"logicalKey":"candidate-followup","type":"SEMANTIC","scope":"PERSONAL",
                   "content":"续约中断的客户风险为高风险，请按此判断。","confidence":0.6,"expiresAt":"2099-12-31T23:59:59Z",
                   "sourceRef":"human-review:case-17","evidenceRefs":["case:17"]},
                 "evidenceRefs":["case:17"]}
                """;
        var manual = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p5-candidate-memory")
                        .contentType(MediaType.APPLICATION_JSON).content(memoryBody))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var memoryCandidateId = UUID.fromString(manual.path("id").asText());
        assertThat(manual.path("targetType").asText()).isEqualTo("MEMORY_UPSERT");
        assertThat(manual.path("ownerId").asText()).isEqualTo(Ids.ALICE.toString());
        var repeatedManual = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p5-candidate-memory")
                        .contentType(MediaType.APPLICATION_JSON).content(memoryBody))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(repeatedManual.path("id").asText()).isEqualTo(manual.path("id").asText());
        assertThat(jdbc.queryForObject("select count(*) from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'learning:publish'",
                Integer.class, Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from memory.definition where tenant_id = ? and workspace_id = ? and logical_key = 'candidate-followup'",
                Integer.class, Ids.TENANT_A, Ids.WORKSPACE_A)).isZero();

        var teamPermission = jdbc.queryForObject("select status from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'memory:scope:team'",
                String.class, Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'memory:scope:team'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p5-candidate-team-denied")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"targetType":"MEMORY_UPSERT","proposedContent":{"logicalKey":"candidate-team","type":"SEMANTIC","scope":"TEAM",
                                  "content":"不应扩大共享范围。","confidence":0.6,"expiresAt":"2099-12-31T23:59:59Z",
                                  "sourceRef":"manual:team","evidenceRefs":["case:team"]},"evidenceRefs":["case:team"]}
                                """))
                .andExpect(status().isForbidden());
        jdbc.update("update workspace.\"grant\" set status = ? where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'memory:scope:team'",
                teamPermission, Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p5-candidate-secret-denied")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"targetType":"MEMORY_UPSERT","proposedContent":{"logicalKey":"candidate-secret","type":"SEMANTIC","scope":"PERSONAL",
                                  "content":"api_key=ABCDEFGHIJKLMNOPQRST","confidence":0.6,"expiresAt":"2099-12-31T23:59:59Z",
                                  "sourceRef":"manual:secret","evidenceRefs":["case:secret"]},"evidenceRefs":["case:secret"]}
                                """))
                .andExpect(status().isBadRequest());

        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "select ?, ?, ?, action, 'ACTIVE' from (values ('learning:read'), ('learning:review'), ('learning:approve'), "
                        + "('memory:read'), ('knowledge:read'), ('evaluation:run'), ('evaluation:read'), ('task:create'), ('task:read')) as grants(action) "
                        + "on conflict do nothing", Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB);
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'learning:approve', 'ACTIVE') on conflict do nothing",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/reviews", Ids.WORKSPACE_A, memoryCandidateId)
                        .param("expectedVersion", "1").header("Authorization", "Bearer eaf-local-bob")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"decision\":\"ACCEPTED\",\"reason\":\"确认事实\",\"factEvidenceRefs\":[]}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/reviews", Ids.WORKSPACE_A, memoryCandidateId)
                        .param("expectedVersion", "1").header("Authorization", "Bearer eaf-local-bob")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"decision\":\"ACCEPTED\",\"reason\":\"独立复核并给出事实出处。\",\"factEvidenceRefs\":[\"manual:fact-check-17\"]}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("IN_REVIEW"));

        var memorySnapshotId = jdbc.queryForObject("select id from evaluation.candidate_context_snapshot where candidate_id = ? and candidate_revision = 1",
                UUID.class, memoryCandidateId);
        var memoryPair = evaluations.runCandidatePair(BOB, Ids.WORKSPACE_A, memorySnapshotId, "同一输入：隔离验证记忆候选");
        assertThat(memoryPair.baselineTaskId()).isNotNull();
        assertThat(memoryPair.candidateTaskId()).isNotNull();
        assertThat(jdbc.queryForObject("select count(*) from task.task where id in (?, ?) and source = 'EVALUATION' "
                        + "and business_entity_type = 'CANDIDATE_EXPERIMENT' and business_entity_id = ? and input_text = ?",
                Integer.class, memoryPair.baselineTaskId(), memoryPair.candidateTaskId(), memorySnapshotId.toString(),
                "同一输入：隔离验证记忆候选")).isEqualTo(2);
        var taskContexts = jdbc.query("select task_id, content from agent_runtime.step where task_id in (?, ?) and type = 'CONTEXT_SNAPSHOT'",
                (rs, row) -> Map.entry(rs.getObject("task_id", UUID.class), rs.getString("content")),
                memoryPair.baselineTaskId(), memoryPair.candidateTaskId());
        assertThat(taskContexts).hasSize(2);
        var baselineTaskContext = taskContexts.stream().filter(row -> row.getKey().equals(memoryPair.baselineTaskId())).findFirst().orElseThrow();
        var candidateTaskContext = taskContexts.stream().filter(row -> row.getKey().equals(memoryPair.candidateTaskId())).findFirst().orElseThrow();
        var baselineSnapshot = json.readValue(baselineTaskContext.getValue(), EnterpriseContext.class);
        var candidateSnapshot = json.readValue(candidateTaskContext.getValue(), EnterpriseContext.class);
        assertThat(baselineSnapshot.status()).isEqualTo("NO_EVIDENCE");
        assertThat(candidateSnapshot.items()).singleElement().satisfies(item -> assertThat(item.content())
                .isEqualTo("续约中断的客户风险为高风险，请按此判断。"));

        var otherTenant = new ActorContext(Ids.ALICE, Ids.TENANT_B, ActorType.HUMAN, Set.of());
        assertThat(evaluationContexts.readForTask(otherTenant, Ids.WORKSPACE_B, memoryPair.candidateTaskId(), memorySnapshotId)).isEmpty();
        var storedCandidateContext = jdbc.queryForObject("select candidate_context::text from evaluation.candidate_context_snapshot where id = ?",
                String.class, memorySnapshotId);
        var tamperedContext = (com.fasterxml.jackson.databind.node.ObjectNode) json.readTree(storedCandidateContext);
        ((com.fasterxml.jackson.databind.node.ObjectNode) tamperedContext.withArray("items").get(0)).put("content", "被篡改的正文");
        jdbc.update("update evaluation.candidate_context_snapshot set candidate_context = ?::jsonb where id = ?",
                json.writeValueAsString(tamperedContext), memorySnapshotId);
        assertThat(evaluationContexts.readForTask(BOB, Ids.WORKSPACE_A, memoryPair.candidateTaskId(), memorySnapshotId)).isEmpty();
        jdbc.update("update evaluation.candidate_context_snapshot set candidate_context = ?::jsonb where id = ?",
                storedCandidateContext, memorySnapshotId);

        var candidateReport = evaluations.runCandidateEvaluation(BOB, Ids.WORKSPACE_A, memorySnapshotId);
        assertThat(candidateReport.status()).isEqualTo("PASSED");
        assertThat(candidateReport.purpose()).isEqualTo("LEARNING_CANDIDATE_HELD_OUT");
        assertThat(candidateReport.source()).isEqualTo("EVALUATION");
        assertThat(candidateReport.spendScopeId()).isEqualTo(candidateReport.id());
        assertThat(candidateReport.failedSamples()).isZero();
        assertThat(candidateReport.unknownUsageSamples()).isZero();
        assertThat(candidateReport.candidateAccuracyVariance()).isNotNull();
        assertThat(candidateReport.latencyP50Ms()).isNotNull();
        assertThat(candidateReport.latencyP95Ms()).isNotNull();
        // 清单只固定配置与哈希；答案留在 Evaluation 内部，开发集仍与保留集分离。
        assertThat(candidateReport.manifest().datasetRole()).isEqualTo("HELD_OUT");
        assertThat(candidateReport.manifest().developmentDatasetVersion()).isNull();
        assertThat(candidateReport.manifest().datasetHash()).isEqualTo(candidateReport.datasetHash());
        assertThat(jdbc.queryForObject("select count(*) from evaluation.candidate_dev_case", Integer.class)).isZero();
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'evaluation:run', 'ACTIVE') "
                        + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        var reviewCaseId = candidateReport.samples().get(0).caseId();
        assertThatThrownBy(() -> evaluations.reviewCandidateSample(new CandidateSampleReviewCommand(BOB,
                Ids.WORKSPACE_A, candidateReport.id(), reviewCaseId, 1, "AMBIGUOUS", "AMBIGUOUS",
                "运行发起人不得自行复核。")))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                        error -> assertThat(error.code()).isEqualTo("POLICY_DENIED"));
        var independentReview = evaluations.reviewCandidateSample(new CandidateSampleReviewCommand(ALICE,
                Ids.WORKSPACE_A, candidateReport.id(), reviewCaseId, 1, "AMBIGUOUS", "AMBIGUOUS",
                "本样本需要独立事实复核。"));
        assertThat(independentReview.reviewerId()).isEqualTo(Ids.ALICE);
        assertThatThrownBy(() -> evaluations.reviewCandidateSample(new CandidateSampleReviewCommand(ALICE,
                Ids.WORKSPACE_A, candidateReport.id(), reviewCaseId, 1, "CONFIRMED", "SUPPORTED",
                "不能覆盖既有独立审核结论。")))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                        error -> assertThat(error.code()).isEqualTo("REVIEW_IMMUTABLE"));
        assertThat(evaluations.listCandidateSampleReviews(BOB, Ids.WORKSPACE_A, candidateReport.id()))
                .containsExactly(independentReview);
        var qualityTaskCount = jdbc.queryForObject("select count(*) from task.task where quality_run_id = ?", Integer.class,
                candidateReport.id());
        var replayedReport = evaluations.runCandidateEvaluation(BOB, Ids.WORKSPACE_A, memorySnapshotId);
        assertThat(replayedReport.id()).isEqualTo(candidateReport.id());
        assertThat(jdbc.queryForObject("select count(*) from task.task where quality_run_id = ?", Integer.class,
                candidateReport.id())).isEqualTo(qualityTaskCount);
        assertThat(candidateReport.datasetCases()).isEqualTo(20);
        assertThat(candidateReport.totalSamples()).isEqualTo(60);
        assertThat(candidateReport.completedSamples()).isEqualTo(60);
        assertThat(candidateReport.targetSamples()).isEqualTo(24);
        assertThat(candidateReport.targetBaselineCorrect()).isZero();
        assertThat(candidateReport.targetCandidateCorrect()).isEqualTo(24);
        assertThat(candidateReport.targetImprovedCases()).isEqualTo(8);
        assertThat(candidateReport.nonTargetRegressionCases()).isZero();
        assertThat(candidateReport.safetyViolations()).isZero();
        assertThat(candidateReport.citationSupportedCalls()).isEqualTo(candidateReport.citationScoredCalls())
                .isEqualTo(120);
        assertThat(candidateReport.modelCalls()).isEqualTo(120);
        assertThat(candidateReport.knownUsageCalls()).isEqualTo(120);
        assertThat(candidateReport.estimatedCost()).isNull();
        assertThat(candidateReport.costStatus()).isEqualTo("UNKNOWN_PRICE");
        assertThat(candidateReport.costCurrency()).isNull();
        assertThat(candidateReport.actualCost()).isNull();
        assertThat(candidateReport.actualCostCurrency()).isNull();
        assertThat(candidateReport.billingStatus()).isEqualTo("UNBILLED");
        assertThat(candidateReport.candidateCorrect()).isEqualTo(60);
        assertThat(candidateReport.samples()).hasSize(60).allSatisfy(sample -> {
            assertThat(sample.baselineEvidence().citationIdStatus()).isIn("NOT_REQUIRED", "IDS_PRESENT_IN_SNAPSHOT");
            assertThat(sample.baselineEvidence().citationSemanticSupport()).isEqualTo("NOT_ASSESSED");
            assertThat(sample.candidateEvidence().citationSemanticSupport()).isEqualTo("NOT_ASSESSED");
            assertThat(sample.baselineUsage()).hasSize(sample.baselineModelCalls()).allSatisfy(record -> {
                assertThat(record.source()).isEqualTo("EVALUATION");
                assertThat(record.usageStatus()).isEqualTo("KNOWN");
            });
            assertThat(sample.candidateUsage()).hasSize(sample.candidateModelCalls()).allSatisfy(record -> {
                assertThat(record.source()).isEqualTo("EVALUATION");
                assertThat(record.usageStatus()).isEqualTo("KNOWN");
            });
        });
        assertThat(jdbc.queryForObject("select count(*) from task.task where source = 'EVALUATION' and input_text in "
                        + "(select expected_risk from evaluation.candidate_eval_answer where dataset_version = 'candidate-p5-v1')",
                Integer.class)).isZero();

        // 停止发生在后台运行中；恢复必须沿用已保存样本 Task 与 Usage callKey，不生成第二批费用。
        var resumableKey = "p7-08-stop-resume-" + memorySnapshotId;
        var running = CompletableFuture.supplyAsync(() -> evaluations.runCandidateEvaluation(BOB,
                Ids.WORKSPACE_A, memorySnapshotId, resumableKey));
        var waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        UUID resumableRunId = null;
        while (System.nanoTime() < waitUntil && resumableRunId == null) {
            resumableRunId = jdbc.query("select id from evaluation.candidate_eval_run where tenant_id = ? and workspace_id = ? and started_by = ? and idempotency_key = ?",
                    rs -> rs.next() ? rs.getObject("id", UUID.class) : null,
                    Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB, resumableKey);
            if (resumableRunId == null) Thread.sleep(10);
        }
        assertThat(resumableRunId).isNotNull();
        var requestedStop = evaluations.stopCandidateEvaluation(BOB, Ids.WORKSPACE_A, resumableRunId);
        assertThat(requestedStop.status()).isIn("STOP_REQUESTED", "STOPPED");
        var stoppedReport = running.get(90, TimeUnit.SECONDS);
        assertThat(stoppedReport.id()).isEqualTo(resumableRunId);
        assertThat(stoppedReport.status()).isEqualTo("STOPPED");
        assertThat(stoppedReport.completedSamples()).isLessThan(60);
        var savedTaskIds = jdbc.query("select id from task.task where quality_run_id = ? order by id",
                (rs, row) -> rs.getObject("id", UUID.class), resumableRunId);
        var savedUsageCount = usage.findForScope(Ids.TENANT_A, Ids.WORKSPACE_A, "EVALUATION", resumableRunId).size();
        assertThat(savedTaskIds).isNotEmpty();
        var resumedReport = evaluations.runCandidateEvaluation(BOB, Ids.WORKSPACE_A, memorySnapshotId, resumableKey);
        assertThat(resumedReport.id()).isEqualTo(resumableRunId);
        assertThat(resumedReport.status()).isEqualTo("PASSED");
        assertThat(resumedReport.completedSamples()).isEqualTo(60);
        assertThat(jdbc.query("select id from task.task where quality_run_id = ? order by id",
                (rs, row) -> rs.getObject("id", UUID.class), resumableRunId)).containsAll(savedTaskIds);
        assertThat(usage.findForScope(Ids.TENANT_A, Ids.WORKSPACE_A, "EVALUATION", resumableRunId)).hasSize(120);
        assertThat(savedUsageCount).isLessThanOrEqualTo(120);
        mvc.perform(get("/api/v1/workspaces/{workspaceId}/evaluations/{reportId}", Ids.WORKSPACE_A, candidateReport.id())
                        .header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("PASSED"))
                .andExpect(jsonPath("$.estimatedCost").doesNotExist())
                .andExpect(jsonPath("$.costStatus").value("UNKNOWN_PRICE"))
                .andExpect(jsonPath("$.actualCost").doesNotExist())
                .andExpect(jsonPath("$.billingStatus").value("UNBILLED"))
                .andExpect(jsonPath("$.manifest.datasetRole").value("HELD_OUT"))
                .andExpect(jsonPath("$.samples.length()").value(60));

        var approvalBody = "{\"decision\":\"APPROVED\",\"reason\":\"独立审阅当前修订与通过报告。\",\"evaluationReportId\":\""
                + candidateReport.id() + "\"}";
        var reviewVersion = jdbc.queryForObject("select row_version from learning.candidate where id = ?", Long.class, memoryCandidateId);
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/approvals", Ids.WORKSPACE_A, memoryCandidateId)
                        .param("expectedVersion", reviewVersion.toString()).header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON).content(approvalBody))
                .andExpect(status().isForbidden());
        var approval = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/approvals", Ids.WORKSPACE_A, memoryCandidateId)
                        .param("expectedVersion", reviewVersion.toString()).header("Authorization", "Bearer eaf-local-bob")
                        .contentType(MediaType.APPLICATION_JSON).content(approvalBody))
                .andExpect(status().isOk()).andExpect(jsonPath("$.decision").value("APPROVED"))
                .andExpect(jsonPath("$.authorizationAction").value("learning:approve"))
                .andReturn().getResponse().getContentAsString());
        assertThat(approval.path("candidateRevision").asInt()).isEqualTo(1);
        assertThat(approval.path("candidateContentHash").asText()).isEqualTo(manual.path("contentHash").asText());
        assertThat(approval.path("evaluationReportId").asText()).isEqualTo(candidateReport.id().toString());
        assertThat(approval.path("evaluationReportHash").asText()).hasSize(64);
        assertThat(approval.path("validUntil").asText()).isNotBlank();
        assertThat(jdbc.queryForObject("select count(*) from memory.definition where tenant_id = ? and workspace_id = ? and logical_key = 'candidate-followup'",
                Integer.class, Ids.TENANT_A, Ids.WORKSPACE_A)).isZero();

        var forgedUserTask = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "3.0.0",
                "伪造候选上下文不得进入模型。", "CANDIDATE_EXPERIMENT", memorySnapshotId.toString(),
                "p5-candidate-forged-user", "trace-p5-candidate-forged-user", "USER"));
        var forgedWork = tasks.claimOne().orElseThrow();
        assertThat(forgedWork.id()).isEqualTo(forgedUserTask.id());
        var forgedOutcome = runtime.run(forgedWork);
        assertThat(forgedOutcome.errorCode()).isEqualTo("EVALUATION_CONTEXT_DENIED");
        assertThat(forgedOutcome.modelCalled()).isFalse();
        tasks.complete(forgedWork, forgedOutcome);

        var knowledgeCandidateId = UUID.fromString(knowledgeCandidate.path("id").asText());
        jdbc.update("insert into knowledge.document_permission(tenant_id, workspace_id, document_id, actor_id, action, status) "
                        + "values (?, ?, ?, ?, 'knowledge:read', 'ACTIVE') on conflict do nothing",
                Ids.TENANT_A, Ids.WORKSPACE_A, knowledgeId, Ids.BOB);
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/reviews", Ids.WORKSPACE_A, knowledgeCandidateId)
                        .param("expectedVersion", "1").header("Authorization", "Bearer eaf-local-bob")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"decision\":\"ACCEPTED\",\"reason\":\"独立确认知识事实。\",\"factEvidenceRefs\":[\"manual:fact-check-knowledge\"]}"))
                .andExpect(status().isOk());
        var knowledgeSnapshotId = jdbc.queryForObject("select id from evaluation.candidate_context_snapshot where candidate_id = ? and candidate_revision = 1",
                UUID.class, knowledgeCandidateId);
        var knowledgePair = evaluations.runCandidatePair(BOB, Ids.WORKSPACE_A, knowledgeSnapshotId, "同一输入：隔离验证知识候选");
        assertThat(knowledgePair.baselineTaskId()).isNotNull();
        var publication = knowledge.getCurrentPublication(ALICE, Ids.WORKSPACE_A, knowledgeId);
        knowledge.revoke(ALICE, Ids.WORKSPACE_A, knowledgeId, publication.documentRowVersion(), "p5-revoke-eval-baseline");
        assertThat(evaluationContexts.readForTask(BOB, Ids.WORKSPACE_A, knowledgePair.baselineTaskId(), knowledgeSnapshotId)).isEmpty();

        mvc.perform(put("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}", Ids.WORKSPACE_A, memoryCandidateId)
                .param("expectedVersion", "3").header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"targetType":"MEMORY_UPSERT","targetId":null,"baseVersion":null,
                                 "proposedContent":{"logicalKey":"candidate-followup","type":"SEMANTIC","scope":"PERSONAL",
                                   "content":"修订后的隔离草稿。","confidence":0.6,"expiresAt":"2099-12-31T23:59:59Z",
                                   "sourceRef":"human-review:case-17","evidenceRefs":["case:17","case:18"]},
                                 "evidenceRefs":["case:17","case:18"]}
                                """))
                .andExpect(status().isOk()).andExpect(jsonPath("$.revision").value(2))
                .andExpect(jsonPath("$.status").value("PROPOSED"));
        var revised = json.readTree(mvc.perform(get("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}", Ids.WORKSPACE_A, memoryCandidateId)
                        .header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(revised.path("reviews")).hasSize(1);
        assertThat(revised.path("reviews").get(0).path("candidateRevision").asInt()).isEqualTo(1);
        assertThat(revised.path("approvals")).hasSize(1);
        assertThat(revised.path("approvals").get(0).path("candidateRevision").asInt()).isEqualTo(1);
        assertThat(revised.path("approvals").get(0).path("decision").asText()).isEqualTo("APPROVED");
        assertThat(revised.path("evidenceGap").asBoolean()).isTrue();
        assertThat(jdbc.queryForObject("select count(*) from learning.candidate_review where candidate_id = ? and candidate_revision = 1",
                Integer.class, memoryCandidateId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from memory.definition where tenant_id = ? and workspace_id = ? and logical_key = 'candidate-followup'",
                Integer.class, Ids.TENANT_A, Ids.WORKSPACE_A)).isZero();
        assertThat(evaluationContexts.readForTask(BOB, Ids.WORKSPACE_A, memoryPair.candidateTaskId(), memorySnapshotId)).isEmpty();
        assertThat(evaluations.getCandidateEvaluation(BOB, Ids.WORKSPACE_A, candidateReport.id()).candidateRevision()).isEqualTo(1);
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/evaluations/candidate-snapshots/{snapshotId}/runs",
                        Ids.WORKSPACE_A, memorySnapshotId).header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(status().isNotFound());
    }

    private UUID createPublishedDocument(String key, String title, String content) throws Exception {
        var document = mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", key)
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
}
