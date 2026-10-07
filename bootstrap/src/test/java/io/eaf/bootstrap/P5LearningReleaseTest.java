package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.context.api.ContextQuery;
import io.eaf.context.api.ContextService;
import io.eaf.learning.api.CandidateService;
import io.eaf.evaluation.api.EvaluationService;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.knowledge.infrastructure.KnowledgeOutboxPublisher;
import io.eaf.memory.api.CreateMemoryCommand;
import io.eaf.memory.api.MemoryService;
import io.eaf.memory.infrastructure.MemoryOutboxPublisher;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Hashing;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import java.sql.Timestamp;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.knowledge.outbox-poll-delay=3600000", "eaf.memory.outbox-poll-delay=3600000"})
@AutoConfigureMockMvc
// 用 PostgreSQL 验证 Learning 发布意图、目标模块原子提交和目标成功后的本地对账恢复。
class P5LearningReleaseTest {
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
    @Autowired CandidateService candidates;
    @Autowired EvaluationService evaluations;
    @Autowired ContextService contexts;
    @Autowired KnowledgeService knowledge;
    @Autowired MemoryService memories;
    @Autowired KnowledgeOutboxPublisher knowledgeOutbox;
    @Autowired MemoryOutboxPublisher memoryOutbox;
    @Autowired TaskService tasks;
    @Autowired TaskRunner taskRunner;
    @Autowired RuntimeQuery runtimeQuery;
    @Test
    void learningReleaseIsFrozenIdempotentAndRecoversAfterKnowledgeCommit() throws Exception {
        var knowledgeId = createPublishedDocument("p5-release-knowledge", "学习发布基线",
                "已发布知识基线：先核对续约证据，再判断客户流失风险。");
        grant(Ids.ALICE, "learning:publish");
        grant(Ids.ALICE, "learning:withdraw");
        var sourceKnowledgeTask = runUserTask("已发布知识基线：先核对续约证据，再判断客户流失风险。", "p5-iteration-knowledge-source");
        assertThat(runtimeQuery.contextSources(ALICE, Ids.WORKSPACE_A, sourceKnowledgeTask))
                .as("源 Task 应保存发布版 Knowledge 来源")
                .anyMatch(source -> knowledgeId.equals(source.documentId()) && source.documentVersion() == 1);
        var sourceKnowledgeFeedback = submitFeedback(sourceKnowledgeTask, "p5-iteration-knowledge-feedback",
                "续约中断且没有已签记录时应判为高风险，并记录续约日期。", "复核续约档案后确认。" );
        var proposedText = "若续约中断且没有有效签署记录，客户流失风险判为高风险并记录续约日期。";
        var candidate = proposeKnowledge(knowledgeId, proposedText, "p5-release-knowledge-candidate", sourceKnowledgeFeedback);
        var candidateId = UUID.fromString(candidate.path("id").asText());
        var approvalId = approveFixture(candidate);
        var releaseExpectedVersion = Long.toString(candidates.get(ALICE, Ids.WORKSPACE_A, candidateId).rowVersion());

        // 缺少学习发布动作时，Owner 身份本身不能越过 Workspace 授权。
        assertThatThrownBy(() -> candidates.publish(new CandidateService.CandidateReleaseCommand(
                BOB, Ids.WORKSPACE_A, candidateId, 2, approvalId)))
                .hasMessageContaining("权限");
        assertThat(jdbc.queryForObject("select count(*) from learning.candidate_release where candidate_id = ?",
                Integer.class, candidateId)).isZero();

        // 让目标 Knowledge 事务先提交，再让 Learning 确认事务失败，复现跨域提交后的恢复窗口。
        jdbc.execute("create function learning.test_abort_release_confirmation() returns trigger language plpgsql as $$ "
                + "begin if old.status = 'PUBLISHING' and new.status = 'RELEASED' then raise exception 'simulated confirmation crash'; end if; return new; end $$");
        jdbc.execute("create trigger test_abort_release_confirmation before update on learning.candidate_release "
                + "for each row execute function learning.test_abort_release_confirmation()");
        var crashed = runReleaseProcess(candidateId, approvalId, releaseExpectedVersion);
        assertThat(crashed.exitCode()).as(crashed.output()).isNotZero();
        assertThat(jdbc.queryForObject("select count(*) from learning.candidate_release where candidate_id = ?",
                Integer.class, candidateId)).as(crashed.output()).isEqualTo(1);

        var learningReleaseId = jdbc.queryForObject("select id from learning.candidate_release where candidate_id = ?",
                UUID.class, candidateId);
        assertThat(jdbc.queryForObject("select status from learning.candidate_release where id = ?", String.class,
                learningReleaseId)).isEqualTo("PUBLISHING");
        assertThat(jdbc.queryForObject("select status from learning.candidate where id = ?", String.class,
                candidateId)).isEqualTo("PUBLISHING");
        assertThat(jdbc.queryForObject("select count(*) from knowledge.publication_event where candidate_id = ? and action = 'PUBLISHED'",
                Integer.class, candidateId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from knowledge.document_version where document_id = ?",
                Integer.class, knowledgeId)).isEqualTo(2);

        var frozenVersion = jdbc.queryForObject("select row_version from learning.candidate where id = ?", Long.class, candidateId);
        mvc.perform(put("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}", Ids.WORKSPACE_A, candidateId)
                        .param("expectedVersion", frozenVersion.toString()).header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON).content("""
                                {"targetType":"KNOWLEDGE_UPDATE","targetId":"%s","baseVersion":"1",
                                 "proposedContent":"不允许在发布恢复期间替换的正文。","evidenceRefs":["case:replacement"]}
                                """.formatted(knowledgeId)))
                .andExpect(status().isConflict());

        jdbc.execute("drop trigger test_abort_release_confirmation on learning.candidate_release");
        jdbc.execute("drop function learning.test_abort_release_confirmation()");
        var restarted = runReleaseProcess(candidateId, approvalId, releaseExpectedVersion);
        assertThat(restarted.exitCode()).as(restarted.output()).isZero();
        var approvalJson = json.writeValueAsString(Map.of("approvalId", approvalId));
        var recovered = json.readTree(mvc.perform(get("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/release",
                                Ids.WORKSPACE_A, candidateId).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("RELEASED"))
                .andReturn().getResponse().getContentAsString());
        assertThat(recovered.path("id").asText()).isEqualTo(learningReleaseId.toString());
        assertThat(recovered.path("targetReleaseId").asText()).isNotBlank();
        assertThat(recovered.path("targetVersion").asText()).isEqualTo("2");
        assertThat(recovered.path("targetContentHash").asText()).isEqualTo(Hashing.sha256(proposedText));
        assertThat(jdbc.queryForObject("select status from learning.candidate where id = ?", String.class,
                candidateId)).isEqualTo("PUBLISHED");
        assertThat(jdbc.queryForObject("select count(*) from knowledge.document_version where document_id = ?",
                Integer.class, knowledgeId)).isEqualTo(2);
        assertThat(jdbc.queryForObject("select count(*) from knowledge.publication_event where candidate_id = ? and action = 'PUBLISHED'",
                Integer.class, candidateId)).isEqualTo(1);
        var targetChunkId = jdbc.queryForObject("select id from knowledge.chunk where document_id = ? and asset_version = 2 order by chunk_order, id limit 1",
                UUID.class, knowledgeId);
        var targetChunkHash = jdbc.queryForObject("select content_hash from knowledge.chunk where id = ?", String.class, targetChunkId);
        var targetBuildId = jdbc.queryForObject("select build_id from knowledge.publication_event where candidate_id = ? and action = 'PUBLISHED'",
                UUID.class, candidateId);
        var baselineChunkId = jdbc.queryForObject("select id from knowledge.chunk where document_id = ? and asset_version = 1 order by chunk_order, id limit 1",
                UUID.class, knowledgeId);
        var baselineChunkHash = jdbc.queryForObject("select content_hash from knowledge.chunk where id = ?", String.class, baselineChunkId);
        var baselineBuildId = jdbc.queryForObject("select build_id from knowledge.publication_event where document_id = ? and asset_version = 1 and action = 'PUBLISHED' order by occurred_at desc limit 1",
                UUID.class, knowledgeId);
        assertThat(knowledge.isUsable(ALICE, Ids.WORKSPACE_A, knowledgeId, 2, targetChunkId,
                targetBuildId, targetChunkHash)).isTrue();
        var knowledgeContext = contexts.query(ALICE, Ids.WORKSPACE_A,
                new ContextQuery("核对续约证据并判断客户流失风险", 5, 2_000));
        assertThat(knowledgeContext.items()).anyMatch(item -> knowledgeId.equals(item.documentId())
                && item.documentVersion() == 2);
        assertThat(contexts.isCurrent(ALICE, Ids.WORKSPACE_A, knowledgeContext)).isTrue();
        var followupKnowledgeTask = runUserTask("核对续约证据并判断客户流失风险",
                "p5-iteration-knowledge-followup");
        assertThat(runtimeQuery.contextSources(ALICE, Ids.WORKSPACE_A, followupKnowledgeTask))
                .as("后续 Task 应实际读取发布版 Knowledge")
                .anyMatch(source -> knowledgeId.equals(source.documentId()) && source.documentVersion() == 2);
        var knowledgeIteration = recordIteration(candidateId, followupKnowledgeTask);
        assertThat(knowledgeIteration.path("evaluationStatus").asText()).isEqualTo("PASSED");
        assertThat(knowledgeIteration.path("releaseStatus").asText()).isEqualTo("RELEASED");
        assertThat(knowledgeIteration.path("targetVersion").asText()).isEqualTo("2");
        assertThat(knowledgeIteration.path("followupTaskId").asText()).isEqualTo(followupKnowledgeTask.toString());
        assertThat(knowledgeIteration.path("usageStatus").asText()).isEqualTo("CITED");
        assertThat(knowledgeIteration.path("targetSource").path("documentVersion").asInt()).isEqualTo(2);
        assertThat(knowledgeIteration.path("resultHash").asText()).hasSize(64);
        assertThat(knowledgeIteration.path("factOutcome").asText()).isEqualTo("UNVERIFIED");
        assertThat(knowledgeIteration.path("reportedRiskLevel").asText()).isEqualTo("LOW");
        // 公开查询只暴露脱敏证据字段，并保留未核实现实结果的标记。
        mvc.perform(get("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/iterations",
                        Ids.WORKSPACE_A, candidateId).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].usageStatus").value("CITED"))
                .andExpect(jsonPath("$[0].factOutcome").value("UNVERIFIED"));
        var sameIteration = recordIteration(candidateId, followupKnowledgeTask);
        assertThat(sameIteration.path("id").asText()).isEqualTo(knowledgeIteration.path("id").asText());
        var sameInputTask = runUserTask("已发布知识基线：先核对续约证据，再判断客户流失风险。", "p5-iteration-knowledge-same-input");
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/iterations",
                        Ids.WORKSPACE_A, candidateId).header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of("taskId", sameInputTask))))
                .andExpect(status().isBadRequest());
        verifyKnowledgeOutboxRedelivery(UUID.fromString(recovered.path("targetReleaseId").asText()));

        // 目标 Knowledge 已提交撤回而 Learning 确认失败时，独立 JVM 必须按来源事实恢复。
        jdbc.execute("create function learning.test_abort_withdrawal_confirmation() returns trigger language plpgsql as $$ "
                + "begin if old.status = 'WITHDRAWING' and new.status = 'WITHDRAWN' then raise exception 'simulated withdrawal confirmation crash'; end if; return new; end $$");
        jdbc.execute("create trigger test_abort_withdrawal_confirmation before update on learning.candidate_withdrawal "
                + "for each row execute function learning.test_abort_withdrawal_confirmation()");
        var publishedCandidateVersion = jdbc.queryForObject("select row_version from learning.candidate where id = ?", Long.class, candidateId);
        assertThatThrownBy(() -> candidates.withdrawRelease(new CandidateService.CandidateWithdrawalCommand(
                BOB, Ids.WORKSPACE_A, candidateId, publishedCandidateVersion, "test:unauthorized")))
                .hasMessageContaining("权限");
        assertThat(jdbc.queryForObject("select count(*) from learning.candidate_withdrawal where candidate_id = ?",
                Integer.class, candidateId)).isZero();
        var withdrawalCrash = runReleaseProcess(candidateId, approvalId, publishedCandidateVersion.toString(), "withdraw");
        assertThat(withdrawalCrash.exitCode()).as(withdrawalCrash.output()).isNotZero();
        var withdrawalId = jdbc.queryForObject("select id from learning.candidate_withdrawal where candidate_id = ?",
                UUID.class, candidateId);
        assertThat(jdbc.queryForObject("select status from learning.candidate_withdrawal where id = ?", String.class,
                withdrawalId)).isEqualTo("WITHDRAWING");
        assertThat(jdbc.queryForObject("select status from learning.candidate where id = ?", String.class,
                candidateId)).isEqualTo("PUBLISHED");
        assertThat(jdbc.queryForObject("select status from knowledge.document_version where document_id = ? and asset_version = 2",
                String.class, knowledgeId)).isEqualTo("REVOKED");
        assertThat(knowledge.getCurrentPublication(ALICE, Ids.WORKSPACE_A, knowledgeId).assetVersion()).isEqualTo(1);
        assertThat(jdbc.queryForObject("select status from knowledge.document where id = ?", String.class,
                knowledgeId)).isEqualTo("PUBLISHED");
        assertThat(jdbc.queryForObject("select count(*) from knowledge.publication_event where candidate_id = ? and action = 'REVOKED'",
                Integer.class, candidateId)).isEqualTo(1);
        assertThat(knowledge.isUsable(ALICE, Ids.WORKSPACE_A, knowledgeId, 2, targetChunkId,
                targetBuildId, targetChunkHash)).isFalse();
        assertThat(knowledge.isUsable(ALICE, Ids.WORKSPACE_A, knowledgeId, 1, baselineChunkId,
                baselineBuildId, baselineChunkHash)).isTrue();
        assertThat(contexts.isCurrent(ALICE, Ids.WORKSPACE_A, knowledgeContext)).isFalse();
        var postKnowledgeWithdrawalContext = contexts.query(ALICE, Ids.WORKSPACE_A,
                new ContextQuery("核对续约证据并判断客户流失风险", 5, 2_000));
        assertThat(postKnowledgeWithdrawalContext.items()).anyMatch(item -> knowledgeId.equals(item.documentId())
                && item.documentVersion() == 1);
        assertThat(postKnowledgeWithdrawalContext.items()).noneMatch(item -> knowledgeId.equals(item.documentId())
                && item.documentVersion() == 2);
        var postWithdrawalKnowledgeTask = runUserTask("续约中断的另一客户现在可以引用哪些有效依据？",
                "p5-iteration-knowledge-after-withdrawal");
        var withdrawnKnowledgeIteration = recordIteration(candidateId, postWithdrawalKnowledgeTask);
        assertThat(withdrawnKnowledgeIteration.path("usageStatus").asText()).isEqualTo("NOT_AVAILABLE");

        jdbc.execute("drop trigger test_abort_withdrawal_confirmation on learning.candidate_withdrawal");
        jdbc.execute("drop function learning.test_abort_withdrawal_confirmation()");
        var withdrawalRestart = runReleaseProcess(candidateId, approvalId, publishedCandidateVersion.toString(), "withdraw");
        assertThat(withdrawalRestart.exitCode()).as(withdrawalRestart.output()).isZero();
        var withdrawn = json.readTree(mvc.perform(get("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/withdrawal",
                                Ids.WORKSPACE_A, candidateId).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("WITHDRAWN"))
                .andReturn().getResponse().getContentAsString());
        assertThat(withdrawn.path("id").asText()).isEqualTo(withdrawalId.toString());
        assertThat(withdrawn.path("targetWithdrawalId").asText()).isNotBlank();
        var withdrawalReplay = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/withdraw-release",
                                Ids.WORKSPACE_A, candidateId)
                        .param("expectedVersion", publishedCandidateVersion.toString())
                        .header("Authorization", "Bearer eaf-local-alice").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"reasonRef\":\"test:eaf-p5-withdrawal\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("WITHDRAWN"))
                .andReturn().getResponse().getContentAsString());
        assertThat(withdrawalReplay.path("targetWithdrawalId").asText()).isEqualTo(withdrawn.path("targetWithdrawalId").asText());
        assertThat(jdbc.queryForObject("select count(*) from knowledge.publication_event where candidate_id = ? and action = 'REVOKED'",
                Integer.class, candidateId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select status from learning.candidate_release where candidate_id = ?", String.class,
                candidateId)).isEqualTo("RELEASED");

        // 已撤回版本的旧发布 outbox 只重放历史事实，不得恢复 Context 可用性。
        var oldKnowledgeReleaseId = UUID.fromString(recovered.path("targetReleaseId").asText());
        jdbc.update("update knowledge.outbox set status = 'PENDING', next_attempt_at = null where event_id = ?", oldKnowledgeReleaseId);
        knowledgeOutbox.publish();
        assertThat(jdbc.queryForObject("select status from knowledge.outbox where event_id = ?", String.class,
                oldKnowledgeReleaseId)).isEqualTo("DELIVERED");
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class,
                "knowledge-outbox:" + oldKnowledgeReleaseId)).isEqualTo(1);
        assertThat(knowledge.isUsable(ALICE, Ids.WORKSPACE_A, knowledgeId, 2, targetChunkId,
                targetBuildId, targetChunkHash)).isFalse();
        assertThat(contexts.isCurrent(ALICE, Ids.WORKSPACE_A, knowledgeContext)).isFalse();
        assertThat(contexts.isCurrent(ALICE, Ids.WORKSPACE_A, postKnowledgeWithdrawalContext)).isTrue();
        assertThat(knowledge.getCurrentPublication(ALICE, Ids.WORKSPACE_A, knowledgeId).assetVersion()).isEqualTo(1);

        var replay = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/publish",
                                Ids.WORKSPACE_A, candidateId)
                        .param("expectedVersion", releaseExpectedVersion).header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON).content(approvalJson))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(replay.path("targetReleaseId").asText()).isEqualTo(recovered.path("targetReleaseId").asText());
        mvc.perform(get("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/release",
                        Ids.WORKSPACE_A, candidateId).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("RELEASED"));
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/publish",
                        Ids.WORKSPACE_A, candidateId)
                        .param("expectedVersion", releaseExpectedVersion).header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON).content(approvalJson))
                .andExpect(status().isOk()).andExpect(jsonPath("$.targetReleaseId").value(recovered.path("targetReleaseId").asText()));

        publishMemoryCandidateIdempotently();
    }

    private void publishMemoryCandidateIdempotently() throws Exception {
        var body = """
                {"targetType":"MEMORY_UPSERT","proposedContent":{"logicalKey":"p5-release-memory",
                 "type":"SEMANTIC","scope":"PERSONAL","content":"先检查已签续约记录再判断流失风险。",
                 "confidence":0.9,"expiresAt":"2099-12-31T23:59:59Z","sourceRef":"review:renewal-17",
                 "evidenceRefs":["renewal:17"]},"evidenceRefs":["renewal:17"]}
                """;
        var candidate = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p5-release-memory")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var candidateId = UUID.fromString(candidate.path("id").asText());
        var approvalId = approveFixture(candidate);
        var approvalJson = json.writeValueAsString(Map.of("approvalId", approvalId));
        var command = new CandidateService.CandidateReleaseCommand(ALICE, Ids.WORKSPACE_A, candidateId, 2, approvalId);
        // 并发重放同一批准请求时，候选行锁和目标来源键必须收敛到唯一 release。
        var firstPublish = CompletableFuture.supplyAsync(() -> candidates.publish(command));
        var secondPublish = CompletableFuture.supplyAsync(() -> candidates.publish(command));
        assertThat(firstPublish.join().id()).isEqualTo(secondPublish.join().id());
        var released = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/publish",
                                Ids.WORKSPACE_A, candidateId)
                        .param("expectedVersion", "2").header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON).content(approvalJson))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("RELEASED"))
                .andReturn().getResponse().getContentAsString());
        assertThat(jdbc.queryForObject("select count(*) from memory.release where candidate_id = ? and action = 'PUBLISHED'",
                Integer.class, candidateId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from memory.version v join memory.definition d on d.id = v.memory_id "
                        + "where d.logical_key = 'p5-release-memory' and v.status = 'PUBLISHED' and v.source_type = 'LEARNING_RELEASE'",
                Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select release_origin from memory.release where candidate_id = ?",
                String.class, candidateId)).isEqualTo("LEARNING_CANDIDATE");
        assertThat(jdbc.queryForObject("select count(*) from memory.outbox where event_id = ? and payload->>'candidateId' = ?",
                Integer.class, UUID.fromString(released.path("targetReleaseId").asText()), candidateId.toString())).isEqualTo(1);
        verifyMemoryOutboxDelivery(UUID.fromString(released.path("targetReleaseId").asText()));

        var replay = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/publish",
                                Ids.WORKSPACE_A, candidateId)
                        .param("expectedVersion", "2").header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON).content(approvalJson))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(replay.path("targetReleaseId").asText()).isEqualTo(released.path("targetReleaseId").asText());
        assertThat(jdbc.queryForObject("select count(*) from memory.release where candidate_id = ? and action = 'PUBLISHED'",
                Integer.class, candidateId)).isEqualTo(1);
        assertThatThrownBy(() -> candidates.publish(new CandidateService.CandidateReleaseCommand(
                ALICE, Ids.WORKSPACE_A, candidateId, 2, UUID.randomUUID())))
                .hasMessageContaining("不同批准");

        // 同一逻辑键已有正式 Memory 时，旧 baseVersion 候选必须在保存发布意图前冲突。
        var staleCandidate = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p5-stale-memory")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var staleCandidateId = UUID.fromString(staleCandidate.path("id").asText());
        var staleApproval = approveFixture(staleCandidate);
        assertThatThrownBy(() -> candidates.publish(new CandidateService.CandidateReleaseCommand(
                ALICE, Ids.WORKSPACE_A, staleCandidateId, 2, staleApproval)))
                .hasMessageContaining("同一 Memory 逻辑键已有当前发布版本");
        assertThat(jdbc.queryForObject("select count(*) from learning.candidate_release where candidate_id = ?",
                Integer.class, staleCandidateId)).isZero();

        // 过期批准即使历史决定仍为 APPROVED，也不能产生发布作业或目标 Memory。
        var expiredBody = body.replace("p5-release-memory", "p5-expired-memory");
        var expiredCandidate = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p5-expired-memory")
                        .contentType(MediaType.APPLICATION_JSON).content(expiredBody))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var expiredCandidateId = UUID.fromString(expiredCandidate.path("id").asText());
        var expiredApproval = approveFixture(expiredCandidate);
        jdbc.update("update learning.candidate_approval set valid_until = now() - interval '1 second' where id = ?", expiredApproval);
        assertThatThrownBy(() -> candidates.publish(new CandidateService.CandidateReleaseCommand(
                ALICE, Ids.WORKSPACE_A, expiredCandidateId, 2, expiredApproval)))
                .hasMessageContaining("批准已过期");
        assertThat(jdbc.queryForObject("select count(*) from learning.candidate_release where candidate_id = ?",
                Integer.class, expiredCandidateId)).isZero();

        // Memory 候选撤回独立追加历史；撤回后实时读取与旧发布事件重投均不能返回该版本。
        var publishedTargetId = jdbc.queryForObject("select memory_id from memory.release where release_id = ?",
                UUID.class, UUID.fromString(released.path("targetReleaseId").asText()));
        var publishedTargetVersion = released.path("targetVersion").asText();
        assertThat(released.path("baseVersion").isNull()).isTrue();
        var memoryContext = contexts.query(ALICE, Ids.WORKSPACE_A,
                new ContextQuery("检查续约记录再判断流失风险", 5, 2_000));
        assertThat(memoryContext.items()).anyMatch(item -> publishedTargetId.equals(item.memoryId())
                && publishedTargetVersion.equals(item.memoryVersion()));
        assertThat(contexts.isCurrent(ALICE, Ids.WORKSPACE_A, memoryContext)).isTrue();
        var candidateVersion = jdbc.queryForObject("select row_version from learning.candidate where id = ?", Long.class, candidateId);
        var withdrawal = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/withdraw-release",
                                Ids.WORKSPACE_A, candidateId)
                        .param("expectedVersion", candidateVersion.toString())
                        .header("Authorization", "Bearer eaf-local-alice").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"reasonRef\":\"test:memory-regression\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("WITHDRAWN"))
                .andReturn().getResponse().getContentAsString());
        assertThat(withdrawal.path("targetId").asText()).isEqualTo(publishedTargetId.toString());
        assertThat(withdrawal.path("targetVersion").asText()).isEqualTo(publishedTargetVersion);
        assertThat(jdbc.queryForObject("select status from learning.candidate where id = ?", String.class,
                candidateId)).isEqualTo("PUBLISHED");
        assertThat(jdbc.queryForObject("select count(*) from memory.release where candidate_id = ? and action = 'REVOKED'",
                Integer.class, candidateId)).isEqualTo(1);
        assertThatThrownBy(() -> memories.requireUsable(ALICE, Ids.WORKSPACE_A, publishedTargetId, publishedTargetVersion))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class, error -> assertThat(error.status()).isEqualTo(404));
        assertThat(contexts.isCurrent(ALICE, Ids.WORKSPACE_A, memoryContext)).isFalse();
        var postMemoryWithdrawalContext = contexts.query(ALICE, Ids.WORKSPACE_A,
                new ContextQuery("检查续约记录再判断流失风险", 5, 2_000));
        assertThat(postMemoryWithdrawalContext.items()).noneMatch(item -> publishedTargetId.equals(item.memoryId()));
        var oldMemoryReleaseId = UUID.fromString(released.path("targetReleaseId").asText());
        jdbc.update("update memory.outbox set status = 'PENDING', attempt_count = 0, next_attempt_at = null where event_id = ?",
                oldMemoryReleaseId);
        memoryOutbox.publish();
        assertThat(jdbc.queryForObject("select status from memory.outbox where event_id = ?", String.class,
                oldMemoryReleaseId)).isEqualTo("DELIVERED");
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class,
                "memory-outbox:" + oldMemoryReleaseId)).isEqualTo(1);
        assertThatThrownBy(() -> memories.requireUsable(ALICE, Ids.WORKSPACE_A, publishedTargetId, publishedTargetVersion))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class, error -> assertThat(error.status()).isEqualTo(404));
        assertThat(contexts.isCurrent(ALICE, Ids.WORKSPACE_A, memoryContext)).isFalse();
        runMemoryIteration();
    }

    private void runMemoryIteration() throws Exception {
        grant(Ids.ALICE, "memory:scope:team");
        var baseline = memories.create(new CreateMemoryCommand(ALICE, Ids.WORKSPACE_A, "p5-learning-memory", "1.0.0",
                "SEMANTIC", "TEAM", "当前续约状态需对照签署档案后再判断流失风险。", 0.7,
                Instant.parse("2099-12-31T23:59:59Z"), "review:renewal-baseline", List.of("renewal:baseline")));
        memories.publish(ALICE, Ids.WORKSPACE_A, baseline.id(), baseline.version(), baseline.rowVersion());
        var sourceTask = runUserTask("按已签续约档案判断一个客户的流失风险。", "p5-iteration-memory-source");
        assertThat(runtimeQuery.contextSources(ALICE, Ids.WORKSPACE_A, sourceTask))
                .as("源 Task 应保存发布版 Memory 来源")
                .anyMatch(source -> baseline.id().equals(source.memoryId()) && baseline.version().equals(source.memoryVersion()));
        var feedbackId = submitFeedback(sourceTask, "p5-iteration-memory-feedback",
                "若续约中断且没有有效签署记录，应升级为高风险并记录跟进依据。", "已复核续约档案。" );
        var proposedMemory = Map.of("logicalKey", "p5-learning-memory", "type", "SEMANTIC", "scope", "TEAM",
                "content", "若续约中断且没有有效签署记录，客户流失风险应标记为高风险。", "confidence", 0.9,
                "expiresAt", "2099-12-31T23:59:59Z", "sourceRef", "review:renewal-followup",
                "evidenceRefs", List.of("renewal:followup"));
        var candidateBody = Map.of("targetType", "MEMORY_UPSERT", "targetId", baseline.id(),
                "baseVersion", baseline.version(), "proposedContent", proposedMemory,
                "evidenceRefs", List.of("renewal:followup"), "sourceFeedbackId", feedbackId);
        var candidate = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", "p5-learning-memory-iteration")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(candidateBody)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var candidateId = UUID.fromString(candidate.path("id").asText());
        var approvalId = approveFixture(candidate);
        var expectedVersion = candidates.get(ALICE, Ids.WORKSPACE_A, candidateId).rowVersion();
        var release = candidates.publish(new CandidateService.CandidateReleaseCommand(ALICE, Ids.WORKSPACE_A,
                candidateId, expectedVersion, approvalId));
        assertThat(release.status()).isEqualTo("RELEASED");
        // 用只有 Memory 读取权限的执行者，保证实际引用能归因到本次 Memory 发布版本。
        grant(Ids.BOB, "context:read");
        jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'knowledge:read'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB);
        var followupTask = runUserTask(BOB, "为新续约档案识别风险等级，并列出需要补查的签署依据。",
                "p5-iteration-memory-followup");
        assertThat(runtimeQuery.contextSources(BOB, Ids.WORKSPACE_A, followupTask))
                .as("后续 Task 应实际读取发布版 Memory")
                .anyMatch(source -> baseline.id().equals(source.memoryId()) && release.targetVersion().equals(source.memoryVersion()));
        var iteration = recordIteration(candidateId, followupTask);
        assertThat(iteration.path("targetType").asText()).isEqualTo("MEMORY_UPSERT");
        assertThat(iteration.path("evaluationStatus").asText()).isEqualTo("PASSED");
        assertThat(iteration.path("releaseStatus").asText()).isEqualTo("RELEASED");
        assertThat(iteration.path("targetId").asText()).isEqualTo(baseline.id().toString());
        assertThat(iteration.path("targetVersion").asText()).isEqualTo(release.targetVersion());
        assertThat(iteration.path("usageStatus").asText()).isEqualTo("CITED");
        assertThat(iteration.path("targetSource").path("memoryId").asText()).isEqualTo(baseline.id().toString());
        assertThat(iteration.path("factOutcome").asText()).isEqualTo("UNVERIFIED");
        mvc.perform(get("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/iterations",
                        Ids.WORKSPACE_A, candidateId).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].targetType").value("MEMORY_UPSERT"))
                .andExpect(jsonPath("$[0].usageStatus").value("CITED"));
    }

    // 通过指定 HUMAN 建立 USER Task，供不同权限下的实际 Context 来源验证复用。
    private UUID runUserTask(String input, String key) {
        return runUserTask(ALICE, input, key);
    }

    private UUID runUserTask(ActorContext actor, String input, String key) {
        var task = tasks.create(new CreateTaskCommand(actor, Ids.WORKSPACE_A, Ids.AGENT_RISK, "3.0.0",
                input, null, null, key, key, "USER"));
        var work = tasks.claimOne().filter(item -> item.id().equals(task.id())).orElseThrow();
        tasks.complete(work, taskRunner.run(work));
        return task.id();
    }

    private UUID submitFeedback(UUID taskId, String key, String correction, String evidence) throws Exception {
        var response = mvc.perform(post("/api/v1/workspaces/{workspaceId}/tasks/{taskId}/feedback", Ids.WORKSPACE_A, taskId)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of(
                                "correction", correction, "evidence", evidence))))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        return UUID.fromString(json.readTree(response).path("id").asText());
    }

    private JsonNode recordIteration(UUID candidateId, UUID taskId) throws Exception {
        return json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates/{candidateId}/iterations",
                                Ids.WORKSPACE_A, candidateId).header("Authorization", "Bearer eaf-local-alice")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of("taskId", taskId))))
                .andExpect(status().is2xxSuccessful()).andReturn().getResponse().getContentAsString());
    }

    private UUID approveFixture(JsonNode candidate) throws Exception {
        var candidateId = UUID.fromString(candidate.path("id").asText());
        var revision = candidate.path("revision").asInt();
        if (!candidate.path("sourceFeedbackId").isMissingNode() && !candidate.path("sourceFeedbackId").isNull()) {
            grantIterationReviewer(candidate.path("targetType").asText(), UUID.fromString(candidate.path("targetId").asText()));
            candidates.review(new CandidateService.CandidateReviewCommand(BOB, Ids.WORKSPACE_A, candidateId,
                    candidate.path("rowVersion").asLong(), "ACCEPTED", "独立核验候选所述事实。", List.of("case:p5-independent-fact")));
            var snapshotId = jdbc.queryForObject("select id from evaluation.candidate_context_snapshot where candidate_id = ? and candidate_revision = ?",
                    UUID.class, candidateId, revision);
            var report = evaluations.runCandidateEvaluation(BOB, Ids.WORKSPACE_A, snapshotId);
            assertThat(report.status()).isEqualTo("PASSED");
            var reviewed = candidates.get(BOB, Ids.WORKSPACE_A, candidateId);
            return candidates.approve(new CandidateService.CandidateApprovalCommand(BOB, Ids.WORKSPACE_A,
                    candidateId, reviewed.rowVersion(), "APPROVED", "批准通过独立保留集的候选修订。", report.id())).id();
        }
        var targetType = candidate.path("targetType").asText();
        var targetId = candidate.path("targetId").isMissingNode() || candidate.path("targetId").isNull()
                ? null : UUID.fromString(candidate.path("targetId").asText());
        var baseVersion = candidate.path("baseVersion").isMissingNode() || candidate.path("baseVersion").isNull()
                ? null : candidate.path("baseVersion").asText();
        var ownerId = UUID.fromString(candidate.path("ownerId").asText());
        var scope = candidate.path("scope").asText();
        var contentHash = candidate.path("contentHash").asText();
        jdbc.update("update learning.candidate set status = 'APPROVED', row_version = 2 where id = ? and status = 'PROPOSED'",
                candidateId);
        var approvalId = UUID.randomUUID();
        var hash = Hashing.sha256("release-fixture:" + candidateId);
        jdbc.update("insert into learning.candidate_approval(id, tenant_id, workspace_id, candidate_id, candidate_revision, approver_id, decision, target_type, target_id, base_version, owner_id, scope, candidate_content_hash, evidence_hash, evaluation_report_id, evaluation_report_hash, evaluation_configuration_hash, dataset_hash, evaluation_summary, authorization_action, valid_until, reason, created_at) "
                        + "values (?, ?, ?, ?, ?, ?, 'APPROVED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{\"status\":\"PASSED\"}'::jsonb, 'learning:approve', ?, ' 集成用例既有批准记录', ?)",
                approvalId, Ids.TENANT_A, Ids.WORKSPACE_A, candidateId, revision, Ids.BOB, targetType, targetId,
                baseVersion, ownerId, scope, contentHash, hash, UUID.randomUUID(), hash, hash, hash,
                Timestamp.from(Instant.now().plusSeconds(600)), Timestamp.from(Instant.now()));
        return approvalId;
    }

    private void grantIterationReviewer(String targetType, UUID targetId) {
        for (var action : List.of("learning:read", "learning:review", "learning:approve", "evaluation:run",
                "evaluation:read", "task:create", "task:read", "knowledge:read", "memory:read"))
            grant(Ids.BOB, action);
        if ("KNOWLEDGE_UPDATE".equals(targetType))
            jdbc.update("insert into knowledge.document_permission(tenant_id, workspace_id, document_id, actor_id, action, status) "
                            + "values (?, ?, ?, ?, 'knowledge:read', 'ACTIVE') on conflict (tenant_id, document_id, actor_id, action) "
                            + "do update set status = 'ACTIVE'",
                    Ids.TENANT_A, Ids.WORKSPACE_A, targetId, Ids.BOB);
    }

    private JsonNode proposeKnowledge(UUID documentId, String content, String key, UUID sourceFeedbackId) throws Exception {
        var response = mvc.perform(post("/api/v1/workspaces/{workspaceId}/learning-candidates", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("targetType", "KNOWLEDGE_UPDATE", "targetId", documentId,
                                "baseVersion", "1", "proposedContent", content, "evidenceRefs", List.of("renewal:17"),
                                "sourceFeedbackId", sourceFeedbackId))))
                .andReturn().getResponse();
        assertThat(response.getStatus()).as(response.getContentAsString()).isEqualTo(201);
        return json.readTree(response.getContentAsString());
    }

    private void grant(UUID actorId, String action) {
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, ?, 'ACTIVE') on conflict do nothing",
                Ids.TENANT_A, Ids.WORKSPACE_A, actorId, action);
    }

    private void verifyKnowledgeOutboxRedelivery(UUID eventId) {
        knowledgeOutbox.publish();
        var status = jdbc.queryForObject("select status from knowledge.outbox where event_id = ?", String.class, eventId);
        var lastError = jdbc.queryForObject("select last_error from knowledge.outbox where event_id = ?", String.class, eventId);
        assertThat(status).as(lastError)
                .isEqualTo("DELIVERED");
        var factKey = "knowledge-outbox:" + eventId;
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class, factKey))
                .isEqualTo(1);
        // 模拟消费事实已提交但生产者确认丢失；重投仍由 Audit 的稳定 factKey 去重。
        jdbc.update("update knowledge.outbox set status = 'PENDING', next_attempt_at = null where event_id = ?", eventId);
        knowledgeOutbox.publish();
        assertThat(jdbc.queryForObject("select status from knowledge.outbox where event_id = ?", String.class, eventId))
                .isEqualTo("DELIVERED");
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class, factKey))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("select task_id from audit.audit_event where fact_key = ?", UUID.class, factKey))
                .isNull();
    }

    private void verifyMemoryOutboxDelivery(UUID eventId) {
        memoryOutbox.publish();
        assertThat(jdbc.queryForObject("select status from memory.outbox where event_id = ?", String.class, eventId))
                .isEqualTo("DELIVERED");
        var factKey = "memory-outbox:" + eventId;
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class, factKey))
                .isEqualTo(1);

        // 故障注入只阻断该测试 Memory 事件，验证失败退避、错误归类和五次重试上限。
        jdbc.execute("create function audit.test_fail_memory_outbox() returns trigger language plpgsql as $$ "
                + "begin if new.fact_key like 'memory-outbox:%' then raise exception 'simulated transient failure'; end if; return new; end $$");
        jdbc.execute("create trigger test_fail_memory_outbox before insert on audit.audit_event "
                + "for each row execute function audit.test_fail_memory_outbox()");
        jdbc.update("update memory.outbox set status = 'PENDING', attempt_count = 0, next_attempt_at = null where event_id = ?", eventId);
        memoryOutbox.publish();
        assertThat(jdbc.queryForObject("select status from memory.outbox where event_id = ?", String.class, eventId))
                .isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("select attempt_count from memory.outbox where event_id = ?", Integer.class, eventId))
                .isEqualTo(1);
        assertThat(jdbc.queryForObject("select last_error from memory.outbox where event_id = ?", String.class, eventId))
                .isNotBlank();
        assertThat(jdbc.queryForObject("select next_attempt_at > now() from memory.outbox where event_id = ?", Boolean.class, eventId))
                .isTrue();

        jdbc.execute("drop trigger test_fail_memory_outbox on audit.audit_event");
        jdbc.execute("drop function audit.test_fail_memory_outbox()");
        jdbc.update("update memory.outbox set next_attempt_at = now() where event_id = ?", eventId);
        memoryOutbox.publish();
        assertThat(jdbc.queryForObject("select status from memory.outbox where event_id = ?", String.class, eventId))
                .isEqualTo("DELIVERED");
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class, factKey))
                .isEqualTo(1);

        jdbc.execute("create function audit.test_fail_memory_outbox() returns trigger language plpgsql as $$ "
                + "begin if new.fact_key like 'memory-outbox:%' then raise exception 'simulated transient failure'; end if; return new; end $$");
        jdbc.execute("create trigger test_fail_memory_outbox before insert on audit.audit_event "
                + "for each row execute function audit.test_fail_memory_outbox()");
        jdbc.update("update memory.outbox set status = 'PENDING', attempt_count = 5, next_attempt_at = null where event_id = ?", eventId);
        memoryOutbox.publish();
        assertThat(jdbc.queryForObject("select attempt_count from memory.outbox where event_id = ?", Integer.class, eventId))
                .isEqualTo(6);
        assertThat(jdbc.queryForObject("select next_attempt_at from memory.outbox where event_id = ?", Timestamp.class, eventId))
                .isNull();
        memoryOutbox.publish();
        assertThat(jdbc.queryForObject("select attempt_count from memory.outbox where event_id = ?", Integer.class, eventId))
                .isEqualTo(6);
        jdbc.execute("drop trigger test_fail_memory_outbox on audit.audit_event");
        jdbc.execute("drop function audit.test_fail_memory_outbox()");
    }

    private ProcessResult runReleaseProcess(UUID candidateId, UUID approvalId, String expectedVersion) throws Exception {
        return runReleaseProcess(candidateId, approvalId, expectedVersion, "publish");
    }

    private ProcessResult runReleaseProcess(UUID candidateId, UUID approvalId, String expectedVersion, String action) throws Exception {
        var classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var java = Path.of(System.getProperty("java.home"), "bin", "java.exe").toString();
        var process = new ProcessBuilder(java, "-cp", classpath, P5ReleaseCrashRunner.class.getName(),
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--server.port=0", "--eaf.security.mode=local",
                "--eaf.task.dispatcher-enabled=false", "--eaf.execution.outbox-publisher-enabled=false",
                "--eaf.knowledge.outbox-publisher-enabled=false", "--eaf.memory.outbox-publisher-enabled=false",
                "--p5.release.candidate-id=" + candidateId, "--p5.release.approval-id=" + approvalId,
                "--p5.release.workspace-id=" + Ids.WORKSPACE_A,
                "--p5.release.expected-version=" + expectedVersion,
                "--p5.release.action=" + action).redirectErrorStream(true).start();
        var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        return new ProcessResult(process.waitFor(), output);
    }

    private UUID createPublishedDocument(String key, String title, String content) throws Exception {
        var result = mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents", Ids.WORKSPACE_A)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("title", title, "sourceRef", "manual://" + key,
                                "content", content, "metadata", Map.of()))))
                .andExpect(status().isCreated()).andReturn();
        var documentId = UUID.fromString(json.readTree(result.getResponse().getContentAsString()).path("id").asText());
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents/{documentId}/chunks",
                        Ids.WORKSPACE_A, documentId).header("Authorization", "Bearer eaf-local-alice"))
                .andExpect(status().isOk());
        var build = json.readTree(mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents/{documentId}/index-builds",
                                Ids.WORKSPACE_A, documentId)
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", key + "-build"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/knowledge/documents/{documentId}/publish",
                        Ids.WORKSPACE_A, documentId)
                        .param("expectedVersion", "1").param("buildId", build.path("id").asText())
                        .header("Authorization", "Bearer eaf-local-alice").header("Idempotency-Key", key + "-publish"))
                .andExpect(status().isOk());
        return documentId;
    }

    private record ProcessResult(int exitCode, String output) { }
}
