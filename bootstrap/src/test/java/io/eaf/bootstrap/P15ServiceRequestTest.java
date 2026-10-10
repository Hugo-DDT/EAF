package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.eaf.approval.api.ApprovalDecisionCommand;
import io.eaf.approval.api.ApprovalService;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.audit.api.AuditPort;
import io.eaf.capability.api.CapabilityService;
import io.eaf.execution.api.ExecutionService;
import io.eaf.knowledge.api.CreateKnowledgeDocumentCommand;
import io.eaf.knowledge.api.KnowledgeService;
import io.eaf.knowledge.infrastructure.KnowledgeOutboxPublisher;
import io.eaf.evaluation.api.EvaluationService;
import io.eaf.evaluation.api.PromptAnalysisEvaluationService;
import io.eaf.evaluation.api.ScenarioEvaluationService;
import io.eaf.evaluation.infrastructure.JdbcEvaluationService;
import io.eaf.evaluation.infrastructure.JdbcScenarioEvaluationService;
import io.eaf.learning.api.CandidateService;
import io.eaf.learning.infrastructure.JdbcCandidateService;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelProfileRef;
import io.eaf.model.api.ModelRequest;
import io.eaf.model.api.ModelResult;
import io.eaf.model.infrastructure.DeterministicModelGateway;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Hashing;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.CreateQualityRunTaskCommand;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.api.WorkflowAutomationService;
import io.eaf.workflow.infrastructure.JdbcWorkflowService;
import io.eaf.workflow.infrastructure.WorkflowDispatcher;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false", "eaf.memory.outbox-publisher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false", "eaf.model.mode=deterministic",
        "eaf.workflow.automation.enabled=false",
        "eaf.model.scenario=SERVICE_REQUEST_SEARCH", "eaf.model.jev.mode=deterministic",
        "eaf.model.jev.evidence-mode=deterministic", "eaf.credentials.p15-service-desk.token=p15-synthetic-token",
        "eaf.credentials.p27-oa.token=p27-oa-token",
        "eaf.credentials.p27-service-desk-result.token=p27-service-token",
        "eaf.learning.improvement.enabled=true", "eaf.learning.improvement.poll-delay-ms=3600000",
        "eaf.learning.improvement-evaluation-poll-delay-ms=3600000",
        "eaf.learning.prompt-improvement-poll-delay-ms=3600000",
        "eaf.evaluation.team-preparation-poll-delay-ms=3600000", "eaf.evaluation.scenario-poll-delay-ms=3600000"})
@AutoConfigureMockMvc
class P15ServiceRequestTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID PLAN_AGENT = UUID.fromString("20000000-0000-4000-8000-000000000010");
    private static final UUID PLAN_CAPABILITY = UUID.fromString("54000000-0000-4000-8000-000000000012");
    private static final UUID REGISTER_CAPABILITY = UUID.fromString("54000000-0000-4000-8000-000000000013");
    private static final UUID REGISTER_WORKFLOW = UUID.fromString("58000000-0000-4000-8000-00000000000e");
    private static final String AUTH = "Bearer eaf-local-alice";
    private static final UUID WORKSPACE = Ids.WORKSPACE_A;
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
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;
    @Autowired JdbcTemplate jdbc;
    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired RuntimeQuery runtimeQuery;
    @Autowired KnowledgeService knowledge;
    @Autowired CapabilityService capabilities;
    @Autowired WorkflowService workflows;
    @Autowired WorkflowAutomationService automations;
    @Autowired CandidateService candidates;
    @Autowired JdbcCandidateService candidateDispatcher;
    @Autowired EvaluationService evaluations;
    @Autowired JdbcEvaluationService evaluationDispatcher;
    @Autowired ScenarioEvaluationService scenarioEvaluations;
    @Autowired PromptAnalysisEvaluationService promptEvaluations;
    @Autowired JdbcScenarioEvaluationService scenarioDispatcher;
    @Autowired WorkflowDispatcher workflowDispatcher;
    @Autowired JdbcWorkflowService jdbcWorkflows;
    @Autowired ExecutionService executions;
    @Autowired ApprovalService approvals;
    @MockitoSpyBean ModelGateway model;
    @Autowired AuditPort audit;
    @Autowired PlatformTransactionManager transactionManager;

    @Test
    void p31PromptRunAcceptsOptionalFeedbackAndReplaysDefaultDeadline() throws Exception {
        var targetPath = "/api/v1/workspaces/" + WORKSPACE + "/prompt-improvement-targets";
        var target = json.readTree(mvc.perform(post(targetPath).header("Authorization", AUTH)
                        .header("Idempotency-Key", "p31-target-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var runPath = "/api/v1/workspaces/" + WORKSPACE + "/prompt-improvement-runs";
        var body = json.createObjectNode().put("targetId", target.path("id").asText())
                .put("expectedTargetVersion", target.path("rowVersion").asLong())
                .put("changeNote", "让首轮分析先区分已有证据与待补信息。")
                .put("instructionAppendix", "先列出已知事实及引用，再明确说明仍缺少哪些信息。");
        body.putArray("sourceFeedbackIds");
        var key = "p31-run-" + UUID.randomUUID();
        var firstResponse = mvc.perform(post(runPath).header("Authorization", AUTH).header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(body)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        var first = json.readTree(firstResponse);
        mvc.perform(get(targetPath + "/" + target.path("id").asText()).header("Authorization", AUTH))
                .andExpect(status().isOk());
        assertThat(first.path("status").asText()).isEqualTo("AWAITING_REVIEW");
        assertThat(first.path("candidateId").asText()).isNotBlank();
        mvc.perform(get("/api/v1/workspaces/" + WORKSPACE + "/learning-candidates/" + first.path("candidateId").asText())
                        .header("Authorization", AUTH))
                .andExpect(status().isOk());
        assertThat(first.path("changeNote").asText()).isEqualTo(body.path("changeNote").asText());
        assertThat(first.path("sourceFeedbackIds")).isEmpty();
        var replayResponse = mvc.perform(post(runPath).header("Authorization", AUTH).header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(body)))
                .andReturn().getResponse();
        assertThat(replayResponse.getStatus()).as(replayResponse.getContentAsString()).isEqualTo(200);
        var replay = json.readTree(replayResponse.getContentAsString());
        assertThat(replay.path("id").asText()).isEqualTo(first.path("id").asText());
        assertThat(replay.path("deadlineAt").asText()).isEqualTo(first.path("deadlineAt").asText());
        mvc.perform(get(runPath + "/" + first.path("id").asText()).header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(status().isForbidden());
    }

    @Test
    void p31ControlledPromptCandidateCanBeReviewedPublishedAdoptedAndWithdrawn() throws Exception {
        grantP23(ALICE, "prompt:manage", "prompt:publish", "learning:propose", "learning:read", "learning:publish",
                "learning:withdraw", "evaluation:run", "evaluation:read", "task:create", "agent:write", "agent:publish",
                "skill:write", "skill:publish", "capability:write", "capability:publish");
        grantP23(ALICE, "prompt:read");
        grantP23(BOB, "learning:read", "learning:review", "learning:approve", "evaluation:read", "prompt:read");
        var marker = "P31_TEST_CONTROLLED_APPENDIX";
        doAnswer(invocation -> p31ControlledPromptResult(invocation.getArgument(0), marker))
                .when(model).call(any(ModelRequest.class));

        var fixtureKey = "p31-controlled-" + UUID.randomUUID();
        publishKnowledge("P31 合成服务请求依据",
                "新员工无法访问项目管理系统，请确认应如何开通账号。会议室 B201 的顶灯闪烁，影响会议，请协助处理。新增育儿假申请需要哪些材料。项目管理系统账号开通属于 IT 服务；会议室照明故障属于设施服务；育儿假申请应核对人事材料清单。",
                fixtureKey);
        new KnowledgeOutboxPublisher(jdbc, audit, transactionManager).publish();

        var targetPath = path("/prompt-improvement-targets");
        var target = json.readTree(mvc.perform(post(targetPath).header("Authorization", AUTH)
                        .header("Idempotency-Key", fixtureKey + "-target").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var runBody = json.createObjectNode().put("targetId", target.path("id").asText())
                .put("expectedTargetVersion", target.path("rowVersion").asLong())
                .put("changeNote", "受控 Gateway 检查候选是否区分完整请求、待澄清请求和证据不足。")
                .put("instructionAppendix", "P31_TEST_CONTROLLED_APPENDIX：依据请求与已授权资料区分分类、待补信息和证据不足。");
        runBody.putArray("sourceFeedbackIds");
        var runResponse = json.readTree(mvc.perform(post(path("/prompt-improvement-runs"))
                        .header("Authorization", AUTH).header("Idempotency-Key", fixtureKey + "-run")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(runBody)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var runId = UUID.fromString(runResponse.path("id").asText());
        var candidateId = UUID.fromString(runResponse.path("candidateId").asText());
        var candidate = candidates.get(ALICE, WORKSPACE, candidateId);
        var proposedVersion = candidate.rowVersion();
        assertThatThrownBy(() -> candidates.review(new CandidateService.CandidateReviewCommand(ALICE, WORKSPACE,
                candidateId, proposedVersion, "ACCEPTED", "proposer cannot review", List.of("policy:service-request-facts"))))
                .isInstanceOf(io.eaf.shared.EafException.class);
        candidate = candidates.review(new CandidateService.CandidateReviewCommand(BOB, WORKSPACE, candidateId,
                candidate.rowVersion(), "ACCEPTED", "按固定合成规则核对 Prompt 补充和基线边界。",
                List.of("policy:service-request-facts")));

        var evaluating = candidates.getPromptImprovementRun(ALICE, WORKSPACE, runId);
        assertThat(evaluating.status()).isEqualTo("DEV_EVALUATING");
        var devReport = drivePromptReport(evaluating.devReportId());
        assertThat(devReport.improvedPairs()).isPositive();
        assertThat(devReport.regressedPairs()).isZero();
        candidateDispatcher.dispatchNextPromptImprovementEvaluation();

        evaluating = candidates.getPromptImprovementRun(ALICE, WORKSPACE, runId);
        assertThat(evaluating.status()).isEqualTo("HELD_OUT_EVALUATING");
        var heldOutReport = drivePromptReport(evaluating.heldOutReportId());
        assertThat(heldOutReport.improvedPairs()).isPositive();
        assertThat(heldOutReport.regressedPairs()).isZero();
        candidateDispatcher.dispatchNextPromptImprovementEvaluation();
        var ready = candidates.getPromptImprovementRun(ALICE, WORKSPACE, runId);
        assertThat(ready.status()).as("P31 prompt run: %s", ready).isEqualTo("READY");
        var evidence = promptEvaluations.releaseEvidence(ALICE, WORKSPACE, candidateId, 1,
                ready.devReportId(), ready.heldOutReportId());
        assertThat(evidence.eligibility()).isEqualTo("ELIGIBLE");
        assertThat(evidence.current()).isTrue();

        var approval = candidates.approve(new CandidateService.CandidateApprovalCommand(BOB, WORKSPACE,
                candidateId, candidate.rowVersion(), "APPROVED", "批准当前 DEV 与 HELD_OUT 工程证据。", ready.heldOutReportId()));
        var currentEvidence = promptEvaluations.releaseEvidence(ALICE, WORKSPACE, candidateId, 1,
                ready.devReportId(), ready.heldOutReportId());
        assertThat(approval.evaluationReportId()).as("approval=%s evidence=%s", approval, currentEvidence)
                .isEqualTo(currentEvidence.reportId());
        assertThat(approval.evaluationReportHash()).as("approval=%s evidence=%s", approval, currentEvidence)
                .isEqualTo(currentEvidence.reportHash());
        assertThat(approval.evaluationConfigurationHash()).as("approval=%s evidence=%s", approval, currentEvidence)
                .isEqualTo(currentEvidence.configurationHash());
        assertThat(approval.datasetHash()).as("approval=%s evidence=%s", approval, currentEvidence)
                .isEqualTo(currentEvidence.datasetHash());
        candidate = candidates.get(ALICE, WORKSPACE, candidateId);
        assertThat(candidates.publish(new CandidateService.CandidateReleaseCommand(ALICE, WORKSPACE,
                candidateId, candidate.rowVersion(), approval.id())).status()).isEqualTo("RELEASED");
        var adoption = candidates.adoptPromptImprovementRun(new CandidateService.PromptAdoptionCommand(ALICE, WORKSPACE,
                candidateId, fixtureKey + "-adopt"));
        assertThat(adoption.status()).isEqualTo("ADOPTED");
        var capability = capabilities.requirePublished(ALICE, WORKSPACE, adoption.capabilityId(), adoption.capabilityVersion());
        var trial = tasks.create(new CreateTaskCommand(ALICE, WORKSPACE, adoption.agentId(), adoption.agentVersion(),
                "新员工无法访问项目管理系统，请确认应如何开通账号。", null, null, fixtureKey + "-trial", "trace-" + fixtureKey,
                "USER", new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(), capability.skillId(),
                        capability.skillVersion(), capability.skillContentHash()), "REST"));
        var work = tasks.claimOne().orElseThrow(() -> new AssertionError(
                "P31 adopted USER trial is not claimable: " + tasks.get(ALICE, WORKSPACE, trial.id())));
        assertThat(work.id()).isEqualTo(trial.id());
        tasks.complete(work, runtime.run(work));
        assertThat(tasks.get(ALICE, WORKSPACE, trial.id()).status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(candidates.recordIteration(ALICE, WORKSPACE, candidateId, trial.id()).iteration().taskStatus())
                .isEqualTo("SUCCEEDED");

        candidate = candidates.get(ALICE, WORKSPACE, candidateId);
        assertThat(candidates.withdrawRelease(new CandidateService.CandidateWithdrawalCommand(ALICE, WORKSPACE,
                candidateId, candidate.rowVersion(), fixtureKey + "-withdraw")).status()).isEqualTo("WITHDRAWN");
        assertThatThrownBy(() -> tasks.create(new CreateTaskCommand(ALICE, WORKSPACE, adoption.agentId(), adoption.agentVersion(),
                "新员工无法访问项目管理系统，请确认应如何开通账号。", null, null, fixtureKey + "-after-withdrawal",
                "trace-" + fixtureKey + "-withdrawn", "USER", new TaskAssetBinding(capability.id(),
                        capability.version(), capability.contentHash(), capability.skillId(),
                        capability.skillVersion(), capability.skillContentHash()), "REST")))
                .isInstanceOf(io.eaf.shared.EafException.class);

        doAnswer(invocation -> new ModelResult("deterministic", "p15-test",
                "{\"action\":\"FINAL\",\"outcome\":\"NEEDS_INPUT\",\"citations\":[],"
                        + "\"questions\":[\"请补充受影响的系统或地点。\"]}", 64, 18, "KNOWN"))
                .when(model).call(any(ModelRequest.class));
        var unchangedKey = fixtureKey + "-unchanged";
        var unchangedTarget = json.readTree(mvc.perform(post(targetPath).header("Authorization", AUTH)
                        .header("Idempotency-Key", unchangedKey + "-target").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var unchangedBody = json.createObjectNode().put("targetId", unchangedTarget.path("id").asText())
                .put("expectedTargetVersion", unchangedTarget.path("rowVersion").asLong())
                .put("changeNote", "验证默认确定性模型对固定提示词保持相同输出。")
                .put("instructionAppendix", "保留原有请求分类和输出行为。");
        unchangedBody.putArray("sourceFeedbackIds");
        var unchangedCreated = json.readTree(mvc.perform(post(path("/prompt-improvement-runs"))
                        .header("Authorization", AUTH).header("Idempotency-Key", unchangedKey + "-run")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(unchangedBody)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var unchangedRunId = UUID.fromString(unchangedCreated.path("id").asText());
        var unchangedCandidateId = UUID.fromString(unchangedCreated.path("candidateId").asText());
        var unchangedCandidate = candidates.get(ALICE, WORKSPACE, unchangedCandidateId);
        candidates.review(new CandidateService.CandidateReviewCommand(BOB, WORKSPACE, unchangedCandidateId,
                unchangedCandidate.rowVersion(), "ACCEPTED", "核对该候选保持固定提示模板的说明。",
                List.of("policy:service-request-facts")));
        var unchangedRun = candidates.getPromptImprovementRun(ALICE, WORKSPACE, unchangedRunId);
        var unchangedDev = drivePromptReport(unchangedRun.devReportId());
        assertThat(unchangedDev.improvedPairs()).isZero();
        assertThat(unchangedDev.unchangedPairs()).isEqualTo(4);
        candidateDispatcher.dispatchNextPromptImprovementEvaluation();
        unchangedRun = candidates.getPromptImprovementRun(ALICE, WORKSPACE, unchangedRunId);
        var unchangedHeldOut = drivePromptReport(unchangedRun.heldOutReportId());
        assertThat(unchangedHeldOut.improvedPairs()).isZero();
        assertThat(unchangedHeldOut.unchangedPairs()).isEqualTo(1);
        candidateDispatcher.dispatchNextPromptImprovementEvaluation();
        assertThat(candidates.getPromptImprovementRun(ALICE, WORKSPACE, unchangedRunId).status())
                .isEqualTo("NOT_SELECTED");
    }

    @Test
    void p21BatchRunsFixedBranchesWithOwnerAndSourceBoundResults() throws Exception {
        var workflowId = UUID.fromString("58000000-0000-4000-8000-000000000018");
        var definition = workflows.get(ALICE, WORKSPACE, workflowId, "1.0.0");
        assertThat(definition.status()).isEqualTo("PUBLISHED");
        assertThat(definition.dependencies()).hasSize(2);
        assertThat(definition.steps()).extracting(io.eaf.workflow.api.WorkflowStepSpec::type)
                .containsExactly(io.eaf.workflow.api.WorkflowStepType.PARALLEL_READ,
                        io.eaf.workflow.api.WorkflowStepType.COMPLETE);
        assertThat(workflowContentHash(definition)).isEqualTo(definition.contentHash());

        publishKnowledge("设备处理知识", "IT 设备故障应先核对设备位置和现象，再依照正式检修流程处理。", "p21-device");
        new KnowledgeOutboxPublisher(jdbc, audit, transactionManager).publish();
        var batchRequest = json.createObjectNode().put("maxActiveItems", 1);
        batchRequest.putArray("items").addObject().put("itemKey", "req-01")
                .put("requestText", "办公投影仪无法连接，需要了解处理方式。").putArray("experienceRefs");
        var createBatchPath = "/api/v1/workspaces/" + WORKSPACE + "/service-request-batches";
        var batchKey = "p21-batch-" + UUID.randomUUID();
        var createdJson = json.readTree(mvc.perform(post(createBatchPath).header("Authorization", AUTH)
                        .header("Idempotency-Key", batchKey).contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(batchRequest)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var batchId = UUID.fromString(createdJson.path("id").asText());
        var replayJson = json.readTree(mvc.perform(post(createBatchPath).header("Authorization", AUTH)
                        .header("Idempotency-Key", batchKey).contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(batchRequest)))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(replayJson.path("id").asText()).isEqualTo(batchId.toString());
        mvc.perform(get(createBatchPath + "/" + batchId).header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(response -> assertThat(response.getResponse().getStatus()).isIn(403, 404));
        assertThat(jdbcWorkflows.dispatchBatchOne()).isTrue();
        var instanceId = jdbc.queryForObject("select workflow_instance_id from workflow.batch_item where batch_id = ? and item_key = 'req-01'",
                UUID.class, batchId);

        for (var attempt = 0; attempt < 24; attempt++) {
            jdbc.update("update workflow.instance set next_poll_at = now() where id = ? and status not in ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')",
                    instanceId);
            workflowDispatcher.dispatchOne();
            var taskIds = jdbc.queryForList("select child_task_id from workflow.parallel_branch where instance_id = ? and child_task_id is not null",
                    UUID.class, instanceId);
            if (taskIds.size() == 2 && taskIds.stream().allMatch(taskId ->
                    tasks.get(ALICE, WORKSPACE, taskId).status() == TaskStatus.QUEUED)) {
                var branchWork = java.util.List.of(tasks.claimOne().orElseThrow(), tasks.claimOne().orElseThrow());
                assertThat(branchWork).extracting(io.eaf.task.api.TaskWorkItem::id).containsExactlyInAnyOrderElementsOf(taskIds);
                var ready = new CountDownLatch(branchWork.size());
                var start = new CountDownLatch(1);
                try (var executor = Executors.newFixedThreadPool(branchWork.size())) {
                    var executions = branchWork.stream().map(work -> executor.submit(() -> {
                        ready.countDown();
                        if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("并行执行屏障超时。");
                        tasks.complete(work, runtime.run(work));
                        return null;
                    })).toList();
                    assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                    assertThat(branchWork).allSatisfy(work ->
                            assertThat(tasks.get(ALICE, WORKSPACE, work.id()).status()).isEqualTo(TaskStatus.RUNNING));
                    start.countDown();
                    for (var execution : executions) execution.get(30, TimeUnit.SECONDS);
                }
            }
            var status = jdbc.queryForObject("select status from workflow.instance where id = ?", String.class, instanceId);
            if (Set.of("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT").contains(status)) break;
        }
        var finalStatus = jdbc.queryForObject("select status from workflow.instance where id = ?", String.class, instanceId);
        assertThat(finalStatus).as("steps=%s branches=%s tasks=%s lease=%s",
                jdbc.queryForList("select step_id, status, selected_next_step_id from workflow.step where instance_id = ?", instanceId),
                jdbc.queryForList("select role, status, child_task_id, error_code from workflow.parallel_branch where instance_id = ?", instanceId),
                jdbc.queryForList("select id, status, error_code, error_detail from task.task where workflow_instance_id = ?", instanceId),
                jdbc.queryForList("select lease_owner_id, lease_fence, lease_until, next_poll_at from workflow.instance where id = ?", instanceId))
                .isEqualTo("SUCCEEDED");
        jdbc.update("update workflow.batch set next_poll_at = now() where id = ?", batchId);
        assertThat(jdbcWorkflows.dispatchBatchOne()).isTrue();
        var item = workflows.getServiceRequestBatchItem(ALICE, WORKSPACE, batchId, "req-01").item();
        assertThat(item.status()).isEqualTo("SUCCEEDED");
        assertThat(item.knowledgeTaskId()).isNotNull();
        assertThat(item.experienceTaskId()).isNotNull();
        var knowledgeResult = json.readTree(tasks.get(ALICE, WORKSPACE, item.knowledgeTaskId()).resultJson());
        var experienceResult = json.readTree(tasks.get(ALICE, WORKSPACE, item.experienceTaskId()).resultJson());
        assertThat(knowledgeResult.path("outcome").asText()).isEqualTo("ANALYZED");
        assertThat(knowledgeResult.path("citations")).hasSize(1);
        assertThat(experienceResult.path("outcome").asText()).isEqualTo("NOT_SELECTED");
        assertThat(experienceResult.path("usedExperienceRefs")).isEmpty();
        assertThatThrownBy(() -> workflows.cancelInstance(ALICE, WORKSPACE, instanceId,
                workflows.getInstance(ALICE, WORKSPACE, instanceId).rowVersion()))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                        error -> assertThat(error.code()).isEqualTo("BATCH_WORKFLOW_MANAGED"));
    }

    @Test
    void p24ExplicitVersionSupportsUserRegistrationAndPairedReport() throws Exception {
        var documentId = publishKnowledge("服务请求知识", "办公电脑维修先记录故障现象并按 IT 设备流程处理。",
                "p24-" + UUID.randomUUID(), Map.of("synthetic", "true"));
        publishKnowledge("IT 设备说明", "办公电脑无法启动属于 IT 设备维修事项；员工希望登记设备检修。",
                "p24-it-" + UUID.randomUUID());
        publishKnowledge("服务请求规范", "服务请求登记所需信息包括设备地点和故障现象；按设备处理规范核对后受理。",
                "p24-policy-" + UUID.randomUUID());
        new KnowledgeOutboxPublisher(jdbc, audit, transactionManager).publish();
        var compact = capabilities.requirePublished(ALICE, WORKSPACE, PLAN_CAPABILITY, "1.1.0");
        assertThat(compact.agentVersion()).isEqualTo("1.1.0");
        assertThat(compact.promptVersion()).isEqualTo("1.0.0");
        assertThat(compact.skillVersion()).isEqualTo("1.0.0");
        assertThat(compact.toolDependencies()).isEmpty();
        var profileCatalog = json.readTree(mvc.perform(get(path("/model-profiles"))
                        .param("capabilityId", PLAN_CAPABILITY.toString()).param("capabilityVersion", "1.1.0")
                        .header("Authorization", AUTH))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(profileCatalog.size()).isEqualTo(2);
        assertThat(profileCatalog.findValuesAsText("profileId")).contains(
                "22000000-0000-4000-8000-000000000001", "22000000-0000-4000-8000-000000000002");

        var baseline = capabilities.requirePublished(ALICE, WORKSPACE, PLAN_CAPABILITY, "1.0.0");
        var baselineTask = tasks.create(new CreateTaskCommand(ALICE, WORKSPACE, PLAN_AGENT, "1.0.0", "办公电脑无法启动",
                null, null, "p24-baseline-" + UUID.randomUUID(), "trace-p24-baseline", "USER",
                new TaskAssetBinding(baseline.id(), baseline.version(), baseline.contentHash(), baseline.skillId(),
                        baseline.skillVersion(), baseline.skillContentHash()), "REST"));
        var baselineWork = tasks.claimOne().orElseThrow();
        tasks.complete(baselineWork, runtime.run(baselineWork));
        var baselinePresentation = runtimeQuery.steps(ALICE, WORKSPACE, baselineTask.id()).stream()
                .filter(step -> "SERVICE_REQUEST_PRESENTATION".equals(step.type())).findFirst().orElseThrow();
        var baselineMeta = json.readTree(baselinePresentation.content());
        assertThat(baselineMeta.path("strategy").asText()).isEqualTo("FULL_CONTEXT_V1");
        assertThat(baselineMeta.path("contextUtf8Bytes").asLong()).isEqualTo(baselineMeta.path("fullContextUtf8Bytes").asLong());

        var userTaskRequest = json.createObjectNode().put("agentId", PLAN_AGENT.toString()).put("agentVersion", "1.1.0")
                .put("capabilityId", PLAN_CAPABILITY.toString()).put("capabilityVersion", "1.1.0")
                .put("input", "办公电脑无法启动");
        userTaskRequest.set("modelProfileRef", json.createObjectNode()
                .put("profileId", "22000000-0000-4000-8000-000000000002").put("version", "1.0.0"));
        var createdUserTask = json.readTree(mvc.perform(post(path("/tasks"))
                        .header("Authorization", AUTH).header("Idempotency-Key", "p24-user-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(userTaskRequest)))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        var userTask = tasks.get(ALICE, WORKSPACE, UUID.fromString(createdUserTask.path("id").asText()));
        var userWork = tasks.claimOne().orElseThrow();
        assertThat(userWork.id()).isEqualTo(userTask.id());
        tasks.complete(userWork, runtime.run(userWork));
        var userResult = tasks.get(ALICE, WORKSPACE, userTask.id());
        assertThat(userResult.status()).isEqualTo(TaskStatus.SUCCEEDED);
        var usageResponse = json.readTree(mvc.perform(get(path("/tasks/" + userTask.id() + "/usage"))
                        .header("Authorization", AUTH))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(usageResponse.path("modelSelection").path("effectiveProfile").path("profileId").asText())
                .isEqualTo("22000000-0000-4000-8000-000000000002");
        assertThat(usageResponse.path("modelSelection").path("effectiveProfile").path("maxOutputTokens").asInt())
                .isEqualTo(1024);
        var userOutput = json.readTree(userResult.resultJson());
        assertThat(userOutput.path("readyToSubmit").asBoolean()).isTrue();
        var userPresentation = runtimeQuery.steps(ALICE, WORKSPACE, userTask.id()).stream()
                .filter(step -> "SERVICE_REQUEST_PRESENTATION".equals(step.type())).findFirst().orElseThrow();
        var userMeta = json.readTree(userPresentation.content());
        assertThat(userMeta.path("strategy").asText()).isEqualTo("KNOWLEDGE_COMPACT_V1");
        assertThat(userMeta.path("messageHash").asText()).hasSize(64);
        assertThat(userMeta.path("contextUtf8Bytes").asLong()).isLessThan(userMeta.path("fullContextUtf8Bytes").asLong());
        var submissionResponse = mvc.perform(post(path("/tasks/" + userTask.id() + "/service-requests"))
                        .header("Authorization", AUTH).header("Idempotency-Key", "p24-confirm-" + userTask.id())
                        .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":" + userResult.version() + "}"))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString();
        var submission = json.readTree(submissionResponse);
        var replay = json.readTree(mvc.perform(post(path("/tasks/" + userTask.id() + "/service-requests"))
                        .header("Authorization", AUTH).header("Idempotency-Key", "p24-confirm-" + userTask.id())
                        .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":" + userResult.version() + "}"))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        assertThat(replay.path("submissionId").asText()).isEqualTo(submission.path("submissionId").asText());
        try (var fixture = new ServiceDeskFixture(json)) {
            jdbc.update("update connector.instance set base_url = ?, status = 'ACTIVE' where tenant_id = ? and workspace_id = ? and provider = ?",
                    fixture.url(), Ids.TENANT_A, WORKSPACE, "P15_INTERNAL_SERVICE_DESK_FIXTURE");
            var workflowId = UUID.fromString(submission.path("workflowId").asText());
            assertThat(workflowDispatcher.dispatchOne()).isTrue();
            assertThat(workflowDispatcher.dispatchOne()).isTrue();
            var childTaskId = jdbc.queryForObject("select child_task_id from workflow.step where instance_id = ? and step_id = 'register'",
                    UUID.class, workflowId);
            var registerWork = tasks.claimOne().orElseThrow();
            assertThat(registerWork.id()).isEqualTo(childTaskId);
            tasks.complete(registerWork, runtime.run(registerWork));
            var waiting = tasks.get(ALICE, WORKSPACE, childTaskId);
            assertThat(waiting.status()).isEqualTo(TaskStatus.WAITING_APPROVAL);
            var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, childTaskId);
            var execution = executions.get(ALICE, WORKSPACE, executionId);
            assertThat(execution.status()).isEqualTo("AWAITING_APPROVAL");
            var approval = approvals.get(reviewer(), WORKSPACE, execution.approvalId());
            jdbc.update("update workflow.instance set next_poll_at = null where id = ?", workflowId);
            assertThat(workflowDispatcher.dispatchOne()).isTrue();
            approvals.decide(new ApprovalDecisionCommand(reviewer(), WORKSPACE, approval.id(), "APPROVED", approval.version()));
            tasks.resume(ALICE, WORKSPACE, childTaskId, waiting.version(), "p24-resume-" + UUID.randomUUID());
            var resumed = tasks.claimOne().orElseThrow();
            tasks.complete(resumed, runtime.run(resumed));
            assertThat(executions.get(ALICE, WORKSPACE, executionId).status()).isEqualTo("SUCCEEDED");
            assertThat(fixture.posts()).hasValue(1);
            assertThat(fixture.gets()).hasValue(1);
        }

        var request = new ScenarioEvaluationService.ScenarioRunRequest("service-request-synthetic", "1.0.0", "DEV", "PAIRED",
                new ScenarioEvaluationService.CapabilityVersion(PLAN_CAPABILITY, "1.0.0"),
                new ScenarioEvaluationService.CapabilityVersion(PLAN_CAPABILITY, "1.1.0"), Instant.now().plusSeconds(900));
        var created = scenarioEvaluations.createRun(ALICE, WORKSPACE, request, "p24-paired-" + UUID.randomUUID());
        var runId = created.report().runId();
        assertThat(jdbc.queryForMap("select manifest::text from evaluation.scenario_run where id = ?", runId)
                .get("manifest").toString()).contains("P15_KNOWLEDGE_PRESENTATION_V1");
        for (var attempt = 0; attempt < 100; attempt++) {
            jdbc.update("update evaluation.scenario_run set next_poll_at = now(), lease_until = null where id = ?", runId);
            scenarioDispatcher.dispatchNext();
            var activeTaskId = jdbc.query("select task_id from evaluation.scenario_sample where run_id = ? and status = 'ACTIVE' "
                            + "and task_id is not null order by case_id, side limit 1", rs -> rs.next() ? rs.getObject(1, UUID.class) : null, runId);
            var work = activeTaskId == null ? java.util.Optional.<io.eaf.task.api.TaskWorkItem>empty() : tasks.claim(activeTaskId);
            if (work.isPresent()) tasks.complete(work.get(), runtime.run(work.get()));
            var report = scenarioEvaluations.getRun(ALICE, WORKSPACE, runId);
            if (Set.of("COMPLETED", "COMPLETED_WITH_ERRORS", "STOPPED", "FAILED", "TIMED_OUT").contains(report.status())) break;
        }
        var report = scenarioEvaluations.getRun(ALICE, WORKSPACE, runId);
        assertThat(report.status()).as("samples=%s tasks=%s run=%s",
                jdbc.queryForList("select case_id, side, status, task_id from evaluation.scenario_sample where run_id = ? order by case_id, side", runId),
                jdbc.queryForList("select id, status, error_code from task.task where id in "
                        + "(select task_id from evaluation.scenario_sample where run_id = ? and task_id is not null)", runId),
                jdbc.queryForMap("select status, stop_reason, next_poll_at, lease_until from evaluation.scenario_run where id = ?", runId))
                .isEqualTo("COMPLETED");
        assertThat(report.optimization().status()).isEqualTo("AVAILABLE");
        assertThat(report.optimization().baseline().preparedRequests()).isPositive();
        assertThat(report.optimization().comparison().preparedRequests()).isPositive();
        assertThat(report.optimization().comparison().contextReductionUtf8Bytes()).isPositive();
        assertThat(report.optimization().conditionsComparable()).isFalse();
        assertThat(report.optimization().conditionsReasons()).contains("MODEL_IDENTITY_NOT_RECORDED");
        assertThat(report.optimization().pairs()).hasSize(4);
        assertThat(report.optimization().pairs()).allSatisfy(pair ->
                assertThat(pair.metricDeltas()).containsKeys("queueMillis", "taskMillis"));
        assertThat(report.optimization().pairedSummary().metricDeltas()).containsKeys("queueMillis", "taskMillis");

        var profileRun = scenarioEvaluations.createRun(ALICE, WORKSPACE,
                new ScenarioEvaluationService.ScenarioRunRequest("service-request-synthetic", "1.0.0", "DEV", "PAIRED",
                        new ScenarioEvaluationService.CapabilityVersion(PLAN_CAPABILITY, "1.0.0"),
                        new ScenarioEvaluationService.CapabilityVersion(PLAN_CAPABILITY, "1.0.0"),
                        Instant.now().plusSeconds(900), "P15_MODEL_PROFILE_V1",
                        new ModelProfileRef(UUID.fromString("22000000-0000-4000-8000-000000000001"), "1.0.0"),
                        new ModelProfileRef(UUID.fromString("22000000-0000-4000-8000-000000000002"), "1.0.0")),
                "p32-profile-paired-" + UUID.randomUUID()).report();
        for (var attempt = 0; attempt < 100; attempt++) {
            jdbc.update("update evaluation.scenario_run set next_poll_at = now(), lease_until = null where id = ?", profileRun.runId());
            scenarioDispatcher.dispatchNext();
            var activeTaskId = jdbc.query("select task_id from evaluation.scenario_sample where run_id = ? and status = 'ACTIVE' "
                            + "and task_id is not null order by case_id, side limit 1", rs -> rs.next() ? rs.getObject(1, UUID.class) : null,
                    profileRun.runId());
            var work = activeTaskId == null ? java.util.Optional.<io.eaf.task.api.TaskWorkItem>empty() : tasks.claim(activeTaskId);
            if (work.isPresent()) tasks.complete(work.get(), runtime.run(work.get()));
            var current = scenarioEvaluations.getRun(ALICE, WORKSPACE, profileRun.runId());
            if (Set.of("COMPLETED", "COMPLETED_WITH_ERRORS", "STOPPED", "FAILED", "TIMED_OUT").contains(current.status())) break;
        }
        var profileReport = scenarioEvaluations.getRun(ALICE, WORKSPACE, profileRun.runId());
        assertThat(profileReport.status()).isEqualTo("COMPLETED");
        assertThat(profileReport.modelProfileEvidence().experiment()).isEqualTo("P15_MODEL_PROFILE_V1");
        assertThat(profileReport.modelProfileEvidence().configurationComparable()).isTrue();
        assertThat(profileReport.modelProfileEvidence().baseline().effectiveProfile().maxOutputTokens()).isEqualTo(8_000);
        assertThat(profileReport.modelProfileEvidence().comparison().effectiveProfile().maxOutputTokens()).isEqualTo(1_024);
        assertThat(profileReport.optimization().pairs()).hasSize(4);
        var recordedOutputLimits = jdbc.queryForList("select distinct effective_output_token_limit from usage.model_usage "
                        + "where scope_type = 'EVALUATION' and scope_id = ? and effective_output_token_limit is not null",
                Integer.class, profileRun.runId());
        assertThat(recordedOutputLimits).contains(1_024);
        assertThat(recordedOutputLimits).anySatisfy(limit -> assertThat(limit).isBetween(1_025, 8_000));

        var evaluationTaskId = scenarioEvaluations.listSamples(ALICE, WORKSPACE, runId, 40).stream()
                .filter(sample -> "COMPARISON".equals(sample.side()) && sample.taskId() != null)
                .map(ScenarioEvaluationService.ScenarioSampleView::taskId).findFirst().orElseThrow();
        var evaluationTask = tasks.getScenarioEvaluationTask(ALICE, WORKSPACE, evaluationTaskId);
        assertThat(json.readTree(evaluationTask.resultJson()).path("submittable").asBoolean()).isFalse();
        assertThatThrownBy(() -> tasks.confirmServiceRequest(ALICE, WORKSPACE, evaluationTaskId,
                evaluationTask.version(), "p24-evaluation-confirm-" + UUID.randomUUID()))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class, error ->
                        assertThat(error.code()).isEqualTo("SERVICE_REQUEST_SOURCE_INVALID"));

        var stopped = scenarioEvaluations.createRun(ALICE, WORKSPACE, request, "p24-stopped-" + UUID.randomUUID()).report();
        var document = knowledge.get(ALICE, WORKSPACE, documentId);
        knowledge.revoke(ALICE, WORKSPACE, documentId, document.rowVersion(), "p24-revoke-" + UUID.randomUUID());
        var invalidated = scenarioEvaluations.getRun(ALICE, WORKSPACE, runId);
        assertThat(invalidated.optimization().status()).isEqualTo("PARTIAL");
        assertThat(invalidated.optimization().reasonCodes()).contains("SOURCE_UNAVAILABLE");

        scenarioEvaluations.stopRun(ALICE, WORKSPACE, stopped.runId(), stopped.version());
        jdbc.update("update evaluation.scenario_run set next_poll_at = now() where id = ?", stopped.runId());
        scenarioDispatcher.dispatchNext();
        var missingReport = scenarioEvaluations.getRun(ALICE, WORKSPACE, stopped.runId());
        assertThat(missingReport.status()).isEqualTo("STOPPED");
        assertThat(missingReport.optimization().status()).isEqualTo("PARTIAL");
        assertThat(missingReport.optimization().baseline().notRunSamples()).isPositive();
        assertThat(missingReport.optimization().baseline().preparedRequests()).isZero();
        assertThat(missingReport.optimization().baseline().usage().calls()).isZero();
        assertThat(missingReport.optimization().baseline().usage().estimatedCostStatus()).isEqualTo("KNOWN");
        assertThat(missingReport.optimization().baseline().usage().actualCostStatus()).isEqualTo("KNOWN");
    }

    @Test
    void boundedAnalysisConfirmationApprovalAndIdempotentLoopbackRegistration() throws Exception {
        var methodModel = new DeterministicModelGateway("SERVICE_REQUEST_SEARCH");
        doAnswer(invocation -> methodModel.call(invocation.getArgument(0))).when(model).call(any(ModelRequest.class));
        doAnswer(invocation -> methodModel.callCount()).when(model).callCount();
        var registration = capabilities.requirePublished(ALICE, WORKSPACE, REGISTER_CAPABILITY, "1.0.0");
        assertThat(registration.contentHash()).isEqualTo("43517612934f783d09a9e801e6c520153a4b20faaeccc5bfbfa626c439de34cb");
        assertThat(registration.toolDependencies()).extracting("name").containsExactly("service.request.register");
        var definition = workflows.get(ALICE, WORKSPACE, REGISTER_WORKFLOW, "1.0.0");
        assertThat(definition.contentHash()).isEqualTo("051a3b499b2450a6f38147422fe78d06c81405b5008bef8bcfac4cec60bcca0d");
        assertThat(definition.dependencies()).singleElement().satisfies(dependency ->
                assertThat(dependency.contentHash()).isEqualTo(registration.contentHash()));

        var itGuideId = publishKnowledge("IT 设备说明", "办公电脑无法启动属于 IT 设备维修事项；员工希望登记设备检修。", "p15-it");
        var requestPolicyId = publishKnowledge("服务请求登记规范", "内部服务请求登记所需信息包括设备地点和故障现象；按设备处理规范核对后受理。", "p15-policy");
        new KnowledgeOutboxPublisher(jdbc, audit, transactionManager).publish();

        var analysis = capabilities.requirePublished(ALICE, WORKSPACE, PLAN_CAPABILITY, "1.0.0");
        assertThat(analysis.toolDependencies()).isEmpty();
        var boundedTask = tasks.create(new CreateTaskCommand(ALICE, WORKSPACE, PLAN_AGENT, "1.0.0", "办公电脑无法启动",
                null, null, "p15-bounded-" + UUID.randomUUID(), "trace-p15-bounded", "USER",
                new TaskAssetBinding(analysis.id(), analysis.version(), analysis.contentHash(), analysis.skillId(),
                        analysis.skillVersion(), analysis.skillContentHash()), "REST"));
        var boundedWork = tasks.claimOne().orElseThrow();
        assertThat(boundedWork.id()).isEqualTo(boundedTask.id());
        tasks.complete(boundedWork, runtime.run(boundedWork));
        var boundedResult = json.readTree(tasks.get(ALICE, WORKSPACE, boundedTask.id()).resultJson());
        assertThat(boundedResult.path("outcome").asText()).isEqualTo("INSUFFICIENT_EVIDENCE");
        assertThat(boundedResult.path("readyToSubmit").asBoolean()).isFalse();
        assertThat(boundedResult.path("searchCount").asInt()).isEqualTo(2);
        assertThat(boundedResult.path("generationCount").asInt()).isEqualTo(1);
        assertThat(boundedResult.path("stopReason").asText()).isEqualTo("NO_NEW_USABLE_EVIDENCE");

        var task = tasks.create(new CreateTaskCommand(ALICE, WORKSPACE, PLAN_AGENT, "1.0.0", "办公电脑无法启动",
                null, null, "p15-analysis-" + UUID.randomUUID(), "trace-p15-analysis", "USER",
                new TaskAssetBinding(analysis.id(), analysis.version(), analysis.contentHash(), analysis.skillId(),
                        analysis.skillVersion(), analysis.skillContentHash()), "REST"));
        var firstWork = tasks.claimOne().orElseThrow();
        assertThat(firstWork.id()).isEqualTo(task.id());
        tasks.complete(firstWork, runtime.run(firstWork));
        var completed = tasks.get(ALICE, WORKSPACE, task.id());
        assertThat(completed.status()).isEqualTo(TaskStatus.SUCCEEDED);
        var result = json.readTree(completed.resultJson());
        assertThat(result.path("outcome").asText()).isEqualTo("READY");
        assertThat(result.path("readyToSubmit").asBoolean()).isTrue();
        assertThat(result.path("searchCount").asInt()).isEqualTo(1);
        assertThat(result.path("generationCount").asInt()).isEqualTo(1);
        var contextRefs = result.path("contextRefs");
        var citedDocumentIds = java.util.stream.StreamSupport.stream(contextRefs.spliterator(), false)
                .map(ref -> ref.path("documentId").asText()).toList();
        assertThat(citedDocumentIds).contains(itGuideId.toString(), requestPolicyId.toString());
        assertThat(java.util.stream.StreamSupport.stream(contextRefs.spliterator(), false)
                .allMatch(ref -> "KNOWLEDGE".equals(ref.path("sourceType").asText()))).isTrue();
        var p15ModelCallBaseline = methodModel.callCount();
        assertThat(p15ModelCallBaseline).isPositive();

        mvc.perform(post(path("/tasks/" + task.id() + "/service-requests"))
                        .header("Authorization", "Bearer eaf-local-bob")
                        .header("Idempotency-Key", "p15-denied-" + task.id())
                        .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":" + completed.version() + "}"))
                .andExpect(status().isForbidden());
        assertThat(jdbc.queryForObject("select count(*) from task.service_request_submission where source_task_id = ?",
                Integer.class, task.id())).isZero();

        var createResponse = mvc.perform(post(path("/tasks/" + task.id() + "/service-requests"))
                        .header("Authorization", AUTH).header("Idempotency-Key", "p15-confirm-" + task.id())
                        .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":" + completed.version() + "}"))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString();
        var submission = json.readTree(createResponse);
        assertThat(submission.path("workflowId").asText()).isNotBlank();
        var replay = json.readTree(mvc.perform(post(path("/tasks/" + task.id() + "/service-requests"))
                        .header("Authorization", AUTH).header("Idempotency-Key", "p15-confirm-" + task.id())
                        .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":" + completed.version() + "}"))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        assertThat(replay.path("submissionId").asText()).isEqualTo(submission.path("submissionId").asText());
        assertThat(jdbc.queryForObject("select count(*) from task.service_request_submission where source_task_id = ?",
                Integer.class, task.id())).isEqualTo(1);
        mvc.perform(get(path("/tasks/" + task.id() + "/service-requests")).header("Authorization", AUTH))
                .andExpect(status().isOk());

        try (var fixture = new ServiceDeskFixture(json)) {
            jdbc.update("update connector.instance set base_url = ?, status = 'ACTIVE' where tenant_id = ? and workspace_id = ? and provider = ?",
                    fixture.url(), Ids.TENANT_A, WORKSPACE, "P15_INTERNAL_SERVICE_DESK_FIXTURE");
            var workflowId = UUID.fromString(submission.path("workflowId").asText());
            var childTaskId = dispatchUntilChildTask(workflowId, "register");
            var waitingWork = tasks.claimOne().orElseThrow();
            assertThat(waitingWork.id()).isEqualTo(childTaskId);
            tasks.complete(waitingWork, runtime.run(waitingWork));
            var waiting = tasks.get(ALICE, WORKSPACE, childTaskId);
            assertThat(waiting.status()).isEqualTo(TaskStatus.WAITING_APPROVAL);
            var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, childTaskId);
            var execution = executions.get(ALICE, WORKSPACE, executionId);
            assertThat(execution.status()).isEqualTo("AWAITING_APPROVAL");
            assertThat(fixture.posts()).hasValue(0);
            var approval = approvals.get(reviewer(), WORKSPACE, execution.approvalId());
            assertThat(approval.state()).isEqualTo("PENDING");

            jdbc.update("update workflow.instance set next_poll_at = null where id = ?", workflowId);
            assertThat(workflowDispatcher.dispatchOne()).isTrue();
            approvals.decide(new ApprovalDecisionCommand(reviewer(), WORKSPACE, approval.id(), "APPROVED", approval.version()));
            tasks.resume(ALICE, WORKSPACE, childTaskId, waiting.version(), "p15-resume-" + UUID.randomUUID());
            var resumed = tasks.claimOne().orElseThrow();
            assertThat(resumed.id()).isEqualTo(childTaskId);
            tasks.complete(resumed, runtime.run(resumed));
            assertThat(tasks.get(ALICE, WORKSPACE, childTaskId).status()).isEqualTo(TaskStatus.SUCCEEDED);
            var verified = executions.get(ALICE, WORKSPACE, executionId);
            assertThat(verified.status()).isEqualTo("SUCCEEDED");
            assertThat(verified.resultJson()).contains("REGISTERED", "requestId", "operationId");
            assertThat(fixture.posts()).hasValue(1);
            assertThat(fixture.gets()).hasValue(1);
            assertThat(fixture.records()).hasSize(1);
            assertThat(fixture.records().values().iterator().next().has("contextRefs")).isFalse();

            for (var i = 0; i < 8; i++) {
                var state = workflows.getInstance(ALICE, WORKSPACE, workflowId);
                if ("SUCCEEDED".equals(state.status()) || "FAILED".equals(state.status())) break;
                jdbc.update("update workflow.instance set next_poll_at = null where id = ?", workflowId);
                if (!workflowDispatcher.dispatchOne()) break;
            }
            assertThat(workflows.getInstance(ALICE, WORKSPACE, workflowId).status()).isEqualTo("SUCCEEDED");

            var prepare = capabilities.requirePublished(ALICE, WORKSPACE,
                    UUID.fromString("54000000-0000-4000-8000-000000000014"), "1.0.0");
            var summary = capabilities.requirePublished(ALICE, WORKSPACE,
                    UUID.fromString("54000000-0000-4000-8000-000000000015"), "1.0.0");
            var handlingDefinition = workflows.get(ALICE, WORKSPACE,
                    UUID.fromString("58000000-0000-4000-8000-00000000000f"), "1.0.0");
            assertThat(handlingDefinition.dependencies()).extracting("contentHash")
                    .containsExactlyInAnyOrder(prepare.contentHash(), summary.contentHash());
            assertThat(workflowContentHash(handlingDefinition)).isEqualTo(handlingDefinition.contentHash());
            mvc.perform(post(path("/service-request-handlings"))
                            .header("Authorization", AUTH).header("Idempotency-Key", "p16-unverified-" + UUID.randomUUID())
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"submissionId\":\"" + UUID.randomUUID() + "\",\"assigneeId\":\""
                                    + Ids.BOB + "\",\"sharedBrief\":\"未核验来源\"}"))
                    .andExpect(status().is4xxClientError());
            var handlingBody = json.createObjectNode().put("submissionId", submission.path("submissionId").asText())
                    .put("assigneeId", Ids.BOB.toString()).put("sharedBrief", "办公电脑已登记；请核对设备状态并反馈处理结果。");
            var handlingKey = "p16-" + UUID.randomUUID();
            var handlingResponse = json.readTree(mvc.perform(post(path("/service-request-handlings"))
                            .header("Authorization", AUTH).header("Idempotency-Key", handlingKey)
                            .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(handlingBody)))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
            var handlingId = UUID.fromString(handlingResponse.path("instanceId").asText());
            var handlingReplay = json.readTree(mvc.perform(post(path("/service-request-handlings"))
                            .header("Authorization", AUTH).header("Idempotency-Key", handlingKey)
                            .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(handlingBody)))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
            assertThat(handlingReplay.path("instanceId").asText()).isEqualTo(handlingId.toString());

            driveHandlingUntil(handlingId, "WAITING_HUMAN");
            var workItemId = jdbc.queryForObject("select id from workflow.human_work_item where instance_id = ?",
                    UUID.class, handlingId);
            assertThat(jdbc.queryForObject("select status = 'WAITING_HUMAN' and lease_owner_id is null "
                    + "and lease_until is null and next_poll_at = deadline_at from workflow.instance where id = ?",
                    Boolean.class, handlingId)).isTrue();
            var bobItem = json.readTree(mvc.perform(get(path("/work-items/" + workItemId))
                            .header("Authorization", "Bearer eaf-local-bob"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            assertThat(bobItem.path("sharedBrief").asText()).contains("已登记");
            assertThat(bobItem.path("allowedActions").toString()).contains("complete");
            mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch(
                            path("/work-items/" + workItemId + "/assignee"))
                            .header("Authorization", AUTH).contentType(MediaType.APPLICATION_JSON)
                            .content("{\"expectedVersion\":1,\"assigneeId\":\"" + Ids.ALICE + "\"}"))
                    .andExpect(status().isOk());
            mvc.perform(get(path("/work-items/" + workItemId)).header("Authorization", "Bearer eaf-local-bob"))
                    .andExpect(status().isNotFound());

            var p30 = exerciseP30Weekly(workItemId);

            var completion = "{\"expectedVersion\":2,\"outcome\":\"NEEDS_FOLLOWUP\","
                    + "\"summary\":\"已完成初步检查，仍需设备维护人员确认。\","
                    + "\"nextAction\":\"安排设备维护人员复查。\"}";
            var p16Completed = json.readTree(mvc.perform(post(path("/work-items/" + workItemId + "/complete"))
                            .header("Authorization", AUTH).header("Idempotency-Key", "p16-result-1")
                            .contentType(MediaType.APPLICATION_JSON).content(completion))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            assertThat(p16Completed.path("status").asText()).isEqualTo("COMPLETED");
            var replayedCompletion = json.readTree(mvc.perform(post(path("/work-items/" + workItemId + "/complete"))
                            .header("Authorization", AUTH).header("Idempotency-Key", "p16-result-1")
                            .contentType(MediaType.APPLICATION_JSON).content(completion))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            assertThat(replayedCompletion.path("completedAt").asText()).isEqualTo(p16Completed.path("completedAt").asText());
            mvc.perform(post(path("/work-items/" + workItemId + "/complete"))
                            .header("Authorization", AUTH).header("Idempotency-Key", "p16-result-1")
                            .contentType(MediaType.APPLICATION_JSON).content(completion.replace("初步检查", "其他处理")))
                    .andExpect(status().isConflict());

            exerciseP30Event(p30.eventSubscriptionId());
            assertThatThrownBy(() -> tasks.get(ALICE, WORKSPACE, p30.weeklyTaskId()))
                    .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                            error -> assertThat(error.code()).isEqualTo("AUTOMATION_SOURCE_CHANGED"));

            driveHandlingUntil(handlingId, "SUCCEEDED");
            var handlingResult = json.readTree(workflows.getInstance(ALICE, WORKSPACE, handlingId).resultJson());
            assertThat(handlingResult.path("outcome").asText()).isEqualTo("NEEDS_FOLLOWUP");
            assertThat(handlingResult.path("requestId").asText()).isEqualTo(handlingResponse.path("requestId").asText());
            assertThat(handlingResult.path("resultSummary").asText()).isNotBlank();
            assertThat(jdbc.queryForObject("select count(*) from task.task "
                    + "where workflow_instance_id = ? and workflow_step_id in ('prepare','summarize')", Integer.class, handlingId))
                    .isEqualTo(2);
            assertThat(jdbc.queryForList("select agent_id from task.task where workflow_instance_id = ? "
                            + "and workflow_step_id in ('prepare','summarize')", UUID.class, handlingId))
                    .containsExactlyInAnyOrder(UUID.fromString("20000000-0000-4000-8000-000000000011"),
                            UUID.fromString("20000000-0000-4000-8000-000000000012"));
            assertThat(jdbc.queryForObject("select count(distinct root_task_id) from task.task "
                    + "where workflow_instance_id = ? and workflow_step_id in ('prepare','summarize')", Integer.class, handlingId))
                    .isEqualTo(1);
            assertThat(model.callCount()).isEqualTo(p15ModelCallBaseline + 4);
            exerciseProjectBrief(workItemId);
            assertThat(model.callCount()).isEqualTo(p15ModelCallBaseline + 5);

            var p27Capability = capabilities.requirePublished(ALICE, WORKSPACE,
                    UUID.fromString("54000000-0000-4000-8000-000000000022"), "1.0.0");
            assertThat(p27Capability.contentHash()).isEqualTo("aa7ba842a57cb162eabd0d720ec31f2a3cfaf03d6d028ac5ebc56efa088c8708");
            var p27WorkflowHashes = Map.of(
                    "58000000-0000-4000-8000-000000000019", "d0de3577d3e637dc8e9cc8c6d4aa2e56dc48e46ed32d96b332a7d6b82b5b7997",
                    "58000000-0000-4000-8000-00000000001a", "998666cbd099054627b001414e38772160345feca99fdb53650db92727faf336",
                    "58000000-0000-4000-8000-00000000001b", "1404ed1622636164f51f3f3a8a294c2585fb7de2928eb5197ab1fc3a5ce75901",
                    "58000000-0000-4000-8000-00000000001c", "c760e8765af106694594a1c7cda9d5ce954c4f0ab4c2a13f37dc90e2925e3b14");
            for (var entry : p27WorkflowHashes.entrySet()) {
                var fixedWorkflow = workflows.get(ALICE, WORKSPACE, UUID.fromString(entry.getKey()), "1.0.0");
                assertThat(fixedWorkflow.contentHash()).isEqualTo(entry.getValue());
                assertThat(fixedWorkflow.dependencies()).singleElement().satisfies(dependency ->
                        assertThat(dependency.contentHash()).isEqualTo(p27Capability.contentHash()));
            }

            jdbc.update("update connector.instance set base_url = ?, status = 'ACTIVE' where tenant_id = ? and workspace_id = ? "
                            + "and provider in ('P27_OA_TODO_FIXTURE','P27_SERVICE_DESK_RESULT_FIXTURE')",
                    fixture.url(), Ids.TENANT_A, WORKSPACE);
            var oaList = json.readTree(mvc.perform(post(path("/oa/todo-queries"))
                            .header("Authorization", AUTH).header("Idempotency-Key", "p27-oa-list-" + UUID.randomUUID())
                            .contentType(MediaType.APPLICATION_JSON).content("{}"))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
            var oaListId = UUID.fromString(oaList.path("queryId").asText());
            driveP27Workflow(oaListId, "read", false);
            var oaListResult = json.readTree(mvc.perform(get(path("/oa/todo-queries/" + oaListId)).header("Authorization", AUTH))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            assertThat(oaListResult.path("result").path("page").path("items").get(0).path("todoId").asText())
                    .isEqualTo("todo-1");
            mvc.perform(get(path("/oa/todo-queries/" + oaListId)).header("Authorization", "Bearer eaf-local-bob"))
                    .andExpect(status().isNotFound());
            var oaItem = json.readTree(mvc.perform(post(path("/oa/todo-queries/item"))
                            .header("Authorization", AUTH).header("Idempotency-Key", "p27-oa-item-" + UUID.randomUUID())
                            .contentType(MediaType.APPLICATION_JSON).content("{\"todoId\":\"todo-1\"}"))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
            var oaItemId = UUID.fromString(oaItem.path("queryId").asText());
            driveP27Workflow(oaItemId, "read", false);
            var oaItemResult = json.readTree(mvc.perform(get(path("/oa/todo-queries/" + oaItemId)).header("Authorization", AUTH))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            assertThat(oaItemResult.path("result").path("found").asBoolean()).isTrue();
            mvc.perform(post(path("/oa/todo-queries")).header("Authorization", AUTH)
                            .header("Idempotency-Key", "p27-oa-invalid-" + UUID.randomUUID())
                            .contentType(MediaType.APPLICATION_JSON).content("{\"ownerId\":\"other\"}"))
                    .andExpect(status().isBadRequest());

            var stateQuery = json.readTree(mvc.perform(post(path("/work-items/" + workItemId + "/external-state-queries"))
                            .header("Authorization", AUTH).header("Idempotency-Key", "p27-state-" + UUID.randomUUID())
                            .contentType(MediaType.APPLICATION_JSON).content("{}"))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
            var stateQueryId = UUID.fromString(stateQuery.path("queryId").asText());
            driveP27Workflow(stateQueryId, "read", false);
            var stateResult = json.readTree(mvc.perform(get(path("/work-items/" + workItemId
                                    + "/external-state-queries/" + stateQueryId)).header("Authorization", AUTH))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            assertThat(stateResult.path("result").path("state").path("status").asText()).isEqualTo("REGISTERED");

            var workItemVersion = jdbc.queryForObject("select row_version from workflow.human_work_item where id = ?",
                    Long.class, workItemId);
            var syncPath = path("/work-items/" + workItemId + "/result-syncs");
            var staleSourceBody = "{\"expectedWorkItemVersion\":" + (workItemVersion - 1)
                    + ",\"stateQueryId\":\"" + stateQueryId + "\"}";
            mvc.perform(post(syncPath).header("Authorization", AUTH).header("Idempotency-Key", "p27-stale-source-" + UUID.randomUUID())
                            .contentType(MediaType.APPLICATION_JSON).content(staleSourceBody))
                    .andExpect(status().isConflict());

            var requestId = handlingResult.path("requestId").asText();
            fixture.advanceExternalVersion(requestId);
            var conflictKey = "p27-conflict-" + UUID.randomUUID();
            var conflictBody = "{\"expectedWorkItemVersion\":" + workItemVersion
                    + ",\"stateQueryId\":\"" + stateQueryId + "\"}";
            var conflictSync = json.readTree(mvc.perform(post(syncPath).header("Authorization", AUTH)
                            .header("Idempotency-Key", conflictKey).contentType(MediaType.APPLICATION_JSON).content(conflictBody))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
            var conflictId = UUID.fromString(conflictSync.path("id").asText());
            var conflictWorkflowId = UUID.fromString(conflictSync.path("workflowInstanceId").asText());
            var conflictTaskId = driveP27Workflow(conflictWorkflowId, "record", true);
            var conflictTask = tasks.get(ALICE, WORKSPACE, conflictTaskId);
            assertThat(conflictTask.status()).isEqualTo(TaskStatus.WAITING_APPROVAL);
            var conflictExecutionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, conflictTaskId);
            var conflictExecution = executions.get(ALICE, WORKSPACE, conflictExecutionId);
            assertThat(conflictExecution.status()).isEqualTo("AWAITING_APPROVAL");
            assertThat(fixture.p27Posts()).hasValue(0);
            var conflictApproval = approvals.get(reviewer(), WORKSPACE, conflictExecution.approvalId());
            approvals.decide(new ApprovalDecisionCommand(reviewer(), WORKSPACE, conflictApproval.id(), "APPROVED", conflictApproval.version()));
            tasks.resume(ALICE, WORKSPACE, conflictTaskId, conflictTask.version(), "p27-conflict-resume-" + UUID.randomUUID());
            var conflictWork = tasks.claimOne().orElseThrow();
            assertThat(conflictWork.id()).isEqualTo(conflictTaskId);
            tasks.complete(conflictWork, runtime.run(conflictWork));
            assertThat(executions.get(ALICE, WORKSPACE, conflictExecutionId).status()).isEqualTo("FAILED");
            driveP27Workflow(conflictWorkflowId, "record", false, "FAILED");
            assertThat(fixture.p27Posts()).hasValue(1);
            assertThat(fixture.p27Readbacks()).hasValue(0);
            var conflictRead = mvc.perform(get(syncPath + "/" + conflictId).header("Authorization", AUTH))
                    .andReturn().getResponse();
            assertThat(conflictRead.getStatus()).withFailMessage(conflictRead.getContentAsString()).isEqualTo(200);
            var conflictResult = json.readTree(conflictRead.getContentAsString());
            assertThat(conflictResult.path("syncStatus").asText()).isEqualTo("FAILED_SAFE");

            var currentStateQuery = json.readTree(mvc.perform(post(path("/work-items/" + workItemId + "/external-state-queries"))
                            .header("Authorization", AUTH).header("Idempotency-Key", "p27-state-current-" + UUID.randomUUID())
                            .contentType(MediaType.APPLICATION_JSON).content("{}"))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
            var currentStateQueryId = UUID.fromString(currentStateQuery.path("queryId").asText());
            driveP27Workflow(currentStateQueryId, "read", false);
            var syncKey = "p27-sync-" + UUID.randomUUID();
            var syncBody = "{\"expectedWorkItemVersion\":" + workItemVersion
                    + ",\"stateQueryId\":\"" + currentStateQueryId + "\"}";
            var sync = json.readTree(mvc.perform(post(syncPath).header("Authorization", AUTH)
                            .header("Idempotency-Key", syncKey).contentType(MediaType.APPLICATION_JSON).content(syncBody))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
            var syncId = UUID.fromString(sync.path("id").asText());
            var syncWorkflowId = UUID.fromString(sync.path("workflowInstanceId").asText());
            var syncTaskId = driveP27Workflow(syncWorkflowId, "record", true);
            var syncTask = tasks.get(ALICE, WORKSPACE, syncTaskId);
            assertThat(syncTask.status()).isEqualTo(TaskStatus.WAITING_APPROVAL);
            var syncExecutionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, syncTaskId);
            var syncExecution = executions.get(ALICE, WORKSPACE, syncExecutionId);
            assertThat(syncExecution.status()).isEqualTo("AWAITING_APPROVAL");
            var syncApproval = approvals.get(reviewer(), WORKSPACE, syncExecution.approvalId());
            approvals.decide(new ApprovalDecisionCommand(reviewer(), WORKSPACE, syncApproval.id(), "APPROVED", syncApproval.version()));
            tasks.resume(ALICE, WORKSPACE, syncTaskId, syncTask.version(), "p27-resume-" + UUID.randomUUID());
            var syncWork = tasks.claimOne().orElseThrow();
            assertThat(syncWork.id()).isEqualTo(syncTaskId);
            tasks.complete(syncWork, runtime.run(syncWork));
            assertThat(executions.get(ALICE, WORKSPACE, syncExecutionId).status()).isEqualTo("SUCCEEDED");
            driveP27Workflow(syncWorkflowId, "record", false);
            assertThat(fixture.p27Posts()).hasValue(2);
            assertThat(fixture.p27Readbacks()).hasValue(1);
            var syncReadResponse = mvc.perform(get(syncPath + "/" + syncId).header("Authorization", AUTH))
                    .andReturn().getResponse();
            assertThat(syncReadResponse.getStatus()).withFailMessage(syncReadResponse.getContentAsString()).isEqualTo(200);
            var synced = json.readTree(syncReadResponse.getContentAsString());
            assertThat(synced.path("syncStatus").asText()).isEqualTo("SUCCEEDED");
            assertThat(synced.path("externalStatus").asText()).isEqualTo("IN_PROGRESS");
            var syncReplay = json.readTree(mvc.perform(post(syncPath).header("Authorization", AUTH)
                            .header("Idempotency-Key", syncKey).contentType(MediaType.APPLICATION_JSON).content(syncBody))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
            assertThat(syncReplay.path("id").asText()).isEqualTo(syncId.toString());
            assertThat(fixture.p27Posts()).hasValue(2);

            var teamPath = path("/team-experiences");
            var teamBody = json.createObjectNode().put("scenarioKey", "printer-jam")
                    .put("title", "反复卡纸时先检查进纸组件")
                    .put("appliesWhen", "完成基本清理后仍出现卡纸的办公打印机。")
                    .put("content", "记录复现条件并安排维护人员检查进纸组件，不把一次清理当成已解决。")
                    .put("sourceWorkItemId", workItemId.toString());
            var card = json.readTree(mvc.perform(post(teamPath).header("Authorization", AUTH)
                            .header("Idempotency-Key", "p17-create-" + workItemId)
                            .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(teamBody)))
                    .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
            var cardId = UUID.fromString(card.path("cardId").asText());
            assertThat(card.path("revision").asInt()).isEqualTo(1);
            var cardReplay = json.readTree(mvc.perform(post(teamPath).header("Authorization", AUTH)
                            .header("Idempotency-Key", "p17-create-" + workItemId)
                            .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(teamBody)))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            assertThat(cardReplay.path("replayed").asBoolean()).isTrue();
            var changedTeamBody = teamBody.deepCopy().put("title", "同键内容不能替换");
            mvc.perform(post(teamPath).header("Authorization", AUTH)
                            .header("Idempotency-Key", "p17-create-" + workItemId)
                            .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(changedTeamBody)))
                    .andExpect(status().isConflict());
            var personalCards = mvc.perform(get(path("/experience-cards")).header("Authorization", AUTH))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
            assertThat(personalCards).doesNotContain(cardId.toString());
            mvc.perform(get("/api/v1/workspaces/" + Ids.WORKSPACE_B + "/team-experiences/" + cardId)
                            .header("Authorization", AUTH))
                    .andExpect(responseCheck -> assertThat(responseCheck.getResponse().getStatus()).isIn(403, 404));
            mvc.perform(get(path("/team-experiences/" + cardId)).header("Authorization", "Bearer eaf-local-bob"))
                    .andExpect(status().isNotFound());
            mvc.perform(post(teamPath + "/" + cardId + "/versions/1/publish")
                            .header("Authorization", AUTH).header("Idempotency-Key", "p17-publish-" + cardId)
                            .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":1}"))
                    .andExpect(status().isOk());
            var sharedCard = json.readTree(mvc.perform(get(path("/team-experiences/" + cardId))
                            .header("Authorization", "Bearer eaf-local-bob"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            assertThat(sharedCard.path("activeRevision").asInt()).isEqualTo(1);
            var discovered = json.readTree(mvc.perform(post(teamPath + "/discover").header("Authorization", AUTH)
                            .contentType(MediaType.APPLICATION_JSON).content("{\"scenarioKey\":\"printer-jam\",\"keywords\":[\"进纸组件\"],\"limit\":10}"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            assertThat(discovered.path("algorithmVersion").asText()).isEqualTo("TEAM_KEYWORD_DISCOVERY_V1");
            assertThat(discovered.path("items")).anySatisfy(item -> {
                assertThat(item.path("cardId").asText()).isEqualTo(cardId.toString());
                assertThat(item.path("revision").asInt()).isEqualTo(1);
                assertThat(item.path("matchedTerms")).extracting(JsonNode::asText).containsExactly("进纸组件");
                assertThat(item.path("matchedFields").toString()).contains("title", "content");
            });

            var duplicateCard = json.readTree(mvc.perform(post(teamPath).header("Authorization", AUTH)
                            .header("Idempotency-Key", "p17-create-duplicate-" + workItemId)
                            .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(teamBody)))
                    .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
            var duplicateCardId = UUID.fromString(duplicateCard.path("cardId").asText());
            mvc.perform(post(teamPath + "/" + duplicateCardId + "/versions/1/publish")
                            .header("Authorization", AUTH).header("Idempotency-Key", "p17-publish-duplicate-" + duplicateCardId)
                            .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":1}"))
                    .andExpect(status().isOk());

            var oversized = teamBody.deepCopy().put("title", "标".repeat(80))
                    .put("appliesWhen", "条件".repeat(150)).put("content", "建议".repeat(400));
            var oversizedRefs = new ArrayList<Map<String, Object>>();
            for (var i = 0; i < 3; i++) {
                var extra = json.readTree(mvc.perform(post(teamPath).header("Authorization", AUTH)
                                .header("Idempotency-Key", "p18-oversize-" + i + "-" + workItemId)
                                .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(oversized)))
                        .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
                var extraId = extra.path("cardId").asText();
                mvc.perform(post(teamPath + "/" + extraId + "/versions/1/publish")
                                .header("Authorization", AUTH).header("Idempotency-Key", "p18-oversize-publish-" + extraId)
                                .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":1}"))
                        .andExpect(status().isOk());
                oversizedRefs.add(Map.of("cardId", extraId, "revision", 1));
            }
            var oversizedHandling = json.createObjectNode().put("submissionId", submission.path("submissionId").asText())
                    .put("assigneeId", Ids.BOB.toString()).put("sharedBrief", "合成请求")
                    .put("scenarioKey", "printer-jam");
            oversizedHandling.set("teamExperienceRefs", json.valueToTree(oversizedRefs));
            mvc.perform(post(path("/service-request-handlings")).header("Authorization", AUTH)
                            .header("Idempotency-Key", "p18-original-limit-" + workItemId)
                            .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(oversizedHandling)))
                    .andExpect(status().isBadRequest());

            var handlingBodyP17 = json.createObjectNode().put("submissionId", submission.path("submissionId").asText())
                    .put("assigneeId", Ids.BOB.toString())
                    .put("sharedBrief", "打印机已登记；请核对卡纸现象并反馈处理结果。")
                    .put("scenarioKey", "printer-jam");
            handlingBodyP17.set("teamExperienceRefs", json.valueToTree(java.util.List.of(
                    Map.of("cardId", cardId.toString(), "revision", 1),
                    Map.of("cardId", duplicateCardId.toString(), "revision", 1))));
            var handlingP17 = json.readTree(mvc.perform(post(path("/service-request-handlings"))
                            .header("Authorization", AUTH).header("Idempotency-Key", "p17-handling-" + cardId)
                            .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(handlingBodyP17)))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
            var handlingP17Id = UUID.fromString(handlingP17.path("instanceId").asText());
            assertThat(workflows.getInstance(ALICE, WORKSPACE, handlingP17Id).workflowVersion()).isEqualTo("1.2.0");
            var definitionP17 = workflows.get(ALICE, WORKSPACE,
                    UUID.fromString("58000000-0000-4000-8000-00000000000f"), "1.1.0");
            assertThat(definitionP17.contentHash()).isEqualTo("b1738b650c09d282068f9a4f93010dd25854b2b8ddeb35cd4c1e87394655743b");
            assertThat(workflowContentHash(definitionP17)).isEqualTo(definitionP17.contentHash());
            assertThat(definitionP17.dependencies()).extracting("capabilityVersion")
                    .containsExactlyInAnyOrder("1.1.0", "1.0.0");
            var definitionP18 = workflows.get(ALICE, WORKSPACE,
                    UUID.fromString("58000000-0000-4000-8000-00000000000f"), "1.2.0");
            assertThat(definitionP18.contentHash())
                    .isEqualTo("ec85a6ebae19f84be36ddc49234aab9006a0c2fd1750fb52a9dc3292c3abacde");
            assertThat(workflowContentHash(definitionP18)).isEqualTo(definitionP18.contentHash());
            assertThat(definitionP18.dependencies()).extracting("capabilityVersion")
                    .containsExactlyInAnyOrder("1.2.0", "1.0.0");
            driveHandlingUntil(handlingP17Id, "WAITING_HUMAN");
            var handlingP17ItemId = jdbc.queryForObject("select id from workflow.human_work_item where instance_id = ?",
                    UUID.class, handlingP17Id);
            var teamUsage = json.readTree(mvc.perform(get(path("/work-items/" + handlingP17ItemId))
                            .header("Authorization", "Bearer eaf-local-bob"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString()).path("teamExperienceUsage");
            assertThat(teamUsage.path("included").size()).isEqualTo(2);
            assertThat(teamUsage.path("included").findValuesAsText("cardId"))
                    .containsExactlyInAnyOrder(cardId.toString(), duplicateCardId.toString());
            assertThat(teamUsage.path("included").get(0).path("revision").asInt()).isEqualTo(1);
            var prepareTaskId = jdbc.queryForObject("select child_task_id from workflow.step where instance_id = ? and step_id = 'prepare'",
                    UUID.class, handlingP17Id);
            var usageEvidence = jdbc.queryForMap("select content, validation from agent_runtime.step where task_id = ? and type = 'TEAM_EXPERIENCE_USAGE'",
                    prepareTaskId);
            assertThat(usageEvidence.get("validation")).isEqualTo("USED");
            assertThat((String) usageEvidence.get("content")).contains(cardId.toString(), "contentHash");
            var presentationEvidence = json.readTree(jdbc.queryForObject(
                    "select content from agent_runtime.step where task_id = ? and type = 'TEAM_EXPERIENCE_PRESENTATION'",
                    String.class, prepareTaskId));
            assertThat(presentationEvidence.path("strategy").asText()).isEqualTo("TEAM_EXPERIENCE_EXACT_DEDUP_V1");
            assertThat(presentationEvidence.path("state").asText()).isEqualTo("PREPARED");
            assertThat(presentationEvidence.path("sourceCount").asInt()).isEqualTo(2);
            assertThat(presentationEvidence.path("renderedBlockCount").asInt()).isEqualTo(1);
            assertThat(presentationEvidence.path("originalChars").asInt())
                    .isGreaterThan(presentationEvidence.path("renderedChars").asInt());
            assertThat(presentationEvidence.path("modelMessagesHash").asText()).matches("[0-9a-f]{64}");
            assertThat(presentationEvidence.path("groups").get(0).path("citationIds").toString())
                    .contains("team-experience-1", "team-experience-2");
            assertThat((String) usageEvidence.get("content")).doesNotContain("记录卡纸位置并安排维护人员检查进纸组件");
            var p17Completion = "{\"expectedVersion\":1,\"outcome\":\"COMPLETED\","
                    + "\"summary\":\"已检查进纸组件并记录复现条件。\",\"nextAction\":\"如再次卡纸，安排维护人员复查。\"}";
            mvc.perform(post(path("/work-items/" + handlingP17ItemId + "/complete"))
                            .header("Authorization", "Bearer eaf-local-bob")
                            .header("Idempotency-Key", "p17-result-" + handlingP17Id)
                            .contentType(MediaType.APPLICATION_JSON).content(p17Completion))
                    .andExpect(status().isOk());
            driveHandlingUntil(handlingP17Id, "SUCCEEDED");
            var p17Result = json.readTree(workflows.getInstance(ALICE, WORKSPACE, handlingP17Id).resultJson());
            assertThat(p17Result.path("teamExperienceUsage").path("included").get(0).path("contentHash").asText())
                    .isEqualTo(teamUsage.path("included").get(0).path("contentHash").asText());
            assertThat(model.callCount()).isEqualTo(p15ModelCallBaseline + 7);

            // 已创建的精确版本引用不会被悄悄换成纠正后的新修订。
            var staleV1 = json.readTree(mvc.perform(post(path("/service-request-handlings"))
                            .header("Authorization", AUTH).header("Idempotency-Key", "p17-stale-v1-" + cardId)
                            .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(handlingBodyP17)))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
            var saveV2 = json.readTree(mvc.perform(post(teamPath + "/" + cardId + "/versions")
                            .header("Authorization", AUTH).header("Idempotency-Key", "p17-save-v2-" + cardId)
                            .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(Map.of(
                                    "expectedVersion", 2, "title", "重复卡纸先检查进纸组件", "appliesWhen", "清理后仍有卡纸的打印机。",
                                    "content", "记录纸张规格和卡纸位置，再转维护人员检查进纸组件。",
                                    "sourceWorkItemId", workItemId.toString()))))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            var teamDraftHidden = json.readTree(mvc.perform(get(path("/team-experiences/" + cardId))
                            .header("Authorization", "Bearer eaf-local-bob"))
                    .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
            assertThat(teamDraftHidden.path("latestRevision").asInt()).isEqualTo(1);
            assertThat(teamDraftHidden.path("latest").isNull() || teamDraftHidden.path("latest").isMissingNode()).isTrue();
            mvc.perform(post(teamPath + "/" + cardId + "/versions/1/publish")
                            .header("Authorization", AUTH).header("Idempotency-Key", "p17-stale-publish-" + cardId)
                            .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":3}"))
                    .andExpect(status().isConflict());
            mvc.perform(post(teamPath + "/" + cardId + "/versions/2/publish")
                            .header("Authorization", AUTH).header("Idempotency-Key", "p17-publish-v2-" + cardId)
                            .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":3}"))
                    .andExpect(status().isOk());
            var beforeStaleRunCalls = model.callCount();
            var staleV1Id = UUID.fromString(staleV1.path("instanceId").asText());
            driveHandlingUntil(staleV1Id, "FAILED");
            assertThat(model.callCount()).isEqualTo(beforeStaleRunCalls);
            assertThat(saveV2.path("revision").asInt()).isEqualTo(2);

            var staleV2Body = handlingBodyP17.deepCopy();
            staleV2Body.set("teamExperienceRefs", json.valueToTree(java.util.List.of(
                    Map.of("cardId", cardId.toString(), "revision", 2),
                    Map.of("cardId", duplicateCardId.toString(), "revision", 1))));
            var staleV2 = json.readTree(mvc.perform(post(path("/service-request-handlings"))
                            .header("Authorization", AUTH).header("Idempotency-Key", "p17-stale-v2-" + cardId)
                            .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(staleV2Body)))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
            mvc.perform(post(teamPath + "/" + cardId + "/revoke")
                            .header("Authorization", AUTH).header("Idempotency-Key", "p17-revoke-v2-" + cardId)
                            .contentType(MediaType.APPLICATION_JSON).content("{\"expectedVersion\":4,\"expectedActiveRevision\":2}"))
                    .andExpect(status().isOk());
            var beforeRevokedRunCalls = model.callCount();
            driveHandlingUntil(UUID.fromString(staleV2.path("instanceId").asText()), "FAILED");
            assertThat(model.callCount()).isEqualTo(beforeRevokedRunCalls);
            mvc.perform(get(path("/team-experiences/" + cardId)).header("Authorization", "Bearer eaf-local-bob"))
                    .andExpect(status().isNotFound());

            runP23Lifecycle(createP23SourceTask(duplicateCardId, 1, "source"), duplicateCardId);
        }
    }

    private UUID createP23SourceTask(UUID cardId, int revision, String suffix) throws Exception {
        var request = json.createObjectNode().put("maxActiveItems", 1);
        var item = request.putArray("items").addObject().put("itemKey", "p23-" + suffix)
                .put("requestText", "打印机反复卡纸，先核实进纸位置和影响范围。")
                .put("scenarioKey", "printer-jam");
        item.putArray("experienceRefs").addObject().put("cardId", cardId.toString()).put("revision", revision);
        var batchPath = path("/service-request-batches");
        var response = json.readTree(mvc.perform(post(batchPath).header("Authorization", AUTH)
                        .header("Idempotency-Key", "p23-source-batch-" + cardId + "-" + suffix)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(request)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var batchId = UUID.fromString(response.path("id").asText());
        assertThat(jdbcWorkflows.dispatchBatchOne()).isTrue();
        var instanceId = jdbc.queryForObject("select workflow_instance_id from workflow.batch_item where batch_id = ?",
                UUID.class, batchId);
        for (var attempt = 0; attempt < 24; attempt++) {
            jdbc.update("update workflow.instance set next_poll_at = now() where id = ? "
                    + "and status not in ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')", instanceId);
            workflowDispatcher.dispatchOne();
            var queued = jdbc.queryForList("select id from task.task where workflow_instance_id = ? and status = 'QUEUED' "
                    + "order by created_at", UUID.class, instanceId);
            for (var taskId : queued) {
                var work = tasks.claimOne().orElseThrow(() -> new AssertionError("source Task could not be claimed: " + taskId));
                assertThat(work.id()).isEqualTo(taskId);
                tasks.complete(work, runtime.run(work));
            }
            var status = jdbc.queryForObject("select status from workflow.instance where id = ?", String.class, instanceId);
            if (Set.of("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT").contains(status)) break;
        }
        assertThat(workflows.getInstance(ALICE, WORKSPACE, instanceId).status()).isEqualTo("SUCCEEDED");
        jdbc.update("update workflow.batch set next_poll_at = now() where id = ?", batchId);
        assertThat(jdbcWorkflows.dispatchBatchOne()).isTrue();
        var sourceTaskId = jdbc.queryForObject("select child_task_id from workflow.parallel_branch "
                + "where instance_id = ? and role = 'experience'", UUID.class, instanceId);
        assertThat(tasks.get(ALICE, WORKSPACE, sourceTaskId).status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(runtimeQuery.teamExperienceUsage(ALICE, WORKSPACE, sourceTaskId).included())
                .anyMatch(used -> used.cardId().equals(cardId) && used.revision() == revision);
        return sourceTaskId;
    }

    private void runP23Lifecycle(UUID sourceTaskId, UUID cardId) throws Exception {
        grantP23(ALICE, "feedback:create", "learning:propose", "learning:read", "learning:publish",
                "learning:withdraw", "task:create", "task:cancel", "evaluation:run", "evaluation:read",
                "memory:read", "memory:write", "memory:publish", "memory:scope:team");
        grantP23(BOB, "learning:read", "learning:review", "learning:approve", "evaluation:read",
                "evaluation:review", "evaluation:run", "memory:read", "memory:scope:team");

        var feedbackPath = path("/tasks/" + sourceTaskId + "/feedback");
        var feedback = json.readTree(mvc.perform(post(feedbackPath).header("Authorization", AUTH)
                        .header("Idempotency-Key", "p23-source-feedback-" + sourceTaskId)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsString(Map.of(
                                "correction", "先核对设备现象和影响范围；信息不足时提出补充问题。",
                                "evidence", "合成交接处理记录要求区分观察事实和待确认事项。"))))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var card = json.readTree(mvc.perform(get(path("/team-experiences/" + cardId)).header("Authorization", AUTH))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(card.path("activeRevision").asInt()).isEqualTo(1);

        var runBody = Map.of("cardId", cardId, "baseRevision", 1, "expectedCardVersion", card.path("version").asLong(),
                "sourceFeedbackIds", List.of(feedback.path("id").asText()),
                "sharedCorrection", "建议记录可观察现象、影响范围和已核实信息，不把推测当成事实。",
                "datasetKey", "service-request-preparation", "datasetVersion", "1.0.0");
        var runPath = path("/learning/improvement-runs");
        var run = json.readTree(mvc.perform(post(runPath).header("Authorization", AUTH)
                        .header("Idempotency-Key", "p23-run-" + cardId).contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(runBody)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var runId = UUID.fromString(run.path("id").asText());
        var generationTaskId = UUID.fromString(run.path("generationTaskId").asText());
        var generation = tasks.claim(generationTaskId).orElseThrow();
        tasks.complete(generation, runtime.run(generation));
        candidateDispatcher.dispatchNextImprovementGeneration();

        var awaitingReview = candidates.getImprovementRun(ALICE, WORKSPACE, runId);
        assertThat(awaitingReview.status()).as("P23 run stop reason: %s, generation error: %s", awaitingReview.stopReason(),
                tasks.getTeamImprovementGenerationTask(ALICE, WORKSPACE, generationTaskId).errorDetail()).isEqualTo("AWAITING_REVIEW");
        var candidate = candidates.get(ALICE, WORKSPACE, awaitingReview.candidateId());
        candidate = candidates.review(new CandidateService.CandidateReviewCommand(BOB, WORKSPACE,
                candidate.id(), candidate.rowVersion(), "ACCEPTED", "按已知处理规范独立核对候选事实。",
                List.of("policy:service-request-facts")));
        var reviewedCandidateId = candidate.id();
        var reviewedRevision = candidate.revision();
        var teamSnapshot = evaluations.findTeamPreparationSnapshot(ALICE, WORKSPACE, reviewedCandidateId, reviewedRevision)
                .orElseThrow();
        assertThat(teamSnapshot.targetType()).isEqualTo("TEAM_EXPERIENCE_UPDATE");
        assertThat(teamSnapshot.candidateContentHash()).matches("[0-9a-f]{64}");
        assertThat(evaluations.findTeamPreparationSnapshot(BOB, WORKSPACE, reviewedCandidateId, reviewedRevision)).isEmpty();
        var delegatedEvaluationActor = new ActorContext(UUID.randomUUID(), Ids.TENANT_A, ActorType.AGENT,
                Set.of("evaluation:run"), ALICE.actorId(), UUID.randomUUID(), WORKSPACE, "a".repeat(64));
        assertThatThrownBy(() -> evaluations.findTeamPreparationSnapshot(delegatedEvaluationActor, WORKSPACE,
                reviewedCandidateId, reviewedRevision))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                        error -> assertThat(error.code()).isEqualTo("POLICY_DENIED"));
        var devReportId = candidates.getImprovementRun(ALICE, WORKSPACE, runId).devReportId();
        driveP23Report(devReportId);
        var sampleTaskId = jdbc.queryForObject("select task_id from evaluation.team_preparation_sample "
                + "where run_id = ? and status = 'SUCCEEDED' order by case_id limit 1", UUID.class, devReportId);
        var redactedSampleTask = tasks.get(ALICE, WORKSPACE, sampleTaskId);
        assertThat(redactedSampleTask.inputText()).isNull();
        assertThat(redactedSampleTask.resultJson()).isNull();
        candidateDispatcher.dispatchNextImprovementEvaluation();

        var heldOutReportId = candidates.getImprovementRun(ALICE, WORKSPACE, runId).heldOutReportId();
        assertThat(heldOutReportId).isNotNull();
        assertThatThrownBy(() -> evaluations.getTeamPreparationPair(ALICE, WORKSPACE, heldOutReportId, "unregistered-case"))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                        error -> assertThat(error.code()).isEqualTo("RESOURCE_NOT_FOUND"));
        driveP23Report(heldOutReportId);
        candidateDispatcher.dispatchNextImprovementEvaluation();
        for (var caseId : jdbc.queryForList("select distinct case_id from evaluation.team_preparation_sample where run_id = ?",
                String.class, heldOutReportId)) {
            // 该确定性集成夹具只验证人工复核记录和发布门连接，不宣称候选有真实业务改进。
            evaluations.reviewTeamPreparationPair(BOB, WORKSPACE, heldOutReportId, caseId,
                    new EvaluationService.TeamPreparationReviewCommand("BETTER", "测试夹具确认比较结果已由独立 HUMAN 记录。",
                            List.of("policy:synthetic-service-request-rubric"), null), "p23-review-" + caseId);
        }
        candidateDispatcher.dispatchNextImprovementEvaluation();
        var ready = candidates.getImprovementRun(ALICE, WORKSPACE, runId);
        assertThat(ready.status()).isEqualTo("READY");
        var evidence = evaluations.getTeamPreparationReleaseEvidence(ALICE, WORKSPACE, ready.candidateId(),
                ready.candidateRevision());
        assertThat(evidence.eligibility()).isEqualTo("ELIGIBLE");
        var currentCard = json.readTree(mvc.perform(get(path("/team-experiences/" + cardId)).header("Authorization", AUTH))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        var reviewerCard = json.readTree(mvc.perform(get(path("/team-experiences/" + cardId))
                        .header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(reviewerCard.path("latest").isNull()).isTrue();
        assertThat(reviewerCard.path("version").asLong()).isEqualTo(reviewerCard.path("activeRevision").asInt());
        assertThat(currentCard.path("ownerId").asText()).isEqualTo(candidate.ownerId().toString());
        assertThat(currentCard.path("version").asLong()).as("card version").isEqualTo(candidate.baseSnapshot().path("expectedCardVersion").asLong());
        assertThat(currentCard.path("latestRevision").asInt()).as("latest revision").isEqualTo(candidate.baseSnapshot().path("baseRevision").asInt());
        assertThat(currentCard.path("activeRevision").asInt()).as("active revision").isEqualTo(candidate.baseSnapshot().path("baseRevision").asInt());
        assertThat(currentCard.path("active").path("memoryVersion").asText()).isEqualTo(candidate.baseVersion());
        assertThat(currentCard.path("active").path("expiresAt").asText()).isEqualTo(candidate.baseSnapshot().path("base").path("expiresAt").asText());
        assertThat(currentCard.path("scenarioKey").asText()).isEqualTo(candidate.baseSnapshot().path("scenarioKey").asText());

        var approval = candidates.approve(new CandidateService.CandidateApprovalCommand(BOB, WORKSPACE,
                candidate.id(), candidate.rowVersion(), "APPROVED", "独立批准当前完整的保留集证据。", heldOutReportId));
        var publishedCandidate = candidates.get(ALICE, WORKSPACE, candidate.id());
        var release = candidates.publish(new CandidateService.CandidateReleaseCommand(ALICE, WORKSPACE,
                candidate.id(), publishedCandidate.rowVersion(), approval.id()));
        assertThat(release.status()).isEqualTo("RELEASED");
        var releasedCard = json.readTree(mvc.perform(get(path("/team-experiences/" + cardId)).header("Authorization", AUTH))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(releasedCard.path("activeRevision").asInt()).isEqualTo(2);
        var followupTaskId = createP23SourceTask(cardId, 2, "followup");
        var iteration = candidates.recordIteration(ALICE, WORKSPACE, candidate.id(), followupTaskId).iteration();
        assertThat(iteration.taskStatus()).isEqualTo("SUCCEEDED");
        assertThat(iteration.usageStatus()).isNotEqualTo("NOT_AVAILABLE");
        assertThat(iteration.targetVersion()).isEqualTo(release.targetVersion());
        assertThat(iteration.targetSource()).isNotNull();
        assertThat(iteration.factOutcome()).isEqualTo("UNVERIFIED");

        var afterRelease = candidates.get(ALICE, WORKSPACE, candidate.id());
        var withdrawal = candidates.withdrawRelease(new CandidateService.CandidateWithdrawalCommand(ALICE, WORKSPACE,
                candidate.id(), afterRelease.rowVersion(), "p23-withdrawal-fixture"));
        assertThat(withdrawal.status()).isEqualTo("WITHDRAWN");
    }

    private ModelResult p31ControlledPromptResult(ModelRequest request, String appendixMarker) {
        var userText = request.messages().stream().filter(message -> "user".equalsIgnoreCase(message.role()))
                .map(message -> message.content()).reduce((first, second) -> second).orElse("");
        var candidate = request.messages().stream().filter(message -> "system".equals(message.role()))
                .anyMatch(message -> message.content().contains(appendixMarker));
        if (userText.contains("天王星"))
            return new ModelResult("deterministic", "p15-test",
                    "{\"action\":\"FINAL\",\"outcome\":\"INSUFFICIENT_EVIDENCE\","
                            + "\"citations\":[],\"questions\":[],\"missingInformation\":\"缺少可核验的正式依据。\"}", 64, 18, "KNOWN");
        if (userText.contains("我需要访问一个系统") || !candidate)
            return new ModelResult("deterministic", "p15-test",
                    "{\"action\":\"FINAL\",\"outcome\":\"NEEDS_INPUT\",\"citations\":[],"
                            + "\"questions\":[\"请补充受影响的系统或地点。\"]}", 64, 18, "KNOWN");
        var originalRequest = Pattern.compile("\\\"originalRequest\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"")
                .matcher(userText);
        var requestText = originalRequest.find() ? originalRequest.group(1) : userText;
        var category = requestText.contains("会议室 B201") ? "FACILITIES" : requestText.contains("育儿假") ? "HR" : "IT";
        var matcher = Pattern.compile("\\\"citationId\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"").matcher(userText);
        if (!matcher.find()) return new ModelResult("deterministic", "p15-test",
                "{\"action\":\"FINAL\",\"outcome\":\"INSUFFICIENT_EVIDENCE\","
                        + "\"citations\":[],\"questions\":[],\"missingInformation\":\"没有检索到可引用的正式依据。\"}",
                64, 18, "KNOWN");
        var citation = "[\"" + matcher.group(1) + "\"]";
        var output = ("{\"action\":\"FINAL\",\"outcome\":\"READY\",\"category\":\"%s\","
                + "\"title\":\"合成服务请求\",\"summary\":\"按授权资料整理的待处理请求。\","
                + "\"handlingSuggestion\":\"依据当前适用规范核验后处理。\",\"citations\":%s,\"questions\":[]}")
                .formatted(category, citation);
        return new ModelResult("deterministic", "p15-test", output, 64, 18, "KNOWN");
    }

    private ScenarioEvaluationService.ScenarioRunReport drivePromptReport(UUID reportId) {
        for (var attempt = 0; attempt < 50; attempt++) {
            jdbc.update("update evaluation.scenario_run set next_poll_at = now(), lease_until = null where id = ?", reportId);
            scenarioDispatcher.dispatchNext();
            var active = jdbc.queryForList("select task_id from evaluation.scenario_sample where run_id = ? and status = 'ACTIVE' "
                    + "and task_id is not null order by case_id, side", UUID.class, reportId);
            for (var taskId : active) {
                var work = tasks.claim(taskId).orElseThrow(() -> new AssertionError("Prompt evaluation Task could not be claimed: " + taskId));
                tasks.complete(work, runtime.run(work));
            }
            var report = scenarioEvaluations.getRun(ALICE, WORKSPACE, reportId);
            if (Set.of("COMPLETED", "COMPLETED_WITH_ERRORS", "STOPPED", "FAILED", "TIMED_OUT").contains(report.status())) {
                assertThat(report.status()).as("Prompt report %s samples=%s", reportId,
                        jdbc.queryForList("select case_id, side, status, error_code, task_id from evaluation.scenario_sample "
                                + "where run_id = ? order by case_id, side", reportId)).isEqualTo("COMPLETED");
                assertThat(report.completedSamples()).isEqualTo(report.plannedSamples());
                return report;
            }
        }
        throw new AssertionError("Prompt evaluation did not complete: " + scenarioEvaluations.getRun(ALICE, WORKSPACE, reportId));
    }

    private void driveP23Report(UUID reportId) {
        for (var attempt = 0; attempt < 40; attempt++) {
            jdbc.update("update evaluation.team_preparation_run set next_poll_at = now() where id = ? and lease_owner is null", reportId);
            evaluationDispatcher.dispatchNextTeamPreparation();
            var active = jdbc.queryForList("select task_id from evaluation.team_preparation_sample "
                    + "where run_id = ? and status = 'ACTIVE' and task_id is not null order by case_id, side", UUID.class, reportId);
            if (attempt == 0 && !active.isEmpty()) assertTeamPreparationSampleCannotBeReused(reportId, active.get(0));
            for (var taskId : active) {
                var work = tasks.claim(taskId).orElseThrow(() -> new AssertionError("sample Task could not be claimed: " + taskId));
                tasks.complete(work, runtime.run(work));
            }
            var report = evaluations.getTeamPreparationRun(ALICE, WORKSPACE, reportId);
            if (Set.of("COMPLETED", "COMPLETED_WITH_ERRORS", "STOPPED", "FAILED", "TIMED_OUT").contains(report.status())) {
                assertThat(report.status()).isEqualTo("COMPLETED");
                assertThat(report.completedPairs()).isEqualTo(report.plannedPairs());
                return;
            }
        }
        throw new AssertionError("evaluation did not complete: " + evaluations.getTeamPreparationRun(ALICE, WORKSPACE, reportId));
    }

    private void assertTeamPreparationSampleCannotBeReused(UUID reportId, UUID taskId) {
        var command = jdbc.query("select s.id, s.snapshot_id, s.task_key, s.input_text, s.agent_id, s.agent_version, "
                        + "s.capability_id, s.capability_version, s.capability_hash, s.skill_id, s.skill_version, s.skill_hash, "
                        + "r.quality_run_id, r.deadline_at from evaluation.team_preparation_sample s "
                        + "join evaluation.team_preparation_run r on r.id = s.run_id where s.run_id = ? and s.task_id = ?",
                rs -> rs.next() ? new CreateQualityRunTaskCommand(ALICE, WORKSPACE,
                        rs.getObject("quality_run_id", UUID.class),
                        rs.getObject("agent_id", UUID.class), rs.getString("agent_version"), rs.getString("input_text"),
                        "TEAM_EXPERIENCE_PREPARATION", rs.getObject("snapshot_id", UUID.class).toString(),
                        rs.getString("task_key") + "-duplicate", "p23-duplicate-sample", "EVALUATION",
                        rs.getObject("id", UUID.class), new io.eaf.task.api.TaskAssetBinding(
                                rs.getObject("capability_id", UUID.class), rs.getString("capability_version"),
                                rs.getString("capability_hash"), rs.getObject("skill_id", UUID.class),
                                rs.getString("skill_version"), rs.getString("skill_hash")),
                        rs.getTimestamp("deadline_at").toInstant()) : null,
                reportId, taskId);
        assertThat(command).isNotNull();
        assertThatThrownBy(() -> tasks.createQualityRunTask(command))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                        error -> assertThat(error.code()).isEqualTo("TEAM_PREPARATION_SAMPLE_BINDING_MISMATCH"));
        var unregistered = new CreateQualityRunTaskCommand(command.actor(), command.workspaceId(), command.qualityRunId(),
                command.agentId(), command.agentVersion(), command.input(), command.businessEntityType(),
                command.businessEntityId(), command.idempotencyKey(), command.traceId(), command.source(),
                UUID.randomUUID(), command.assetBinding(), command.deadlineAt());
        assertThatThrownBy(() -> tasks.createQualityRunTask(unregistered))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                        error -> assertThat(error.code()).isEqualTo("TEAM_PREPARATION_SAMPLE_BINDING_MISMATCH"));
    }

    private void grantP23(ActorContext actor, String... actions) {
        for (var action : actions) jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                actor.tenantId(), WORKSPACE, actor.actorId(), action);
    }

    private UUID dispatchUntilChildTask(UUID workflowInstanceId, String stepId) {
        for (var attempt = 0; attempt < 20; attempt++) {
            var childTaskId = jdbc.query("select child_task_id from workflow.step where instance_id = ? and step_id = ?",
                    rs -> rs.next() ? rs.getObject("child_task_id", UUID.class) : null, workflowInstanceId, stepId);
            if (childTaskId != null) return childTaskId;
            jdbc.update("update workflow.instance set next_poll_at = null, lease_until = null where id = ?", workflowInstanceId);
            if (!workflowDispatcher.dispatchOne()) break;
        }
        throw new AssertionError("Workflow 没有为指定步骤创建 Task：" + workflowInstanceId + "/" + stepId);
    }

    private UUID driveP27Workflow(UUID instanceId, String stepId, boolean stopAtApproval) {
        return driveP27Workflow(instanceId, stepId, stopAtApproval, "SUCCEEDED");
    }

    private UUID driveP27Workflow(UUID instanceId, String stepId, boolean stopAtApproval, String expectedTerminalStatus) {
        UUID taskId = null;
        for (var attempt = 0; attempt < 30; attempt++) {
            var instance = workflows.getInstance(ALICE, WORKSPACE, instanceId);
            if (Set.of("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT").contains(instance.status())) {
                if (expectedTerminalStatus.equals(instance.status())) return taskId;
                throw new AssertionError("P27 workflow ended at " + instance.status() + " with " + instance.errorCode());
            }
            jdbc.update("update workflow.instance set next_poll_at = now() where id = ?", instanceId);
            workflowDispatcher.dispatchOne();
            taskId = jdbc.query("select child_task_id from workflow.step where instance_id = ? and step_id = ?",
                    rs -> rs.next() ? rs.getObject("child_task_id", UUID.class) : null, instanceId, stepId);
            if (taskId == null) continue;
            var task = tasks.get(ALICE, WORKSPACE, taskId);
            if (stopAtApproval && task.status() == TaskStatus.WAITING_APPROVAL) return taskId;
            if (task.status() == TaskStatus.QUEUED) {
                var work = tasks.claimOne().orElseThrow();
                assertThat(work.id()).isEqualTo(taskId);
                tasks.complete(work, runtime.run(work));
            }
        }
        throw new AssertionError("P27 workflow did not reach its expected state: " + instanceId);
    }

    private void driveHandlingUntil(UUID instanceId, String targetStatus) {
        for (var attempt = 0; attempt < 20; attempt++) {
            var instance = workflows.getInstance(ALICE, WORKSPACE, instanceId);
            if (targetStatus.equals(instance.status())) return;
            if (java.util.Set.of("FAILED", "CANCELLED", "TIMED_OUT").contains(instance.status())) {
                var failures = jdbc.queryForList("select workflow_step_id, error_code, error_detail from task.task "
                        + "where workflow_instance_id = ? order by created_at", instanceId);
                throw new AssertionError("workflow ended at " + instance.status() + " with error "
                        + instance.errorCode() + "; task failures=" + failures);
            }
            if ("WAITING_HUMAN".equals(instance.status())) {
                var item = jdbc.queryForObject("select status from workflow.human_work_item where instance_id = ?",
                        String.class, instanceId);
                if ("WAITING_HUMAN".equals(targetStatus) && "OPEN".equals(item)) return;
                throw new AssertionError("stopped at an unexpected human wait.");
            }
            jdbc.update("update workflow.instance set next_poll_at = null where id = ? and status <> 'WAITING_HUMAN'", instanceId);
            workflowDispatcher.dispatchOne();
            var work = tasks.claimOne();
            if (work.isPresent()) tasks.complete(work.get(), runtime.run(work.get()));
        }
        throw new AssertionError("workflow did not reach " + targetStatus + ".");
    }

    private P30Subscriptions exerciseP30Weekly(UUID p16WorkItemId) throws Exception {
        var endpoint = path("/automation-subscriptions");
        var weeklyBody = json.createObjectNode().put("name", "本人每周 P16 待办摘要")
                .put("triggerKind", "WEEKLY").put("dayOfWeek", "MONDAY").put("localTime", "09:00")
                .put("timeZone", "UTC").put("maxItems", 20)
                .put("expiresAt", Instant.now().plusSeconds(60L * 24 * 60 * 60).toString()).put("maxRuns", 3);
        var weeklyKey = "p30-weekly-" + UUID.randomUUID();
        var weekly = json.readTree(mvc.perform(post(endpoint).header("Authorization", AUTH)
                        .header("Idempotency-Key", weeklyKey).contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(weeklyBody)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var weeklyId = UUID.fromString(weekly.path("id").asText());
        var replay = json.readTree(mvc.perform(post(endpoint).header("Authorization", AUTH)
                        .header("Idempotency-Key", weeklyKey).contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(weeklyBody)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        assertThat(replay.path("id").asText()).isEqualTo(weeklyId.toString());
        mvc.perform(get(endpoint + "/" + weeklyId).header("Authorization", "Bearer eaf-local-bob"))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isIn(403, 404));

        var planned = Instant.now().minusSeconds(90).truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
        var local = java.time.LocalDateTime.ofInstant(planned, java.time.ZoneOffset.UTC);
        jdbc.update("update workflow.automation_subscription set day_of_week = ?, local_time = ?, time_zone = 'UTC', "
                        + "next_fire_at = ?, updated_at = now() where id = ?",
                local.getDayOfWeek().name(), java.sql.Time.valueOf(local.toLocalTime()),
                java.sql.Timestamp.from(planned), weeklyId);
        assertThat(automations.dispatchOne()).isTrue();
        assertThat(automations.dispatchOne()).isTrue();
        var weeklyRunId = jdbc.queryForObject("select id from workflow.automation_run where subscription_id = ?",
                UUID.class, weeklyId);
        var weeklyTaskId = jdbc.queryForObject("select task_id from workflow.automation_run where id = ?", UUID.class, weeklyRunId);
        var weeklyWork = tasks.claimOne().orElseThrow();
        assertThat(weeklyWork.id()).isEqualTo(weeklyTaskId);
        tasks.complete(weeklyWork, runtime.run(weeklyWork));
        assertThat(automations.dispatchOne()).isTrue();
        assertThat(jdbc.queryForObject("select status from workflow.automation_run where id = ?", String.class, weeklyRunId))
                .as("P30 Task state: %s", jdbc.queryForMap("select status, error_code, error_detail from task.task where id = ?", weeklyTaskId))
                .isEqualTo("SUCCEEDED");
        assertThat(jdbc.queryForObject("select source = 'USER' and run_kind = 'AGENT' and workflow_id is null "
                        + "and business_entity_type is null from task.task where id = ?", Boolean.class, weeklyTaskId)).isTrue();
        var weeklyResult = tasks.get(ALICE, WORKSPACE, weeklyTaskId);
        assertThat(json.readTree(weeklyResult.resultJson()).path("markdown").asText()).contains("本人 P16 待办摘要", "W1");
        assertThat(tasks.listMyRootResults(ALICE, WORKSPACE, null, null, 20).items())
                .anyMatch(resultItem -> resultItem.id().equals(weeklyTaskId));
        assertThatThrownBy(() -> tasks.get(BOB, WORKSPACE, weeklyTaskId))
                .isInstanceOf(io.eaf.shared.EafException.class);

        var current = automations.get(ALICE, WORKSPACE, weeklyId);
        var pauseBody = json.createObjectNode().put("expectedVersion", current.version());
        var paused = json.readTree(mvc.perform(post(endpoint + "/" + weeklyId + "/pause")
                        .header("Authorization", AUTH).header("Idempotency-Key", "p30-pause-" + weeklyId)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(pauseBody)))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(paused.path("status").asText()).isEqualTo("PAUSED");
        var resumeBody = json.createObjectNode().put("expectedVersion", paused.path("version").asLong());
        var resumed = json.readTree(mvc.perform(post(endpoint + "/" + weeklyId + "/resume")
                        .header("Authorization", AUTH).header("Idempotency-Key", "p30-resume-" + weeklyId)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(resumeBody)))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(resumed.path("status").asText()).isEqualTo("ACTIVE");
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete(endpoint + "/" + weeklyId)
                        .param("expectedVersion", resumed.path("version").asText())
                        .header("Authorization", AUTH).header("Idempotency-Key", "p30-delete-" + weeklyId))
                .andExpect(status().isOk())
                .andExpect(result -> assertThat(json.readTree(result.getResponse().getContentAsString()).path("status").asText())
                        .isEqualTo("DELETED"));

        var eventBody = json.createObjectNode().put("name", "指定 P16 工作项变化摘要")
                .put("triggerKind", "P16_WORK_ITEM_CHANGED").put("workItemId", p16WorkItemId.toString())
                .put("expiresAt", Instant.now().plusSeconds(60L * 24 * 60 * 60).toString()).put("maxRuns", 1);
        var event = json.readTree(mvc.perform(post(endpoint).header("Authorization", AUTH)
                        .header("Idempotency-Key", "p30-event-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(eventBody)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());

        var revokedWeeklyBody = weeklyBody.deepCopy().put("name", "撤销来源权限后停止的周摘要");
        var revokedWeekly = json.readTree(mvc.perform(post(endpoint).header("Authorization", AUTH)
                        .header("Idempotency-Key", "p30-revoked-weekly-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(revokedWeeklyBody)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        var revokedWeeklyId = UUID.fromString(revokedWeekly.path("id").asText());
        var revokedPlanned = Instant.now().minusSeconds(90).truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
        var revokedLocal = java.time.LocalDateTime.ofInstant(revokedPlanned, java.time.ZoneOffset.UTC);
        jdbc.update("update workflow.automation_subscription set day_of_week = ?, local_time = ?, time_zone = 'UTC', "
                        + "next_fire_at = ?, updated_at = now() where id = ?",
                revokedLocal.getDayOfWeek().name(), java.sql.Time.valueOf(revokedLocal.toLocalTime()),
                java.sql.Timestamp.from(revokedPlanned), revokedWeeklyId);
        var revokedGrant = jdbc.update("update workspace.\"grant\" set status = 'REVOKED' "
                        + "where workspace_id = ? and actor_id = ? and action = 'work-item:read' and status = 'ACTIVE'",
                WORKSPACE, Ids.ALICE);
        assertThat(revokedGrant).isEqualTo(1);
        try {
            assertThat(automations.dispatchOne()).isTrue(); // 登记已到期的周计划槽。
            assertThat(automations.dispatchOne()).isTrue(); // Task 入队前重新核验权限。
        } finally {
            jdbc.update("update workspace.\"grant\" set status = 'ACTIVE' "
                            + "where workspace_id = ? and actor_id = ? and action = 'work-item:read' and status = 'REVOKED'",
                    WORKSPACE, Ids.ALICE);
        }
        assertThat(jdbc.queryForObject("select status from workflow.automation_subscription where id = ?",
                String.class, revokedWeeklyId)).isEqualTo("BLOCKED");
        assertThat(jdbc.queryForObject("select reason from workflow.automation_run where subscription_id = ?",
                String.class, revokedWeeklyId)).isEqualTo("AUTHORIZATION_REVOKED");
        assertThat(jdbc.queryForObject("select count(*) from workflow.automation_run where subscription_id = ? and task_id is not null",
                Integer.class, revokedWeeklyId)).isZero();
        return new P30Subscriptions(UUID.fromString(event.path("id").asText()), weeklyTaskId);
    }

    private record P30Subscriptions(UUID eventSubscriptionId, UUID weeklyTaskId) { }

    private void exerciseP30Event(UUID subscriptionId) throws Exception {
        UUID eventRunId = null;
        for (var attempt = 0; attempt < 100; attempt++) {
            if (!automations.consumeOneEvent()) break;
            eventRunId = jdbc.query("select id from workflow.automation_run where subscription_id = ? and status = 'READY' "
                            + "order by created_at desc limit 1", rs -> rs.next() ? rs.getObject(1, UUID.class) : null,
                    subscriptionId);
            if (eventRunId != null) break;
        }
        assertThat(eventRunId).as("current P16 completion event creates one ready run").isNotNull();
        assertThat(jdbc.queryForObject("select count(*) from workflow.automation_run where subscription_id = ? "
                + "and status = 'SKIPPED' and reason = 'SUBSCRIPTION_NOT_ACTIVE'", Integer.class, subscriptionId))
                .isGreaterThanOrEqualTo(1);
        assertThat(automations.dispatchOne()).isTrue();
        var taskId = jdbc.queryForObject("select task_id from workflow.automation_run where id = ?", UUID.class, eventRunId);
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(taskId);
        tasks.complete(work, runtime.run(work));
        assertThat(automations.dispatchOne()).isTrue();
        assertThat(jdbc.queryForObject("select status from workflow.automation_run where id = ?", String.class, eventRunId))
                .isEqualTo("SUCCEEDED");
        assertThat(json.readTree(tasks.get(ALICE, WORKSPACE, taskId).resultJson()).path("markdown").asText())
                .contains("COMPLETED");
        var subscription = automations.get(ALICE, WORKSPACE, subscriptionId);
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete(
                        path("/automation-subscriptions/" + subscriptionId))
                        .param("expectedVersion", Long.toString(subscription.version()))
                        .header("Authorization", AUTH).header("Idempotency-Key", "p30-event-delete-" + subscriptionId))
                .andExpect(status().isOk());
        assertThat(tasks.get(ALICE, WORKSPACE, taskId).resultJson()).isNotBlank();
    }

    private void exerciseProjectBrief(UUID p16WorkItemId) throws Exception {
        var documentId = publishKnowledge("P29 合成项目资料", "设备维护记录：现场人员报告设备间歇性停机，尚未完成复测。",
                "p29-" + UUID.randomUUID());
        var ref = jdbc.queryForObject("select c.document_id, c.asset_version, c.id, p.build_id, trim(c.content_hash) "
                        + "from knowledge.chunk c join knowledge.document_publication p on p.tenant_id = c.tenant_id "
                        + "and p.workspace_id = c.workspace_id and p.document_id = c.document_id "
                        + "and p.asset_version = c.asset_version and p.status = 'ACTIVE' "
                        + "where c.document_id = ? order by c.chunk_order limit 1",
                (rs, row) -> new io.eaf.knowledge.api.PublishedKnowledgeChunk.Ref(rs.getObject(1, UUID.class),
                        rs.getInt(2), rs.getObject(3, UUID.class), rs.getObject(4, UUID.class), rs.getString(5)), documentId);
        var sourceItem = workflows.getHumanWorkItem(ALICE, WORKSPACE, p16WorkItemId);
        var body = json.createObjectNode().put("title", "设备稳定性跟进").put("goal", "整理当前依据与待办，交接复测安排。");
        body.putArray("knowledgeRefs").addObject().put("documentId", ref.documentId().toString())
                .put("documentVersion", ref.documentVersion()).put("chunkId", ref.chunkId().toString())
                .put("buildId", ref.buildId().toString()).put("contentHash", ref.contentHash());
        body.putArray("workItemRefs").addObject().put("workItemId", p16WorkItemId.toString())
                .put("expectedRowVersion", sourceItem.rowVersion());
        var endpoint = path("/project-briefs");
        var key = "p29-create-" + UUID.randomUUID();
        var created = json.readTree(mvc.perform(post(endpoint).header("Authorization", AUTH)
                        .header("Idempotency-Key", key).contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(body)))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        var briefId = UUID.fromString(created.path("briefId").asText());
        var replay = json.readTree(mvc.perform(post(endpoint).header("Authorization", AUTH)
                        .header("Idempotency-Key", key).contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(body)))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());
        assertThat(replay.path("briefId").asText()).isEqualTo(briefId.toString());
        driveProjectBriefUntil(briefId, "review");
        var prepareTaskId = jdbc.queryForObject("select child_task_id from workflow.step where instance_id = ? and step_id = 'prepare'",
                UUID.class, briefId);
        var v1 = workflows.getProjectBriefArtifact(ALICE, WORKSPACE, briefId, 1);
        assertThat(v1.blocked()).isFalse();
        assertThat(v1.markdown()).contains("整理目标", "分析建议", "K1");
        assertThat(tasks.get(ALICE, WORKSPACE, prepareTaskId).status()).isEqualTo(TaskStatus.SUCCEEDED);
        var briefInbox = json.readTree(mvc.perform(get(path("/work-inbox?kind=BRIEF"))
                        .header("Authorization", AUTH)).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString());
        assertThat(briefInbox.path("items").findValuesAsText("id")).contains(briefId.toString());

        var reviewId = jdbc.queryForObject("select id from workflow.human_work_item where instance_id = ? and step_id = 'review'",
                UUID.class, briefId);
        var review = workflows.getProjectBriefWorkItem(ALICE, WORKSPACE, reviewId);
        var reviewBody = json.createObjectNode().put("expectedVersion", review.rowVersion())
                .put("decision", "CONFIRMED").put("notes", "复核后补充：现场仍需记录复测时间。")
                .put("nextAction", "由交接人安排复测并回填观察结果。");
        var reviewPath = path("/project-brief-work-items/" + reviewId + "/review");
        mvc.perform(post(reviewPath).header("Authorization", "Bearer eaf-local-bob")
                        .header("Idempotency-Key", "p29-review-denied")
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(reviewBody)))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isIn(403, 404));
        var reviewKey = "p29-review-" + briefId;
        var reviewed = json.readTree(mvc.perform(post(reviewPath).header("Authorization", AUTH)
                        .header("Idempotency-Key", reviewKey).contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(reviewBody)))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        var reviewReplay = json.readTree(mvc.perform(post(reviewPath).header("Authorization", AUTH)
                        .header("Idempotency-Key", reviewKey).contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsBytes(reviewBody)))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(reviewed.path("status").asText()).isEqualTo("COMPLETED");
        assertThat(reviewReplay.path("completedAt").asText()).isEqualTo(reviewed.path("completedAt").asText());
        var v2 = workflows.getProjectBriefArtifact(ALICE, WORKSPACE, briefId, 2);
        assertThat(v2.markdown()).contains("人工补充与复核", "现场仍需记录复测时间");
        driveProjectBriefUntil(briefId, "handoff");
        var handoffId = jdbc.queryForObject("select id from workflow.human_work_item where instance_id = ? and step_id = 'handoff'",
                UUID.class, briefId);
        var handoff = workflows.getProjectBriefWorkItem(ALICE, WORKSPACE, handoffId);
        var handoffBody = json.createObjectNode().put("expectedVersion", handoff.rowVersion())
                .put("disposition", "RECEIVED").put("note", "已收到 v2，将按记录安排复测。");
        mvc.perform(post(path("/project-brief-work-items/" + handoffId + "/receive"))
                        .header("Authorization", AUTH).header("Idempotency-Key", "p29-handoff-" + briefId)
                        .contentType(MediaType.APPLICATION_JSON).content(json.writeValueAsBytes(handoffBody)))
                .andExpect(status().isOk());
        driveProjectBriefUntil(briefId, "complete");
        assertThat(workflows.getInstance(ALICE, WORKSPACE, briefId).status()).isEqualTo("SUCCEEDED");
        for (var version : List.of(1, 2)) {
            mvc.perform(get(path("/project-briefs/" + briefId + "/artifacts/" + version + ".md"))
                            .header("Authorization", AUTH)).andExpect(status().isOk())
                    .andExpect(result -> assertThat(result.getResponse().getContentAsString()).isNotBlank());
        }
        var doc = knowledge.get(ALICE, WORKSPACE, documentId);
        knowledge.revoke(ALICE, WORKSPACE, documentId, doc.rowVersion(), "p29-revoke-" + briefId);
        assertThat(workflows.getProjectBriefArtifact(ALICE, WORKSPACE, briefId, 1).blocked()).isTrue();
        mvc.perform(get(path("/project-briefs/" + briefId + "/artifacts/2.md"))
                        .header("Authorization", AUTH)).andExpect(status().isConflict());
        assertThatThrownBy(() -> tasks.get(ALICE, WORKSPACE, prepareTaskId))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                        error -> assertThat(error.code()).isIn("BRIEF_SOURCE_UNAVAILABLE", "NOT_FOUND", "RESOURCE_NOT_FOUND"));
    }

    private void driveProjectBriefUntil(UUID briefId, String stepId) {
        for (var attempt = 0; attempt < 20; attempt++) {
            var instance = workflows.getInstance(ALICE, WORKSPACE, briefId);
            if ("complete".equals(stepId) && "SUCCEEDED".equals(instance.status())) return;
            if ("WAITING_HUMAN".equals(instance.status()) && stepId.equals(instance.currentStepId())) return;
            if (java.util.Set.of("FAILED", "CANCELLED", "TIMED_OUT").contains(instance.status()))
                throw new AssertionError("P29 workflow ended at " + instance.status() + " with " + instance.errorCode());
            jdbc.update("update workflow.instance set next_poll_at = null where id = ? and status <> 'WAITING_HUMAN'", briefId);
            workflowDispatcher.dispatchOne();
            var taskId = jdbc.query("select child_task_id from workflow.step where instance_id = ? and step_id = 'prepare'",
                    rs -> rs.next() ? rs.getObject(1, UUID.class) : null, briefId);
            if (taskId != null && tasks.get(ALICE, WORKSPACE, taskId).status() == TaskStatus.QUEUED) {
                var work = tasks.claimOne().orElseThrow();
                assertThat(work.id()).isEqualTo(taskId);
                tasks.complete(work, runtime.run(work));
            }
        }
        throw new AssertionError("P29 workflow did not reach " + stepId + ".");
    }

    private String workflowContentHash(io.eaf.workflow.api.WorkflowDefinition definition) throws Exception {
        var content = json.createObjectNode().put("name", definition.name()).put("description", definition.description())
                .put("version", definition.version()).put("entryStepId", definition.entryStepId());
        content.set("inputSchema", json.readTree(definition.inputSchema()));
        content.set("outputSchema", json.readTree(definition.outputSchema()));
        content.set("steps", json.valueToTree(definition.steps()));
        var dependencies = definition.dependencies().stream()
                .sorted(Comparator.comparing(value -> value.capabilityId().toString() + "@" + value.capabilityVersion())).toList();
        content.set("dependencies", json.valueToTree(dependencies));
        return Hashing.sha256(json.writeValueAsString(sortJson(content)));
    }

    private JsonNode sortJson(JsonNode node) {
        if (node.isObject()) {
            var sorted = json.createObjectNode();
            var names = new ArrayList<String>();
            node.fieldNames().forEachRemaining(names::add);
            names.sort(String::compareTo);
            for (var name : names) sorted.set(name, sortJson(node.get(name)));
            return sorted;
        }
        if (node.isArray()) {
            var sorted = json.createArrayNode();
            node.forEach(value -> sorted.add(sortJson(value)));
            return sorted;
        }
        return node.deepCopy();
    }

    private UUID publishKnowledge(String title, String content, String key) {
        return publishKnowledge(title, content, key, Map.of("synthetic", "true"));
    }

    private UUID publishKnowledge(String title, String content, String key, Map<String, String> metadata) {
        var document = knowledge.create(new CreateKnowledgeDocumentCommand(ALICE, WORKSPACE, title,
                "manual://p15/" + key, content, metadata, key + "-create", "trace-" + key));
        knowledge.chunk(ALICE, WORKSPACE, document.id(), "p3-plain-1");
        var build = knowledge.buildIndex(ALICE, WORKSPACE, document.id(), "p3-plain-1", key + "-index");
        assertThat(build.status()).isEqualTo("READY");
        knowledge.publish(ALICE, WORKSPACE, document.id(), document.rowVersion(), build.id(), key + "-publish");
        return document.id();
    }

    private ActorContext reviewer() {
        return new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN,
                Set.of("approval:read", "approval:decide", "execution:read"));
    }

    private String path(String value) { return "/api/v1/workspaces/" + WORKSPACE + value; }

    private static final class ServiceDeskFixture implements AutoCloseable {
        private final HttpServer server;
        private final ObjectMapper json;
        private final Map<String, JsonNode> records = new ConcurrentHashMap<>();
        private final Map<String, JsonNode> p27Results = new ConcurrentHashMap<>();
        private final Map<String, String> statuses = new ConcurrentHashMap<>();
        private final Map<String, String> externalVersions = new ConcurrentHashMap<>();
        private final AtomicInteger posts = new AtomicInteger();
        private final AtomicInteger gets = new AtomicInteger();
        private final AtomicInteger p27Posts = new AtomicInteger();
        private final AtomicInteger p27Readbacks = new AtomicInteger();

        private ServiceDeskFixture(ObjectMapper json) throws Exception {
            this.json = json;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/users/", exchange -> {
                var path = exchange.getRequestURI().getPath();
                if (!"alice".equals(exchange.getRequestHeaders().getFirst("X-External-Subject"))
                        || !"Bearer p27-oa-token".equals(exchange.getRequestHeaders().getFirst("Authorization"))
                        || !path.matches("/users/alice/todos(?:/todo-1)?")) {
                    send(exchange, 404, ""); return;
                }
                if ("HEAD".equals(exchange.getRequestMethod())) { send(exchange, 200, ""); return; }
                if (!"GET".equals(exchange.getRequestMethod())) { send(exchange, 405, ""); return; }
                var todo = json.createObjectNode().put("sourceId", "EAF-OA-TODO-V1").put("todoId", "todo-1")
                        .put("title", "确认办公电脑检修安排").put("status", "OPEN").put("sourceVersion", "v1")
                        .put("externalSubjectId", "alice").put("updatedAt", Instant.now().toString());
                if (path.endsWith("/todo-1")) { send(exchange, 200, json.writeValueAsString(todo)); return; }
                var page = json.createObjectNode().put("sourceId", "EAF-OA-TODO-V1").putNull("nextCursor");
                page.putArray("items").add(todo);
                send(exchange, 200, json.writeValueAsString(page));
            });
            server.createContext("/requests/by-operation/", exchange -> {
                gets.incrementAndGet();
                var operationId = exchange.getRequestURI().getPath().substring("/requests/by-operation/".length());
                var record = records.get(operationId);
                if (record == null) { send(exchange, 404, ""); return; }
                send(exchange, 200, json.writeValueAsString(record));
            });
            server.createContext("/handling-results/by-operation/", exchange -> {
                p27Readbacks.incrementAndGet();
                if (!"Bearer p27-service-token".equals(exchange.getRequestHeaders().getFirst("Authorization"))
                        || !"alice".equals(exchange.getRequestHeaders().getFirst("X-External-Subject"))) {
                    send(exchange, 403, ""); return;
                }
                var operationId = exchange.getRequestURI().getPath().substring("/handling-results/by-operation/".length());
                var result = p27Results.get(operationId);
                if (result == null) { send(exchange, 404, ""); return; }
                send(exchange, 200, json.writeValueAsString(result));
            });
            server.createContext("/requests", exchange -> {
                var path = exchange.getRequestURI().getPath();
                if (path.matches("/requests/[^/]+/state")) {
                    var requestId = path.substring("/requests/".length(), path.length() - "/state".length());
                    var registered = registeredRequest(requestId);
                    if (!authorizedP27(exchange) || registered == null) { send(exchange, 404, ""); return; }
                    if ("HEAD".equals(exchange.getRequestMethod())) { send(exchange, 200, ""); return; }
                    if (!"GET".equals(exchange.getRequestMethod())) { send(exchange, 405, ""); return; }
                    var state = json.createObjectNode().put("contractVersion", "EAF-SERVICE-DESK-HANDLING-V1")
                            .put("sourceId", "P15_INTERNAL_SERVICE_DESK_FIXTURE").put("requestId", requestId)
                            .put("registrationOperationId", registered.path("operationId").asText())
                            .put("status", statuses.computeIfAbsent(requestId, ignored -> "REGISTERED"))
                            .put("externalVersion", externalVersions.computeIfAbsent(requestId, ignored -> "v1"))
                            .put("updatedAt", Instant.now().toString());
                    send(exchange, 200, json.writeValueAsString(state)); return;
                }
                if (path.matches("/requests/[^/]+/handling-results")) {
                    p27Posts.incrementAndGet();
                    var requestId = path.substring("/requests/".length(), path.length() - "/handling-results".length());
                    if (!authorizedP27(exchange) || registeredRequest(requestId) == null) { send(exchange, 403, ""); return; }
                    var body = json.readTree(exchange.getRequestBody().readAllBytes());
                    var operationId = exchange.getRequestHeaders().getFirst("Idempotency-Key");
                    var version = externalVersions.computeIfAbsent(requestId, ignored -> "v1");
                    if (operationId == null || !operationId.equals(body.path("operationId").asText())
                            || !requestId.equals(body.path("requestId").asText())
                            || !version.equals(exchange.getRequestHeaders().getFirst("If-Match"))) {
                        send(exchange, 412, ""); return;
                    }
                    var completed = body.path("outcome").asText();
                    var result = json.createObjectNode().put("contractVersion", "EAF-SERVICE-DESK-HANDLING-V1")
                            .put("operationId", operationId).put("requestId", requestId)
                            .put("registrationOperationId", body.path("registrationOperationId").asText())
                            .put("resultId", "RESULT-" + operationId).put("workItemId", body.path("workItemId").asText())
                            .put("workItemVersion", body.path("workItemVersion").asLong())
                            .put("sourceResultHash", body.path("sourceResultHash").asText())
                            .put("externalSubjectId", body.path("externalSubjectId").asText())
                            .put("completedBy", body.path("completedBy").asText())
                            .put("completedAt", body.path("completedAt").asText()).put("outcome", completed)
                            .put("summary", body.path("summary").asText()).put("nextAction", body.path("nextAction").asText())
                            .put("recordState", "RECORDED")
                            .put("previousExternalVersion", version).put("resultingExternalVersion", nextExternalVersion(version))
                            .put("resultingStatus", "COMPLETED".equals(completed) ? "RESOLVED" : "IN_PROGRESS")
                            .put("acceptedAt", Instant.now().toString());
                    var previous = p27Results.putIfAbsent(operationId, result);
                    if (previous != null) { send(exchange, 200, json.writeValueAsString(previous)); return; }
                    statuses.put(requestId, result.path("resultingStatus").asText());
                    externalVersions.put(requestId, result.path("resultingExternalVersion").asText());
                    // 首次写入后模拟响应丢失，验证服务按原 operationId 回读而不重发。
                    send(exchange, 500, ""); return;
                }
                if (!"POST".equals(exchange.getRequestMethod())) { send(exchange, 405, ""); return; }
                posts.incrementAndGet();
                if (!"Bearer p15-synthetic-token".equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                    send(exchange, 401, ""); return;
                }
                var body = json.readTree(exchange.getRequestBody().readAllBytes());
                var operationId = exchange.getRequestHeaders().getFirst("Idempotency-Key");
                if (operationId == null || !operationId.equals(body.path("operationId").asText())) {
                    send(exchange, 400, ""); return;
                }
                var result = body.deepCopy();
                ((com.fasterxml.jackson.databind.node.ObjectNode) result).put("requestId", "P15-" + operationId);
                ((com.fasterxml.jackson.databind.node.ObjectNode) result).put("status", "REGISTERED");
                var previous = records.putIfAbsent(operationId, result);
                statuses.putIfAbsent("P15-" + operationId, "REGISTERED");
                externalVersions.putIfAbsent("P15-" + operationId, "v1");
                send(exchange, previous == null ? 201 : 200, json.writeValueAsString(previous == null ? result : previous));
            });
            server.start();
        }

        private boolean authorizedP27(com.sun.net.httpserver.HttpExchange exchange) {
            return "Bearer p27-service-token".equals(exchange.getRequestHeaders().getFirst("Authorization"))
                    && "alice".equals(exchange.getRequestHeaders().getFirst("X-External-Subject"));
        }

        private JsonNode registeredRequest(String requestId) {
            return records.values().stream().filter(record -> requestId.equals(record.path("requestId").asText()))
                    .findFirst().orElse(null);
        }

        private void advanceExternalVersion(String requestId) {
            externalVersions.compute(requestId, (ignored, version) -> nextExternalVersion(version == null ? "v1" : version));
        }

        private String nextExternalVersion(String version) {
            return "v" + (Integer.parseInt(version.substring(1)) + 1);
        }

        private String url() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        private AtomicInteger posts() { return posts; }
        private AtomicInteger gets() { return gets; }
        private AtomicInteger p27Posts() { return p27Posts; }
        private AtomicInteger p27Readbacks() { return p27Readbacks; }
        private Map<String, JsonNode> records() { return records; }

        private static void send(com.sun.net.httpserver.HttpExchange exchange, int status, String body) throws java.io.IOException {
            var bytes = body.getBytes(StandardCharsets.UTF_8);
            if (bytes.length > 0) exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) exchange.getResponseBody().write(bytes);
            exchange.close();
        }

        @Override public void close() { server.stop(0); }
    }
}
