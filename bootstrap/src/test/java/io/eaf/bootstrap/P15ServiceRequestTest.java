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
import io.eaf.evaluation.api.ScenarioEvaluationService;
import io.eaf.evaluation.infrastructure.JdbcEvaluationService;
import io.eaf.evaluation.infrastructure.JdbcScenarioEvaluationService;
import io.eaf.learning.api.CandidateService;
import io.eaf.learning.infrastructure.JdbcCandidateService;
import io.eaf.model.api.ModelGateway;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Hashing;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.infrastructure.JdbcWorkflowService;
import io.eaf.workflow.infrastructure.WorkflowDispatcher;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false", "eaf.memory.outbox-publisher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false", "eaf.model.mode=deterministic",
        "eaf.model.scenario=SERVICE_REQUEST_SEARCH", "eaf.model.jev.mode=deterministic",
        "eaf.model.jev.evidence-mode=deterministic", "eaf.credentials.p15-service-desk.token=p15-synthetic-token",
        "eaf.learning.improvement.enabled=true", "eaf.learning.improvement.poll-delay-ms=3600000",
        "eaf.learning.improvement-evaluation-poll-delay-ms=3600000",
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
    @Autowired CandidateService candidates;
    @Autowired JdbcCandidateService candidateDispatcher;
    @Autowired EvaluationService evaluations;
    @Autowired JdbcEvaluationService evaluationDispatcher;
    @Autowired ScenarioEvaluationService scenarioEvaluations;
    @Autowired JdbcScenarioEvaluationService scenarioDispatcher;
    @Autowired WorkflowDispatcher workflowDispatcher;
    @Autowired JdbcWorkflowService jdbcWorkflows;
    @Autowired ExecutionService executions;
    @Autowired ApprovalService approvals;
    @Autowired ModelGateway model;
    @Autowired AuditPort audit;
    @Autowired PlatformTransactionManager transactionManager;

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

        var userTask = tasks.create(new CreateTaskCommand(ALICE, WORKSPACE, PLAN_AGENT, "1.1.0", "办公电脑无法启动",
                null, null, "p24-user-" + UUID.randomUUID(), "trace-p24-user", "USER",
                new TaskAssetBinding(compact.id(), compact.version(), compact.contentHash(), compact.skillId(),
                        compact.skillVersion(), compact.skillContentHash()), "REST"));
        var userWork = tasks.claimOne().orElseThrow();
        assertThat(userWork.id()).isEqualTo(userTask.id());
        tasks.complete(userWork, runtime.run(userWork));
        var userResult = tasks.get(ALICE, WORKSPACE, userTask.id());
        assertThat(userResult.status()).isEqualTo(TaskStatus.SUCCEEDED);
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
        assertThat(report.optimization().conditionsReasons()).contains("PROVIDER_CONFIGURATION_UNVERIFIED");
        assertThat(report.optimization().pairs()).hasSize(4);
        assertThat(report.optimization().pairs()).allSatisfy(pair ->
                assertThat(pair.metricDeltas()).containsKeys("queueMillis", "taskMillis"));
        assertThat(report.optimization().pairedSummary().metricDeltas()).containsKeys("queueMillis", "taskMillis");

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
        var registration = capabilities.requirePublished(ALICE, WORKSPACE, REGISTER_CAPABILITY, "1.0.0");
        assertThat(registration.contentHash()).isEqualTo("43517612934f783d09a9e801e6c520153a4b20faaeccc5bfbfa626c439de34cb");
        assertThat(registration.toolDependencies()).extracting("name").containsExactly("service.request.register");
        var definition = workflows.get(ALICE, WORKSPACE, REGISTER_WORKFLOW, "1.0.0");
        assertThat(definition.contentHash()).isEqualTo("051a3b499b2450a6f38147422fe78d06c81405b5008bef8bcfac4cec60bcca0d");
        assertThat(definition.dependencies()).singleElement().satisfies(dependency ->
                assertThat(dependency.contentHash()).isEqualTo(registration.contentHash()));

        publishKnowledge("IT 设备说明", "办公电脑无法启动属于 IT 设备维修事项；员工希望登记设备检修。", "p15-it");
        publishKnowledge("服务请求登记规范", "内部服务请求登记所需信息包括设备地点和故障现象；按设备处理规范核对后受理。", "p15-policy");
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
        assertThat(result.path("contextRefs").size()).isEqualTo(2);
        assertThat(model.callCount()).isEqualTo(2);

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
            assertThat(workflowDispatcher.dispatchOne()).isTrue();
            assertThat(workflowDispatcher.dispatchOne()).isTrue();
            var childTaskId = jdbc.queryForObject("select child_task_id from workflow.step where instance_id = ? and step_id = 'register'",
                    UUID.class, workflowId);
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
            assertThat(model.callCount()).isEqualTo(4);

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
            assertThat(model.callCount()).isEqualTo(6);

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
        assertThat(awaitingReview.status()).isEqualTo("AWAITING_REVIEW");
        var candidate = candidates.get(ALICE, WORKSPACE, awaitingReview.candidateId());
        candidate = candidates.review(new CandidateService.CandidateReviewCommand(BOB, WORKSPACE,
                candidate.id(), candidate.rowVersion(), "ACCEPTED", "按已知处理规范独立核对候选事实。",
                List.of("policy:service-request-facts")));
        var devReportId = candidates.getImprovementRun(ALICE, WORKSPACE, runId).devReportId();
        driveP23Report(devReportId);
        candidateDispatcher.dispatchNextImprovementEvaluation();

        var heldOutReportId = candidates.getImprovementRun(ALICE, WORKSPACE, runId).heldOutReportId();
        assertThat(heldOutReportId).isNotNull();
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

    private void driveP23Report(UUID reportId) {
        for (var attempt = 0; attempt < 40; attempt++) {
            jdbc.update("update evaluation.team_preparation_run set next_poll_at = now() where id = ? and lease_owner is null", reportId);
            evaluationDispatcher.dispatchNextTeamPreparation();
            var active = jdbc.queryForList("select task_id from evaluation.team_preparation_sample "
                    + "where run_id = ? and status = 'ACTIVE' and task_id is not null order by case_id, side", UUID.class, reportId);
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

    private void grantP23(ActorContext actor, String... actions) {
        for (var action : actions) jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                        + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                actor.tenantId(), WORKSPACE, actor.actorId(), action);
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
        private final AtomicInteger posts = new AtomicInteger();
        private final AtomicInteger gets = new AtomicInteger();

        private ServiceDeskFixture(ObjectMapper json) throws Exception {
            this.json = json;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/requests/by-operation/", exchange -> {
                gets.incrementAndGet();
                var operationId = exchange.getRequestURI().getPath().substring("/requests/by-operation/".length());
                var record = records.get(operationId);
                if (record == null) { send(exchange, 404, ""); return; }
                send(exchange, 200, json.writeValueAsString(record));
            });
            server.createContext("/requests", exchange -> {
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
                send(exchange, previous == null ? 201 : 200, json.writeValueAsString(previous == null ? result : previous));
            });
            server.start();
        }

        private String url() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        private AtomicInteger posts() { return posts; }
        private AtomicInteger gets() { return gets; }
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
