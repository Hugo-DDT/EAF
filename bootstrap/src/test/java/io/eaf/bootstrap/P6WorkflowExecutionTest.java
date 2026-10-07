package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.eaf.approval.api.ApprovalService;
import io.eaf.approval.api.ApprovalDecisionCommand;
import io.eaf.capability.api.CapabilityService;
import io.eaf.capability.api.CapabilityToolReference;
import io.eaf.capability.api.CreateCapabilityVersionCommand;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateWorkflowTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.execution.api.ExecutionService;
import java.net.InetSocketAddress;
import io.eaf.workflow.api.CreateWorkflowInstanceCommand;
import io.eaf.workflow.api.CreateWorkflowVersionCommand;
import io.eaf.workflow.api.WorkflowInstance;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.infrastructure.JdbcWorkflowService;
import io.eaf.workflow.infrastructure.WorkflowDispatcher;
import io.eaf.workflow.infrastructure.WorkflowLease;
import io.eaf.workflow.infrastructure.WorkflowStepRuntime;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false"})
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
// 用真实 PostgreSQL 验证 Workflow 持久推进、Task 幂等对账和租约接管。
class P6WorkflowExecutionTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String ALICE_TOKEN = "Bearer eaf-local-alice";
    private static final String BOB_TOKEN = "Bearer eaf-local-bob";
    private static final UUID SEED_ID = UUID.fromString("58000000-0000-4000-8000-000000000001");
    private static final UUID CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000001");
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());
    private static final AtomicInteger VERSIONS = new AtomicInteger();
    private static final AtomicInteger CRM_WRITES = new AtomicInteger();
    private static final AtomicBoolean CRM_UNKNOWN_WRITE = new AtomicBoolean();
    private static final AtomicBoolean CRM_HOLD_POST = new AtomicBoolean();
    private static final AtomicInteger REVIEW_PEER_SENDS = new AtomicInteger();
    private static final AtomicInteger REVIEW_PEER_GETS = new AtomicInteger();
    private static final AtomicInteger REVIEW_PEER_IDS = new AtomicInteger();
    private static final AtomicReference<String> REVIEW_PEER_RISK = new AtomicReference<>("HIGH");
    private static final AtomicBoolean REVIEW_PEER_UNAVAILABLE = new AtomicBoolean();
    private static final ConcurrentHashMap<String, String> REVIEW_PEER_CONTEXTS = new ConcurrentHashMap<>();
    private static volatile CountDownLatch CRM_POST_STARTED = new CountDownLatch(1);
    private static volatile CountDownLatch CRM_POST_RELEASE = new CountDownLatch(1);
    private static final ConcurrentHashMap<String, String> CRM_RECORDS = new ConcurrentHashMap<>();

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eaf.security.mode", () -> "local");
        registry.add("eaf.credentials.test-crm.token", () -> "p6-workflow-credential");
        registry.add("eaf.credentials.a2a-peer-review.token", () -> "p6-workflow-a2a");
    }

    @Autowired WorkflowService workflows;
    @Autowired JdbcWorkflowService workflowStore;
    @Autowired WorkflowDispatcher dispatcher;
    @Autowired TaskService tasks;
    @Autowired TaskRunner runner;
    @Autowired CapabilityService capabilities;
    @Autowired JdbcTemplate jdbc;
    @Autowired ApprovalService approvals;
    @Autowired ExecutionService executions;
    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;

    @Test
    void cancellationBeforeChildCreateLeavesTombstoneAndStopsWorkflow() {
        var instance = startInstance("customer-001", "p6-09-cancel-before-create");
        dispatch(instance.id());
        var intent = workflowStore.stepRuntime(instance.id(), "analyze");
        var current = workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id());
        var cancelling = workflows.cancelInstance(ALICE, Ids.WORKSPACE_A, instance.id(), current.rowVersion());

        assertThat(cancelling.status()).isEqualTo("CANCELLING");
        assertThat(jdbc.queryForObject("select state from task.workflow_dispatch where dispatch_key = ?",
                String.class, intent.dispatchKey())).isEqualTo("CANCELLED");
        assertThatThrownBy(() -> createAnalyzeTask(instance, intent))
                .isInstanceOfSatisfying(EafException.class,
                        error -> assertThat(error.code()).isEqualTo("WORKFLOW_CHILD_CANCELLED"));
        dispatch(instance.id());
        assertThat(workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id()).status()).isEqualTo("CANCELLED");
        assertThat(countByKey(intent.dispatchKey())).isZero();
    }

    @Test
    void waitingApprovalIsProjectedAndCancellationClosesApprovalWithoutWriting() throws Exception {
        var crm = startCustomerCrm();
        try {
            var started = startApprovedToolWorkflow("p6-09-cancel-approval");
            var waiting = workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id());
            assertThat(waiting.status()).isEqualTo("WAITING_CHILD");
            assertThat(waiting.waitingReason()).isEqualTo("APPROVAL_REQUIRED");
            assertThat(waiting.childTaskId()).isEqualTo(started.taskId());
            jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                    Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB, "workflow:read");
            jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                    Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB, "task:read");
            mvc.perform(get("/api/v1/workspaces/{workspaceId}/workflows/{workflowId}/instances/{instanceId}",
                            Ids.WORKSPACE_A, started.instance().workflowId(), started.instance().id())
                            .header("Authorization", "Bearer eaf-local-bob"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.childTaskId").value(started.taskId().toString()))
                    .andExpect(jsonPath("$.approvalId").value(started.approvalId().toString()))
                    .andExpect(jsonPath("$.childTaskVersion").isNumber())
                    .andExpect(jsonPath("$.resultJson").doesNotExist());
            assertThat(jdbc.queryForObject("select status from execution.execution where id = ?", String.class,
                    started.executionId())).isEqualTo("AWAITING_APPROVAL");
            assertThat(CRM_WRITES).hasValue(0);

            var cancelling = workflows.cancelInstance(ALICE, Ids.WORKSPACE_A, started.instance().id(), waiting.rowVersion());
            assertThat(cancelling.status()).isEqualTo("CANCELLING");
            assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId()).status()).isEqualTo(TaskStatus.CANCELLED);
            assertThat(jdbc.queryForObject("select state from approval.request where id = ?", String.class,
                    started.approvalId())).isEqualTo("CANCELLED");
            assertThat(jdbc.queryForObject("select status from execution.execution where id = ?", String.class,
                    started.executionId())).isEqualTo("CANCELLED");

            dispatch(started.instance().id());
            var cancelled = workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id());
            assertThat(cancelled.status()).isEqualTo("CANCELLED");
            assertThat(cancelled.businessEffectStatus()).isEqualTo("NONE");
            assertThat(CRM_WRITES).hasValue(0);
            assertThat(jdbc.queryForObject("select count(*) from workflow.step where instance_id = ? and child_task_id is not null",
                    Integer.class, started.instance().id())).isEqualTo(2);
        } finally {
            jdbc.update("delete from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and actor_id = ? and action in ('workflow:read','task:read')",
                    Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB);
            crm.stop(0);
        }
    }

    @Test
    void deadlineTimesOutBeforeCreatingAnyChildTask() {
        var instance = startInstance("customer-001", "p6-09-deadline");
        jdbc.update("update workflow.instance set deadline_at = now() - interval '1 second' where id = ?", instance.id());

        dispatch(instance.id());

        var timedOut = workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id());
        assertThat(timedOut.status()).isEqualTo("TIMED_OUT");
        assertThat(timedOut.errorCode()).isEqualTo("DEADLINE_EXCEEDED");
        assertThat(jdbc.queryForObject("select count(*) from workflow.step where instance_id = ?", Integer.class, instance.id())).isZero();
    }

    @Test
    void withdrawalCancelsWaitingApprovalAndStopsTheWorkflow() throws Exception {
        var crm = startCustomerCrm();
        try {
            var started = startApprovedToolWorkflow("p6-09-withdrawal");
            var version = workflows.get(ALICE, Ids.WORKSPACE_A, started.instance().workflowId(), started.instance().workflowVersion());
            workflows.withdraw(ALICE, Ids.WORKSPACE_A, started.instance().workflowId(), started.instance().workflowVersion(), version.rowVersion());

            dispatch(started.instance().id());

            assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId()).status()).isEqualTo(TaskStatus.CANCELLED);
            assertThat(jdbc.queryForObject("select state from approval.request where id = ?", String.class,
                    started.approvalId())).isEqualTo("CANCELLED");
            assertThat(jdbc.queryForObject("select status from execution.execution where id = ?", String.class,
                    started.executionId())).isEqualTo("CANCELLED");
            assertThat(workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id()).status()).isEqualTo("CANCELLING");

            dispatch(started.instance().id());

            var failed = workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id());
            assertThat(failed.status()).isEqualTo("FAILED");
            assertThat(failed.errorCode()).isEqualTo("WORKFLOW_WITHDRAWN");
            assertThat(CRM_WRITES).hasValue(0);
        } finally {
            crm.stop(0);
        }
    }

    @Test
    void cancellationDuringUnknownWriteWaitsForReadOnlyVerification() throws Exception {
        var crm = startCustomerCrm();
        try {
            var started = startApprovedToolWorkflow("p6-09-cancel-unknown-write");
            printP6Evidence("unknown-write", started);
            var bob = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN,
                    Set.of("approval:read", "approval:decide", "execution:read"));
            var approval = approvals.get(bob, Ids.WORKSPACE_A, started.approvalId());
            approvals.decide(new ApprovalDecisionCommand(bob, Ids.WORKSPACE_A, approval.id(), "APPROVED", approval.version()));
            CRM_UNKNOWN_WRITE.set(true);
            var waiting = tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId());
            tasks.resume(ALICE, Ids.WORKSPACE_A, started.taskId(), waiting.version(), "p6-09-resume-approved-write");
            var work = tasks.claimOne().orElseThrow();
            assertThat(work.id()).isEqualTo(started.taskId());
            tasks.complete(work, runner.run(work));
            assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId()).externalEffectStatus()).isEqualTo("UNKNOWN");
            assertThat(CRM_WRITES).hasValue(1);

            var current = workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id());
            var cancelling = workflows.cancelInstance(ALICE, Ids.WORKSPACE_A, current.id(), current.rowVersion());
            assertThat(cancelling.status()).isEqualTo("CANCELLING");
            assertThat(cancelling.waitingReason()).isEqualTo("EXTERNAL_EFFECT_VERIFYING");
            dispatch(started.instance().id());
            assertThat(workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id()).status()).isEqualTo("CANCELLING");

            assertThat(executions.verify(ALICE, Ids.WORKSPACE_A, started.executionId()).status()).isEqualTo("SUCCEEDED");
            var verifiedTask = tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId());
            tasks.resume(ALICE, Ids.WORKSPACE_A, started.taskId(), verifiedTask.version(), "p6-09-resume-verified-write");
            var finalWork = tasks.claimOne().orElseThrow();
            assertThat(finalWork.id()).isEqualTo(started.taskId());
            tasks.complete(finalWork, runner.run(finalWork));
            dispatch(started.instance().id());

            var cancelled = workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id());
            assertThat(cancelled.status()).isEqualTo("CANCELLED");
            assertThat(cancelled.businessEffectStatus()).isEqualTo("SUCCEEDED");
            assertThat(CRM_WRITES).hasValue(1);
        } finally {
            CRM_UNKNOWN_WRITE.set(false);
            crm.stop(0);
        }
    }

    @Test
    void selfApprovalAndPostApprovalArgumentMutationCannotWrite() throws Exception {
        var crm = startCustomerCrm();
        try {
            var started = startApprovedToolWorkflow("p6-09-approval-binding");
            var approval = approvals.get(ALICE, Ids.WORKSPACE_A, started.approvalId());
            jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'approval:decide', 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                    Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
            try {
                assertThatThrownBy(() -> approvals.decide(new ApprovalDecisionCommand(ALICE, Ids.WORKSPACE_A,
                        approval.id(), "APPROVED", approval.version())))
                        .isInstanceOfSatisfying(EafException.class,
                                error -> assertThat(error.getMessage()).contains("发起人不能审批"));
            } finally {
                jdbc.update("delete from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'approval:decide'",
                        Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
            }

            jdbc.update("update execution.execution set arguments_json = jsonb_set(arguments_json, '{summary}', to_jsonb('已篡改'::text)) where id = ?",
                    started.executionId());
            var bob = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN,
                    Set.of("approval:read", "approval:decide", "execution:read"));
            var approved = approvals.get(bob, Ids.WORKSPACE_A, started.approvalId());
            approvals.decide(new ApprovalDecisionCommand(bob, Ids.WORKSPACE_A, approved.id(), "APPROVED", approved.version()));

            runResumedToolTask(started, "p6-09-resume-mutated-arguments");

            var denied = executions.get(ALICE, Ids.WORKSPACE_A, started.executionId());
            assertThat(denied.status()).isEqualTo("DENIED");
            assertThat(denied.errorCode()).isEqualTo("APPROVAL_BINDING_CHANGED");
            assertThat(CRM_WRITES).hasValue(0);
        } finally {
            jdbc.update("delete from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'approval:decide'",
                    Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
            crm.stop(0);
        }
    }

    // 审批通过后若 Task 执行授权撤销，领取边界应先关闭 Task，不能进入 Runtime 或 CRM。
    @Test
    void revokedWorkspacePolicyAfterApprovalCannotWrite() throws Exception {
        var crm = startCustomerCrm();
        var previousGrant = jdbc.queryForObject("select status from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'task:create'",
                String.class, Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        try {
            var started = startApprovedToolWorkflow("p6-09-revoked-policy");
            var bob = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN,
                    Set.of("approval:read", "approval:decide", "execution:read"));
            var approval = approvals.get(bob, Ids.WORKSPACE_A, started.approvalId());
            approvals.decide(new ApprovalDecisionCommand(bob, Ids.WORKSPACE_A, approval.id(), "APPROVED", approval.version()));
            jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'task:create'",
                    Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);

            var waiting = tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId());
            tasks.resume(ALICE, Ids.WORKSPACE_A, started.taskId(), waiting.version(), "p6-09-resume-revoked-policy");
            assertThat(tasks.claimOne()).isEmpty();

            var deniedTask = tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId());
            assertThat(deniedTask.status()).isEqualTo(TaskStatus.FAILED);
            assertThat(deniedTask.errorCode()).isEqualTo("AUTHORIZATION_REVOKED");
            assertThat(executions.get(ALICE, Ids.WORKSPACE_A, started.executionId()).status()).isEqualTo("AWAITING_APPROVAL");
            assertThat(CRM_WRITES).hasValue(0);
        } finally {
            jdbc.update("update workspace.\"grant\" set status = ? where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'task:create'",
                    previousGrant, Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
            crm.stop(0);
        }
    }

    @Test
    void expiredApprovalCannotResumeExternalWrite() throws Exception {
        var crm = startCustomerCrm();
        try {
            var started = startApprovedToolWorkflow("p6-09-expired-approval");
            var expiredAt = Instant.now().minusSeconds(1);
            jdbc.update("update approval.request set expires_at = ?, binding_json = jsonb_set(binding_json, '{expiresAt}', to_jsonb(?::text)) where id = ?",
                    Timestamp.from(expiredAt), expiredAt.toString(), started.approvalId());
            var bob = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN,
                    Set.of("approval:read", "approval:decide", "execution:read"));
            var approval = approvals.get(bob, Ids.WORKSPACE_A, started.approvalId());
            assertThat(approvals.decide(new ApprovalDecisionCommand(bob, Ids.WORKSPACE_A, approval.id(),
                    "APPROVED", approval.version())).state()).isEqualTo("EXPIRED");

            runResumedToolTask(started, "p6-09-resume-expired-approval");

            assertThat(executions.get(ALICE, Ids.WORKSPACE_A, started.executionId()).status()).isEqualTo("DENIED");
            assertThat(CRM_WRITES).hasValue(0);
        } finally {
            crm.stop(0);
        }
    }

    @Test
    void cancellationRacingWithCommittedPostWaitsAndKeepsBusinessResult() throws Exception {
        var crm = startCustomerCrm();
        try {
            var started = startApprovedToolWorkflow("p6-09-cancel-submit-race");
            printP6Evidence("cancel-submit-race", started);
            var bob = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN,
                    Set.of("approval:read", "approval:decide", "execution:read"));
            var approval = approvals.get(bob, Ids.WORKSPACE_A, started.approvalId());
            approvals.decide(new ApprovalDecisionCommand(bob, Ids.WORKSPACE_A, approval.id(), "APPROVED", approval.version()));
            var waiting = tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId());
            tasks.resume(ALICE, Ids.WORKSPACE_A, started.taskId(), waiting.version(), "p6-09-resume-submit-race");
            var work = tasks.claimOne().orElseThrow();
            assertThat(work.id()).isEqualTo(started.taskId());
            CRM_HOLD_POST.set(true);
            var run = CompletableFuture.supplyAsync(() -> runner.run(work));
            assertThat(CRM_POST_STARTED.await(10, TimeUnit.SECONDS)).isTrue();

            var current = workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id());
            var cancelling = workflows.cancelInstance(ALICE, Ids.WORKSPACE_A, current.id(), current.rowVersion());
            assertThat(cancelling.status()).isEqualTo("CANCELLING");
            assertThat(cancelling.businessEffectStatus()).isEqualTo("IN_PROGRESS");
            assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId()).status()).isEqualTo(TaskStatus.RUNNING);
            assertThat(CRM_WRITES).hasValue(1);

            CRM_POST_RELEASE.countDown();
            var outcome = run.get(20, TimeUnit.SECONDS);
            assertThat(outcome.status()).isEqualTo(TaskStatus.SUCCEEDED);
            tasks.complete(work, outcome);
            dispatch(started.instance().id());

            var cancelled = workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id());
            assertThat(cancelled.status()).isEqualTo("CANCELLED");
            assertThat(cancelled.businessEffectStatus()).isEqualTo("SUCCEEDED");
            assertThat(executions.get(ALICE, Ids.WORKSPACE_A, started.executionId()).status()).isEqualTo("SUCCEEDED");
            assertThat(CRM_WRITES).hasValue(1);
        } finally {
            CRM_HOLD_POST.set(false);
            CRM_POST_RELEASE.countDown();
            crm.stop(0);
        }
    }

    // 高风险分支获批后只提交一次 CRM 写入，并把核验后的结果关联回固定流程。
    @Test
    void highRiskApprovedFollowupCompletesOnce() throws Exception {
        var crm = startCustomerCrm();
        try {
            var started = startApprovedToolWorkflowViaRest("p6-10-high-approved");
            printP6Evidence("high-approved", started);
            var bob = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN,
                    Set.of("approval:read", "approval:decide", "execution:read"));
            var approval = approvals.get(bob, Ids.WORKSPACE_A, started.approvalId());
            mvc.perform(post("/api/v1/workspaces/{workspaceId}/approvals/{approvalId}/decisions",
                            Ids.WORKSPACE_A, approval.id())
                            .header("Authorization", BOB_TOKEN).contentType(MediaType.APPLICATION_JSON)
                            .content("{\"decision\":\"APPROVED\",\"expectedVersion\":" + approval.version() + "}"))
                    .andExpect(status().isOk());
            var waiting = tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId());
            tasks.resume(ALICE, Ids.WORKSPACE_A, started.taskId(), waiting.version(), "p6-10-run-high-approved");
            var work = tasks.claimOne().orElseThrow();
            assertThat(work.id()).isEqualTo(started.taskId());
            var outcome = runner.run(work);
            var duplicateResume = runner.run(work);
            assertThat(outcome.status()).isEqualTo(TaskStatus.SUCCEEDED)
                    .withFailMessage("执行结果=%s，Execution=%s", outcome,
                            executions.get(ALICE, Ids.WORKSPACE_A, started.executionId()));
            assertThat(duplicateResume.errorCode()).isEqualTo("RUNTIME_LEASE_CONFLICT");
            tasks.complete(work, outcome);
            drainWithoutWaiting(started.instance().id());

            var completed = workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id());
            assertThat(completed.status()).withFailMessage("workflow=%s steps=%s child=%s execution=%s", completed,
                    jdbc.queryForList("select step_id, status, selected_next_step_id, output_json::text output_json from workflow.step where instance_id = ? order by created_at",
                            started.instance().id()), tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId()),
                    executions.get(ALICE, Ids.WORKSPACE_A, started.executionId())).isEqualTo("SUCCEEDED");
            var result = json.readTree(completed.resultJson());
            assertThat(result.path("riskLevel").asText()).isEqualTo("HIGH");
            assertThat(result.path("followupCreated").asBoolean()).isTrue();
            assertThat(completed.businessEffectStatus()).isEqualTo("SUCCEEDED");
            var completedTask = tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId());
            assertThat(completedTask.status()).isEqualTo(TaskStatus.SUCCEEDED);
            var publicToolResult = json.readTree(completedTask.resultJson());
            var outputFields = new java.util.HashSet<String>();
            publicToolResult.fieldNames().forEachRemaining(outputFields::add);
            assertThat(outputFields).containsExactlyInAnyOrder("operationId", "externalId", "status");
            assertThat(executions.get(ALICE, Ids.WORKSPACE_A, started.executionId()).operationId()).isEqualTo(
                    jdbc.queryForObject("select operation_id from execution.execution where id = ?", UUID.class, started.executionId()));
            assertThat(CRM_WRITES).hasValue(1);
        } finally {
            crm.stop(0);
        }
    }

    // 低风险和证据不足都走固定的无跟进分支，不创建 RUN_TOOL 子 Task。
    @Test
    void lowRiskAndInsufficientEvidenceStopBeforeToolStep() throws Exception {
        var crm = startCustomerCrm();
        try {
            var lowRisk = startInstanceViaRest("customer-001", "p6-10-low-risk");
            dispatch(lowRisk.id());
            dispatch(lowRisk.id());
            var lowStep = workflowStore.stepRuntime(lowRisk.id(), "analyze");
            var lowWork = tasks.claimOne().orElseThrow();
            assertThat(lowWork.id()).isEqualTo(lowStep.childTaskId());
            tasks.complete(lowWork, TaskRunner.RunOutcome.success(
                    "{\"riskLevel\":\"LOW\",\"summary\":\"续约状态已核实。\",\"reasons\":[\"近期已联系\"],\"uncertainties\":[]}",
                    false, null, null));
            drainWithoutWaiting(lowRisk.id());
            var lowResult = workflows.getInstance(ALICE, Ids.WORKSPACE_A, lowRisk.id());
            assertThat(lowResult.status()).isEqualTo("SUCCEEDED");
            var lowOutput = json.readTree(lowResult.resultJson());
            assertThat(lowOutput.path("riskLevel").asText()).isEqualTo("LOW");
            assertThat(lowOutput.path("followupCreated").asBoolean()).isFalse();
            assertThat(workflowStore.stepRuntime(lowRisk.id(), "create-followup")).isNull();

            var noEvidence = startInstanceViaRest("customer-001", "p6-10-no-evidence");
            dispatch(noEvidence.id());
            dispatch(noEvidence.id());
            var unknownStep = workflowStore.stepRuntime(noEvidence.id(), "analyze");
            var unknownWork = tasks.claimOne().orElseThrow();
            assertThat(unknownWork.id()).isEqualTo(unknownStep.childTaskId());
            tasks.complete(unknownWork, TaskRunner.RunOutcome.success(
                    "{\"riskLevel\":\"UNKNOWN\",\"summary\":\"证据不足，停止创建跟进。\",\"reasons\":[],\"uncertainties\":[\"续约记录尚未核实\"]}",
                    false, null, null));
            drainWithoutWaiting(noEvidence.id());
            var stopped = workflows.getInstance(ALICE, Ids.WORKSPACE_A, noEvidence.id());
            assertThat(stopped.status()).isEqualTo("SUCCEEDED");
            var stoppedOutput = json.readTree(stopped.resultJson());
            assertThat(stoppedOutput.path("riskLevel").asText()).isEqualTo("UNKNOWN");
            assertThat(stoppedOutput.path("followupCreated").asBoolean()).isFalse();
            assertThat(workflowStore.stepRuntime(noEvidence.id(), "create-followup")).isNull();
            assertThat(CRM_WRITES).hasValue(0);
        } finally {
            crm.stop(0);
        }
    }

    // 审批拒绝沿 Task/Execution 原链路失败关闭，Workflow 不得触发 CRM POST。
    @Test
    void rejectedApprovalFailsWorkflowWithoutBusinessWrite() throws Exception {
        var crm = startCustomerCrm();
        try {
            var started = startApprovedToolWorkflowViaRest("p6-10-rejected");
            var bob = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN,
                    Set.of("approval:read", "approval:decide", "execution:read"));
            var approval = approvals.get(bob, Ids.WORKSPACE_A, started.approvalId());
            mvc.perform(post("/api/v1/workspaces/{workspaceId}/approvals/{approvalId}/decisions",
                            Ids.WORKSPACE_A, approval.id())
                            .header("Authorization", BOB_TOKEN).contentType(MediaType.APPLICATION_JSON)
                            .content("{\"decision\":\"REJECTED\",\"expectedVersion\":" + approval.version() + "}"))
                    .andExpect(status().isOk());

            var failedTask = runResumedToolTask(started, "p6-10-run-rejected");
            drainWithoutWaiting(started.instance().id());

            var failed = workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id());
            assertThat(failedTask.status()).isEqualTo(TaskStatus.FAILED);
            assertThat(failed.status()).isEqualTo("FAILED");
            assertThat(failed.errorCode()).isEqualTo("APPROVAL_DENIED");
            assertThat(CRM_WRITES).hasValue(0);
        } finally {
            crm.stop(0);
        }
    }

    // 审批等待时取消后由全新 JVM 接管 CANCELLING，确认 Task/Execution/Workflow 共享持久状态。
    @Test
    void cancelledApprovalWorkflowFinishesAfterProcessRestart() throws Exception {
        var crm = startCustomerCrm();
        try {
            var started = startApprovedToolWorkflowViaRest("p6-10-cancel-process-restart");
            printP6Evidence("cancel-after-process-restart", started);
            var waiting = workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id());
            assertThat(waiting.waitingReason()).isEqualTo("APPROVAL_REQUIRED");
            var cancelling = workflows.cancelInstance(ALICE, Ids.WORKSPACE_A, started.instance().id(), waiting.rowVersion());
            assertThat(cancelling.status()).isEqualTo("CANCELLING");
            assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId()).status()).isEqualTo(TaskStatus.CANCELLED);
            assertThat(jdbc.queryForObject("select state from approval.request where id = ?", String.class,
                    started.approvalId())).isEqualTo("CANCELLED");
            assertThat(executions.get(ALICE, Ids.WORKSPACE_A, started.executionId()).status()).isEqualTo("CANCELLED");
            assertThat(CRM_WRITES).hasValue(0);

            var resumed = startWorkflowProcess(started.instance().id(), "resume");
            assertThat(awaitLine(resumed, "P6_WORKFLOW_RESULT=CANCELLED", Duration.ofSeconds(60))).isTrue();
            assertThat(resumed.process().waitFor(30, TimeUnit.SECONDS)).isTrue();
            assertThat(resumed.process().exitValue()).isZero();
            assertThat(workflows.getInstance(ALICE, Ids.WORKSPACE_A, started.instance().id()).status()).isEqualTo("CANCELLED");
            assertThat(CRM_WRITES).hasValue(0);
        } finally {
            crm.stop(0);
        }
    }

    @Test
    void recoversIntentLostCreateAndUnrecordedResultThenCompletesNoWriteBranch() {
        var instance = startInstance("C-100", "p6-08-recovery");

        // 第一次领取只持久化 inputHash/dispatchKey；即使进程此处中断，也没有业务 Task 副作用。
        assertThat(dispatcher.dispatchOne()).isTrue();
        var intent = workflowStore.stepRuntime(instance.id(), "analyze");
        assertThat(intent.status()).isEqualTo("INTENT");
        assertThat(intent.childTaskId()).isNull();
        assertThat(countByKey(intent.dispatchKey())).isZero();

        // 模拟子 Task 创建提交后响应丢失：步骤仍是 INTENT，重启 Worker 必须按来源键找回同一 Task。
        var lease = workflowStore.claimOne().orElseThrow();
        var createdBeforeLostResponse = createAnalyzeTask(instance, intent);
        workflowStore.release(lease, "RUNNING", "analyze");
        assertThat(dispatcher.dispatchOne()).isTrue();
        var linked = workflowStore.stepRuntime(instance.id(), "analyze");
        assertThat(linked.childTaskId()).isEqualTo(createdBeforeLostResponse.id());
        assertThat(countByKey(intent.dispatchKey())).isEqualTo(1);
        assertThat(jdbc.queryForObject("select root_task_id = ? from task.budget_scope where scope_id = ?",
                Boolean.class, createdBeforeLostResponse.id(), instance.rootBudgetScopeId())).isTrue();

        // 子 Task 已完成而 Workflow 尚未记录输出时，下一 Worker 读取原 Task，不重复模型调用或创建 Task。
        runQueuedTask(createdBeforeLostResponse.id());
        jdbc.update("update workflow.instance set next_poll_at = null where id = ?", instance.id());
        assertThat(workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id()).status()).isEqualTo("WAITING_CHILD");
        dispatcher.dispatchOne();
        var completedAnalyze = workflowStore.stepRuntime(instance.id(), "analyze");
        assertThat(completedAnalyze.status()).isEqualTo("SUCCEEDED");
        assertThat(completedAnalyze.outputJson()).contains("riskLevel");
        assertThat(workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id()).currentStepId()).isEqualTo("risk-branch");

        // 固定图按真实已存风险字段走低风险出口，COMPLETE 的输出经过输出 Schema 校验。
        drainWithoutWaiting(instance.id());
        var finished = workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id());
        var stepStates = jdbc.queryForList("select step_id, status, child_task_id, selected_next_step_id from workflow.step where instance_id = ? order by created_at",
                instance.id());
        assertThat(finished.status()).withFailMessage("Workflow state=%s step=%s error=%s steps=%s",
                finished.status(), finished.currentStepId(), finished.errorCode(), stepStates).isEqualTo("SUCCEEDED");
        assertThat(finished.resultJson()).contains("followupCreated").contains("false");
        assertThat(jdbc.queryForObject("select count(*) from workflow.step where instance_id = ? and child_task_id is not null",
                Integer.class, instance.id())).isEqualTo(1);
    }

    @Test
    void invalidChildOutputFailsClosedAndLeaseFenceRejectsOldWorker() {
        var instance = startInstance("C-200", "p6-08-invalid-output");
        dispatcher.dispatchOne();
        dispatcher.dispatchOne();
        var linked = workflowStore.stepRuntime(instance.id(), "analyze");
        var taskId = linked.childTaskId();
        runQueuedTask(taskId);
        jdbc.update("update task.task set result_json = '{}'::jsonb where id = ?", taskId);
        jdbc.update("update workflow.instance set next_poll_at = null where id = ?", instance.id());

        var oldLease = workflowStore.claimOne().orElseThrow(() -> new AssertionError("Workflow 不可领取："
                + workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id())));
        var newLease = new WorkflowLease(oldLease.instanceId(), UUID.randomUUID(), oldLease.fence() + 1);
        jdbc.update("update workflow.instance set lease_owner_id = ?, lease_fence = ?, lease_until = now() + interval '30 seconds' where id = ?",
                newLease.ownerId(), newLease.fence(), instance.id());
        assertThatThrownBy(() -> workflowStore.linkChildTask(oldLease, "analyze", taskId, taskId))
                .isInstanceOfSatisfying(EafException.class,
                        error -> assertThat(error.code()).isEqualTo("WORKFLOW_LEASE_LOST"));

        dispatcher.advance(oldLease);
        assertThat(workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id()).status()).isEqualTo("WAITING_CHILD");
        dispatcher.advance(newLease);
        assertThat(workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id()).status()).isEqualTo("CANCELLING");
        // 失败步骤先持久进入终止流程，再由后续租约收敛到 FAILED。
        dispatch(instance.id());
        var failed = workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id());
        assertThat(failed.status()).isEqualTo("FAILED");
        assertThat(failed.errorCode()).isEqualTo("INVALID_REQUEST");
        assertThat(countByKey(linked.dispatchKey())).isEqualTo(1);
    }

    // 三个独立子 JVM 在数据库提交边界被强制终止，检查新进程从原恢复点继续。
    @Test
    void resumesAfterThreeRealJvmCrashCheckpoints() throws Exception {
        var instance = startInstance("C-300", "p6-08-process-recovery");

        killAtCheckpoint(instance.id(), "after-intent");
        var intent = workflowStore.stepRuntime(instance.id(), "analyze");
        assertThat(intent.status()).isEqualTo("INTENT");
        assertThat(intent.childTaskId()).isNull();

        killAtCheckpoint(instance.id(), "after-create");
        var created = workflowStore.stepRuntime(instance.id(), "analyze");
        assertThat(created.status()).isEqualTo("INTENT");
        assertThat(created.childTaskId()).isNull();
        assertThat(countByKey(created.dispatchKey())).isEqualTo(1);

        killAtCheckpoint(instance.id(), "after-child-result");
        var linked = workflowStore.stepRuntime(instance.id(), "analyze");
        assertThat(linked.childTaskId()).isNotNull();
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, linked.childTaskId()).status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(linked.status()).isEqualTo("WAITING_CHILD");
        assertThat(countByKey(linked.dispatchKey())).isEqualTo(1);
        assertThat(jdbc.queryForObject("select root_task_id = ? from task.budget_scope where scope_id = ?",
                Boolean.class, linked.childTaskId(), instance.rootBudgetScopeId())).isTrue();

        var resumed = startWorkflowProcess(instance.id(), "resume");
        assertThat(awaitLine(resumed, "P6_WORKFLOW_RESULT=SUCCEEDED", Duration.ofSeconds(60))).isTrue();
        assertThat(resumed.process().waitFor(30, TimeUnit.SECONDS)).isTrue();
        assertThat(resumed.process().exitValue()).isZero();
        var finished = workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id());
        assertThat(finished.status()).isEqualTo("SUCCEEDED");
        assertThat(finished.resultJson()).contains("followupCreated").contains("false");
        var childCount = jdbc.queryForObject("select count(*) from task.task where source = 'USER' and idempotency_key = ?",
                Integer.class, "workflow:" + linked.dispatchKey());
        assertThat(childCount).isEqualTo(1);
        System.out.println("P6_WORKFLOW_EVIDENCE instanceId=" + instance.id() + " childTaskId=" + linked.childTaskId()
                + " restartCheckpoints=3 childTaskCount=" + childCount + " rootBudgetAttached=true status=" + finished.status());
    }

    @Test
    void customerFollowupCollaborationVersionPreservesBaselineAndFailsClosed() throws Exception {
        var crm = startCustomerCrm();
        var peer = startReviewPeer();
        try {
            publishCollaborationVersions();
            var baseline = createFixedInstance("1.0.0", "p6-18-baseline", "USER");
            completeAnalysis(baseline, "需要跟进。");
            runApprovedFollowup(baseline);
            var baselineResult = json.readTree(workflows.getInstance(ALICE, Ids.WORKSPACE_A, baseline.id()).resultJson());
            assertThat(baselineResult.path("followupCreated").asBoolean()).isTrue();
            assertThat(REVIEW_PEER_SENDS).hasValue(0);
            assertThat(CRM_WRITES).hasValue(1);

            REVIEW_PEER_RISK.set("HIGH");
            var collaborative = createFixedInstance("1.1.0", "p6-18-collaboration", "USER");
            completeAnalysis(collaborative, "需要跟进。");
            completePeerReview(collaborative);
            var reviewTaskId = workflowStore.stepRuntime(collaborative.id(), "peer-review").childTaskId();
            var acceptance = jdbc.queryForMap("select actor_id, task_id, trace_id, payload_json::text payload from audit.audit_event where task_id = ? and action = 'REMOTE_AGENT_TASK_ACCEPTED' order by occurred_at desc limit 1", reviewTaskId);
            var acceptancePayload = json.readTree((String) acceptance.get("payload"));
            var expectedTrace = "workflow:" + collaborative.id() + "@1.1.0:peer-review";
            assertThat(acceptance.get("actor_id")).isEqualTo(Ids.ALICE);
            assertThat(acceptance.get("task_id")).isEqualTo(reviewTaskId);
            assertThat(acceptance.get("trace_id")).isEqualTo(expectedTrace);
            assertThat(acceptancePayload.path("entryProtocol").asText()).isEqualTo("REST");
            assertThat(acceptancePayload.path("principalId").asText()).isEqualTo(Ids.ALICE.toString());
            assertThat(acceptancePayload.path("remoteTaskId").asText()).isNotBlank();
            assertThat(acceptancePayload.toString()).doesNotContain("riskSummary", "a2a-peer-review");
            runApprovedFollowup(collaborative);
            var collaborativeInstance = workflows.getInstance(ALICE, Ids.WORKSPACE_A, collaborative.id());
            var collaborativeResult = json.readTree(collaborativeInstance.resultJson());
            assertThat(collaborativeResult.path("reviewRiskLevel").asText()).isEqualTo("HIGH");
            assertThat(collaborativeResult.path("reviewConflict").asBoolean()).isFalse();
            assertThat(REVIEW_PEER_SENDS).hasValue(1);
            assertThat(REVIEW_PEER_GETS).hasValue(1);
            assertThat(CRM_WRITES).hasValue(2);

            REVIEW_PEER_RISK.set("LOW");
            var conflict = createFixedInstance("1.1.0", "p6-18-conflict", "USER");
            completeAnalysis(conflict, "需要跟进。");
            completePeerReview(conflict);
            drainWithoutWaiting(conflict.id());
            var conflictInstance = workflows.getInstance(ALICE, Ids.WORKSPACE_A, conflict.id());
            var conflictResult = json.readTree(conflictInstance.resultJson());
            assertThat(conflictInstance.status()).isEqualTo("SUCCEEDED");
            assertThat(conflictResult.path("reviewConflict").asBoolean()).isTrue();
            assertThat(conflictResult.path("followupCreated").asBoolean()).isFalse();
            assertThat(workflowStore.stepRuntime(conflict.id(), "create-followup")).isNull();
            assertThat(REVIEW_PEER_SENDS).hasValue(2);
            assertThat(CRM_WRITES).hasValue(2);

            var evaluation = createFixedInstance("1.1.0", "p6-18-evaluation", "EVALUATION");
            completeAnalysis(evaluation, "需要跟进。");
            var reviewStep = ensureChild(evaluation.id(), "peer-review");
            runChild(reviewStep.childTaskId());
            drainWithoutWaiting(evaluation.id());
            assertThat(workflows.getInstance(ALICE, Ids.WORKSPACE_A, evaluation.id()).status()).isEqualTo("FAILED");
            assertThat(REVIEW_PEER_SENDS).hasValue(2);
            assertThat(CRM_WRITES).hasValue(2);

            REVIEW_PEER_UNAVAILABLE.set(true);
            var unavailable = createFixedInstance("1.1.0", "p6-18-peer-unavailable", "USER");
            completeAnalysis(unavailable, "需要跟进。");
            var unavailableStep = ensureChild(unavailable.id(), "peer-review");
            var unavailableTask = runChild(unavailableStep.childTaskId());
            assertThat(unavailableTask.status()).isIn(TaskStatus.WAITING_VERIFICATION, TaskStatus.FAILED);
            assertThat(workflowStore.stepRuntime(unavailable.id(), "create-followup")).isNull();
            assertThat(CRM_WRITES).hasValue(2);
            REVIEW_PEER_UNAVAILABLE.set(false);

            var missingEvidence = createFixedInstance("1.1.0", "p6-18-missing-evidence", "USER");
            completeAnalysis(missingEvidence, "");
            var missingStep = ensureChild(missingEvidence.id(), "peer-review");
            runChild(missingStep.childTaskId());
            drainWithoutWaiting(missingEvidence.id());
            assertThat(REVIEW_PEER_SENDS).hasValue(2);
            assertThat(workflowStore.stepRuntime(missingEvidence.id(), "create-followup")).isNull();
            assertThat(CRM_WRITES).hasValue(2);

            var oldInstance = workflows.getInstance(ALICE, Ids.WORKSPACE_A, baseline.id());
            assertThat(oldInstance.workflowVersion()).isEqualTo("1.0.0");
            var metrics = jdbc.queryForMap("select model_calls, tool_calls, tool_executions, token_used, token_reserved from task.budget_scope where scope_id = ?",
                    collaborative.rootBudgetScopeId());
            System.out.printf("P6_18_PAIR baselineVersion=1.0.0 collaborativeVersion=1.1.0 baselineWrites=1 collaborativeWrites=1 peerSends=%d peerPolls=%d sharedBudget=%s syntheticAnalysis=true%n",
                    REVIEW_PEER_SENDS.get(), REVIEW_PEER_GETS.get(), metrics);
        } finally {
            REVIEW_PEER_UNAVAILABLE.set(false);
            peer.stop(0);
            crm.stop(0);
        }
    }

    private WorkflowInstance startInstance(String customerId, String key) {
        var seed = workflows.get(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0");
        var version = "9.0." + VERSIONS.incrementAndGet();
        var draft = workflows.addVersion(ALICE, Ids.WORKSPACE_A, SEED_ID,
                new CreateWorkflowVersionCommand(version, seed.inputSchema(), seed.outputSchema(), seed.entryStepId(), seed.steps()));
        var published = workflows.publish(ALICE, Ids.WORKSPACE_A, SEED_ID, version, draft.rowVersion());
        return workflows.createInstance(new CreateWorkflowInstanceCommand(ALICE, Ids.WORKSPACE_A,
                SEED_ID, published.version(), "{\"customerId\":\"" + customerId + "\"}", key, "USER"));
    }

    private void publishCollaborationVersions() {
        var capability = capabilities.get(ALICE, Ids.WORKSPACE_A, CAPABILITY_ID, "1.1.0");
        if ("DRAFT".equals(capability.status()))
            capabilities.publish(ALICE, Ids.WORKSPACE_A, CAPABILITY_ID, capability.version(), capability.rowVersion());
        var baseline = workflows.get(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0");
        if ("DRAFT".equals(baseline.status()))
            workflows.publish(ALICE, Ids.WORKSPACE_A, SEED_ID, baseline.version(), baseline.rowVersion());
        var collaboration = workflows.get(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.1.0");
        if ("DRAFT".equals(collaboration.status()))
            workflows.publish(ALICE, Ids.WORKSPACE_A, SEED_ID, collaboration.version(), collaboration.rowVersion());
    }

    private WorkflowInstance createFixedInstance(String version, String key, String source) {
        return workflows.createInstance(new CreateWorkflowInstanceCommand(ALICE, Ids.WORKSPACE_A,
                SEED_ID, version, "{\"customerId\":\"customer-001\"}", key, source));
    }

    private void completeAnalysis(WorkflowInstance instance, String summary) {
        var step = ensureChild(instance.id(), "analyze");
        var work = "EVALUATION".equals(instance.source())
                ? tasks.claim(step.childTaskId()).orElseThrow() : tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(step.childTaskId());
        var output = "{\"riskLevel\":\"HIGH\",\"summary\":\"" + summary
                + "\",\"reasons\":[\"synthetic\"],\"uncertainties\":[]}";
        tasks.complete(work, TaskRunner.RunOutcome.success(output, false, null, null));
    }

    private WorkflowStepRuntime ensureChild(UUID instanceId, String stepId) {
        for (int i = 0; i < 12; i++) {
            var step = workflowStore.stepRuntime(instanceId, stepId);
            if (step != null && step.childTaskId() != null) return step;
            var instance = workflows.getInstance(ALICE, Ids.WORKSPACE_A, instanceId);
            if (Set.of("FAILED", "CANCELLED", "TIMED_OUT").contains(instance.status())) break;
            dispatch(instanceId);
        }
        throw new IllegalStateException("Workflow 未创建预期步骤 Task: " + stepId);
    }

    private TaskSnapshot runChild(UUID taskId) {
        var source = tasks.get(ALICE, Ids.WORKSPACE_A, taskId).source();
        var work = "EVALUATION".equals(source) ? tasks.claim(taskId).orElseThrow() : tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(taskId);
        var outcome = runner.run(work);
        tasks.complete(work, outcome);
        return tasks.get(ALICE, Ids.WORKSPACE_A, taskId);
    }

    private void completePeerReview(WorkflowInstance instance) {
        var step = ensureChild(instance.id(), "peer-review");
        assertThat(runChild(step.childTaskId()).status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        jdbc.update("update execution.remote_a2a_operation set next_poll_at = now() - interval '1 second' where task_id = ?", step.childTaskId());
        executions.wakeDueRemoteTasks();
        var polled = runChild(step.childTaskId());
        var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, step.childTaskId());
        assertThat(polled.status()).withFailMessage("远端轮询结果 Task=%s Execution=%s", polled,
                executions.get(ALICE, Ids.WORKSPACE_A, executionId)).isEqualTo(TaskStatus.SUCCEEDED);
    }

    private void runApprovedFollowup(WorkflowInstance instance) {
        var step = ensureChild(instance.id(), "create-followup");
        assertThat(runChild(step.childTaskId()).status()).isEqualTo(TaskStatus.WAITING_APPROVAL);
        var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, step.childTaskId());
        var approvalId = jdbc.queryForObject("select id from approval.request where execution_id = ?", UUID.class, executionId);
        var bob = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN,
                Set.of("approval:read", "approval:decide", "execution:read"));
        var approval = approvals.get(bob, Ids.WORKSPACE_A, approvalId);
        approvals.decide(new ApprovalDecisionCommand(bob, Ids.WORKSPACE_A, approval.id(), "APPROVED", approval.version()));
        var waiting = tasks.get(ALICE, Ids.WORKSPACE_A, step.childTaskId());
        tasks.resume(ALICE, Ids.WORKSPACE_A, step.childTaskId(), waiting.version(), "p6-18-resume-" + UUID.randomUUID());
        assertThat(runChild(step.childTaskId()).status()).isEqualTo(TaskStatus.SUCCEEDED);
        drainWithoutWaiting(instance.id());
        assertThat(workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id()).status()).isEqualTo("SUCCEEDED");
    }

    private HttpServer startReviewPeer() throws IOException {
        REVIEW_PEER_SENDS.set(0);
        REVIEW_PEER_GETS.set(0);
        REVIEW_PEER_IDS.set(0);
        REVIEW_PEER_CONTEXTS.clear();
        REVIEW_PEER_RISK.set("HIGH");
        REVIEW_PEER_UNAVAILABLE.set(false);
        var peer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        peer.createContext("/a2a", this::handleReviewPeer);
        peer.start();
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = 'A2A_REVIEW_PEER'",
                "http://127.0.0.1:" + peer.getAddress().getPort() + "/a2a", Ids.TENANT_A, Ids.WORKSPACE_A);
        return peer;
    }

    private void handleReviewPeer(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"Bearer p6-workflow-a2a".equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                exchange.sendResponseHeaders(401, -1);
                return;
            }
            var request = json.readTree(exchange.getRequestBody());
            var method = request.path("method").asText();
            if ("SendMessage".equals(method) && REVIEW_PEER_UNAVAILABLE.get()) {
                exchange.sendResponseHeaders(503, -1);
                return;
            }
            var task = json.createObjectNode();
            if ("SendMessage".equals(method)) {
                var id = REVIEW_PEER_IDS.incrementAndGet();
                var taskId = "workflow-peer-task-" + id;
                var contextId = "workflow-peer-context-" + id;
                REVIEW_PEER_CONTEXTS.put(taskId, contextId);
                task.put("id", taskId).put("contextId", contextId);
                task.putObject("status").put("state", "TASK_STATE_SUBMITTED");
                task.putArray("artifacts");
                REVIEW_PEER_SENDS.incrementAndGet();
            } else if ("GetTask".equals(method)) {
                REVIEW_PEER_GETS.incrementAndGet();
                var taskId = request.path("params").path("id").asText();
                task.put("id", taskId);
                task.put("contextId", REVIEW_PEER_CONTEXTS.getOrDefault(taskId, "unknown-context"));
                task.putObject("status").put("state", "TASK_STATE_COMPLETED");
                var data = task.putArray("artifacts").addObject().put("name", "risk-review")
                        .putArray("parts").addObject().put("kind", "data").putObject("data")
                        .put("riskLevel", REVIEW_PEER_RISK.get()).put("rationale", "synthetic peer opinion");
                data.putArray("citations").add("synthetic-review-source");
            } else {
                task.put("id", request.path("params").path("id").asText());
                task.put("contextId", request.path("params").path("contextId").asText());
                task.putObject("status").put("state", "TASK_STATE_CANCELED");
                task.putArray("artifacts");
            }
            var result = json.createObjectNode();
            if ("SendMessage".equals(method)) result.putObject("task").setAll(task);
            else result.setAll(task);
            var response = json.createObjectNode().put("jsonrpc", "2.0");
            response.set("id", request.get("id"));
            response.set("result", result);
            var bytes = json.writeValueAsBytes(response);
            exchange.getResponseHeaders().add("Content-Type", "application/a2a+json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }

    //  演示从受保护 REST 入口创建固定图和实例；后续断言仍读取各域公开服务。
    private WorkflowInstance startInstanceViaRest(String customerId, String key) throws Exception {
        var seed = workflows.get(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0");
        var create = json.createObjectNode().put("name", "p6-10-rest-" + UUID.randomUUID())
                .put("description", "deterministic REST scenario")
                .put("version", "1.0.0").put("inputSchema", seed.inputSchema())
                .put("outputSchema", seed.outputSchema()).put("entryStepId", seed.entryStepId());
        create.set("steps", json.valueToTree(seed.steps()));
        var createdWorkflow = mvc.perform(post("/api/v1/workspaces/{workspaceId}/workflows", Ids.WORKSPACE_A)
                        .header("Authorization", ALICE_TOKEN).contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(create)))
                .andExpect(status().isCreated()).andReturn();
        var workflow = json.readTree(createdWorkflow.getResponse().getContentAsString());
        var workflowId = UUID.fromString(workflow.path("id").asText());
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/workflows/{workflowId}/versions/1.0.0/publish?expectedVersion={version}",
                        Ids.WORKSPACE_A, workflowId, workflow.path("rowVersion").asLong())
                        .header("Authorization", ALICE_TOKEN))
                .andExpect(status().isOk());

        var instanceResponse = mvc.perform(post("/api/v1/workspaces/{workspaceId}/workflows/{workflowId}/instances",
                        Ids.WORKSPACE_A, workflowId)
                        .header("Authorization", ALICE_TOKEN).header("Idempotency-Key", key + "-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"version\":\"1.0.0\",\"input\":{\"customerId\":\"" + customerId + "\"}}"))
                .andExpect(status().isAccepted()).andReturn();
        var instanceId = UUID.fromString(json.readTree(instanceResponse.getResponse().getContentAsString())
                .path("id").asText());
        System.out.printf("P6_10_REST_ENTRY workflowId=%s workflowVersion=1.0.0 instanceId=%s key=%s%n",
                workflowId, instanceId, key);
        return workflows.getInstance(ALICE, Ids.WORKSPACE_A, instanceId);
    }

    private TaskSnapshot createAnalyzeTask(WorkflowInstance instance, WorkflowStepRuntime step) {
        var capability = capabilities.requirePublished(ALICE, Ids.WORKSPACE_A, CAPABILITY_ID, "1.0.0");
        var binding = new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash());
        // 模拟 Dispatcher 的同一幂等请求时也携带服务端生成的 Workflow 步骤 provenance。
        return tasks.createWorkflowTask(new CreateWorkflowTaskCommand(ALICE, Ids.WORKSPACE_A,
                instance.rootBudgetScopeId(), instance.rootTaskId(), step.dispatchKey(), capability.agentId(),
                capability.agentVersion(), step.inputJson(), instance.source(), binding, null, null, null,
                "workflow:" + instance.id() + "@" + instance.workflowVersion() + ":analyze", instance.qualityRunId(),
                new io.eaf.task.api.WorkflowTaskProvenance(instance.id(), instance.workflowId(),
                        instance.workflowVersion(), "analyze")));
    }

    private void runQueuedTask(UUID expectedId) {
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(expectedId);
        tasks.complete(work, runner.run(work));
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, expectedId).status()).isEqualTo(TaskStatus.SUCCEEDED);
    }

    private void drainWithoutWaiting(UUID instanceId) {
        for (int i = 0; i < 8; i++) {
            jdbc.update("update workflow.instance set next_poll_at = null where id = ?", instanceId);
            if (!dispatcher.dispatchOne()) break;
            var instance = workflows.getInstance(ALICE, Ids.WORKSPACE_A, instanceId);
            if ("SUCCEEDED".equals(instance.status()) || "FAILED".equals(instance.status())) return;
        }
    }

    private int countByKey(String dispatchKey) {
        return jdbc.queryForObject("select count(*) from task.task where idempotency_key = ?", Integer.class,
                "workflow:" + dispatchKey);
    }

    // 用固定调度轮次把分析推进到工具 Task，并保留真实审批门禁，不调用 CRM 写端点。
    private WorkflowToolStart startApprovedToolWorkflow(String key) {
        return startApprovedToolWorkflow(startInstance("customer-001", key));
    }

    //  四个业务结果都从 REST 创建 Workflow，批准/拒绝走现有审批端点。
    private WorkflowToolStart startApprovedToolWorkflowViaRest(String key) throws Exception {
        return startApprovedToolWorkflow(startInstanceViaRest("customer-001", key));
    }

    private WorkflowToolStart startApprovedToolWorkflow(WorkflowInstance instance) {
        dispatch(instance.id());
        dispatch(instance.id());
        var analyze = workflowStore.stepRuntime(instance.id(), "analyze");
        var analysisWork = tasks.claimOne().orElseThrow();
        assertThat(analysisWork.id()).isEqualTo(analyze.childTaskId());
        tasks.complete(analysisWork, TaskRunner.RunOutcome.success(
                "{\"riskLevel\":\"HIGH\",\"summary\":\"需要跟进。\",\"reasons\":[\"测试\"],\"uncertainties\":[]}",
                false, null, null));
        dispatch(instance.id());
        dispatch(instance.id());
        dispatch(instance.id());
        dispatch(instance.id());
        dispatch(instance.id());
        var tool = workflowStore.stepRuntime(instance.id(), "create-followup");
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(tool.childTaskId());
        tasks.complete(work, runner.run(work));
        dispatch(instance.id());
        var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class,
                tool.childTaskId());
        var approvalId = jdbc.queryForObject("select id from approval.request where execution_id = ?", UUID.class,
                executionId);
        return new WorkflowToolStart(instance, tool.childTaskId(), executionId, approvalId);
    }

    // 记录端到端外部效果测试的关联键，便于把 Surefire 输出与数据库侧副作用计数对应。
    private void printP6Evidence(String scenario, WorkflowToolStart started) {
        var operationId = jdbc.queryForObject("select operation_id from execution.execution where id = ?", UUID.class,
                started.executionId());
        System.out.printf("P6_WORKFLOW_EVIDENCE scenario=%s instanceId=%s taskId=%s executionId=%s approvalId=%s operationId=%s%n",
                scenario, started.instance().id(), started.taskId(), started.executionId(), started.approvalId(), operationId);
    }

    // Task 恢复后仍由 Runtime 读取原 Execution，避免测试绕过实际执行入口。
    private TaskSnapshot runResumedToolTask(WorkflowToolStart started, String idempotencyKey) {
        var waiting = tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId());
        tasks.resume(ALICE, Ids.WORKSPACE_A, started.taskId(), waiting.version(), idempotencyKey);
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(started.taskId());
        tasks.complete(work, runner.run(work));
        return tasks.get(ALICE, Ids.WORKSPACE_A, started.taskId());
    }

    // 每轮只领取指定实例，确保断言对应持久化步骤而不是测试执行顺序。
    private void dispatch(UUID instanceId) {
        jdbc.update("update workflow.instance set next_poll_at = now() + interval '1 day' where id <> ? and status in ('QUEUED','RUNNING','WAITING_CHILD','CANCELLING')",
                instanceId);
        jdbc.update("update workflow.instance set next_poll_at = null where id = ?", instanceId);
        var lease = workflowStore.claimOne().orElseThrow();
        assertThat(lease.instanceId()).isEqualTo(instanceId);
        dispatcher.advance(lease);
    }

    // 本地 CRM 只提供读接口并计数写请求，凭据仍由 Connector 注入。
    private HttpServer startCustomerCrm() throws IOException {
        // Workflow 正向写入场景显式授予 HUMAN Workspace 动作，撤销测试再单独覆盖失权路径。
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'crm:followup:create', 'ACTIVE') "
                        + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        CRM_WRITES.set(0);
        CRM_RECORDS.clear();
        CRM_UNKNOWN_WRITE.set(false);
        CRM_HOLD_POST.set(false);
        CRM_POST_STARTED = new CountDownLatch(1);
        CRM_POST_RELEASE = new CountDownLatch(1);
        var crm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        crm.createContext("/customers/customer-001", exchange -> {
            if (!"Bearer p6-workflow-credential".equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
            }
            var body = "{\"customerId\":\"customer-001\",\"renewalStatus\":\"ACTIVE\",\"lastContactDate\":\"2026-09-20\",\"complaintSummary\":\"none\",\"sourceId\":\"crm-test-001\"}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        crm.createContext("/followups", exchange -> {
            if (!"POST".equals(exchange.getRequestMethod())
                    || !"Bearer p6-workflow-credential".equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
                return;
            }
            var request = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            var operationId = jsonValue(request, "operationId");
            var record = "{\"operationId\":\"" + operationId + "\",\"externalId\":\"fu-" + operationId
                    + "\",\"customerId\":\"" + jsonValue(request, "customerId") + "\",\"summary\":\""
                    + jsonValue(request, "summary") + "\",\"ownerId\":\"" + jsonValue(request, "ownerId")
                    + "\",\"status\":\"CREATED\",\"acceptedAt\":\"2026-09-29T00:00:00Z\"}";
            CRM_RECORDS.putIfAbsent(operationId, record);
            CRM_WRITES.incrementAndGet();
            CRM_POST_STARTED.countDown();
            if (CRM_HOLD_POST.get()) {
                try {
                    if (!CRM_POST_RELEASE.await(15, TimeUnit.SECONDS)) {
                        exchange.sendResponseHeaders(504, -1);
                        exchange.close();
                        return;
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    exchange.sendResponseHeaders(503, -1);
                    exchange.close();
                    return;
                }
            }
            var status = CRM_UNKNOWN_WRITE.get() ? 500 : 201;
            var body = CRM_RECORDS.get(operationId).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        crm.createContext("/followups/by-operation/", exchange -> {
            if (!"Bearer p6-workflow-credential".equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
            }
            var operationId = exchange.getRequestURI().getPath().substring("/followups/by-operation/".length());
            var record = CRM_RECORDS.get(operationId);
            if (record == null) {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
                return;
            }
            var body = record.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        crm.start();
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'",
                "http://127.0.0.1:" + crm.getAddress().getPort(), Ids.TENANT_A, Ids.WORKSPACE_A);
        return crm;
    }

    private static String jsonValue(String json, String field) {
        var marker = "\"" + field + "\":\"";
        var start = json.indexOf(marker);
        if (start < 0) throw new IllegalArgumentException("缺少测试请求字段：" + field);
        start += marker.length();
        return json.substring(start, json.indexOf('"', start));
    }

    private record WorkflowToolStart(WorkflowInstance instance, UUID taskId, UUID executionId, UUID approvalId) { }

    // 子 JVM 必须先输出提交标记，父测试才强制终止它，避免把未提交事务误报成恢复证据。
    private void killAtCheckpoint(UUID instanceId, String checkpoint) throws Exception {
        var child = startWorkflowProcess(instanceId, checkpoint);
        assertThat(awaitLine(child, "P6_WORKFLOW_CHECKPOINT=" + checkpoint, Duration.ofSeconds(60)))
                .as("Workflow 子 JVM 应到达已提交检查点 %s", checkpoint).isTrue();
        child.process().destroyForcibly();
        assertThat(child.process().waitFor(15, TimeUnit.SECONDS)).isTrue();
    }

    // 所有进程共用 Testcontainers PostgreSQL；禁用自动调度，由 runner 精确控制断点。
    private WorkflowProcess startWorkflowProcess(UUID instanceId, String action) throws IOException {
        var classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var executable = System.getProperty("os.name", "").toLowerCase().contains("win") ? "java.exe" : "java";
        var java = Path.of(System.getProperty("java.home"), "bin", executable).toString();
        var process = new ProcessBuilder(java, "-cp", classpath, P6WorkflowCrashRunner.class.getName(),
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--server.port=0", "--eaf.security.mode=local", "--eaf.task.dispatcher-enabled=false",
                "--eaf.workflow.dispatcher-enabled=false", "--eaf.execution.outbox-publisher-enabled=false",
                "--eaf.knowledge.outbox-publisher-enabled=false", "--eaf.memory.outbox-publisher-enabled=false",
                "--p6.workflow.workspace-id=" + Ids.WORKSPACE_A,
                "--p6.workflow.instance-id=" + instanceId,
                "--p6.workflow.action=" + action).redirectErrorStream(true).start();
        var lines = new LinkedBlockingQueue<String>();
        var outputDrained = new CompletableFuture<Void>();
        Thread.ofVirtual().start(() -> {
            try (var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) lines.offer(line);
            } catch (IOException ignored) { }
            finally { outputDrained.complete(null); }
        });
        return new WorkflowProcess(process, lines, outputDrained);
    }

    private boolean awaitLine(WorkflowProcess child, String marker, Duration timeout) throws InterruptedException {
        var deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            var line = child.lines().poll(250, TimeUnit.MILLISECONDS);
            if (line != null && line.contains(marker)) return true;
            if (!child.process().isAlive() && child.outputDrained().isDone() && child.lines().isEmpty()) return false;
        }
        return false;
    }

    private record WorkflowProcess(Process process, LinkedBlockingQueue<String> lines,
                                   CompletableFuture<Void> outputDrained) { }
}
