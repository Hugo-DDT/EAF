package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.eaf.agent.api.AgentCatalog;
import io.eaf.agentruntime.infrastructure.JdbcAgentRuntime;
import io.eaf.approval.api.ApprovalDecisionCommand;
import io.eaf.approval.api.ApprovalService;
import io.eaf.approval.api.ApprovalSnapshot;
import io.eaf.audit.api.AuditPort;
import io.eaf.capability.api.CapabilityService;
import io.eaf.context.api.ContextService;
import io.eaf.context.api.EvaluationContextSnapshotReader;
import io.eaf.execution.api.ExecutionCommand;
import io.eaf.execution.api.ExecutionService;
import io.eaf.execution.api.ExecutionSnapshot;
import io.eaf.execution.infrastructure.ExecutionOutboxPublisher;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelResult;
import io.eaf.model.api.ModelToolCall;
import io.eaf.observability.api.TraceRecorder;
import io.eaf.prompt.api.PromptCatalog;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskSnapshot;
import io.eaf.task.api.TaskStatus;
import io.eaf.tool.api.ToolCatalog;
import io.eaf.usage.api.UsageRecorder;
import io.eaf.workflow.api.CreateWorkflowInstanceCommand;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.infrastructure.WorkflowDispatcher;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false", "eaf.memory.outbox-publisher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false", "eaf.execution.remote-poller-enabled=false", "eaf.task.lease-duration=PT10M",
        "eaf.model.scenario=WRITE", "eaf.security.mode=local"})
//  在版本化 loopback 合同上验证审批写入、稳定 operationId 和未知结果回读。
class P7EnterpriseCrmWriteTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String CREDENTIAL = "p7-crm-write-contract-fixture-token";
    private static final UUID WORKSPACE = Ids.WORKSPACE_A;
    private static final UUID CAPABILITY = UUID.fromString("54000000-0000-4000-8000-000000000001");
    private static final UUID P9_FOLLOWUP_WORKFLOW = UUID.fromString("58000000-0000-4000-8000-000000000009");
    private static final String CONNECTOR_PROVIDER = "P7_CRM_WRITE_CONTRACT_FIXTURE";
    private static final String FOLLOWUP_BINDING = "p7-crm-write-contract.followup-create";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ConcurrentHashMap<String, FixtureRecord> FOLLOWUPS = new ConcurrentHashMap<>();
    private static final AtomicInteger CUSTOMER_READS = new AtomicInteger();
    private static final AtomicInteger FOLLOWUP_POSTS = new AtomicInteger();
    private static final AtomicInteger FOLLOWUP_GETS = new AtomicInteger();
    private static final AtomicBoolean LOSE_NEXT_POST_RESPONSE = new AtomicBoolean();
    private static final AtomicBoolean HOLD_NEXT_POST_RESPONSE = new AtomicBoolean();
    private static final AtomicBoolean MISMATCH_READBACK = new AtomicBoolean();
    private static volatile CountDownLatch postCommitted = new CountDownLatch(0);
    private static volatile CountDownLatch releasePostResponse = new CountDownLatch(0);

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));
    private static HttpServer crm;

    @BeforeAll
    static void startCrmFixture() throws Exception {
        crm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        crm.createContext("/customers/", P7EnterpriseCrmWriteTest::readCustomer);
        crm.createContext("/followups", P7EnterpriseCrmWriteTest::createFollowup);
        crm.createContext("/followups/by-operation/", P7EnterpriseCrmWriteTest::findFollowup);
        crm.start();
    }

    @AfterAll
    static void stopCrmFixture() {
        releasePostResponse.countDown();
        if (crm != null) crm.stop(0);
    }

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.credentials.p7-crm-write.token", () -> CREDENTIAL);
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired ApprovalService approvals;
    @Autowired ExecutionService executions;
    @Autowired CapabilityService capabilities;
    @Autowired AgentCatalog agents;
    @Autowired PromptCatalog prompts;
    @Autowired AuditPort audit;
    @Autowired UsageRecorder usage;
    @Autowired TraceRecorder traces;
    @Autowired ToolCatalog toolCatalog;
    @Autowired ContextService contextService;
    @Autowired WorkflowService workflows;
    @Autowired WorkflowDispatcher workflowDispatcher;
    @Autowired ObjectMapper objectMapper;
    @Autowired java.time.Clock clock;
    @Autowired EvaluationContextSnapshotReader evaluationContexts;

    @BeforeEach
    void enableOnlyTheLocalWriteFixture() {
        CUSTOMER_READS.set(0);
        FOLLOWUP_POSTS.set(0);
        FOLLOWUP_GETS.set(0);
        FOLLOWUPS.clear();
        LOSE_NEXT_POST_RESPONSE.set(false);
        HOLD_NEXT_POST_RESPONSE.set(false);
        MISMATCH_READBACK.set(false);
        postCommitted = new CountDownLatch(0);
        releasePostResponse = new CountDownLatch(0);
        jdbc.update("update connector.instance set status = 'ACTIVE', base_url = ? where tenant_id = ? and workspace_id = ? and provider = ?",
                fixtureUrl(), Ids.TENANT_A, WORKSPACE, CONNECTOR_PROVIDER);
        jdbc.update("update workspace.operational_gate set enabled = true, changed_by = null, command_id = null "
                        + "where tenant_id = ? and workspace_id = ? and gate_name = 'BUSINESS_OUTBOUND'",
                Ids.TENANT_A, WORKSPACE);
        grantWorkspace("crm:followup:create");
        grantWorkspace("crm:customer:read");
        grantWorkspace("execution:verify");
        grantWorkspace("execution:outbox:replay");
        grantWorkspace("approval:read");
        grantWorkspace("approval:decide");
        grantWorkspace("task:resume");
        jdbc.update("insert into policy.customer_grant(tenant_id, workspace_id, actor_id, customer_id, status) "
                        + "values (?, ?, ?, 'customer-001', 'ACTIVE') on conflict (tenant_id, workspace_id, actor_id, customer_id) "
                        + "do update set status = 'ACTIVE'",
                Ids.TENANT_A, WORKSPACE, Ids.ALICE);
    }

    @Test
    void executionOutboxReplayPreservesEventPayloadAndCumulativeAttempts() {
        var pending = preparePendingWrite("p7-23-outbox-replay");
        var eventId = jdbc.queryForObject("select event_id from execution.outbox where execution_id = ? order by created_at desc limit 1",
                UUID.class, pending.execution().id());
        var payload = jdbc.queryForObject("select payload_json::text from execution.outbox where event_id = ?", String.class, eventId);
        jdbc.update("update execution.outbox set status = 'FAILED', attempts = 6, next_attempt_at = now() - interval '1 second' where event_id = ?",
                eventId);
        var key = "p7-23-replay-" + UUID.randomUUID();

        var accepted = executions.replayOutbox(alice(), WORKSPACE, eventId, key, "修复 Audit 暂时不可用后恢复投递");
        assertThat(accepted.status()).isEqualTo("PENDING");
        assertThat(accepted.attempts()).isEqualTo(6);
        assertThat(accepted.replayed()).isFalse();
        assertThat(jdbc.queryForObject("select event_id from execution.outbox where execution_id = ? order by created_at desc limit 1",
                UUID.class, pending.execution().id())).isEqualTo(eventId);
        assertThat(jdbc.queryForObject("select payload_json::text from execution.outbox where event_id = ?", String.class, eventId))
                .isEqualTo(payload);
        var replay = executions.replayOutbox(alice(), WORKSPACE, eventId, key, "修复 Audit 暂时不可用后恢复投递");
        assertThat(replay.commandId()).isEqualTo(accepted.commandId());
        assertThat(replay.replayed()).isTrue();
        assertThatThrownBy(() -> executions.replayOutbox(alice(), WORKSPACE, eventId, key, "不同原因"))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                        failure -> assertThat(failure.code()).isEqualTo("EXECUTION_COMMAND_CONFLICT"));
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class,
                "execution-outbox-replay:" + accepted.commandId())).isEqualTo(1);

        // 即使累计失败数已达上限，Owner 人工重置后的 PENDING 事件仍只调度一次且不清零历史计数。
        new ExecutionOutboxPublisher(jdbc, audit).publish();
        assertThat(jdbc.queryForObject("select status from execution.outbox where event_id = ?", String.class, eventId))
                .isEqualTo("SENT");
        assertThat(jdbc.queryForObject("select attempts from execution.outbox where event_id = ?", Integer.class, eventId))
                .isEqualTo(6);
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class,
                "execution-outbox:" + eventId)).isEqualTo(1);
    }

    @Test
    void stoppedOutboundGateLeavesApprovedWriteWaitingAndReadbackAvailable() {
        var pending = preparePendingWrite("p7-23-outbound-stop");
        approve(pending);
        assertThat(jdbc.queryForObject("select enabled from workspace.operational_gate where tenant_id = ? and workspace_id = ? and gate_name = 'BUSINESS_OUTBOUND'",
                Boolean.class, Ids.TENANT_A, WORKSPACE)).isTrue();
        tasks.resume(alice(), WORKSPACE, pending.task().id(), pending.waitingTask().version(), "p7-23-outbound-resume-" + UUID.randomUUID());
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(pending.task().id());
        jdbc.update("update workspace.operational_gate set enabled = false where tenant_id = ? and workspace_id = ? and gate_name = 'BUSINESS_OUTBOUND'",
                Ids.TENANT_A, WORKSPACE);
        assertThat(executions.resume(alice(), WORKSPACE, pending.execution().id()).status()).isEqualTo("AWAITING_APPROVAL");
        assertThat(FOLLOWUP_POSTS).hasValue(0);
        assertThat(FOLLOWUPS).isEmpty();

        jdbc.update("update workspace.operational_gate set enabled = true where tenant_id = ? and workspace_id = ? and gate_name = 'BUSINESS_OUTBOUND'",
                Ids.TENANT_A, WORKSPACE);
        var outcome = runtimeUsing(pending.gateway()).run(work);
        tasks.complete(work, outcome);
        assertThat(executions.get(alice(), WORKSPACE, pending.execution().id()).status()).isEqualTo("SUCCEEDED");
        assertThat(FOLLOWUP_POSTS).hasValue(1);
    }

    @Test
    void approvedWriteCreatesOneRecordAndRepeatedResumeDoesNotResend() {
        var pending = preparePendingWrite("p7-19-one-record");
        approve(pending);
        runApprovedTask(pending);

        var completed = tasks.get(alice(), WORKSPACE, pending.task().id());
        assertThat(completed.status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(executions.get(alice(), WORKSPACE, pending.execution().id()).status()).isEqualTo("SUCCEEDED");
        assertThat(FOLLOWUP_POSTS).hasValue(1);
        assertThat(FOLLOWUP_GETS).hasValue(0);
        assertThat(FOLLOWUPS).hasSize(1);
        assertThat(actionCount(pending.task().id(), "WRITE_PREVIEW_CREATED")).isEqualTo(1);
        assertThat(actionCount(pending.task().id(), "WRITE_VERIFIED")).isEqualTo(1);
        assertThat(jdbc.queryForObject("select tool_executions from task.budget_scope where root_task_id = ?",
                Integer.class, pending.task().id())).isEqualTo(1);
        int modelReservations = jdbc.queryForObject("select count(*) from task.budget_reservation where task_id = ? and kind = 'MODEL'",
                Integer.class, pending.task().id());

        assertThat(executions.resume(alice(), WORKSPACE, pending.execution().id()).status()).isEqualTo("SUCCEEDED");
        assertThat(executions.verify(alice(), WORKSPACE, pending.execution().id()).status()).isEqualTo("SUCCEEDED");
        assertThat(FOLLOWUP_POSTS).hasValue(1);
        assertThat(FOLLOWUP_GETS).hasValue(0);
        assertThat(FOLLOWUPS).hasSize(1);
        assertThat(actionCount(pending.task().id(), "WRITE_VERIFIED")).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from task.budget_reservation where task_id = ? and kind = 'MODEL'",
                Integer.class, pending.task().id())).isEqualTo(modelReservations);
    }

    @Test
    void lostPostResponseStaysUnknownUntilReadbackAndNeverRetriesPost() {
        var pending = preparePendingWrite("p7-19-lost-response");
        approve(pending);
        LOSE_NEXT_POST_RESPONSE.set(true);
        runApprovedTask(pending);

        var unknown = executions.get(alice(), WORKSPACE, pending.execution().id());
        assertThat(unknown.status()).isEqualTo("UNKNOWN");
        assertThat(FOLLOWUP_POSTS).hasValue(1);
        assertThat(FOLLOWUPS).hasSize(1);
        assertThat(FOLLOWUP_GETS).hasValue(0);

        // UNKNOWN 的核验是只读回读；停止新的业务出站后仍须允许确认已发生的外部事实。
        jdbc.update("update workspace.operational_gate set enabled = false where tenant_id = ? and workspace_id = ? and gate_name = 'BUSINESS_OUTBOUND'",
                Ids.TENANT_A, WORKSPACE);
        var key = "p7-23-verify-" + UUID.randomUUID();
        var receipt = executions.verifyOperational(alice(), WORKSPACE, pending.execution().id(), key, "核验原 operationId 的 CRM 回执");
        assertThat(receipt.executionStatus()).isEqualTo("SUCCEEDED");
        assertThat(receipt.completed()).isTrue();
        assertThat(receipt.replayed()).isFalse();
        assertThat(receipt.operationId()).isEqualTo(pending.execution().operationId());
        var replay = executions.verifyOperational(alice(), WORKSPACE, pending.execution().id(), key, "核验原 operationId 的 CRM 回执");
        assertThat(replay.commandId()).isEqualTo(receipt.commandId());
        assertThat(replay.replayed()).isTrue();
        assertThat(FOLLOWUP_GETS).hasValue(1);
        assertThatThrownBy(() -> executions.verifyOperational(alice(), WORKSPACE, pending.execution().id(), key, "异参复用请求键"))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                        failure -> assertThat(failure.code()).isEqualTo("EXECUTION_COMMAND_CONFLICT"));
        assertThat(executions.verify(alice(), WORKSPACE, pending.execution().id()).status()).isEqualTo("SUCCEEDED");
        assertThat(FOLLOWUP_POSTS).hasValue(1);
        assertThat(FOLLOWUP_GETS).hasValue(1);
        assertThat(FOLLOWUPS).hasSize(1);
        assertThat(actionCount(pending.task().id(), "WRITE_VERIFIED")).isEqualTo(1);
        assertThat(jdbc.queryForObject("select tool_executions from task.budget_scope where root_task_id = ?",
                Integer.class, pending.task().id())).isEqualTo(1);
    }

    @Test
    void p9FixedWorkflowUsesPinnedWriteToolThroughIndependentApprovalAndVerification() throws Exception {
        var sourceTaskId = UUID.randomUUID();
        var summary = "由用户确认的合成客户跟进摘要。";
        var input = objectMapper.writeValueAsString(java.util.Map.of(
                "customerId", "customer-001", "sourceTaskId", sourceTaskId.toString(),
                "sourceTaskVersion", "1", "summary", summary));
        var instance = workflows.createInstance(new CreateWorkflowInstanceCommand(alice(), WORKSPACE,
                P9_FOLLOWUP_WORKFLOW, "1.1.0", input,
                "p9-approved-followup-" + UUID.randomUUID(), "USER"));
        assertThat(instance.workflowVersion()).isEqualTo("1.1.0");

        // 首轮建立恢复点，第二轮按固定依赖创建写工具 Task。
        assertThat(workflowDispatcher.dispatchOne()).isTrue();
        assertThat(workflowDispatcher.dispatchOne()).isTrue();
        var childTaskId = jdbc.queryForObject("select child_task_id from workflow.step where instance_id = ? and step_id = 'write'",
                UUID.class, instance.id());
        assertThat(childTaskId).isNotNull();

        var initialWork = tasks.claimOne().orElseThrow();
        assertThat(initialWork.id()).isEqualTo(childTaskId);
        tasks.complete(initialWork, runtime.run(initialWork));
        var waitingTask = tasks.get(alice(), WORKSPACE, childTaskId);
        assertThat(waitingTask.status()).isEqualTo(TaskStatus.WAITING_APPROVAL);
        var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, childTaskId);
        var execution = executions.get(alice(), WORKSPACE, executionId);
        assertThat(execution.status()).isEqualTo("AWAITING_APPROVAL");
        var approval = approvals.get(reviewer(), WORKSPACE, execution.approvalId());
        assertThat(approval.state()).isEqualTo("PENDING");
        assertThat(FOLLOWUP_POSTS).hasValue(0);

        jdbc.update("update workflow.instance set next_poll_at = null where id = ?", instance.id());
        assertThat(workflowDispatcher.dispatchOne()).isTrue();
        approvals.decide(new ApprovalDecisionCommand(reviewer(), WORKSPACE, approval.id(),
                "APPROVED", approval.version()));
        tasks.resume(alice(), WORKSPACE, childTaskId, waitingTask.version(), "p9-followup-resume-" + UUID.randomUUID());
        var resumedWork = tasks.claimOne().orElseThrow();
        assertThat(resumedWork.id()).isEqualTo(childTaskId);
        tasks.complete(resumedWork, runtime.run(resumedWork));

        assertThat(tasks.get(alice(), WORKSPACE, childTaskId).status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(executions.get(alice(), WORKSPACE, executionId).status()).isEqualTo("SUCCEEDED");
        assertThat(FOLLOWUP_POSTS).hasValue(1);
        assertThat(FOLLOWUPS).hasSize(1);
        assertThat(FOLLOWUPS.values().iterator().next().summary()).isEqualTo(summary);
        assertThat(actionCount(childTaskId, "WRITE_VERIFIED")).isEqualTo(1);

        for (var i = 0; i < 8; i++) {
            var current = workflows.getInstance(alice(), WORKSPACE, instance.id());
            if ("SUCCEEDED".equals(current.status()) || "FAILED".equals(current.status())) break;
            jdbc.update("update workflow.instance set next_poll_at = null where id = ?", instance.id());
            if (!workflowDispatcher.dispatchOne()) break;
        }
        var completed = workflows.getInstance(alice(), WORKSPACE, instance.id());
        assertThat(completed.status()).withFailMessage("Workflow status=%s step=%s error=%s",
                completed.status(), completed.currentStepId(), completed.errorCode()).isEqualTo("SUCCEEDED");
        assertThat(completed.resultJson()).contains("operationId", "externalId", "CREATED");
    }

    @Test
    void changedTargetOrRevokedWritePermissionBlocksPostAndReadback() {
        var targetChanged = preparePendingWrite("p7-19-target-change");
        approve(targetChanged);
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = ?",
                "http://127.0.0.1:1", Ids.TENANT_A, WORKSPACE, CONNECTOR_PROVIDER);
        assertThat(executions.resume(alice(), WORKSPACE, targetChanged.execution().id()).status()).isEqualTo("DENIED");
        assertThat(FOLLOWUP_POSTS).hasValue(0);
        assertThat(FOLLOWUPS).isEmpty();

        enableOnlyTheLocalWriteFixture();
        var permissionRevoked = preparePendingWrite("p7-19-policy-revoke");
        approve(permissionRevoked);
        jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'crm:followup:create'",
                Ids.TENANT_A, WORKSPACE, Ids.ALICE);
        assertThat(executions.resume(alice(), WORKSPACE, permissionRevoked.execution().id()).status()).isEqualTo("DENIED");
        assertThat(FOLLOWUP_POSTS).hasValue(0);
        assertThat(FOLLOWUPS).isEmpty();

        enableOnlyTheLocalWriteFixture();
        var readback = preparePendingWrite("p7-19-readback-target-revoke");
        approve(readback);
        LOSE_NEXT_POST_RESPONSE.set(true);
        runApprovedTask(readback);
        assertThat(executions.get(alice(), WORKSPACE, readback.execution().id()).status()).isEqualTo("UNKNOWN");
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = ?",
                "http://127.0.0.1:1", Ids.TENANT_A, WORKSPACE, CONNECTOR_PROVIDER);
        assertThat(executions.verify(alice(), WORKSPACE, readback.execution().id()).status()).isEqualTo("UNKNOWN");
        assertThat(FOLLOWUP_GETS).hasValue(0);

        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = ?",
                fixtureUrl(), Ids.TENANT_A, WORKSPACE, CONNECTOR_PROVIDER);
        jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'crm:followup:create'",
                Ids.TENANT_A, WORKSPACE, Ids.ALICE);
        assertThat(executions.verify(alice(), WORKSPACE, readback.execution().id()).status()).isEqualTo("UNKNOWN");
        assertThat(FOLLOWUP_GETS).hasValue(0);
    }

    @Test
    void sameIdempotencyKeyWithDifferentArgumentsIsRejectedBeforeCrm() {
        var actor = alice();
        var capability = capabilities.requirePublished(actor, WORKSPACE, CAPABILITY, "1.4.0");
        var binding = new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash());
        var created = tasks.create(new CreateTaskCommand(actor, WORKSPACE, Ids.AGENT_RISK, "2.2.0", "创建一次已审批 CRM 跟进。",
                null, null, "p7-19-idempotency-task-" + UUID.randomUUID(), "trace-p7-19-idempotency", "USER", binding, "REST"));
        var work = tasks.claimOne().orElseThrow();
        var command = new ExecutionCommand(actor, WORKSPACE, created.id(), work.attempt(), Ids.AGENT_RISK, "2.2.0",
                "crm.followup.create", "1.1.0", "{\"customerId\":\"customer-001\",\"summary\":\"原始预览\"}",
                "p7-19-same-operation-key", "trace-p7-19-idempotency");
        var first = executions.submit(command);
        assertThat(executions.submit(command).id()).isEqualTo(first.id());
        assertThatThrownBy(() -> executions.submit(new ExecutionCommand(actor, WORKSPACE, created.id(), work.attempt(),
                Ids.AGENT_RISK, "2.2.0", "crm.followup.create", "1.1.0",
                "{\"customerId\":\"customer-001\",\"summary\":\"修改后的预览\"}",
                "p7-19-same-operation-key", "trace-p7-19-idempotency")))
                .isInstanceOfSatisfying(io.eaf.shared.EafException.class,
                        failure -> assertThat(failure.code()).isEqualTo("IDEMPOTENCY_CONFLICT"));

        assertThat(first.status()).isEqualTo("AWAITING_APPROVAL");
        assertThat(CUSTOMER_READS).hasValue(1);
        assertThat(FOLLOWUP_POSTS).hasValue(0);
        assertThat(FOLLOWUPS).isEmpty();
        tasks.cancel(actor, WORKSPACE, created.id(), tasks.get(actor, WORKSPACE, created.id()).version());
    }

    @Test
    void requesterCannotSelfApproveAndExpiredApprovalCannotSubmit() {
        var selfApproval = preparePendingWrite("p7-19-self-approval");
        assertThatThrownBy(() -> approvals.decide(new ApprovalDecisionCommand(alice(), WORKSPACE,
                selfApproval.approval().id(), "APPROVED", selfApproval.approval().version())))
                .isInstanceOf(io.eaf.shared.EafException.class);
        assertThat(approvals.get(reviewer(), WORKSPACE, selfApproval.approval().id()).state()).isEqualTo("PENDING");

        enableOnlyTheLocalWriteFixture();
        var expired = preparePendingWrite("p7-19-expired-approval");
        var expiresAt = Instant.now(clock).minusSeconds(1);
        jdbc.update("update approval.request set expires_at = ?, "
                        + "binding_json = jsonb_set(binding_json, '{expiresAt}', to_jsonb(?::text)) where id = ?",
                java.sql.Timestamp.from(expiresAt), expiresAt.toString(), expired.approval().id());
        assertThat(approvals.decide(new ApprovalDecisionCommand(reviewer(), WORKSPACE, expired.approval().id(),
                "APPROVED", expired.approval().version())).state()).isEqualTo("EXPIRED");
        assertThat(executions.resume(alice(), WORKSPACE, expired.execution().id()).status()).isEqualTo("DENIED");
        assertThat(FOLLOWUP_POSTS).hasValue(0);
        assertThat(FOLLOWUPS).isEmpty();
    }

    @Test
    void mismatchedReadbackNeverBecomesSuccess() {
        var pending = preparePendingWrite("p7-19-mismatched-readback");
        approve(pending);
        LOSE_NEXT_POST_RESPONSE.set(true);
        runApprovedTask(pending);
        MISMATCH_READBACK.set(true);

        var failed = executions.verify(alice(), WORKSPACE, pending.execution().id());
        assertThat(failed.status()).isEqualTo("VERIFICATION_FAILED");
        assertThat(failed.errorCode()).isEqualTo("CRM_RESULT_MISMATCH");
        assertThat(FOLLOWUP_POSTS).hasValue(1);
        assertThat(FOLLOWUP_GETS).hasValue(1);
        assertThat(FOLLOWUPS).hasSize(1);
        assertThat(actionCount(pending.task().id(), "WRITE_VERIFIED")).isZero();
        assertThat(jdbc.queryForObject("select tool_executions from task.budget_scope where root_task_id = ?",
                Integer.class, pending.task().id())).isZero();
    }

    @Test
    void killedWriterRecoversTheCommittedOperationInANewJvm() throws Exception {
        var pending = preparePendingWrite("p7-19-process-crash");
        approve(pending);
        claimResumedTask(pending);
        postCommitted = new CountDownLatch(1);
        releasePostResponse = new CountDownLatch(1);
        HOLD_NEXT_POST_RESPONSE.set(true);
        WorkerProcess writer = null;
        try {
            writer = startWorker("resume", pending.execution().id());
            var committed = postCommitted.await(45, TimeUnit.SECONDS);
            if (!committed) writer.process().destroyForcibly();
            assertThat(committed).as("子 JVM 未到达 CRM 提交点：%s", writer.output().get(15, TimeUnit.SECONDS)).isTrue();
            writer.process().destroyForcibly();
            assertThat(writer.process().waitFor(15, TimeUnit.SECONDS)).isTrue();
        } finally {
            releasePostResponse.countDown();
            if (writer != null && writer.process().isAlive()) {
                writer.process().destroyForcibly();
                writer.process().waitFor(15, TimeUnit.SECONDS);
            }
        }

        assertThat(FOLLOWUP_POSTS).hasValue(1);
        assertThat(FOLLOWUPS).hasSize(1);
        jdbc.update("update execution.execution set lease_until = now() - interval '1 second' where id = ?", pending.execution().id());
        executions.recoverOnStartup();
        assertThat(executions.get(alice(), WORKSPACE, pending.execution().id()).status()).isEqualTo("UNKNOWN");

        var verifier = startWorker("verify", pending.execution().id());
        assertThat(verifier.process().waitFor(60, TimeUnit.SECONDS)).isTrue();
        assertThat(verifier.output().get(5, TimeUnit.SECONDS)).contains("P7_CRM_EXECUTION_RESULT=SUCCEEDED");
        assertThat(executions.get(alice(), WORKSPACE, pending.execution().id()).status()).isEqualTo("SUCCEEDED");
        assertThat(FOLLOWUP_POSTS).hasValue(1);
        assertThat(FOLLOWUP_GETS).hasValue(1);
        assertThat(FOLLOWUPS).hasSize(1);
        assertThat(actionCount(pending.task().id(), "WRITE_VERIFIED")).isEqualTo(1);
        assertThat(jdbc.queryForObject("select tool_executions from task.budget_scope where root_task_id = ?",
                Integer.class, pending.task().id())).isEqualTo(1);
    }

    private PendingWrite preparePendingWrite(String key) {
        // 一次 helper 调用应新增且只新增一次 CRM 只读回读，允许同一测试准备多个 Operation。
        var customerReadsBefore = CUSTOMER_READS.get();
        var actor = alice();
        var capability = capabilities.requirePublished(actor, WORKSPACE, CAPABILITY, "1.4.0");
        var binding = new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash());
        var created = tasks.create(new CreateTaskCommand(actor, WORKSPACE, Ids.AGENT_RISK, "2.2.0",
                "请为 customerId=customer-001 创建一条跟进，并等待指定审批。", null, null,
                key + "-task-" + UUID.randomUUID(), "trace-" + key, "USER", binding, "REST"));
        var gateway = newWriteGateway();
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(created.id());
        var first = runtimeUsing(gateway).run(work);
        tasks.complete(work, first);
        assertThat(first.status()).as("status=%s code=%s detail=%s", first.status(), first.errorCode(), first.errorDetail())
                .isEqualTo(TaskStatus.WAITING_APPROVAL);
        var waiting = tasks.get(actor, WORKSPACE, created.id());
        var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, created.id());
        var execution = executions.get(actor, WORKSPACE, executionId);
        var bob = reviewer();
        var approval = approvals.get(bob, WORKSPACE, execution.approvalId());
        assertThat(execution.status()).isEqualTo("AWAITING_APPROVAL");
        assertThat(CUSTOMER_READS).hasValue(customerReadsBefore + 1);
        return new PendingWrite(created, waiting, execution, approval, gateway);
    }

    private void approve(PendingWrite pending) {
        approvals.decide(new ApprovalDecisionCommand(reviewer(), WORKSPACE, pending.approval().id(),
                "APPROVED", pending.approval().version()));
    }

    private void runApprovedTask(PendingWrite pending) {
        var waiting = tasks.get(alice(), WORKSPACE, pending.task().id());
        tasks.resume(alice(), WORKSPACE, pending.task().id(), waiting.version(), "p7-19-resume-" + UUID.randomUUID());
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(pending.task().id());
        var outcome = runtimeUsing(pending.gateway()).run(work);
        tasks.complete(work, outcome);
    }

    private void claimResumedTask(PendingWrite pending) {
        // 子 JVM 只恢复 Execution；先由父测试恢复 Task Worker，使 Task 租约检查符合真实运行顺序。
        var waiting = tasks.get(alice(), WORKSPACE, pending.task().id());
        tasks.resume(alice(), WORKSPACE, pending.task().id(), waiting.version(), "p7-19-child-resume-" + UUID.randomUUID());
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(pending.task().id());
    }

    private JdbcAgentRuntime runtimeUsing(ModelGateway gateway) {
        return new JdbcAgentRuntime(jdbc, agents, prompts, gateway, audit, usage, traces, tasks,
                toolCatalog, executions, contextService, objectMapper, clock, evaluationContexts, capabilities);
    }

    private ModelGateway newWriteGateway() {
        var calls = new AtomicInteger();
        return new ModelGateway() {
            @Override public ModelResult call(io.eaf.model.api.ModelRequest request) {
                if (calls.incrementAndGet() == 1)
                    return new ModelResult("local-fixture", "fixture-model", null, 15, 4, "KNOWN",
                            List.of(new ModelToolCall("p7-19-followup-" + UUID.randomUUID(), "crm.followup.create",
                                    "{\"customerId\":\"customer-001\",\"summary\":\"安排一次客户跟进。\"}")), "TOOL_CALLS");
                return new ModelResult("local-fixture", "fixture-model",
                        "{\"riskLevel\":\"LOW\",\"summary\":\"客户跟进已按审批结果处理。\",\"reasons\":[\"Execution 核验 CRM 返回记录\"],\"uncertainties\":[]}",
                        20, 6, "KNOWN");
            }
            @Override public int callCount() { return calls.get(); }
        };
    }

    private ActorContext alice() {
        return new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of("task:create", "task:read", "task:resume",
                "task:cancel", "approval:read", "approval:decide", "execution:read", "execution:verify", "tool:read", "crm:customer:read",
                "crm:followup:create", "agent:read", "capability:read", "skill:read", "prompt:read", "context:read",
                "knowledge:read", "memory:read"));
    }

    private ActorContext reviewer() {
        return new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN,
                Set.of("approval:read", "approval:decide", "execution:read"));
    }

    private void grantWorkspace(String action) {
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, ?, 'ACTIVE') "
                        + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, WORKSPACE, Ids.ALICE, action);
    }

    private int actionCount(UUID taskId, String action) {
        return jdbc.queryForObject("select count(*) from audit.audit_event where task_id = ? and action = ?", Integer.class, taskId, action);
    }

    private String fixtureUrl() {
        return "http://127.0.0.1:" + crm.getAddress().getPort();
    }

    private static void readCustomer(HttpExchange exchange) throws IOException {
        CUSTOMER_READS.incrementAndGet();
        if (!authorized(exchange)) { respond(exchange, 401, ""); return; }
        var customerId = exchange.getRequestURI().getPath().substring("/customers/".length());
        if (!"GET".equals(exchange.getRequestMethod()) || !"customer-001".equals(customerId)) {
            respond(exchange, 404, "");
            return;
        }
        var body = "{\"contractVersion\":\"EAF-CRM-READ-V1\",\"customerId\":\"customer-001\",\"renewalStatus\":\"ACTIVE\","
                + "\"lastContactDate\":\"2026-09-20\",\"complaintSummary\":\"none\",\"sourceId\":\"p7-write-fixture\","
                + "\"externalVersion\":\"crm-v-write-1\"}";
        respond(exchange, 200, body);
    }

    private static void createFollowup(HttpExchange exchange) throws IOException {
        FOLLOWUP_POSTS.incrementAndGet();
        if (!authorized(exchange)) { respond(exchange, 401, ""); return; }
        if (!"POST".equals(exchange.getRequestMethod())) { respond(exchange, 405, ""); return; }
        JsonNode request;
        try { request = JSON.readTree(exchange.getRequestBody().readAllBytes()); }
        catch (Exception invalid) { respond(exchange, 400, ""); return; }
        var operationId = request.path("operationId").asText("");
        var customerId = request.path("customerId").asText("");
        var summary = request.path("summary").asText("");
        var ownerId = request.path("ownerId").asText("");
        if (!"EAF-CRM-WRITE-V1".equals(request.path("contractVersion").asText())
                || operationId.isBlank() || !operationId.equals(exchange.getRequestHeaders().getFirst("Idempotency-Key"))
                || customerId.isBlank() || summary.isBlank() || ownerId.isBlank()) {
            respond(exchange, 400, "");
            return;
        }
        var candidate = new FixtureRecord(operationId, "fu-" + operationId, customerId, summary, ownerId,
                "CREATED", "2026-10-01T00:00:00Z");
        var record = FOLLOWUPS.putIfAbsent(operationId, candidate);
        if (record != null && !record.sameRequest(candidate)) {
            respond(exchange, 409, "");
            return;
        }
        record = record == null ? candidate : record;

        // 夹具先持久化唯一记录，再模拟响应丢失或写入进程被强制终止。
        if (HOLD_NEXT_POST_RESPONSE.compareAndSet(true, false)) {
            postCommitted.countDown();
            try { releasePostResponse.await(45, TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            exchange.close();
            return;
        }
        if (LOSE_NEXT_POST_RESPONSE.compareAndSet(true, false)) {
            exchange.close();
            return;
        }
        respond(exchange, 201, writeRecord(record));
    }

    private static void findFollowup(HttpExchange exchange) throws IOException {
        FOLLOWUP_GETS.incrementAndGet();
        if (!authorized(exchange)) { respond(exchange, 401, ""); return; }
        var operationId = exchange.getRequestURI().getPath().substring("/followups/by-operation/".length());
        var record = FOLLOWUPS.get(operationId);
        if (record == null) { respond(exchange, 404, ""); return; }
        if (MISMATCH_READBACK.get()) record = new FixtureRecord(record.operationId(), record.externalId(), record.customerId(),
                record.summary() + "-mismatch", record.ownerId(), record.status(), record.acceptedAt());
        respond(exchange, 200, writeRecord(record));
    }

    private static String writeRecord(FixtureRecord record) throws IOException {
        var node = JSON.createObjectNode();
        node.put("contractVersion", "EAF-CRM-WRITE-V1");
        node.put("operationId", record.operationId());
        node.put("externalId", record.externalId());
        node.put("customerId", record.customerId());
        node.put("summary", record.summary());
        node.put("ownerId", record.ownerId());
        node.put("status", record.status());
        node.put("acceptedAt", record.acceptedAt());
        return JSON.writeValueAsString(node);
    }

    private static boolean authorized(HttpExchange exchange) {
        return ("GET".equals(exchange.getRequestMethod()) || "POST".equals(exchange.getRequestMethod()))
                && ("Bearer " + CREDENTIAL).equals(exchange.getRequestHeaders().getFirst("Authorization"));
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        var bytes = body.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0) {
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
            return;
        }
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }

    private static WorkerProcess startWorker(String action, UUID executionId) throws IOException {
        var classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var executable = System.getProperty("os.name", "").toLowerCase().contains("win") ? "java.exe" : "java";
        var java = Path.of(System.getProperty("java.home"), "bin", executable).toString();
        var process = new ProcessBuilder(java, "-cp", classpath, P7EnterpriseCrmWriteRunner.class.getName(),
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--server.port=0", "--eaf.security.mode=local", "--eaf.task.dispatcher-enabled=false",
                "--eaf.execution.outbox-publisher-enabled=false", "--eaf.knowledge.outbox-publisher-enabled=false",
                "--eaf.memory.outbox-publisher-enabled=false", "--eaf.workflow.dispatcher-enabled=false",
                "--eaf.execution.remote-poller-enabled=false", "--eaf.task.lease-duration=PT10M",
                "--eaf.credentials.p7-crm-write.token=" + CREDENTIAL,
                "--p7.crm.workspace-id=" + WORKSPACE, "--p7.crm.execution-id=" + executionId, "--p7.crm.action=" + action)
                .redirectErrorStream(true).start();
        return new WorkerProcess(process, CompletableFuture.supplyAsync(() -> {
            try { return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8); }
            catch (IOException ignored) { return ""; }
        }));
    }

    private record PendingWrite(TaskSnapshot task, TaskSnapshot waitingTask, ExecutionSnapshot execution,
                                ApprovalSnapshot approval, ModelGateway gateway) { }
    private record WorkerProcess(Process process, CompletableFuture<String> output) { }

    private record FixtureRecord(String operationId, String externalId, String customerId, String summary,
                                 String ownerId, String status, String acceptedAt) {
        private boolean sameRequest(FixtureRecord other) {
            return operationId.equals(other.operationId) && customerId.equals(other.customerId)
                    && summary.equals(other.summary) && ownerId.equals(other.ownerId);
        }
    }
}
