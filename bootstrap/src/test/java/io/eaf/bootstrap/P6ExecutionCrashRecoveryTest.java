package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.eaf.approval.api.ApprovalDecisionCommand;
import io.eaf.approval.api.ApprovalService;
import io.eaf.execution.api.ExecutionService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Hashing;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.CreateToolExecutionCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
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

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.knowledge.outbox-publisher-enabled=false", "eaf.memory.outbox-publisher-enabled=false",
        "eaf.model.scenario=WRITE", "eaf.task.lease-duration=PT10M"})
// 使用 PostgreSQL 持久化测试 CRM 状态，并强制终止独立 EAF JVM 核验真实进程恢复。
class P6ExecutionCrashRecoveryTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID WORKSPACE = UUID.fromString("10000000-0000-4000-8000-000000000001");
    private static final UUID AGENT = UUID.fromString("20000000-0000-4000-8000-000000000001");
    private static final long AUDIT_GATE = 77334455L;
    private static final long REMOTE_TASK_GATE = 77334456L;
    private static final String A2A_TOKEN = "p6-crash-test-peer-token";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final AtomicReference<CrmPause> CRM_PAUSE = new AtomicReference<>(CrmPause.NONE);
    private static final AtomicInteger peerSends = new AtomicInteger();
    private static volatile CountDownLatch postEntered = new CountDownLatch(0);
    private static volatile CountDownLatch releasePost = new CountDownLatch(0);
    private static volatile CountDownLatch peerSendEntered = new CountDownLatch(0);
    private static volatile CountDownLatch releasePeerSend = new CountDownLatch(0);
    private static volatile boolean holdPeerSend;
    private UUID currentRootTaskId;

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));
    private static HttpServer crm;
    private static HttpServer a2aPeer;
    private static ExecutorService a2aExecutor;

    @BeforeAll
    static void startCrm() throws Exception {
        try (var connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             var statement = connection.createStatement()) {
            statement.execute("create schema if not exists testcrm");
            statement.execute("create table if not exists testcrm.followup (operation_id uuid primary key, request_hash varchar(64) not null, external_id varchar(200) not null unique, customer_id varchar(160) not null, summary varchar(500) not null, owner_id varchar(200) not null, status varchar(30) not null, accepted_at timestamptz not null)");
            statement.execute("create table if not exists testcrm.request_log (id bigserial primary key, method varchar(8) not null, operation_id uuid not null, created_at timestamptz not null default now())");
        }
        crm = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        crm.createContext("/customers/customer-001", exchange -> {
            if (!authorized(exchange)) { respond(exchange, 401, ""); return; }
            respond(exchange, 200, "{\"customerId\":\"customer-001\",\"renewalStatus\":\"ACTIVE\",\"lastContactDate\":\"2026-09-20\",\"complaintSummary\":\"none\",\"sourceId\":\"p6-persistent-crm\"}");
        });
        crm.createContext("/followups", P6ExecutionCrashRecoveryTest::createFollowup);
        crm.createContext("/followups/by-operation/", P6ExecutionCrashRecoveryTest::findFollowup);
        crm.start();
        a2aPeer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        a2aExecutor = Executors.newCachedThreadPool();
        a2aPeer.setExecutor(a2aExecutor);
        a2aPeer.createContext("/a2a", P6ExecutionCrashRecoveryTest::handleA2aPeer);
        a2aPeer.start();
    }

    @AfterAll
    static void stopCrm() {
        if (crm != null) crm.stop(0);
        if (a2aPeer != null) a2aPeer.stop(0);
        if (a2aExecutor != null) a2aExecutor.shutdownNow();
    }

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("eaf.credentials.test-crm.token", () -> "p6-crash-test-token");
        registry.add("eaf.credentials.a2a-peer-review.token", () -> A2A_TOKEN);
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired DataSource dataSource;
    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired ApprovalService approvals;
    @Autowired ExecutionService executions;

    @BeforeEach
    void resetCrm() {
        jdbc.execute("drop trigger if exists p6_test_gate_verified on audit.audit_event");
        jdbc.execute("drop function if exists audit.p6_test_gate_verified()");
        jdbc.update("truncate testcrm.request_log, testcrm.followup restart identity");
        CRM_PAUSE.set(CrmPause.NONE);
        postEntered = new CountDownLatch(1);
        releasePost = new CountDownLatch(1);
        pointCrm();
        peerSends.set(0);
        peerSendEntered = new CountDownLatch(1);
        releasePeerSend = new CountDownLatch(1);
        holdPeerSend = false;
        pointA2aPeer();
    }

    @AfterEach
    void cleanPendingTasks() {
        // 每个 crash case 使用独立根 Task，清除未完成子任务，避免后续 claimOne 取到前一用例的队列项。
        if (currentRootTaskId == null) return;
        jdbc.update("update task.task set external_effect_operation_id = null, external_effect_status = 'FAILED', status = 'CANCELLED', active_reserved_ms = 0, active_budget_reservation_key = null, lease_owner_id = null, lease_until = null, row_version = row_version + 1 where root_task_id = ? and status in ('QUEUED','RUNNING','WAITING_APPROVAL','WAITING_REMOTE','CANCELLING_REMOTE','WAITING_VERIFICATION')",
                currentRootTaskId);
        jdbc.update("update task.budget_scope set active_reserved_ms = 0 where root_task_id = ?", currentRootTaskId);
        currentRootTaskId = null;
    }

    @Test
    void crashBeforeCrmCommitLeavesExecutionUnknown() throws Exception {
        var target = prepareApprovedWrite();
        CRM_PAUSE.set(CrmPause.BEFORE_COMMIT);
        var child = startProcess("resume", target.executionId());
        try {
            assertThat(postEntered.await(45, TimeUnit.SECONDS)).isTrue();
            // 父 JVM 作为第二 Worker 直接争用已提交的 EXECUTING 行，避免启动时间掩盖并发 CAS。
            var competitor = executions.resume(owner(), WORKSPACE, target.executionId());
            assertThat(competitor.status()).isEqualTo("EXECUTING");
            assertThat(count("POST")).isEqualTo(1);
            kill(child.process());
        } finally {
            releasePost.countDown();
            killIfRunning(child.process());
        }

        assertThat(count("POST")).isEqualTo(1);
        assertThat(crmRecords(target.operationId())).isZero();
        reapExpired(target);
        var restarted = runProcess("verify", target.executionId());
        assertThat(restarted.exitCode()).isZero();
        assertThat(restarted.output()).contains("P6_EXECUTION_RESULT=UNKNOWN");
        assertThat(executions.get(owner(), WORKSPACE, target.executionId()).status()).isEqualTo("UNKNOWN");
        assertThat(count("POST")).isEqualTo(1);
        assertThat(count("GET")).isEqualTo(1);
        assertThat(crmRecords(target.operationId())).isZero();
        assertThat(verifiedAuditCount(target.executionId())).isZero();
    }

    @Test
    void peerAcceptedSendThenLocalJvmCrashRecoversAsUnknownWithoutResending() throws Exception {
        // 独立 JVM 在 peer 已受理 SendMessage、但响应尚未返回时被终止，恢复不得猜测或重发。
        var taskId = prepareRemoteReviewTask();
        holdPeerSend = true;
        var child = startTaskProcess(taskId);
        try {
            assertThat(peerSendEntered.await(45, TimeUnit.SECONDS)).isTrue();
            assertThat(peerSends).hasValue(1);
            kill(child.process());
        } finally {
            releasePeerSend.countDown();
            holdPeerSend = false;
            killIfRunning(child.process());
        }

        var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, taskId);
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where execution_id = ?", String.class, executionId)).isEqualTo("SEND_PENDING");
        jdbc.update("update execution.execution set lease_until = now() - interval '1 second' where id = ?", executionId);
        jdbc.update("update task.task set lease_until = now() - interval '1 second' where id = ? and status = 'RUNNING'", taskId);
        var expired = tasks.recoverOnStartup();
        runtime.recoverExpired(expired);

        assertThat(expired).hasSize(1);
        assertThat(executions.get(owner(), WORKSPACE, executionId).status()).isEqualTo("UNKNOWN");
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where execution_id = ?", String.class, executionId)).isEqualTo("SEND_UNKNOWN");
        assertThat(tasks.get(owner(), WORKSPACE, taskId).externalEffectPending()).isTrue();
        var restarted = runProcess("resume", executionId);
        assertThat(restarted.exitCode()).isZero();
        assertThat(restarted.output()).contains("P6_EXECUTION_RESULT=UNKNOWN");
        assertThat(peerSends).hasValue(1);
    }

    @Test
    void savedRemoteTaskIdSurvivesCrashBeforeLocalTaskWaitSettlement() throws Exception {
        // peer 已返回并持久化 remoteTaskId 后，在 Task 切换等待态前终止 Worker，恢复仍须继续同一远端 Task。
        var taskId = prepareRemoteReviewTask();
        var gate = holdRemoteTaskSettlement(taskId);
        StartedProcess child = null;
        try {
            child = startTaskProcess(taskId);
            var lockDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (!waitingOnLock(REMOTE_TASK_GATE) && child.process().isAlive() && System.nanoTime() < lockDeadline)
                Thread.sleep(50);
            var childState = child.process().isAlive() ? "child JVM still running" : child.output().get(5, TimeUnit.SECONDS);
            assertThat(waitingOnLock(REMOTE_TASK_GATE)).withFailMessage("远端 Task settlement gate 未触发：%s", childState).isTrue();
            var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, taskId);
            assertThat(jdbc.queryForObject("select status from execution.execution where id = ?", String.class, executionId)).isEqualTo("AWAITING_REMOTE");
            assertThat(jdbc.queryForObject("select remote_task_id from execution.remote_a2a_operation where execution_id = ?", String.class, executionId)).isEqualTo("crash-peer-task");
            assertThat(tasks.get(owner(), WORKSPACE, taskId).status()).isEqualTo(TaskStatus.RUNNING);
            kill(child.process());
            child = null;
        } finally {
            if (child != null) killIfRunning(child.process());
            releaseRemoteTaskSettlement(gate);
        }

        jdbc.update("update task.task set lease_until = now() - interval '1 second' where id = ? and status = 'RUNNING'", taskId);
        var expired = tasks.recoverOnStartup();
        assertThat(expired).hasSize(1);
        assertThat(tasks.get(owner(), WORKSPACE, taskId).status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, taskId);
        assertThat(executions.get(owner(), WORKSPACE, executionId).status()).isEqualTo("AWAITING_REMOTE");
        assertThat(jdbc.queryForObject("select remote_task_id from execution.remote_a2a_operation where execution_id = ?", String.class, executionId)).isEqualTo("crash-peer-task");
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where task_id = ? and action = 'TASK_REMOTE_WAIT_RECOVERED'", Integer.class, taskId)).isEqualTo(1);
    }

    @Test
    void pendingApprovalSurvivesEafRestartWithoutCreatingCrmWrite() throws Exception {
        var pending = preparePendingWrite();
        var first = runProcess("resume", pending.executionId());
        assertThat(first.exitCode()).isZero();
        assertThat(first.output()).contains("P6_EXECUTION_RESULT=AWAITING_APPROVAL");
        var restarted = runProcess("resume", pending.executionId());
        assertThat(restarted.exitCode()).isZero();
        assertThat(restarted.output()).contains("P6_EXECUTION_RESULT=AWAITING_APPROVAL");
        assertThat(count("POST")).isZero();
        assertThat(tasks.get(owner(), WORKSPACE, pending.taskId()).status()).isEqualTo(TaskStatus.WAITING_APPROVAL);

        var approved = approveAndClaim(pending);
        var result = runProcess("resume", approved.executionId());
        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("P6_EXECUTION_RESULT=SUCCEEDED");
        tasks.complete(approved.workItem(), TaskRunner.RunOutcome.success("{\"verified\":true}", false, null, null));
        assertThat(tasks.get(owner(), WORKSPACE, approved.taskId()).status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(count("POST")).isEqualTo(1);
    }

    @Test
    void crmFiveHundredAfterCommitIsVerifiedWithoutReposting() throws Exception {
        var target = prepareApprovedWrite();
        CRM_PAUSE.set(CrmPause.AFTER_COMMIT_5XX);
        var write = runProcess("resume", target.executionId());
        assertThat(write.exitCode()).isZero();
        assertThat(write.output()).contains("P6_EXECUTION_RESULT=UNKNOWN");
        assertThat(executions.get(owner(), WORKSPACE, target.executionId()).status()).isEqualTo("UNKNOWN");
        assertThat(crmRecords(target.operationId())).isEqualTo(1);
        // 核验端不可达时保持未决且不记成功审计/执行预算，恢复后仍按同一 operationId 查询。
        CRM_PAUSE.set(CrmPause.VERIFY_5XX);
        assertThat(executions.verify(owner(), WORKSPACE, target.executionId()).status()).isEqualTo("UNKNOWN");
        assertThat(verifiedAuditCount(target.executionId())).isZero();
        assertThat(jdbc.queryForObject("select tool_executions from task.budget_scope where root_task_id = ?", Integer.class, target.taskId())).isZero();
        CRM_PAUSE.set(CrmPause.NONE);
        var verify = runProcess("verify", target.executionId());
        assertThat(verify.exitCode()).isZero();
        assertThat(verify.output()).contains("P6_EXECUTION_RESULT=SUCCEEDED");
        tasks.complete(target.workItem(), TaskRunner.RunOutcome.success("{\"verified\":true}", false, null, null));
        assertThat(count("POST")).isEqualTo(1);
        assertThat(count("GET")).isEqualTo(2);
        assertThat(crmRecords(target.operationId())).isEqualTo(1);
        assertThat(verifiedAuditCount(target.executionId())).isEqualTo(1);
    }

    @Test
    void crmTimeoutAfterCommitIsVerifiedWithoutReposting() throws Exception {
        var target = prepareApprovedWrite();
        CRM_PAUSE.set(CrmPause.AFTER_COMMIT);
        ProcessResult write;
        try { write = runProcess("resume", target.executionId()); }
        finally { releasePost.countDown(); }
        assertThat(write.exitCode()).isZero();
        assertThat(write.output()).contains("P6_EXECUTION_RESULT=UNKNOWN");
        assertThat(executions.get(owner(), WORKSPACE, target.executionId()).status()).isEqualTo("UNKNOWN");
        var verify = runProcess("verify", target.executionId());
        assertThat(verify.exitCode()).isZero();
        assertThat(verify.output()).contains("P6_EXECUTION_RESULT=SUCCEEDED");
        tasks.complete(target.workItem(), TaskRunner.RunOutcome.success("{\"verified\":true}", false, null, null));
        assertThat(count("POST")).isEqualTo(1);
        assertThat(count("GET")).isEqualTo(1);
        assertThat(crmRecords(target.operationId())).isEqualTo(1);
    }

    @Test
    void crmVerificationMismatchCannotBecomeSuccess() throws Exception {
        var target = prepareApprovedWrite();
        CRM_PAUSE.set(CrmPause.MISMATCH);
        var result = runProcess("resume", target.executionId());
        assertThat(result.exitCode()).isZero();
        assertThat(result.output()).contains("P6_EXECUTION_RESULT=VERIFICATION_FAILED");
        var execution = executions.get(owner(), WORKSPACE, target.executionId());
        assertThat(execution.status()).isEqualTo("VERIFICATION_FAILED");
        assertThat(execution.errorCode()).isEqualTo("CRM_RESULT_MISMATCH");
        assertThat(execution.resultJson()).contains("-mismatch");
        assertThat(execution.verificationJson()).contains("-mismatch");
        tasks.complete(target.workItem(), TaskRunner.RunOutcome.failed("CRM_RESULT_MISMATCH", "外部记录与预览不一致。"));
        assertThat(count("POST")).isEqualTo(1);
        assertThat(count("GET")).isZero();
        assertThat(crmRecords(target.operationId())).isEqualTo(1);
        assertThat(verifiedAuditCount(target.executionId())).isZero();
    }

    @Test
    void crashAfterCrmCommitAndBeforeAuditRecoversSameOperationExactlyOnce() throws Exception {
        var target = prepareApprovedWrite();
        CRM_PAUSE.set(CrmPause.AFTER_COMMIT);
        var child = startProcess("resume", target.executionId());
        try {
            assertThat(postEntered.await(45, TimeUnit.SECONDS)).isTrue();
            kill(child.process());
        } finally {
            releasePost.countDown();
            killIfRunning(child.process());
        }
        assertThat(count("POST")).isEqualTo(1);
        assertThat(crmRecords(target.operationId())).isEqualTo(1);
        reapExpired(target);
        assertThat(executions.get(owner(), WORKSPACE, target.executionId()).status()).isEqualTo("UNKNOWN");

        var gate = holdVerifiedAudit();
        StartedProcess verify = null;
        try {
            verify = startProcess("verify", target.executionId());
            assertThat(await(() -> count("GET") == 1 && waitingOnAuditGate(), Duration.ofSeconds(45))).isTrue();
            kill(verify.process());
        } finally {
            if (verify != null) killIfRunning(verify.process());
            releaseAuditGate(gate);
        }
        // 模拟进程租约到期后由恢复器回收 VERIFYING；未到期租约不能被并发恢复器抢占。
        jdbc.update("update execution.execution set lease_until = now() - interval '1 second' where id = ? and status = 'VERIFYING'", target.executionId());
        executions.recoverOnStartup();
        assertThat(executions.get(owner(), WORKSPACE, target.executionId()).status()).isEqualTo("UNKNOWN");
        assertThat(verifiedAuditCount(target.executionId())).isZero();
        assertThat(jdbc.queryForObject("select tool_executions from task.budget_scope where root_task_id = ?", Integer.class, target.taskId())).isZero();

        // 恢复耗时从再次启动 EAF 到同一 operationId 核验收敛为 SUCCEEDED，包含子 JVM 启动时间。
        var recoveryStartedAt = System.nanoTime();
        var restarted = runProcess("verify", target.executionId());
        var recoveryMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - recoveryStartedAt);
        // 审计以 Execution 的稳定 factKey 去重；同时记录本次持久化 Audit UUID 供恢复证据追溯。
        var auditEventId = verifiedAuditId(target.executionId());
        System.out.println("P6_RECOVERY_RESULT=SUCCEEDED; taskId=" + target.taskId() + "; executionId=" + target.executionId()
                + "; operationId=" + target.operationId() + "; auditEventId=" + auditEventId + "; elapsedMs=" + recoveryMillis);
        assertThat(auditEventId).isNotNull();
        assertThat(restarted.exitCode()).isZero();
        assertThat(restarted.output()).contains("P6_EXECUTION_RESULT=SUCCEEDED");
        var done = executions.get(owner(), WORKSPACE, target.executionId());
        assertThat(done.status()).isEqualTo("SUCCEEDED");
        assertThat(done.operationId()).isEqualTo(target.operationId());
        assertThat(tasks.get(owner(), WORKSPACE, target.taskId()).status()).isEqualTo(TaskStatus.FAILED);
        assertThat(count("POST")).isEqualTo(1);
        assertThat(count("GET")).isEqualTo(2);
        assertThat(crmRecords(target.operationId())).isEqualTo(1);
        assertThat(verifiedAuditCount(target.executionId())).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from execution.outbox where execution_id = ? and event_type = 'eaf.execution.succeeded.v1'", Integer.class, target.executionId())).isEqualTo(1);
        var budget = jdbc.queryForMap("select tool_calls, tool_executions from task.budget_scope where root_task_id = ?", target.taskId());
        assertThat(budget.get("tool_calls")).isEqualTo(1);
        assertThat(budget.get("tool_executions")).isEqualTo(1);
    }

    private Target preparePendingWrite() {
        var actor = owner();
        var created = tasks.create(new CreateTaskCommand(actor, WORKSPACE, AGENT, "2.0.0",
                "请为 customerId=customer-001 创建跟进。", null, null,
                "p6-crash-" + UUID.randomUUID(), "trace-p6-crash-" + UUID.randomUUID(), "USER"));
        currentRootTaskId = created.id();
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(created.id());
        var first = runtime.run(work);
        tasks.complete(work, first);
        assertThat(first.status()).isEqualTo(TaskStatus.WAITING_APPROVAL);
        var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, created.id());
        var execution = executions.get(actor, WORKSPACE, executionId);
        assertThat(execution.status()).isEqualTo("AWAITING_APPROVAL");
        return new Target(created.id(), executionId, execution.operationId(), null);
    }

    private UUID prepareRemoteReviewTask() {
        var root = tasks.create(new CreateTaskCommand(owner(), WORKSPACE, AGENT, "2.0.0", "remote recovery root",
                null, null, "p6-a2a-root-" + UUID.randomUUID(), "trace-p6-a2a-root", "USER"));
        currentRootTaskId = root.id();
        var rootWork = tasks.claimOne().orElseThrow();
        assertThat(rootWork.id()).isEqualTo(root.id());
        var task = tasks.createToolExecution(new CreateToolExecutionCommand(owner(), WORKSPACE, root.id(),
                "remote-recovery", "agent.risk.review", "1.0.0",
                "{\"customerId\":\"customer-001\",\"riskSummary\":\"restart recovery\"}", "trace-p6-a2a-root"));
        tasks.complete(rootWork, TaskRunner.RunOutcome.success("{}", false, null, null));
        return task.id();
    }

    private Target prepareApprovedWrite() { return approveAndClaim(preparePendingWrite()); }

    private Target approveAndClaim(Target target) {
        var actor = owner();
        var execution = executions.get(actor, WORKSPACE, target.executionId());
        var approval = approvals.get(approver(), WORKSPACE, execution.approvalId());
        approvals.decide(new ApprovalDecisionCommand(approver(), WORKSPACE, approval.id(), "APPROVED", approval.version()));
        var waiting = tasks.get(actor, WORKSPACE, target.taskId());
        tasks.resume(actor, WORKSPACE, target.taskId(), waiting.version(), "p6-resume-" + UUID.randomUUID());
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(target.taskId());
        return new Target(target.taskId(), target.executionId(), target.operationId(), work);
    }

    private void reapExpired(Target target) {
        jdbc.update("update execution.execution set lease_until = now() - interval '1 second' where id = ?", target.executionId());
        jdbc.update("update task.task set lease_until = now() - interval '1 second' where id = ? and status = 'RUNNING'", target.taskId());
        var expired = tasks.recoverOnStartup();
        runtime.recoverExpired(expired);
        assertThat(expired).hasSize(1);
        assertThat(tasks.get(owner(), WORKSPACE, target.taskId()).errorCode()).isEqualTo("WORKER_LEASE_EXPIRED");
    }

    private void pointCrm() {
        // 正向 CRM 写入用例显式授予 HUMAN Workspace 动作，客户资源 grant 仍单独检查。
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = 'TEST_CRM'",
                "http://127.0.0.1:" + crm.getAddress().getPort(), Ids.TENANT_A, WORKSPACE);
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'crm:followup:create', 'ACTIVE') "
                        + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, WORKSPACE, Ids.ALICE);
    }

    private void pointA2aPeer() {
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = 'A2A_REVIEW_PEER'",
                "http://127.0.0.1:" + a2aPeer.getAddress().getPort() + "/a2a", Ids.TENANT_A, WORKSPACE);
    }

    private StartedProcess startProcess(String action, UUID executionId) throws IOException {
        var classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var executable = System.getProperty("os.name", "").toLowerCase().contains("win") ? "java.exe" : "java";
        var java = Path.of(System.getProperty("java.home"), "bin", executable).toString();
        var process = new ProcessBuilder(java, "-cp", classpath, P6ExecutionCrashRunner.class.getName(),
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--server.port=0", "--eaf.security.mode=local", "--eaf.task.dispatcher-enabled=false",
                "--eaf.task.lease-duration=PT10M", "--eaf.execution.outbox-publisher-enabled=false",
                "--eaf.knowledge.outbox-publisher-enabled=false", "--eaf.memory.outbox-publisher-enabled=false",
                "--eaf.credentials.test-crm.token=p6-crash-test-token",
                "--p6.execution.workspace-id=" + WORKSPACE,
                "--p6.execution.id=" + executionId,
                "--p6.execution.action=" + action).redirectErrorStream(true).start();
        return new StartedProcess(process, CompletableFuture.supplyAsync(() -> readOutput(process)));
    }

    private StartedProcess startTaskProcess(UUID taskId) throws IOException {
        var classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var executable = System.getProperty("os.name", "").toLowerCase().contains("win") ? "java.exe" : "java";
        var java = Path.of(System.getProperty("java.home"), "bin", executable).toString();
        var process = new ProcessBuilder(java, "-cp", classpath, P6ExecutionCrashRunner.class.getName(),
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--server.port=0", "--eaf.security.mode=local", "--eaf.task.dispatcher-enabled=false",
                "--eaf.task.lease-duration=PT10M", "--eaf.execution.outbox-publisher-enabled=false",
                "--eaf.execution.remote-poller-enabled=false", "--eaf.knowledge.outbox-publisher-enabled=false",
                "--eaf.memory.outbox-publisher-enabled=false", "--eaf.credentials.a2a-peer-review.token=" + A2A_TOKEN,
                "--p6.execution.workspace-id=" + WORKSPACE, "--p6.execution.action=tool", "--p6.task.id=" + taskId)
                .redirectErrorStream(true).start();
        return new StartedProcess(process, CompletableFuture.supplyAsync(() -> readOutput(process)));
    }

    private ProcessResult runProcess(String action, UUID executionId) throws Exception {
        var child = startProcess(action, executionId);
        if (!child.process().waitFor(60, TimeUnit.SECONDS)) {
            kill(child.process());
            throw new IllegalStateException("EAF 子进程未在截止时间内退出。");
        }
        // 正常重启只检查退出状态与唯一状态标记，不在日志中传播完整连接配置。
        return new ProcessResult(child.process().exitValue(), child.output().get(5, TimeUnit.SECONDS));
    }

    private static String readOutput(Process process) {
        try { return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8); }
        catch (IOException ignored) { return ""; }
    }

    private static void kill(Process process) throws InterruptedException {
        process.destroyForcibly();
        assertThat(process.waitFor(15, TimeUnit.SECONDS)).isTrue();
    }

    private static void killIfRunning(Process process) {
        if (process.isAlive()) {
            process.destroyForcibly();
            try { process.waitFor(15, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    private boolean await(java.util.function.BooleanSupplier condition, Duration timeout) throws InterruptedException {
        var end = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < end) {
            if (condition.getAsBoolean()) return true;
            Thread.sleep(50);
        }
        return condition.getAsBoolean();
    }

    private ConnectionGate holdVerifiedAudit() throws Exception {
        jdbc.execute("create function audit.p6_test_gate_verified() returns trigger language plpgsql as $$ begin if new.fact_key like 'execution-verified:%' then perform pg_advisory_xact_lock(" + AUDIT_GATE + "); end if; return new; end $$");
        jdbc.execute("create trigger p6_test_gate_verified before insert on audit.audit_event for each row execute function audit.p6_test_gate_verified()");
        var connection = dataSource.getConnection();
        connection.createStatement().execute("select pg_advisory_lock(" + AUDIT_GATE + ")");
        return new ConnectionGate(connection);
    }

    private ConnectionGate holdRemoteTaskSettlement(UUID taskId) throws Exception {
        jdbc.execute("create function task.p6_test_gate_remote_settlement() returns trigger language plpgsql as $$ begin if new.id = '" + taskId + "' and new.status = 'WAITING_REMOTE' then perform pg_advisory_xact_lock(" + REMOTE_TASK_GATE + "); end if; return new; end $$");
        jdbc.execute("create trigger p6_test_gate_remote_settlement before update on task.task for each row execute function task.p6_test_gate_remote_settlement()");
        var connection = dataSource.getConnection();
        connection.createStatement().execute("select pg_advisory_lock(" + REMOTE_TASK_GATE + ")");
        return new ConnectionGate(connection);
    }

    private void releaseRemoteTaskSettlement(ConnectionGate gate) {
        try {
            gate.connection().createStatement().execute("select pg_advisory_unlock(" + REMOTE_TASK_GATE + ")");
            gate.connection().close();
            jdbc.execute("drop trigger p6_test_gate_remote_settlement on task.task");
            jdbc.execute("drop function task.p6_test_gate_remote_settlement()");
        } catch (Exception e) { throw new IllegalStateException("无法释放远端 Task 恢复故障点。", e); }
    }

    private boolean waitingOnLock(long lockId) {
        return jdbc.queryForObject("select count(*) from pg_locks where locktype = 'advisory' and classid = 0 and objid = ?::oid and granted = false", Integer.class, lockId) > 0;
    }

    private boolean waitingOnAuditGate() {
        return jdbc.queryForObject("select count(*) from pg_locks where locktype = 'advisory' and classid = 0 and objid = ?::oid and granted = false", Integer.class, AUDIT_GATE) > 0;
    }

    private void releaseAuditGate(ConnectionGate gate) {
        try {
            gate.connection().createStatement().execute("select pg_advisory_unlock(" + AUDIT_GATE + ")");
            gate.connection().close();
            jdbc.execute("drop trigger p6_test_gate_verified on audit.audit_event");
            jdbc.execute("drop function audit.p6_test_gate_verified()");
        } catch (Exception e) { throw new IllegalStateException("无法释放审计故障点。", e); }
    }

    private static void handleA2aPeer(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!("Bearer " + A2A_TOKEN).equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                exchange.sendResponseHeaders(401, -1);
                return;
            }
            var request = JSON.readTree(exchange.getRequestBody());
            if (!"SendMessage".equals(request.path("method").asText())) {
                exchange.sendResponseHeaders(500, -1);
                return;
            }
            peerSends.incrementAndGet();
            if (holdPeerSend) {
                peerSendEntered.countDown();
                try { releasePeerSend.await(30, TimeUnit.SECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            }
            var task = JSON.createObjectNode().put("id", "crash-peer-task").put("contextId", "crash-peer-context");
            task.putObject("status").put("state", "TASK_STATE_SUBMITTED");
            task.putArray("artifacts");
            var response = JSON.createObjectNode().put("jsonrpc", "2.0");
            response.set("id", request.get("id"));
            response.putObject("result").set("task", task);
            var bytes = JSON.writeValueAsBytes(response);
            exchange.getResponseHeaders().add("Content-Type", "application/a2a+json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }

    private static void createFollowup(HttpExchange exchange) {
        if (!authorized(exchange)) { respond(exchange, 401, ""); return; }
        try {
            var request = JSON.readTree(exchange.getRequestBody());
            var operationId = UUID.fromString(request.path("operationId").asText());
            var customerId = request.path("customerId").asText();
            var summary = request.path("summary").asText();
            var ownerId = request.path("ownerId").asText();
            var requestHash = Hashing.sha256(String.join("\n", operationId.toString(), customerId, summary, ownerId));
            recordRequest("POST", operationId);
            if (CRM_PAUSE.get() == CrmPause.BEFORE_COMMIT) {
                holdPost();
                respond(exchange, 503, "");
                return;
            }
            var responseSummary = CRM_PAUSE.get() == CrmPause.MISMATCH ? summary + "-mismatch" : summary;
            persistFollowup(operationId, requestHash, customerId, responseSummary, ownerId);
            if (CRM_PAUSE.get() == CrmPause.AFTER_COMMIT) holdPost();
            if (CRM_PAUSE.get() == CrmPause.AFTER_COMMIT_5XX) { respond(exchange, 503, ""); return; }
            respond(exchange, 201, followupJson(operationId));
        } catch (Exception e) { respond(exchange, 500, ""); }
    }

    private static void findFollowup(HttpExchange exchange) {
        if (!authorized(exchange)) { respond(exchange, 401, ""); return; }
        var operationId = UUID.fromString(exchange.getRequestURI().getPath().substring("/followups/by-operation/".length()));
        try {
            recordRequest("GET", operationId);
            if (CRM_PAUSE.get() == CrmPause.VERIFY_5XX) { respond(exchange, 503, ""); return; }
            var body = followupJsonOrNull(operationId);
            if (body == null) respond(exchange, 404, "");
            else respond(exchange, 200, body);
        } catch (Exception e) { respond(exchange, 500, ""); }
    }

    private static void holdPost() {
        postEntered.countDown();
        try { releasePost.await(45, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static void persistFollowup(UUID operationId, String requestHash, String customerId, String summary, String ownerId) throws Exception {
        try (var connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("insert into testcrm.followup(operation_id, request_hash, external_id, customer_id, summary, owner_id, status, accepted_at) values (?, ?, ?, ?, ?, ?, 'CREATED', '2026-09-29T00:00:00Z'::timestamptz) on conflict(operation_id) do nothing")) {
                statement.setObject(1, operationId); statement.setString(2, requestHash); statement.setString(3, "fu-" + operationId);
                statement.setString(4, customerId); statement.setString(5, summary); statement.setString(6, ownerId); statement.executeUpdate();
            }
            try (var query = connection.prepareStatement("select request_hash from testcrm.followup where operation_id = ?")) {
                query.setObject(1, operationId);
                try (var rows = query.executeQuery()) {
                    if (!rows.next() || !requestHash.equals(rows.getString(1))) throw new IllegalStateException("operationId 请求摘要冲突。");
                }
            }
            connection.commit();
        }
    }

    private static String followupJson(UUID operationId) throws Exception {
        var row = jdbcStatic().queryForMap("select operation_id, external_id, customer_id, summary, owner_id, status from testcrm.followup where operation_id = ?", operationId);
        return JSON.writeValueAsString(Map.of("operationId", row.get("operation_id").toString(), "externalId", row.get("external_id"),
                "customerId", row.get("customer_id"), "summary", row.get("summary"), "ownerId", row.get("owner_id"),
                "status", row.get("status"), "acceptedAt", "2026-09-29T00:00:00Z"));
    }

    private static String followupJsonOrNull(UUID operationId) throws Exception {
        var count = jdbcStatic().queryForObject("select count(*) from testcrm.followup where operation_id = ?", Integer.class, operationId);
        return count == null || count == 0 ? null : followupJson(operationId);
    }

    private static JdbcTemplate jdbcStatic() { return new JdbcTemplate(new org.springframework.jdbc.datasource.DriverManagerDataSource(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())); }

    private static void recordRequest(String method, UUID operationId) {
        jdbcStatic().update("insert into testcrm.request_log(method, operation_id) values (?, ?)", method, operationId);
    }

    private static boolean authorized(HttpExchange exchange) {
        return "Bearer p6-crash-test-token".equals(exchange.getRequestHeaders().getFirst("Authorization"));
    }

    private static void respond(HttpExchange exchange, int status, String body) {
        try {
            var bytes = body.getBytes(StandardCharsets.UTF_8);
            if (!body.isEmpty()) exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) exchange.getResponseBody().write(bytes);
        } catch (IOException ignored) { /* 子 EAF JVM 被强制结束时，CRM 仍保留已提交的数据。 */ }
        finally { exchange.close(); }
    }

    private int count(String method) { return jdbc.queryForObject("select count(*) from testcrm.request_log where method = ?", Integer.class, method); }
    private int crmRecords(UUID operationId) { return jdbc.queryForObject("select count(*) from testcrm.followup where operation_id = ?", Integer.class, operationId); }
    private int verifiedAuditCount(UUID executionId) { return jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class, "execution-verified:" + executionId); }
    private UUID verifiedAuditId(UUID executionId) { return jdbc.query("select id from audit.audit_event where fact_key = ?", rs -> rs.next() ? rs.getObject("id", UUID.class) : null, "execution-verified:" + executionId); }

    private static ActorContext owner() {
        return new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN,
                Set.of("task:create", "task:read", "task:resume", "approval:read", "execution:read", "execution:verify", "tool:read"));
    }

    private static ActorContext approver() {
        return new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN, Set.of("approval:read", "approval:decide", "execution:read"));
    }

    private enum CrmPause { NONE, BEFORE_COMMIT, AFTER_COMMIT, AFTER_COMMIT_5XX, VERIFY_5XX, MISMATCH }
    private record Target(UUID taskId, UUID executionId, UUID operationId, io.eaf.task.api.TaskWorkItem workItem) { }
    private record ProcessResult(int exitCode, String output) { }
    private record StartedProcess(Process process, CompletableFuture<String> output) { }
    private record ConnectionGate(java.sql.Connection connection) { }
}
