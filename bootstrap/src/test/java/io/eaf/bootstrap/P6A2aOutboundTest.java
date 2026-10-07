package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.eaf.agentprotocol.TaskApplicationService;
import io.eaf.execution.api.ExecutionService;
import io.eaf.identity.api.CreateDelegationCommand;
import io.eaf.identity.api.IdentityService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.CreateToolExecutionCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
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

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false",
        "eaf.execution.remote-poller-enabled=false"})
// 独立 JDK A2A peer 与 PostgreSQL 检查 outbound 状态、worker 释放、授权复核和只读输出边界。
class P6A2aOutboundTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String PEER_TOKEN = "p6-synthetic-a2a-peer-token";
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());
    private static final AtomicInteger sends = new AtomicInteger();
    private static final AtomicInteger gets = new AtomicInteger();
    private static final AtomicInteger cancels = new AtomicInteger();
    private static final AtomicInteger getFailures = new AtomicInteger();

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
        registry.add("eaf.credentials.a2a-peer-review.token", () -> PEER_TOKEN);
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper json;
    @Autowired TaskService tasks;
    @Autowired TaskRunner runtime;
    @Autowired ExecutionService executions;
    @Autowired TaskApplicationService taskApplication;
    @Autowired IdentityService identities;
    @Autowired Clock clock;
    private HttpServer peer;
    private ExecutorService peerExecutor;
    private UUID rootTaskId;
    private volatile boolean approvalLike;
    private volatile boolean loseSendResponse;
    private volatile boolean loseCancelResponse;
    private volatile boolean wrongCancelTaskId;
    private volatile boolean oversizedSendResponse;
    private volatile boolean alwaysWorking;
    private volatile boolean wrongTaskId;
    private volatile boolean completeFirstGet;
    private volatile boolean pauseGetResponse;
    private volatile CountDownLatch getEntered = new CountDownLatch(0);
    private volatile CountDownLatch releaseGet = new CountDownLatch(0);
    private UUID revokedCapabilityId;

    @BeforeEach
    void startPeer() throws Exception {
        sends.set(0);
        gets.set(0);
        cancels.set(0);
        getFailures.set(0);
        approvalLike = false;
        loseSendResponse = false;
        loseCancelResponse = false;
        wrongCancelTaskId = false;
        oversizedSendResponse = false;
        alwaysWorking = false;
        wrongTaskId = false;
        completeFirstGet = false;
        pauseGetResponse = false;
        getEntered = new CountDownLatch(0);
        releaseGet = new CountDownLatch(0);
        peer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        peerExecutor = Executors.newCachedThreadPool();
        peer.setExecutor(peerExecutor);
        peer.createContext("/a2a", this::handlePeer);
        peer.start();
        jdbc.update("update connector.instance set base_url = ? where tenant_id = ? and workspace_id = ? and provider = 'A2A_REVIEW_PEER'",
                "http://127.0.0.1:" + peer.getAddress().getPort() + "/a2a", Ids.TENANT_A, Ids.WORKSPACE_A);
    }

    @AfterEach
    void stopPeer() {
        if (peer != null) peer.stop(0);
        if (peerExecutor != null) peerExecutor.shutdownNow();
        if (revokedCapabilityId != null) {
            jdbc.update("update capability.version set status = 'PUBLISHED', row_version = row_version + 1 where capability_id = ? and asset_version = '1.0.0'", revokedCapabilityId);
            revokedCapabilityId = null;
        }
        if (rootTaskId != null) {
            jdbc.update("update task.task set external_effect_operation_id = null, external_effect_status = 'FAILED', status = 'CANCELLED', active_reserved_ms = 0, active_budget_reservation_key = null, lease_owner_id = null, lease_until = null where root_task_id = ? and status in ('QUEUED','RUNNING','WAITING_REMOTE','CANCELLING_REMOTE','WAITING_VERIFICATION')",
                    rootTaskId);
            jdbc.update("update task.budget_scope set active_reserved_ms = 0 where root_task_id = ?", rootTaskId);
        }
    }

    @Test
    void acceptedRemoteTaskReleasesWorkerThenResumesWithoutResending() throws Exception {
        var taskId = startReviewerTask("async-success");
        var first = runNext();
        assertThat(first.status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        assertThat(sends).hasValue(1);
        assertThat(gets).hasValue(0);
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, taskId).externalEffectPending()).isTrue();
        assertThat(jdbc.queryForObject("select active_reserved_ms from task.task where id = ?", Long.class, taskId)).isZero();
        assertThat(jdbc.queryForObject("select status from execution.execution where task_id = ?", String.class, taskId)).isEqualTo("AWAITING_REMOTE");

        wakeAndRun(taskId);
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, taskId).status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        assertThat(gets).hasValue(1);
        wakeAndRun(taskId);
        var done = tasks.get(ALICE, Ids.WORKSPACE_A, taskId);
        assertThat(done.status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(json.readTree(done.resultJson()).path("riskLevel").asText()).isEqualTo("HIGH");
        assertThat(sends).hasValue(1);
        assertThat(gets).hasValue(2);
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where task_id = ?", String.class, taskId)).isEqualTo("COMPLETED");
        assertThat(jdbc.queryForObject("select remote_cost_status from execution.remote_a2a_operation where task_id = ?", String.class, taskId)).isEqualTo("UNKNOWN");
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where task_id = ? and action like 'REMOTE_AGENT_%'", Integer.class, taskId)).isGreaterThanOrEqualTo(3);
        assertThat(jdbc.queryForObject("select count(*) from execution.outbox where execution_id = (select id from execution.execution where task_id = ?) and event_type = 'eaf.execution.awaiting-remote.v1'", Integer.class, taskId)).isEqualTo(1);
        var payload = json.readTree(jdbc.queryForObject("select payload_json::text from execution.outbox where execution_id = (select id from execution.execution where task_id = ?) and event_type = 'eaf.execution.awaiting-remote.v1'", String.class, taskId));
        assertThat(payload.path("taskId").asText()).isEqualTo(taskId.toString());
        assertThat(payload.path("entryProtocol").asText()).isEqualTo("REST");
        assertThat(payload.path("actorId").asText()).isEqualTo(Ids.ALICE.toString());
        assertThat(payload.path("messageId").asText()).isNotBlank();
        assertThat(payload.toString()).doesNotContain(PEER_TOKEN, "riskSummary");
    }

    @Test
    void executionOutboxStopsAtSixAttemptsAndReplaysTheSameAuditFact() throws Exception {
        var outboxEventId = UUID.randomUUID();
        var taskId = UUID.randomUUID();
        var executionId = UUID.randomUUID();
        var traceId = "workflow:58000000-0000-4000-8000-000000000001@1.1.0:peer-review";
        // 让本测试创建的事件成为唯一到期项，避免手动 publisher 消费同库的其它业务事件。
        jdbc.update("update execution.outbox set next_attempt_at = now() + interval '1 day' where status in ('PENDING','FAILED')");
        var payload = json.createObjectNode().put("actorId", Ids.ALICE.toString()).put("taskId", taskId.toString()).put("traceId", traceId);
        jdbc.update("insert into execution.outbox(event_id, tenant_id, workspace_id, execution_id, event_type, aggregate_version, payload_json) values (?, ?, ?, ?, ?, 1, ?::jsonb)",
                outboxEventId, Ids.TENANT_A, Ids.WORKSPACE_A, executionId, "eaf.execution.awaiting-remote.v1", payload.toString());
        var realAudit = new io.eaf.audit.infrastructure.JdbcAuditService(jdbc);
        var attempts = new AtomicInteger();
        var publisher = new io.eaf.execution.infrastructure.ExecutionOutboxPublisher(jdbc, fact -> {
            var call = attempts.incrementAndGet();
            if (call < 6) throw new IllegalStateException("synthetic audit outage");
            realAudit.append(fact);
            if (call == 6) throw new IllegalStateException("audit accepted before local outbox acknowledgement");
        });

        // 每次把退避截止移到现在只用于快速验证生产调度器的失败上限。
        for (int attempt = 0; attempt < 6; attempt++) {
            jdbc.update("update execution.outbox set next_attempt_at = now() - interval '1 second' where event_id = ?", outboxEventId);
            publisher.publish();
        }
        assertThat(jdbc.queryForObject("select status from execution.outbox where event_id = ?", String.class, outboxEventId)).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("select attempts from execution.outbox where event_id = ?", Integer.class, outboxEventId)).isEqualTo(6);
        assertThat(attempts).hasValue(6);
        publisher.publish();
        assertThat(attempts).hasValue(6);
        var factKey = "execution-outbox:" + outboxEventId;
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class, factKey)).isEqualTo(1);

        // 运维重放保留 eventId 和 payload；Audit factKey 收敛“已消费、未确认”的不确定结果。
        jdbc.update("update execution.outbox set status = 'PENDING', attempts = 0, next_attempt_at = now(), lease_until = null where event_id = ? and status = 'FAILED' and attempts = 6", outboxEventId);
        publisher.publish();
        assertThat(jdbc.queryForObject("select status from execution.outbox where event_id = ?", String.class, outboxEventId)).isEqualTo("SENT");
        assertThat(jdbc.queryForObject("select actor_id from audit.audit_event where fact_key = ?", UUID.class, factKey)).isEqualTo(Ids.ALICE);
        assertThat(jdbc.queryForObject("select task_id from audit.audit_event where fact_key = ?", UUID.class, factKey)).isEqualTo(taskId);
        assertThat(jdbc.queryForObject("select trace_id from audit.audit_event where fact_key = ?", String.class, factKey)).isEqualTo(traceId);
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where fact_key = ?", Integer.class, factKey)).isEqualTo(1);
        assertThat(attempts).hasValue(7);
    }

    @Test
    void approvalLikeRemoteFieldsAreRejectedAndNeverBecomeBusinessWrites() throws Exception {
        approvalLike = true;
        var taskId = startReviewerTask("malicious-advice");
        var first = runNext();
        assertThat(first.status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        wakeAndRun(taskId);
        wakeAndRun(taskId);
        var failed = tasks.get(ALICE, Ids.WORKSPACE_A, taskId);
        assertThat(failed.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(failed.resultJson()).isNull();
        assertThat(sends).hasValue(1);
        assertThat(jdbc.queryForObject("select count(*) from execution.remote_a2a_operation where task_id = ? and state = 'FAILED'", Integer.class, taskId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from execution.execution where task_id = ? and tool_name = 'crm.followup.create'", Integer.class, taskId)).isZero();
    }

    @Test
    void acceptedRequestWithLostSendResponseStaysUnknownAndIsNotResent() {
        loseSendResponse = true;
        var taskId = startReviewerTask("lost-send-response");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_VERIFICATION);
        assertThat(jdbc.queryForObject("select status from execution.execution where task_id = ?", String.class, taskId)).isEqualTo("UNKNOWN");
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where task_id = ?", String.class, taskId)).isEqualTo("SEND_UNKNOWN");
        assertThat(sends).hasValue(1);

        var waiting = tasks.get(ALICE, Ids.WORKSPACE_A, taskId);
        tasks.resume(ALICE, Ids.WORKSPACE_A, taskId, waiting.version(), "resume-lost-send");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_VERIFICATION);
        assertThat(sends).hasValue(1);
    }

    @Test
    void peerMustConfirmCancellationBeforeLocalTaskBecomesCancelled() {
        var taskId = startReviewerTask("cancel-confirmed");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        var waiting = tasks.get(ALICE, Ids.WORKSPACE_A, taskId);

        var cancelled = taskApplication.cancel(ALICE, Ids.WORKSPACE_A, taskId, waiting.version());

        assertThat(cancelled.status()).isEqualTo(TaskStatus.CANCELLED.name());
        assertThat(jdbc.queryForObject("select status from execution.execution where task_id = ?", String.class, taskId)).isEqualTo("CANCELLED");
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where task_id = ?", String.class, taskId)).isEqualTo("CANCELLED");
        assertThat(cancels).hasValue(1);
        assertThat(sends).hasValue(1);
        assertThat(gets).hasValue(0);
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where task_id = ? and action = 'REMOTE_AGENT_CANCEL_CONFIRMED'", Integer.class, taskId)).isEqualTo(1);
    }

    @Test
    void lostCancelReplyStaysPendingAndASecondCancelCanConfirmIt() {
        var taskId = startReviewerTask("cancel-unknown");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        loseCancelResponse = true;
        var waiting = tasks.get(ALICE, Ids.WORKSPACE_A, taskId);

        var unconfirmed = taskApplication.cancel(ALICE, Ids.WORKSPACE_A, taskId, waiting.version());

        assertThat(unconfirmed.status()).isEqualTo(TaskStatus.CANCELLING_REMOTE.name());
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where task_id = ?", String.class, taskId)).isEqualTo("CANCEL_UNKNOWN");
        assertThat(jdbc.queryForObject("select external_effect_status from task.task where id = ?", String.class, taskId)).isEqualTo("REMOTE_CANCEL_UNKNOWN");
        assertThat(tasks.claim(taskId)).isEmpty();
        assertThat(sends).hasValue(1);
        assertThat(cancels).hasValue(1);

        loseCancelResponse = false;
        var confirmed = taskApplication.cancel(ALICE, Ids.WORKSPACE_A, taskId);
        assertThat(confirmed.status()).isEqualTo(TaskStatus.CANCELLED.name());
        assertThat(cancels).hasValue(2);
        assertThat(sends).hasValue(1);
    }

    @Test
    void queuedRemotePollCanBeCancelledBeforeAnotherWorkerClaimsIt() {
        var taskId = startReviewerTask("cancel-queued-poll");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        jdbc.update("update execution.remote_a2a_operation set next_poll_at = now() - interval '1 second' where task_id = ?", taskId);
        executions.wakeDueRemoteTasks();
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, taskId).status()).isEqualTo(TaskStatus.QUEUED);

        var cancelled = taskApplication.cancel(ALICE, Ids.WORKSPACE_A, taskId);

        assertThat(cancelled.status()).isEqualTo(TaskStatus.CANCELLED.name());
        assertThat(gets).hasValue(0);
        assertThat(cancels).hasValue(1);
        assertThat(sends).hasValue(1);
    }

    @Test
    void wrongCancelTaskIdCannotConfirmLocalCancellation() {
        var taskId = startReviewerTask("cancel-wrong-peer-id");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        wrongCancelTaskId = true;
        var result = taskApplication.cancel(ALICE, Ids.WORKSPACE_A, taskId);
        wrongCancelTaskId = false;

        assertThat(result.status()).isEqualTo(TaskStatus.CANCELLING_REMOTE.name());
        assertThat(jdbc.queryForObject("select status from task.task where id = ?", String.class, taskId)).isEqualTo("CANCELLING_REMOTE");
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where task_id = ?", String.class, taskId)).isEqualTo("CANCEL_UNKNOWN");
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where task_id = ? and action = 'REMOTE_AGENT_CANCEL_CONFIRMED'", Integer.class, taskId)).isZero();
    }

    @Test
    void interruptedSendIntentRecoversToUnknownWithoutResending() {
        var taskId = startReviewerTask("recover-send-intent");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, taskId);
        // 故障注入模拟 Send 已发出但 remoteTaskId 尚未提交的进程中断窗口。
        jdbc.update("update execution.execution set status = 'EXECUTING', lease_until = now() - interval '1 second' where id = ?", executionId);
        jdbc.update("update execution.remote_a2a_operation set state = 'SEND_PENDING', remote_task_id = null, remote_context_id = null where execution_id = ?", executionId);
        jdbc.update("update task.task set status = 'RUNNING', external_effect_status = 'IN_PROGRESS' where id = ?", taskId);
        jdbc.update("update task.task_attempt set status = 'RUNNING', ended_at = null where task_id = ?", taskId);

        executions.recoverOnStartup();
        tasks.recoverOnStartup();

        assertThat(jdbc.queryForObject("select status from execution.execution where id = ?", String.class, executionId)).isEqualTo("UNKNOWN");
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where execution_id = ?", String.class, executionId)).isEqualTo("SEND_UNKNOWN");
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, taskId).externalEffectPending()).isTrue();
        assertThat(sends).hasValue(1);
        assertThat(gets).hasValue(0);
    }

    @Test
    void oversizedSendResponseIsUnknownAndNeverResent() {
        // 超出适配器读取上限的 Send 回执按受理未知处理，不能再生成第二条远端请求。
        oversizedSendResponse = true;
        var taskId = startReviewerTask("oversized-send-response");

        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_VERIFICATION);
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where task_id = ?", String.class, taskId)).isEqualTo("SEND_UNKNOWN");
        var waiting = tasks.get(ALICE, Ids.WORKSPACE_A, taskId);
        tasks.resume(ALICE, Ids.WORKSPACE_A, taskId, waiting.version(), "resume-oversized-send");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_VERIFICATION);
        assertThat(sends).hasValue(1);
        assertThat(gets).hasValue(0);
    }

    @Test
    void getTaskServerErrorKeepsAcceptedRemoteTaskPending() {
        // GetTask 5xx 只安排同一 remoteTaskId 的后续只读查询，不回退到 SendMessage。
        getFailures.set(1);
        var taskId = startReviewerTask("poll-5xx");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_REMOTE);

        wakeAndRun(taskId);

        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, taskId).status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where task_id = ?", String.class, taskId)).isEqualTo("WORKING");
        assertThat(sends).hasValue(1);
        assertThat(gets).hasValue(1);
    }

    @Test
    void getTaskTimeoutKeepsSameRemoteTaskForLaterRead() throws Exception {
        // 超时回执保持原远端 Task 未决，随后仍可用已保存 ID 查询。
        var taskId = startReviewerTask("poll-timeout");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        jdbc.update("update execution.remote_a2a_operation set next_poll_at = now() - interval '1 second' where task_id = ?", taskId);
        executions.wakeDueRemoteTasks();
        pauseGetResponse = true;
        getEntered = new CountDownLatch(1);
        releaseGet = new CountDownLatch(1);
        var work = tasks.claimOne().orElseThrow();
        var polling = CompletableFuture.supplyAsync(() -> {
            var outcome = runtime.run(work);
            tasks.complete(work, outcome);
            return outcome;
        });
        assertThat(getEntered.await(10, TimeUnit.SECONDS)).isTrue();
        try {
            assertThat(polling.get(8, TimeUnit.SECONDS).status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        } finally {
            releaseGet.countDown();
        }

        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, taskId).status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where task_id = ?", String.class, taskId)).isEqualTo("WORKING");
        assertThat(sends).hasValue(1);
        assertThat(gets).hasValue(1);
    }

    @Test
    void permanentlyWorkingPeerStopsAtPollLimit() {
        // 持续 WORKING 达到持久轮询上限后进入 UNKNOWN，释放本地 Worker 并停止查询。
        alwaysWorking = true;
        var taskId = startReviewerTask("poll-limit");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        jdbc.update("update execution.remote_a2a_operation set poll_limit = 2 where task_id = ?", taskId);

        wakeAndRun(taskId);
        wakeAndRun(taskId);
        wakeAndRun(taskId);

        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, taskId).status()).isEqualTo(TaskStatus.WAITING_VERIFICATION);
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where task_id = ?", String.class, taskId)).isEqualTo("UNKNOWN");
        assertThat(jdbc.queryForObject("select polls_used from execution.remote_a2a_operation where task_id = ?", Integer.class, taskId)).isEqualTo(2);
        assertThat(sends).hasValue(1);
        assertThat(gets).hasValue(2);
    }

    @Test
    void mismatchedGetTaskIdIsIsolatedAsUnknown() {
        // GetTask 返回其他 Task ID 时隔离结果，不保存 Artifact 或业务结果。
        completeFirstGet = true;
        wrongTaskId = true;
        var taskId = startReviewerTask("wrong-remote-task-id");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_REMOTE);

        wakeAndRun(taskId);

        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, taskId).status()).isEqualTo(TaskStatus.WAITING_VERIFICATION);
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, taskId).resultJson()).isNull();
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where task_id = ?", String.class, taskId)).isEqualTo("UNKNOWN");
        assertThat(sends).hasValue(1);
        assertThat(gets).hasValue(1);
    }

    @Test
    void capabilityRevokedWhileGetTaskIsInFlightPreventsAdviceAcceptance() throws Exception {
        // peer 已返回建议但 Asset 在结算前撤回时，Execution 重新验权并丢弃建议。
        var taskId = startReviewerTask("capability-revoked-in-flight");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        var executionId = jdbc.queryForObject("select id from execution.execution where task_id = ?", UUID.class, taskId);
        var capabilityId = jdbc.queryForObject("select capability_id from execution.remote_a2a_operation where execution_id = ?", UUID.class, executionId);
        revokedCapabilityId = capabilityId;
        jdbc.update("update execution.remote_a2a_operation set next_poll_at = now() - interval '1 second' where execution_id = ?", executionId);
        executions.wakeDueRemoteTasks();
        pauseGetResponse = true;
        getEntered = new CountDownLatch(1);
        releaseGet = new CountDownLatch(1);
        var work = tasks.claimOne().orElseThrow();
        var polling = CompletableFuture.runAsync(() -> {
            var outcome = runtime.run(work);
            tasks.complete(work, outcome);
        });
        assertThat(getEntered.await(10, TimeUnit.SECONDS)).isTrue();
        try {
            // 直接翻转只用于模拟别的治理请求在 peer 响应窗口内完成撤回。
            assertThat(jdbc.update("update capability.version set status = 'REVOKED', row_version = row_version + 1 where capability_id = ? and asset_version = '1.0.0' and status = 'PUBLISHED'", capabilityId)).isEqualTo(1);
        } finally {
            releaseGet.countDown();
        }
        polling.get(10, TimeUnit.SECONDS);

        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, taskId).status()).isEqualTo(TaskStatus.WAITING_VERIFICATION);
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, taskId).resultJson()).isNull();
        assertThat(jdbc.queryForObject("select state from execution.remote_a2a_operation where execution_id = ?", String.class, executionId)).isEqualTo("UNKNOWN");
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where task_id = ? and action = 'REMOTE_AGENT_ADVICE_ACCEPTED'", Integer.class, taskId)).isZero();
        assertThat(sends).hasValue(1);
        assertThat(gets).hasValue(1);
    }

    @Test
    void revokedDelegationWhileWaitingPreventsRemotePoll() {
        var owner = identities.resolveToken("alice").orElseThrow();
        // 固定 reviewer 读取绑定的 Capability、Skill 与 Tool 元数据，委托逐项声明这些只读范围。
        var delegation = identities.createDelegation(new CreateDelegationCommand(owner, Ids.WORKSPACE_A, Ids.AGENT_RISK,
                Set.of("task:create", "task:read", "agent:read", "capability:read", "skill:read", "tool:read", "crm:customer:read", "agent:risk-review"),
                Set.of("customer-001"), Instant.now(clock).plusSeconds(300)));
        var delegated = identities.resolveDelegatedToken("risk-agent", delegation.id(), IdentityService.REST_AUDIENCE).orElseThrow();
        var root = tasks.create(new CreateTaskCommand(delegated, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.0.0",
                "delegated remote root", null, null, "delegated-remote-root", "trace-delegated-remote"));
        rootTaskId = root.id();
        var rootWork = tasks.claimOne().orElseThrow();
        assertThat(rootWork.id()).isEqualTo(root.id());
        var task = tasks.createToolExecution(new io.eaf.task.api.CreateToolExecutionCommand(delegated,
                Ids.WORKSPACE_A, root.id(), "delegated-review", "agent.risk.review", "1.0.0",
                "{\"customerId\":\"customer-001\",\"riskSummary\":\"renewal in 30 days\"}", "trace-delegated-remote"));
        tasks.complete(rootWork, TaskRunner.RunOutcome.success("{}", false, null, null));

        var first = runNext();
        assertThat(first.status()).withFailMessage("Task=%s Execution=%s Remote=%s", tasks.get(owner, Ids.WORKSPACE_A, task.id()),
                jdbc.queryForList("select status, error_code, error_detail from execution.execution where task_id = ?", task.id()),
                jdbc.queryForList("select state from execution.remote_a2a_operation where task_id = ?", task.id()))
                .isEqualTo(TaskStatus.WAITING_REMOTE);
        assertThat(sends).hasValue(1);
        identities.revokeDelegation(owner, Ids.WORKSPACE_A, delegation.id());
        jdbc.update("update execution.remote_a2a_operation set next_poll_at = now() - interval '1 second' where task_id = ?", task.id());
        executions.wakeDueRemoteTasks();

        assertThat(tasks.claimOne()).isEmpty();
        assertThat(tasks.get(owner, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.FAILED);
        assertThat(gets).hasValue(0);
        // Task 终态后远端操作只收敛为 UNKNOWN，本地调度器不得再读取 peer 建议。
        jdbc.update("update execution.remote_a2a_operation set next_poll_at = now() - interval '1 second' where task_id = ?", task.id());
        executions.wakeDueRemoteTasks();
        assertThat(jdbc.queryForObject("select status from execution.execution where task_id = ?", String.class, task.id())).isEqualTo("UNKNOWN");
        assertThat(gets).hasValue(0);
    }

    @Test
    void ordinaryAgentCannotInvokeRemoteReviewerOutsideFixedToolTask() {
        // Execution 侧也校验固定工具运行类型，避免直接 REST Execution 越过 Runtime 工具过滤。
        var task = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.0.0",
                "仅创建销售跟进。", null, null, UUID.randomUUID().toString(), "trace-no-model-peer"));
        rootTaskId = task.id();
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.id()).isEqualTo(task.id());

        var denied = executions.submit(new io.eaf.execution.api.ExecutionCommand(ALICE, Ids.WORKSPACE_A, task.id(), work.attempt(),
                Ids.AGENT_RISK, "2.0.0", "agent.risk.review", "1.0.0",
                "{\"customerId\":\"customer-001\",\"riskSummary\":\"review request\"}", UUID.randomUUID().toString(), "trace-no-model-peer"));

        assertThat(denied.status()).isEqualTo("DENIED");
        assertThat(denied.errorCode()).isEqualTo("REMOTE_REVIEW_FIXED_TOOL_REQUIRED");
        assertThat(jdbc.queryForObject("select count(*) from execution.remote_a2a_operation where task_id = ?", Integer.class, task.id())).isZero();
        assertThat(sends).hasValue(0);
    }

    @Test
    void evaluationSourceCannotUseRemoteReviewerOrReturnAsUserAdvice() {
        // EVALUATION 子 Task 继承来源后在发送前拒绝，不能借 peer 将评测身份转换成 USER。
        var root = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.0.0",
                "Evaluation remote review", null, null, UUID.randomUUID().toString(), "trace-evaluation-a2a", "EVALUATION"));
        rootTaskId = root.id();
        var rootWork = tasks.claim(root.id()).orElseThrow();
        var task = tasks.createToolExecution(new CreateToolExecutionCommand(ALICE, Ids.WORKSPACE_A, root.id(),
                "evaluation-review", "agent.risk.review", "1.0.0",
                "{\"customerId\":\"customer-001\",\"riskSummary\":\"evaluation-only\"}", "trace-evaluation-a2a"));
        tasks.complete(rootWork, TaskRunner.RunOutcome.success("{}", false, null, null));
        var work = tasks.claim(task.id()).orElseThrow();

        tasks.complete(work, runtime.run(work));

        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).source()).isEqualTo("EVALUATION");
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.FAILED);
        assertThat(sends).hasValue(0);
        assertThat(jdbc.queryForObject("select count(*) from execution.remote_a2a_operation where task_id = ?", Integer.class, task.id())).isZero();
    }

    @Test
    void lateGetTaskResultAfterConfirmedCancellationIsAuditedAndIgnored() throws Exception {
        var taskId = startReviewerTask("late-poll-after-cancel");
        assertThat(runNext().status()).isEqualTo(TaskStatus.WAITING_REMOTE);
        jdbc.update("update execution.remote_a2a_operation set next_poll_at = now() - interval '1 second' where task_id = ?", taskId);
        executions.wakeDueRemoteTasks();
        pauseGetResponse = true;
        getEntered = new CountDownLatch(1);
        releaseGet = new CountDownLatch(1);
        var work = tasks.claimOne().orElseThrow();
        var runningPoll = CompletableFuture.runAsync(() -> {
            var outcome = runtime.run(work);
            tasks.complete(work, outcome);
        });
        assertThat(getEntered.await(10, TimeUnit.SECONDS)).isTrue();

        var current = tasks.get(ALICE, Ids.WORKSPACE_A, taskId);
        var cancelled = taskApplication.cancel(ALICE, Ids.WORKSPACE_A, taskId, current.version());
        releaseGet.countDown();
        runningPoll.get(10, TimeUnit.SECONDS);

        assertThat(cancelled.status()).isEqualTo(TaskStatus.CANCELLED.name());
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, taskId).status()).isEqualTo(TaskStatus.CANCELLED);
        assertThat(jdbc.queryForObject("select count(*) from audit.audit_event where task_id = ? and action = 'REMOTE_AGENT_LATE_RESULT_IGNORED'", Integer.class, taskId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from execution.execution where task_id = ? and tool_name = 'crm.followup.create'", Integer.class, taskId)).isZero();
        assertThat(sends).hasValue(1);
        assertThat(gets).hasValue(1);
        assertThat(cancels).hasValue(1);
    }

    private UUID startReviewerTask(String suffix) {
        var root = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "2.0.0",
                "remote root " + suffix, null, null, UUID.randomUUID().toString(), "trace-" + suffix));
        rootTaskId = root.id();
        var rootWork = tasks.claimOne().orElseThrow();
        assertThat(rootWork.id()).isEqualTo(root.id());
        var child = tasks.createToolExecution(new CreateToolExecutionCommand(ALICE, Ids.WORKSPACE_A, root.id(),
                "review-" + suffix, "agent.risk.review", "1.0.0",
                "{\"customerId\":\"customer-001\",\"riskSummary\":\"renewal in 30 days\"}", "trace-" + suffix));
        tasks.complete(rootWork, TaskRunner.RunOutcome.success("{}", false, null, null));
        return child.id();
    }

    private TaskRunner.RunOutcome runNext() {
        var work = tasks.claimOne().orElseThrow();
        assertThat(work.runKind()).isEqualTo("TOOL_EXECUTION");
        var outcome = runtime.run(work);
        tasks.complete(work, outcome);
        return outcome;
    }

    private void wakeAndRun(UUID taskId) {
        jdbc.update("update execution.remote_a2a_operation set next_poll_at = now() - interval '1 second' where task_id = ?", taskId);
        executions.wakeDueRemoteTasks();
        var task = tasks.get(ALICE, Ids.WORKSPACE_A, taskId);
        assertThat(task.status()).isEqualTo(TaskStatus.QUEUED);
        assertThat(runNext().status()).isIn(TaskStatus.WAITING_REMOTE, TaskStatus.WAITING_VERIFICATION,
                TaskStatus.FAILED, TaskStatus.SUCCEEDED);
    }

    private void handlePeer(HttpExchange exchange) throws java.io.IOException {
        try (exchange) {
            if (!PEER_TOKEN.equals(exchange.getRequestHeaders().getFirst("Authorization").replace("Bearer ", ""))
                    || !"1.0".equals(exchange.getRequestHeaders().getFirst("A2A-Version"))) {
                exchange.sendResponseHeaders(401, -1);
                return;
            }
            JsonNode request = json.readTree(exchange.getRequestBody());
            var method = request.path("method").asText();
            var result = json.createObjectNode();
            if ("SendMessage".equals(method)) {
                sends.incrementAndGet();
                if (loseSendResponse) {
                    exchange.sendResponseHeaders(500, -1);
                    return;
                }
                if (oversizedSendResponse) {
                    exchange.getResponseHeaders().add("Content-Type", "application/a2a+json");
                    var oversized = "x".repeat(40_000).getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, oversized.length);
                    exchange.getResponseBody().write(oversized);
                    return;
                }
                var task = result.putObject("task");
                task.put("id", "peer-task-1");
                task.put("contextId", "peer-context-1");
                task.putObject("status").put("state", "TASK_STATE_SUBMITTED");
                task.putArray("artifacts");
            } else if ("CancelTask".equals(method)) {
                cancels.incrementAndGet();
                if (loseCancelResponse) {
                    exchange.sendResponseHeaders(500, -1);
                    return;
                }
                result.put("id", wrongCancelTaskId ? "peer-task-other" : "peer-task-1");
                result.put("contextId", "peer-context-1");
                result.putObject("status").put("state", "TASK_STATE_CANCELED");
                result.putArray("artifacts");
            } else {
                int poll = gets.incrementAndGet();
                if (getFailures.getAndUpdate(value -> Math.max(0, value - 1)) > 0) {
                    exchange.sendResponseHeaders(503, -1);
                    return;
                }
                result.put("id", wrongTaskId ? "peer-task-other" : "peer-task-1");
                result.put("contextId", "peer-context-1");
                var working = alwaysWorking || poll == 1 && !completeFirstGet;
                result.putObject("status").put("state", working ? "TASK_STATE_WORKING" : "TASK_STATE_COMPLETED");
                if (working) result.putArray("artifacts");
                else addAdviceArtifact(result, approvalLike);
                if (pauseGetResponse) {
                    getEntered.countDown();
                    try { releaseGet.await(10, TimeUnit.SECONDS); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                }
            }
            var response = json.createObjectNode().put("jsonrpc", "2.0");
            response.set("id", request.get("id"));
            response.set("result", result);
            var bytes = json.writeValueAsBytes(response);
            exchange.getResponseHeaders().add("Content-Type", "application/a2a+json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }

    private void addAdviceArtifact(com.fasterxml.jackson.databind.node.ObjectNode task, boolean approvalLike) {
        var part = task.putArray("artifacts").addObject().put("name", "risk-review").putArray("parts").addObject();
        part.put("kind", "data");
        var data = part.putObject("data").put("riskLevel", "HIGH").put("rationale", "renewal window is short");
        data.putArray("citations").add("customer record");
        if (approvalLike) data.put("approved", true);
    }
}
