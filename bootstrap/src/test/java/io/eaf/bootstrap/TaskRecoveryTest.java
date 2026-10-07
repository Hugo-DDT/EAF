package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.eaf.audit.api.AuditPort;
import io.eaf.agentruntime.infrastructure.JdbcAgentRuntime;
import io.eaf.agentruntime.api.RuntimeQuery;
import io.eaf.agent.api.AgentCatalog;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelResult;
import io.eaf.model.infrastructure.DeterministicModelGateway;
import io.eaf.observability.api.TraceRecorder;
import io.eaf.prompt.api.PromptCatalog;
import io.eaf.prompt.api.RenderedPrompt;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskAttemptRecovery;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import io.eaf.usage.api.UsageRecorder;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
class TaskRecoveryTest {
    // 任务恢复测试沿用完整数据库镜像，避免只在旧迁移集合上通过。
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of("task:create", "task:read", "task:cancel"));

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
        registry.add("eaf.task.dispatcher-enabled", () -> "false");
    }

    @Autowired TaskService tasks;
    @Autowired AgentCatalog agents;
    @Autowired ModelGateway model;
    @Autowired DataSource dataSource;
    @Autowired PromptCatalog prompts;
    @Autowired AuditPort audit;
    @Autowired UsageRecorder usage;
    @Autowired TraceRecorder traces;
    @Autowired RuntimeQuery runtimeQuery;
    @Autowired TaskRunner taskRunner;
    @Autowired ObjectMapper json;
    @Autowired Clock clock;

    @Test
    @Order(1)
    void queuedCancellationDoesNotCallModel() {
        var task = create("cancel");
        var cancelled = tasks.cancel(ALICE, Ids.WORKSPACE_A, task.id(), task.version());
        assertThat(cancelled.status()).isEqualTo(TaskStatus.CANCELLED);
        assertThat(new JdbcTemplate(dataSource).queryForObject("select status from task.task_attempt where task_id = ? and attempt = 1", String.class, task.id())).isEqualTo("CANCELLED");
        assertThat(model.callCount()).isZero();
    }

    @Test
    @Order(2)
    void lateCompletionCannotOverwriteCancellation() {
        var task = create("late");
        var work = tasks.claimOne().orElseThrow();
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.RUNNING);
        tasks.cancel(ALICE, Ids.WORKSPACE_A, task.id(), work.rowVersion());
        tasks.complete(work, TaskRunner.RunOutcome.success("{}"));
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.CANCELLED);
        assertThat(model.callCount()).isZero();
    }

    @Test
    @Order(3)
    void startupRecoveryKeepsLiveWorkerAndOnlyReapsExpiredLease() {
        var task = create("restart");
        var work = tasks.claimOne().orElseThrow();
        new JdbcTemplate(dataSource).update("insert into agent_runtime.run(id, tenant_id, workspace_id, task_id, attempt, source, status, started_at) values (?, ?, ?, ?, 1, 'USER', 'RUNNING', now())",
                UUID.randomUUID(), Ids.TENANT_A, Ids.WORKSPACE_A, task.id());
        assertThat(tasks.recoverOnStartup()).isEmpty();
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.RUNNING);
        new JdbcTemplate(dataSource).update("update task.task set lease_until = now() - interval '1 second' where id = ?", task.id());
        assertThat(tasks.renewLease(work)).isFalse();
        var expired = tasks.recoverOnStartup();
        assertThat(expired).containsExactly(new TaskAttemptRecovery(task.id(), 1));
        taskRunner.recoverExpired(expired);
        var recovered = tasks.get(ALICE, Ids.WORKSPACE_A, task.id());
        assertThat(recovered.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(recovered.errorCode()).isEqualTo("WORKER_LEASE_EXPIRED");
        var jdbc = new JdbcTemplate(dataSource);
        assertThat(jdbc.queryForObject("select status from task.task_attempt where task_id = ? and attempt = 1", String.class, task.id())).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("select status from agent_runtime.run where task_id = ?", String.class, task.id())).isEqualTo("FAILED");
        assertThat(model.callCount()).isZero();
    }

    @Test
    @Order(18)
    void lateWorkerCannotCompleteAfterExpiredAttemptWasRetried() {
        var task = create("lease-fence");
        var oldWork = tasks.claimOne().orElseThrow();
        new JdbcTemplate(dataSource).update("update task.task set lease_until = now() - interval '1 second' where id = ?", task.id());
        var expired = tasks.recoverOnStartup();
        assertThat(expired).containsExactly(new TaskAttemptRecovery(task.id(), 1));
        var failed = tasks.get(ALICE, Ids.WORKSPACE_A, task.id());
        taskRunner.recoverExpired(expired);

        var late = runtime("SUCCESS").run(oldWork);
        assertThat(late.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(late.errorCode()).isEqualTo("TASK_NOT_RUNNING");
        assertThat(late.modelCalled()).isFalse();
        assertThat(new JdbcTemplate(dataSource).queryForObject("select count(*) from agent_runtime.run where task_id = ? and attempt = 1", Integer.class, task.id())).isZero();

        var queued = tasks.retry(ALICE, Ids.WORKSPACE_A, task.id(), failed.version(), "retry-after-expired-lease");
        var newWork = tasks.claimOne().orElseThrow();
        assertThat(newWork.attempt()).isEqualTo(2);
        assertThat(newWork.leaseOwnerId()).isNotEqualTo(oldWork.leaseOwnerId());
        assertThat(newWork.leaseFence()).isGreaterThan(oldWork.leaseFence());

        tasks.complete(oldWork, TaskRunner.RunOutcome.success("{\"late\":true}"));
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.RUNNING);
        tasks.complete(newWork, TaskRunner.RunOutcome.success("{\"accepted\":true}"));
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(queued.attempt()).isEqualTo(2);
    }

    @Test
    @Order(19)
    void competingWorkersCannotClaimTheSameQueuedTask() throws Exception {
        var task = create("competing-workers");
        var start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = executor.submit(() -> { start.await(); return tasks.claimOne(); });
            var second = executor.submit(() -> { start.await(); return tasks.claimOne(); });
            start.countDown();
            var firstClaim = first.get();
            var secondClaim = second.get();
            assertThat(java.util.stream.Stream.of(firstClaim, secondClaim).filter(java.util.Optional::isPresent).count()).isEqualTo(1);
            var work = firstClaim.orElseGet(secondClaim::orElseThrow);
            assertThat(work.id()).isEqualTo(task.id());
            assertThat(work.leaseOwnerId()).isNotNull();
            assertThat(work.leaseFence()).isPositive();
        }
    }

    @Test
    @Order(4)
    void queuedDeadlineConvergesToTimedOutWithoutClaim() {
        var task = create("timeout");
        new JdbcTemplate(dataSource).update("update task.task set active_deadline_at = now() - interval '1 second' where id = ?", task.id());
        assertThat(tasks.claimOne()).isEmpty();
        var expired = tasks.get(ALICE, Ids.WORKSPACE_A, task.id());
        assertThat(expired.status()).isEqualTo(TaskStatus.TIMED_OUT);
        assertThat(expired.errorCode()).isEqualTo("DEADLINE_EXCEEDED");
        assertThat(model.callCount()).isZero();
    }

    @Test
    @Order(5)
    void invalidModelOutputFailsAfterTheModelWasCalled() {
        var task = create("invalid-output");
        var work = tasks.claimOne().orElseThrow();
        var outcome = runtime("INVALID_JSON").run(work);
        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.errorCode()).isEqualTo("INVALID_MODEL_OUTPUT");
        assertThat(outcome.modelCalled()).isTrue();
        tasks.complete(work, outcome);
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.FAILED);
        var usageRow = new JdbcTemplate(dataSource).queryForMap("select status, error_code from usage.model_usage where task_id = ?", task.id());
        assertThat(usageRow.get("status")).isEqualTo("FAILED");
        assertThat(usageRow.get("error_code")).isEqualTo("INVALID_MODEL_OUTPUT");
    }

    @Test
    @Order(17)
    void lowercaseRiskLevelIsNormalizedForOpenAiCompatibleModels() throws Exception {
        var task = create("lowercase-risk");
        var work = tasks.claimOne().orElseThrow();
        ModelGateway lowercaseModel = new ModelGateway() {
            @Override public ModelResult call(io.eaf.model.api.ModelRequest request) {
                return new ModelResult("test", "deepseek-chat",
                        "{\"riskLevel\":\"high\",\"summary\":\"需要关注。\",\"reasons\":[\"合成测试\"],\"uncertainties\":[]}",
                        100, 30, "KNOWN");
            }
            @Override public int callCount() { return 1; }
        };

        var outcome = runtime(lowercaseModel).run(work);

        assertThat(outcome.status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(json.readTree(outcome.resultJson()).path("riskLevel").asText()).isEqualTo("HIGH");
        tasks.complete(work, outcome);
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.SUCCEEDED);
    }

    @Test
    @Order(6)
    void upstreamTimeoutBecomesTimedOut() {
        var task = create("model-timeout");
        var work = tasks.claimOne().orElseThrow();
        var outcome = runtime("TIMEOUT").run(work);
        assertThat(outcome.status()).isEqualTo(TaskStatus.TIMED_OUT);
        assertThat(outcome.errorCode()).isEqualTo("UPSTREAM_TIMEOUT");
        assertThat(outcome.modelCalled()).isTrue();
        tasks.complete(work, outcome);
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.TIMED_OUT);
    }

    @Test
    @Order(7)
    void budgetRejectionDoesNotCountAsAProviderCall() {
        var task = create("budget");
        var work = tasks.claimOne().orElseThrow();
        var outcome = runtime("BUDGET_EXCEEDED").run(work);
        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.errorCode()).isEqualTo("BUDGET_EXCEEDED");
        assertThat(outcome.modelCalled()).isFalse();
        tasks.complete(work, outcome);
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.FAILED);
    }

    @Test
    @Order(8)
    void replayRevalidatesSuccessWithoutCallingModelOrChangingHistory() {
        var task = create("replay-success");
        var work = tasks.claimOne().orElseThrow();
        var outcome = runtime("SUCCESS").run(work);
        tasks.complete(work, outcome);
        var runsBefore = new JdbcTemplate(dataSource).queryForObject("select count(*) from agent_runtime.run where task_id = ?", Integer.class, task.id());

        var replay = runtimeQuery.replay(Ids.TENANT_A, Ids.WORKSPACE_A, task.id());

        assertThat(replay.originalStatus()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(replay.replayStatus()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(replay.matched()).isTrue();
        assertThat(replay.modelCalled()).isFalse();
        assertThat(new JdbcTemplate(dataSource).queryForObject("select count(*) from agent_runtime.run where task_id = ?", Integer.class, task.id()))
                .isEqualTo(runsBefore);
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.SUCCEEDED);
    }

    @Test
    @Order(9)
    void replayDetectsRenderedPromptMismatchWithoutCallingModel() {
        var task = create("replay-prompt-drift");
        var work = tasks.claimOne().orElseThrow();
        var outcome = runtime("SUCCESS").run(work);
        tasks.complete(work, outcome);
        var driftedPrompts = new PromptCatalog() {
            @Override
            public io.eaf.prompt.api.PromptVersion requirePublished(java.util.UUID tenantId, java.util.UUID workspaceId,
                                                                     java.util.UUID promptId, String version) {
                return prompts.requirePublished(tenantId, workspaceId, promptId, version);
            }

            @Override
            public RenderedPrompt render(java.util.UUID tenantId, java.util.UUID workspaceId, java.util.UUID promptId,
                                         String version, String input) {
                var current = prompts.render(tenantId, workspaceId, promptId, version, input);
                var messages = new ArrayList<>(current.messages());
                messages.set(1, new RenderedPrompt.Message("user", messages.get(1).content() + " drift"));
                return new RenderedPrompt(current.version(), List.copyOf(messages));
            }
        };

        var replay = runtime("SUCCESS", driftedPrompts).replay(Ids.TENANT_A, Ids.WORKSPACE_A, task.id());

        assertThat(replay.replayStatus()).isEqualTo(TaskStatus.FAILED);
        assertThat(replay.errorCode()).isEqualTo("REPLAY_MISMATCH");
        assertThat(replay.matched()).isFalse();
        assertThat(replay.modelCalled()).isFalse();
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.SUCCEEDED);
    }

    @Test
    @Order(10)
    void replayReproducesInvalidOutputWithoutCallingModel() {
        var task = create("replay-invalid");
        var work = tasks.claimOne().orElseThrow();
        var outcome = runtime("INVALID_JSON").run(work);
        tasks.complete(work, outcome);

        var replay = runtimeQuery.replay(Ids.TENANT_A, Ids.WORKSPACE_A, task.id());

        assertThat(replay.originalStatus()).isEqualTo(TaskStatus.FAILED);
        assertThat(replay.replayStatus()).isEqualTo(TaskStatus.FAILED);
        assertThat(replay.errorCode()).isEqualTo("INVALID_MODEL_OUTPUT");
        assertThat(replay.matched()).isTrue();
        assertThat(replay.modelCalled()).isFalse();
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.FAILED);
    }

    @Test
    @Order(11)
    void concurrentIdempotencyCreatesOneTask() throws Exception {
        var key = "recovery-concurrent-idempotency";
        var command = new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "1.0.0", "合成材料 concurrent", null, null, key, "trace-concurrent");
        var futures = new ArrayList<java.util.concurrent.Future<io.eaf.task.api.TaskSnapshot>>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var i = 0; i < 8; i++) futures.add(executor.submit(() -> tasks.create(command)));
            var ids = new ArrayList<java.util.UUID>();
            for (var future : futures) ids.add(future.get().id());
            assertThat(ids).containsOnly(ids.get(0));
            assertThat(new JdbcTemplate(dataSource).queryForObject("select count(*) from task.task where actor_id = ? and idempotency_key = ?", Integer.class, Ids.ALICE, key))
                    .isEqualTo(1);
            tasks.cancel(ALICE, Ids.WORKSPACE_A, ids.get(0), 1);
        }
    }

    @Test
    @Order(13)
    void revokedWorkspaceGrantFailsQueuedTaskBeforeCallingModel() {
        var task = create("revoked-workspace-grant");
        var jdbc = new JdbcTemplate(dataSource);
        jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'task:create'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        try {
            assertThat(tasks.claimOne()).isEmpty();
            var failed = tasks.get(ALICE, Ids.WORKSPACE_A, task.id());
            assertThat(failed.status()).isEqualTo(TaskStatus.FAILED);
            assertThat(failed.errorCode()).isEqualTo("AUTHORIZATION_REVOKED");
            assertThat(model.callCount()).isZero();
            assertThat(jdbc.queryForObject("select count(*) from usage.model_usage where task_id = ?", Integer.class, task.id())).isZero();
        } finally {
            jdbc.update("update workspace.\"grant\" set status = 'ACTIVE' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'task:create'",
                    Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        }
    }

    @Test
    @Order(14)
    void revokedWorkspaceGrantAfterClaimFailsBeforeCallingModel() {
        var task = create("revoked-workspace-after-claim");
        var work = tasks.claimOne().orElseThrow();
        var jdbc = new JdbcTemplate(dataSource);
        jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'task:create'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        try {
            var outcome = runtime("SUCCESS").run(work);
            assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
            assertThat(outcome.errorCode()).isEqualTo("AUTHORIZATION_REVOKED");
            assertThat(outcome.modelCalled()).isFalse();
            tasks.complete(work, outcome);
            assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.FAILED);
            assertThat(jdbc.queryForObject("select count(*) from usage.model_usage where task_id = ?", Integer.class, task.id())).isZero();
        } finally {
            jdbc.update("update workspace.\"grant\" set status = 'ACTIVE' where tenant_id = ? and workspace_id = ? and actor_id = ? and action = 'task:create'",
                    Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE);
        }
    }

    @Test
    @Order(15)
    void revokedAgentIsRejectedBeforeTheQueuedTaskCallsModel() {
        var task = create("revoked-agent");
        var work = tasks.claimOne().orElseThrow();
        new JdbcTemplate(dataSource).update("update agent.version set status = 'REVOKED' where id = ? and workspace_id = ? and asset_version = ?",
                Ids.AGENT_RISK, Ids.WORKSPACE_A, "1.0.0");
        try {
            var outcome = runtime("SUCCESS").run(work);

            assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
            assertThat(outcome.errorCode()).isEqualTo("RESOURCE_NOT_FOUND");
            assertThat(outcome.modelCalled()).isFalse();
            tasks.complete(work, outcome);
            assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.FAILED);
        } finally {
            new JdbcTemplate(dataSource).update("update agent.version set status = 'PUBLISHED' where id = ? and workspace_id = ? and asset_version = ?",
                    Ids.AGENT_RISK, Ids.WORKSPACE_A, "1.0.0");
        }
    }

    @Test
    @Order(16)
    void inputBudgetIsRejectedBeforeCallingModel() {
        var input = "x".repeat(8_000);
        var task = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "1.0.0", input,
                null, null, "recovery-input-budget", "trace-recovery-input-budget"));
        var work = tasks.claimOne().orElseThrow();
        var outcome = runtime("SUCCESS").run(work);

        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.errorCode()).isEqualTo("BUDGET_EXCEEDED");
        assertThat(outcome.modelCalled()).isFalse();
        tasks.complete(work, outcome);
        assertThat(tasks.get(ALICE, Ids.WORKSPACE_A, task.id()).status()).isEqualTo(TaskStatus.FAILED);
        assertThat(new JdbcTemplate(dataSource).queryForObject("select count(*) from usage.model_usage where task_id = ? and error_code = 'BUDGET_EXCEEDED'", Integer.class, task.id())).isEqualTo(1);
    }

    @Test
    @Order(12)
    void unknownProviderUsageStaysNullWhileBudgetReservationRemains() {
        var task = create("unknown-usage");
        var work = tasks.claimOne().orElseThrow();
        var outcome = runtime("UNKNOWN_USAGE").run(work);

        assertThat(outcome.status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(outcome.modelCalled()).isTrue();
        tasks.complete(work, outcome);

        var row = new JdbcTemplate(dataSource).queryForMap("select input_tokens, output_tokens, usage_status, estimated_cost, cost_status, cost_source, reserved_tokens, status from usage.model_usage where task_id = ?", task.id());
        assertThat(row.get("input_tokens")).isNull();
        assertThat(row.get("output_tokens")).isNull();
        assertThat(row.get("usage_status")).isEqualTo("UNKNOWN");
        assertThat(row.get("estimated_cost")).isNull();
        assertThat(row.get("cost_status")).isEqualTo("UNKNOWN_PRICE");
        assertThat(row.get("cost_source")).isNull();
        assertThat(row.get("reserved_tokens")).isEqualTo(8000);
        assertThat(row.get("status")).isEqualTo("SUCCEEDED");
    }

    private JdbcAgentRuntime runtime(String scenario) {
        return runtime(scenario, prompts);
    }

    private JdbcAgentRuntime runtime(String scenario, PromptCatalog promptCatalog) {
        return new JdbcAgentRuntime(new JdbcTemplate(dataSource), agents, promptCatalog, new DeterministicModelGateway(scenario),
                audit, usage, traces, tasks, json, clock);
    }

    private JdbcAgentRuntime runtime(ModelGateway gateway) {
        return new JdbcAgentRuntime(new JdbcTemplate(dataSource), agents, prompts, gateway,
                audit, usage, traces, tasks, json, clock);
    }

    private io.eaf.task.api.TaskSnapshot create(String suffix) {
        return tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "1.0.0", "合成材料 " + suffix, null, null, "recovery-" + suffix, "trace-recovery-" + suffix));
    }
}
// 本文件负责实现 EAF 的 TaskRecoveryTest.java 相关代码。
