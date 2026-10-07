package io.eaf.bootstrap;

import io.eaf.capability.api.CapabilityService;
import io.eaf.evaluation.api.EvaluationService;
import io.eaf.model.api.ModelBillingProfile;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelRequest;
import io.eaf.model.api.ModelResult;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.task.api.TaskStatus;
import io.eaf.workflow.api.CreateQualityRunWorkflowCommand;
import io.eaf.workflow.api.WorkflowInstance;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.infrastructure.JdbcWorkflowService;
import io.eaf.workflow.infrastructure.WorkflowDispatcher;
import io.eaf.usage.api.UsageRecorder;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(properties = {
        "eaf.task.dispatcher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false",
        "eaf.execution.remote-poller-enabled=false",
        "eaf.model.live.fee-cap=0.10",
        "eaf.model.live.fee-currency=USD",
        "eaf.model.live.stop-condition=CAP_REACHED,UNKNOWN_COST,SAFETY_VIOLATION,HUMAN_STOP"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
// 验证协作评测 Workflow 取消与幂等重放不会退款、重置根预算或重发已完成模型请求。
class P7CollaborationBudgetCancellationTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String PROVIDER = "p7-collaboration-fixture";
    private static final String MODEL = "p7-collaboration-budget-model";
    private static final UUID CAPABILITY_ID = UUID.fromString("54000000-0000-4000-8000-000000000001");
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
        registry.add("eaf.security.mode", () -> "local");
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class BillingModelFixture {
        private static final AtomicInteger CALLS = new AtomicInteger();

        @Bean
        @Primary
        ModelGateway billingModelFixture() {
            return new ModelGateway() {
                @Override
                public ModelResult call(ModelRequest request) {
                    CALLS.incrementAndGet();
                    return new ModelResult(PROVIDER, MODEL,
                            "{\"riskLevel\":\"LOW\",\"summary\":\"合成客户风险较低\",\"reasons\":[\"fixture\"],\"uncertainties\":[],\"citations\":[]}",
                            100, 20, "KNOWN");
                }

                @Override
                public int callCount() {
                    return CALLS.get();
                }

                @Override
                public ModelBillingProfile billingProfile() {
                    return new ModelBillingProfile(PROVIDER, MODEL);
                }
            };
        }
    }

    @Autowired private JdbcTemplate jdbc;
    @Autowired private EvaluationService evaluations;
    @Autowired private CapabilityService capabilities;
    @Autowired private ModelGateway model;
    @Autowired private TaskService tasks;
    @Autowired private TaskRunner runtime;
    @Autowired private WorkflowService workflows;
    @Autowired private JdbcWorkflowService workflowStore;
    @Autowired private WorkflowDispatcher workflowDispatcher;
    @Autowired private UsageRecorder usage;

    @BeforeEach
    void seedAuthorizedFixturePrice() {
        BillingModelFixture.CALLS.set(0);
        grant("evaluation:run", "task:create", "task:read", "workflow:start", "workflow:read", "workflow:publish",
                "workflow:write", "agent:read", "capability:read", "capability:publish", "skill:read", "tool:read");
        jdbc.update("insert into usage.price_schedule(provider, model, call_type, price_version, source, source_version, currency, billing_unit, input_price_per_million, output_price_per_million, effective_at) "
                        + "values (?, ?, 'CHAT', 'p7-collaboration-fixture-v1', 'local-fixture', 'p7-collaboration-test', 'USD', 'TOKEN_MILLION', ?, ?, ?) ",
                PROVIDER, MODEL, new BigDecimal("1"), new BigDecimal("1"),
                java.sql.Timestamp.from(Instant.parse("2026-09-01T00:00:00Z")));
    }

    @Test
    void cancelledEvaluationReplayKeepsSettledSpendAndRootBudget() {
        // 先让固定分析步骤产生一次有价模型调用，再在只读 reviewer 步骤前取消整轮。
        var definition = evaluations.getCollaborationEvaluationDefinition(ALICE, Ids.WORKSPACE_A);
        var registration = evaluations.registerCollaborationQualityRun(ALICE, Ids.WORKSPACE_A,
                "p7-11-cancel-budget-" + UUID.randomUUID());
        // Reviewer Workflow 固定引用已发布 Capability；先按测试租户中既有版本完成发布校验。
        var capability = capabilities.get(ALICE, Ids.WORKSPACE_A, CAPABILITY_ID, "1.2.0");
        if ("DRAFT".equals(capability.status()))
            capabilities.publish(ALICE, Ids.WORKSPACE_A, CAPABILITY_ID, "1.2.0", capability.rowVersion());
        var workflow = workflows.get(ALICE, Ids.WORKSPACE_A, definition.reviewerWorkflowId(), definition.reviewerWorkflowVersion());
        if ("DRAFT".equals(workflow.status()))
            workflow = workflows.publish(ALICE, Ids.WORKSPACE_A, workflow.id(), workflow.version(), workflow.rowVersion());
        var request = new CreateQualityRunWorkflowCommand(ALICE, Ids.WORKSPACE_A, registration.id(), workflow.id(),
                workflow.version(), "{\"customerId\":\"p7-cancel-budget-customer\"}",
                "p7-11-cancelled-workflow-" + UUID.randomUUID(), "EVALUATION", null);
        var instance = workflows.createQualityRunInstance(request);
        var atPeerReview = advanceReviewerToPeerReview(instance.id());
        assertThat(atPeerReview.currentStepId()).isEqualTo("peer-review");
        assertThat(model.callCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject("select status from task.task where id = ?", String.class, atPeerReview.rootTaskId()))
                .isEqualTo(TaskStatus.SUCCEEDED.name());

        var rootBudgetBeforeRecovery = jdbc.queryForMap(
                "select model_calls, token_used, token_reserved from task.budget_scope where scope_id = ?",
                atPeerReview.rootBudgetScopeId());
        assertThat(((Number) rootBudgetBeforeRecovery.get("model_calls")).intValue()).isEqualTo(1);
        assertThat(((Number) rootBudgetBeforeRecovery.get("token_used")).longValue()).isEqualTo(120);
        assertThat(((Number) rootBudgetBeforeRecovery.get("token_reserved")).longValue()).isZero();
        var spendBeforeRecovery = spendScope(registration.id());
        assertThat((BigDecimal) spendBeforeRecovery.get("spent_amount")).isEqualByComparingTo("0.00012000");
        assertThat((BigDecimal) spendBeforeRecovery.get("reserved_amount")).isEqualByComparingTo("0");
        assertThat(spendBeforeRecovery.get("status")).isEqualTo("ACTIVE");
        assertThat(usage.findForScope(Ids.TENANT_A, Ids.WORKSPACE_A, "EVALUATION", registration.id()))
                .singleElement().satisfies(record -> {
                    assertThat(record.source()).isEqualTo("EVALUATION");
                    assertThat(record.actualCost()).isNull();
                    assertThat(record.estimatedCost()).isEqualByComparingTo("0.00012000");
                });

        // 崩溃恢复时用原幂等请求取回运行中实例，再由 Dispatcher 从固定 reviewer 步骤继续。
        var activeReplay = workflows.createQualityRunInstance(request);
        assertThat(activeReplay.id()).isEqualTo(instance.id());
        assertThat(activeReplay.rootBudgetScopeId()).isEqualTo(atPeerReview.rootBudgetScopeId());
        assertThat(activeReplay.currentStepId()).isEqualTo("peer-review");
        assertThat(budgetScope(atPeerReview.rootBudgetScopeId())).containsAllEntriesOf(rootBudgetBeforeRecovery);
        assertThat(spendScope(registration.id())).containsAllEntriesOf(spendBeforeRecovery);
        var resumedAtPeerReview = resumeReviewerStep(instance.id());
        var reviewerTask = tasks.get(ALICE, Ids.WORKSPACE_A, resumedAtPeerReview.childTaskId());
        assertThat(reviewerTask.status()).isEqualTo(TaskStatus.QUEUED);
        assertThat(model.callCount()).isEqualTo(1);
        assertThat(budgetScope(atPeerReview.rootBudgetScopeId())).containsAllEntriesOf(rootBudgetBeforeRecovery);
        assertThat(spendScope(registration.id())).containsAllEntriesOf(spendBeforeRecovery);

        var cancelling = workflows.cancelInstance(ALICE, Ids.WORKSPACE_A, instance.id(), resumedAtPeerReview.rowVersion());
        var cancelled = finishCancellation(cancelling.id());
        assertThat(cancelled.status()).isEqualTo("CANCELLED");
        var taskIdsAfterCancel = taskIds(registration.id());
        var budgetAfterCancel = budgetScope(atPeerReview.rootBudgetScopeId());
        var spendAfterCancel = spendScope(registration.id());

        // 同幂等鍵重放必须返回原取消实例与原预算；不得重跑分析、重开费用 Scope 或调用 reviewer。
        var replayed = workflows.createQualityRunInstance(request);
        assertThat(replayed.id()).isEqualTo(instance.id());
        assertThat(replayed.status()).isEqualTo("CANCELLED");
        assertThat(replayed.rootBudgetScopeId()).isEqualTo(atPeerReview.rootBudgetScopeId());
        assertThat(taskIds(registration.id())).containsExactlyElementsOf(taskIdsAfterCancel);
        assertThat(budgetScope(atPeerReview.rootBudgetScopeId())).containsAllEntriesOf(budgetAfterCancel);
        assertThat(spendScope(registration.id())).containsAllEntriesOf(spendAfterCancel);
        assertThat(model.callCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from execution.remote_a2a_operation o join task.task t on t.id = o.task_id "
                + "where t.quality_run_id = ?", Integer.class, registration.id())).isZero();
    }

    private WorkflowInstance advanceReviewerToPeerReview(UUID instanceId) {
        for (var turn = 0; turn < 8; turn++) {
            var current = workflows.getInstance(ALICE, Ids.WORKSPACE_A, instanceId);
            if ("peer-review".equals(current.currentStepId())
                    && workflowStore.stepRuntime(instanceId, "peer-review") == null) return current;
            jdbc.update("update workflow.instance set next_poll_at = case when id = ? then null else now() + interval '1 day' end "
                            + "where status in ('QUEUED','RUNNING','WAITING_CHILD')", instanceId);
            var lease = workflowStore.claimOne().orElseThrow();
            assertThat(lease.instanceId()).isEqualTo(instanceId);
            workflowDispatcher.advance(lease);
            current = workflows.getInstance(ALICE, Ids.WORKSPACE_A, instanceId);
            if (current.childTaskId() != null) {
                var child = tasks.get(ALICE, Ids.WORKSPACE_A, current.childTaskId());
                if (child.status() == TaskStatus.QUEUED) {
                    var work = tasks.claim(child.id()).orElseThrow();
                    tasks.complete(work, runtime.run(work));
                }
            }
        }
        throw new AssertionError("协作评测 Workflow 未到达 reviewer 前的可取消边界。");
    }

    private WorkflowInstance resumeReviewerStep(UUID instanceId) {
        for (var turn = 0; turn < 4; turn++) {
            var current = workflows.getInstance(ALICE, Ids.WORKSPACE_A, instanceId);
            var step = workflowStore.stepRuntime(instanceId, "peer-review");
            if (step != null && step.childTaskId() != null) return current;
            // 只唤醒目标实例，复用其持久化根预算和步骤 dispatchKey 完成 Worker 恢复。
            jdbc.update("update workflow.instance set next_poll_at = case when id = ? then null else now() + interval '1 day' end "
                            + "where status in ('QUEUED','RUNNING','WAITING_CHILD')", instanceId);
            var lease = workflowStore.claimOne().orElseThrow();
            assertThat(lease.instanceId()).isEqualTo(instanceId);
            workflowDispatcher.advance(lease);
        }
        throw new AssertionError("协作评测 Workflow 未从 reviewer 步骤恢复出幂等子 Task。");
    }

    private WorkflowInstance finishCancellation(UUID instanceId) {
        for (var turn = 0; turn < 5; turn++) {
            var current = workflows.getInstance(ALICE, Ids.WORKSPACE_A, instanceId);
            if (Set.of("CANCELLED", "FAILED", "TIMED_OUT").contains(current.status())) return current;
            jdbc.update("update workflow.instance set next_poll_at = null where id = ?", instanceId);
            var lease = workflowStore.claimOne().orElseThrow();
            assertThat(lease.instanceId()).isEqualTo(instanceId);
            workflowDispatcher.advance(lease);
        }
        return workflows.getInstance(ALICE, Ids.WORKSPACE_A, instanceId);
    }

    private java.util.List<UUID> taskIds(UUID qualityRunId) {
        return jdbc.query("select id from task.task where tenant_id = ? and workspace_id = ? and quality_run_id = ? order by created_at, id",
                (rs, row) -> rs.getObject("id", UUID.class), Ids.TENANT_A, Ids.WORKSPACE_A, qualityRunId);
    }

    private java.util.Map<String, Object> budgetScope(UUID scopeId) {
        return jdbc.queryForMap("select model_calls, token_used, token_reserved from task.budget_scope where scope_id = ?", scopeId);
    }

    private java.util.Map<String, Object> spendScope(UUID qualityRunId) {
        return jdbc.queryForMap("select spent_amount, reserved_amount, status, stop_reason from usage.spend_scope "
                + "where tenant_id = ? and workspace_id = ? and scope_type = 'EVALUATION' and scope_id = ?",
                Ids.TENANT_A, Ids.WORKSPACE_A, qualityRunId);
    }

    private void grant(String... actions) {
        for (var action : actions)
            jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, ?, 'ACTIVE') "
                            + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                    Ids.TENANT_A, Ids.WORKSPACE_A, Ids.ALICE, action);
    }
}
