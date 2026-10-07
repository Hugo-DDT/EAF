package io.eaf.bootstrap;

import io.eaf.evaluation.api.EvaluationService;
import io.eaf.learning.api.FeedbackService;
import io.eaf.learning.api.SubmitFeedbackCommand;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateChildTaskCommand;
import io.eaf.task.api.CreateQualityRunTaskCommand;
import io.eaf.task.api.CreateTaskCommand;
import io.eaf.task.api.TaskService;
import io.eaf.workflow.api.CreateQualityRunWorkflowCommand;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.infrastructure.JdbcWorkflowService;
import io.eaf.workflow.infrastructure.WorkflowDispatcher;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
// 验证服务端质量运行标记贯穿 Task、Workflow 与 Learning 的拒绝边界。
class P7EvaluationRunTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID SEED_WORKFLOW_ID = UUID.fromString("58000000-0000-4000-8000-000000000001");
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

    @Autowired EvaluationService evaluations;
    @Autowired TaskService tasks;
    @Autowired FeedbackService feedback;
    @Autowired WorkflowService workflows;
    @Autowired JdbcWorkflowService workflowStore;
    @Autowired WorkflowDispatcher workflowDispatcher;
    @Autowired JdbcTemplate jdbc;

    @Test
    void registersAndPropagatesTrustedUserQualityRunWithoutLearningFeedback() {
        grant(Ids.ALICE, "evaluation:run", "task:create", "task:read", "feedback:create", "workflow:start");

        var registration = evaluations.registerQualityRun(ALICE, Ids.WORKSPACE_A,
                "CRM_INTEGRATION_ACCEPTANCE", "USER", "p7-08-user-run");
        var replay = evaluations.registerQualityRun(ALICE, Ids.WORKSPACE_A,
                "CRM_INTEGRATION_ACCEPTANCE", "USER", "p7-08-user-run");
        assertThat(replay.id()).isEqualTo(registration.id());
        assertThat(replay.status()).isEqualTo("REGISTERED");
        assertThatThrownBy(() -> evaluations.registerQualityRun(ALICE, Ids.WORKSPACE_A,
                "CRM_INTEGRATION_ACCEPTANCE", "EVALUATION", "p7-08-user-run"))
                .isInstanceOfSatisfying(EafException.class,
                        error -> assertThat(error.code()).isEqualTo("QUALITY_RUN_IDEMPOTENCY_CONFLICT"));

        // 标记只接受 Evaluation 的受权创建 API，子 Task 从父任务证据继承同一运行 ID。
        var marked = tasks.createQualityRunTask(new CreateQualityRunTaskCommand(ALICE, Ids.WORKSPACE_A,
                registration.id(), Ids.AGENT_RISK, "3.0.0", "集成验收输入", null, null,
                "p7-08-marked-task", "trace-p7-08", "USER"));
        assertThat(tasks.evidence(Ids.TENANT_A, Ids.WORKSPACE_A, marked.id()).qualityRunId()).isEqualTo(registration.id());
        assertThatThrownBy(() -> tasks.createQualityRunTask(new CreateQualityRunTaskCommand(BOB, Ids.WORKSPACE_A,
                registration.id(), Ids.AGENT_RISK, "3.0.0", "缺少运行权限", null, null,
                "p7-08-unauthorized-task", "trace-p7-08-denied", "USER")))
                .isInstanceOfSatisfying(EafException.class,
                        error -> assertThat(error.code()).isEqualTo("POLICY_DENIED"));
        var markedWork = tasks.claimOne().orElseThrow();
        assertThat(markedWork.id()).isEqualTo(marked.id());
        var child = tasks.createChild(new CreateChildTaskCommand(ALICE, Ids.WORKSPACE_A, marked.id(),
                "p7-08-child", "子任务继承质量运行标记", "trace-p7-08-child"));
        assertThat(tasks.evidence(Ids.TENANT_A, Ids.WORKSPACE_A, child.id()).qualityRunId()).isEqualTo(registration.id());

        // Workflow 使用专用入口绑定登记 ID；只推进到分析 Task，不运行模型或产生业务写入。
        var seed = workflows.get(ALICE, Ids.WORKSPACE_A, SEED_WORKFLOW_ID, "1.0.0");
        if ("DRAFT".equals(seed.status()))
            seed = workflows.publish(ALICE, Ids.WORKSPACE_A, SEED_WORKFLOW_ID, "1.0.0", seed.rowVersion());
        var instance = workflows.createQualityRunInstance(new CreateQualityRunWorkflowCommand(ALICE,
                Ids.WORKSPACE_A, registration.id(), SEED_WORKFLOW_ID, seed.version(),
                "{\"customerId\":\"synthetic-p7-08\"}", "p7-08-marked-workflow", "USER", null));
        assertThat(instance.qualityRunId()).isEqualTo(registration.id());
        for (var turn = 0; turn < 2; turn++) {
            jdbc.update("update workflow.instance set next_poll_at = null where id = ?", instance.id());
            var lease = workflowStore.claimOne().orElseThrow();
            assertThat(lease.instanceId()).isEqualTo(instance.id());
            workflowDispatcher.advance(lease);
        }
        var advanced = workflows.getInstance(ALICE, Ids.WORKSPACE_A, instance.id());
        assertThat(advanced.childTaskId()).isNotNull();
        assertThat(tasks.evidence(Ids.TENANT_A, Ids.WORKSPACE_A, advanced.childTaskId()).qualityRunId())
                .isEqualTo(registration.id());

        // Learning 通过 Task 的公开 Evidence API 拒绝验收反馈；普通 USER Task 仍保持无标记。
        assertThatThrownBy(() -> feedback.submit(new SubmitFeedbackCommand(ALICE, Ids.WORKSPACE_A,
                marked.id(), "验收样本不作为学习事实", "质量运行登记 p7-08-user-run", null,
                "p7-08-feedback-denied")))
                .isInstanceOfSatisfying(EafException.class,
                        error -> assertThat(error.code()).isEqualTo("POLICY_DENIED"));
        var ordinary = tasks.create(new CreateTaskCommand(ALICE, Ids.WORKSPACE_A, Ids.AGENT_RISK, "3.0.0",
                "普通 USER 任务不进入质量运行", null, null, "p7-08-ordinary-task", "trace-p7-08-ordinary", "USER"));
        assertThat(tasks.evidence(Ids.TENANT_A, Ids.WORKSPACE_A, ordinary.id()).qualityRunId()).isNull();
        assertThat(markedWork.qualityRunId()).isEqualTo(registration.id());
    }

    private void grant(UUID actorId, String... actions) {
        for (var action : actions)
            jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, ?, 'ACTIVE') "
                            + "on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                    Ids.TENANT_A, Ids.WORKSPACE_A, actorId, action);
    }
}
