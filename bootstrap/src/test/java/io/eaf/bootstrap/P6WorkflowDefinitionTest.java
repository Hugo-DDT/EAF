package io.eaf.bootstrap;

import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Ids;
import io.eaf.workflow.api.CreateWorkflowCommand;
import io.eaf.workflow.api.CreateWorkflowInstanceCommand;
import io.eaf.workflow.api.CreateWorkflowVersionCommand;
import io.eaf.workflow.api.WorkflowDefinition;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.api.WorkflowStepSpec;
import io.eaf.workflow.api.WorkflowStepType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@SpringBootTest(properties = {"eaf.task.dispatcher-enabled=false", "eaf.workflow.dispatcher-enabled=false", "eaf.execution.outbox-publisher-enabled=false"})
// 测试结束关闭调度器和连接池，避免容器数据库停止后后台任务继续访问。
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
// 本用例用真实 PostgreSQL 验证 Workflow 迁移、版本约束、幂等和拒绝路径。
class P6WorkflowDefinitionTest {
    private static final String POSTGRES_IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final UUID SEED_ID = UUID.fromString("58000000-0000-4000-8000-000000000001");
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());
    private static final ActorContext BOB = new ActorContext(Ids.BOB, Ids.TENANT_A, ActorType.HUMAN, Set.of());

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(POSTGRES_IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("eaf.security.mode", () -> "local");
    }

    @Autowired WorkflowService workflows;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;

    @Test
    @Transactional
    void seedPublishesImmutableVersionAndCreatesOneFrozenIdempotentInstance() {
        var draft = workflows.get(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0");
        assertThat(draft.status()).isEqualTo("DRAFT");
        assertThat(draft.dependencies()).hasSize(1);
        assertThat(draft.steps()).extracting(WorkflowStepSpec::type)
                .containsExactly(WorkflowStepType.RUN_CAPABILITY, WorkflowStepType.BRANCH,
                        WorkflowStepType.RUN_TOOL, WorkflowStepType.COMPLETE, WorkflowStepType.COMPLETE);

        var published = workflows.publish(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0", draft.rowVersion());
        assertThat(published.status()).isEqualTo("PUBLISHED");
        assertThat(published.contentHash()).matches("[0-9a-f]{64}");
        var input = "{\"customerId\":\"C-100\"}";
        var created = workflows.createInstance(new CreateWorkflowInstanceCommand(ALICE, Ids.WORKSPACE_A,
                SEED_ID, "1.0.0", input, "workflow-create-once", "USER"));
        var scopeCount = jdbc.queryForObject("select count(*) from task.budget_scope where created_by = ?", Integer.class, Ids.ALICE);
        assertThat(scopeCount).isEqualTo(1);
        assertThat(created.status()).isEqualTo("QUEUED");
        assertThat(created.rootBudgetScopeId()).isNotNull();
        assertThat(jdbc.queryForObject("select root_task_id is null from task.budget_scope where scope_id = ?", Boolean.class,
                created.rootBudgetScopeId())).isTrue();

        var replay = workflows.createInstance(new CreateWorkflowInstanceCommand(ALICE, Ids.WORKSPACE_A,
                SEED_ID, "1.0.0", "{ \"customerId\" : \"C-100\" }", "workflow-create-once", "USER"));
        assertThat(replay.id()).isEqualTo(created.id());
        assertThat(replay.rootBudgetScopeId()).isEqualTo(created.rootBudgetScopeId());
        assertThat(jdbc.queryForObject("select count(*) from task.budget_scope where created_by = ?", Integer.class, Ids.ALICE)).isEqualTo(1);
        assertThatThrownBy(() -> workflows.createInstance(new CreateWorkflowInstanceCommand(ALICE, Ids.WORKSPACE_A,
                SEED_ID, "1.0.0", "{\"customerId\":\"C-200\"}", "workflow-create-once", "USER")))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("IDEMPOTENCY_CONFLICT"));
        assertThatThrownBy(() -> workflows.createInstance(new CreateWorkflowInstanceCommand(ALICE, Ids.WORKSPACE_A,
                SEED_ID, "1.0.0", "{\"customerId\":\"C-100\",\"extra\":true}", "workflow-invalid-input", "USER")))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("INVALID_REQUEST"));

        var snapshot = workflows.getInstance(ALICE, Ids.WORKSPACE_A, created.id());
        assertThat(snapshot.definitionHash()).isEqualTo(published.contentHash());
        assertThat(snapshot.definitionSnapshot()).contains("1.0.0");
        assertThat(snapshot.dependencySnapshot()).contains("customer-risk-followup");

        var nested = new TransactionTemplate(transactionManager);
        nested.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
        // 嵌套事务用保存点回滚触发器拒绝，避免 PostgreSQL 将外层集成事务留在 aborted 状态。
        assertThatThrownBy(() -> nested.execute(status -> jdbc.update(
                "update workflow.version set steps_json = '[]'::jsonb where workflow_id = ? and asset_version = '1.0.0'", SEED_ID)))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> nested.execute(status -> jdbc.update(
                "insert into workflow.capability_dependency(tenant_id, workspace_id, workflow_id, workflow_version, capability_id, capability_version) values (?, ?, ?, '1.0.0', ?, '1.0.0')",
                Ids.TENANT_A, Ids.WORKSPACE_A, SEED_ID, UUID.randomUUID())))
                .isInstanceOf(RuntimeException.class);
        // V102 已预置 1.1.0 协作 Workflow，本用例在更高版本验证发布后定义不可变。
        var nextDraft = workflows.addVersion(ALICE, Ids.WORKSPACE_A, SEED_ID,
                new CreateWorkflowVersionCommand("1.2.0", published.inputSchema(), published.outputSchema(),
                        published.entryStepId(), published.steps()));
        var next = workflows.publish(ALICE, Ids.WORKSPACE_A, SEED_ID, nextDraft.version(), nextDraft.rowVersion());
        assertThat(next.contentHash()).isNotEqualTo(published.contentHash());
        assertThat(workflows.getInstance(ALICE, Ids.WORKSPACE_A, created.id()).workflowVersion()).isEqualTo("1.0.0");

        var withdrawn = workflows.withdraw(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0", published.rowVersion());
        assertThat(withdrawn.status()).isEqualTo("WITHDRAWN");
        assertThat(workflows.getInstance(ALICE, Ids.WORKSPACE_A, created.id()).definitionHash()).isEqualTo(published.contentHash());
        assertThatThrownBy(() -> workflows.requireRunnable(ALICE, Ids.WORKSPACE_A, created.id()))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("WORKFLOW_WITHDRAWN"));
        assertThatThrownBy(() -> workflows.createInstance(new CreateWorkflowInstanceCommand(ALICE, Ids.WORKSPACE_A,
                SEED_ID, "1.0.0", input, "workflow-after-withdraw", "USER")))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(404));
    }

    @Test
    @Transactional
    void invalidGraphsMappingsDependenciesAndToolsCannotPublish() {
        var seed = workflows.get(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0");
        assertThatThrownBy(() -> workflows.create(new CreateWorkflowCommand(ALICE, Ids.WORKSPACE_A,
                "test-schema-" + UUID.randomUUID(), "Schema 边界验证",
                new CreateWorkflowVersionCommand("1.0.0",
                        "{\"type\":\"object\",\"required\":[\"customerId\"],\"additionalProperties\":false,\"properties\":{\"customerId\":{\"type\":\"string\",\"pattern\":\"^C-\"}}}",
                        seed.outputSchema(), seed.entryStepId(), seed.steps()))))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo("INVALID_REQUEST"));

        var cyclic = copy(seed.steps());
        cyclic.set(1, branch(cyclic.get(1), "risk-branch", "analyze", "complete-no-action"));
        rejectDraft("cycle", seed, cyclic, "INVALID_REQUEST");

        var unreachable = copy(seed.steps());
        unreachable.add(new WorkflowStepSpec("never", WorkflowStepType.COMPLETE, null, null, null, null, null,
                Map.of(), null, null, null, null, Map.of("riskLevel", "$.input.customerId",
                "summary", "$.input.customerId", "followupCreated", "literal:false")));
        rejectDraft("unreachable", seed, unreachable, "INVALID_REQUEST");

        var badMapping = copy(seed.steps());
        badMapping.set(0, execution(badMapping.get(0), "analyze", "risk-branch", null,
                Map.of("input", "$.steps.create-followup.output.externalId"), null, null, null));
        rejectDraft("mapping", seed, badMapping, "INVALID_REQUEST");

        var unpublished = copy(seed.steps());
        unpublished.set(0, execution(unpublished.get(0), "analyze", "risk-branch", "9.9.9", null, null, null, null));
        rejectDraft("unpublished-dependency", seed, unpublished, "RESOURCE_NOT_FOUND");

        var unauthorizedTool = copy(seed.steps());
        unauthorizedTool.set(2, execution(unauthorizedTool.get(2), "create-followup", "complete-created", null,
                null, null, "crm.admin.delete", "1.0.0"));
        rejectDraft("unauthorized-tool", seed, unauthorizedTool, "POLICY_DENIED");
    }

    @Test
    @Transactional
    void onlyOwnerCanPublish() {
        var seed = workflows.get(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0");
        jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, 'workflow:publish', 'ACTIVE') on conflict (workspace_id, actor_id, action) do update set status = 'ACTIVE'",
                Ids.TENANT_A, Ids.WORKSPACE_A, Ids.BOB);
        assertThatThrownBy(() -> workflows.publish(BOB, Ids.WORKSPACE_A, SEED_ID, "1.0.0", seed.rowVersion()))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(403));
        var published = workflows.publish(ALICE, Ids.WORKSPACE_A, SEED_ID, "1.0.0", seed.rowVersion());
        assertThatThrownBy(() -> workflows.withdraw(BOB, Ids.WORKSPACE_A, SEED_ID, "1.0.0", published.rowVersion()))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.status()).isEqualTo(403));
    }

    private void rejectDraft(String suffix, WorkflowDefinition seed, List<WorkflowStepSpec> steps, String code) {
        var draft = workflows.create(new CreateWorkflowCommand(ALICE, Ids.WORKSPACE_A,
                "test-" + suffix + "-" + UUID.randomUUID(), "验证草稿",
                new CreateWorkflowVersionCommand("1.0.0", seed.inputSchema(), seed.outputSchema(), seed.entryStepId(), steps)));
        assertThatThrownBy(() -> workflows.publish(ALICE, Ids.WORKSPACE_A, draft.id(), draft.version(), draft.rowVersion()))
                .isInstanceOfSatisfying(EafException.class, error -> assertThat(error.code()).isEqualTo(code));
        assertThat(workflows.get(ALICE, Ids.WORKSPACE_A, draft.id(), draft.version()).status()).isEqualTo("DRAFT");
    }

    private List<WorkflowStepSpec> copy(List<WorkflowStepSpec> input) { return new ArrayList<>(input); }

    private WorkflowStepSpec execution(WorkflowStepSpec step, String id, String next, String capabilityVersion,
                                       Map<String, String> inputMapping, String conditionPath,
                                       String toolName, String toolVersion) {
        return new WorkflowStepSpec(id, step.type(), next, step.capabilityId(),
                capabilityVersion == null ? step.capabilityVersion() : capabilityVersion,
                toolName == null ? step.toolName() : toolName,
                toolVersion == null ? step.toolVersion() : toolVersion,
                inputMapping == null ? step.inputMapping() : inputMapping,
                conditionPath == null ? step.conditionPath() : conditionPath, step.conditionValue(),
                step.whenTrueStepId(), step.whenFalseStepId(), step.outputMapping());
    }

    private WorkflowStepSpec branch(WorkflowStepSpec step, String id, String whenTrue, String whenFalse) {
        return new WorkflowStepSpec(id, step.type(), step.nextStepId(), step.capabilityId(), step.capabilityVersion(),
                step.toolName(), step.toolVersion(), step.inputMapping(), step.conditionPath(), step.conditionValue(),
                whenTrue, whenFalse, step.outputMapping());
    }
}
// 本文件负责实现 WorkflowDefinitionTest.java 相关代码。
