package io.eaf.bootstrap;

import io.eaf.capability.api.CapabilityService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.CreateWorkflowTaskCommand;
import io.eaf.task.api.TaskAssetBinding;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import io.eaf.workflow.api.WorkflowService;
import io.eaf.workflow.infrastructure.JdbcWorkflowService;
import io.eaf.workflow.infrastructure.WorkflowDispatcher;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** Workflow 崩溃检查子进程：可在三个持久化边界暂停，也可从已取消状态启动新 JVM 收敛。 */
public final class P6WorkflowCrashRunner {
    private static final ActorContext ALICE = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());

    private P6WorkflowCrashRunner() { }

    public static void main(String[] args) throws InterruptedException {
        try (var context = new SpringApplicationBuilder(EafApplication.class).run(args)) {
            var environment = context.getEnvironment();
            var instanceId = UUID.fromString(environment.getRequiredProperty("p6.workflow.instance-id"));
            var workflows = context.getBean(WorkflowService.class);
            var action = environment.getRequiredProperty("p6.workflow.action");
            switch (action) {
                case "after-intent" -> {
                    require(context.getBean(WorkflowDispatcher.class).dispatchOne(), "未能领取 Workflow 实例");
                    checkpoint("after-intent");
                }
                case "after-create" -> {
                    createChild(context, workflows, instanceId);
                    checkpoint("after-create");
                }
                case "after-child-result" -> {
                    var dispatcher = context.getBean(WorkflowDispatcher.class);
                    require(dispatcher.dispatchOne(), "未能对账 Workflow 子 Task");
                    var tasks = context.getBean(TaskService.class);
                    var work = tasks.claimOne().orElseThrow(() -> new IllegalStateException("无可执行子 Task"));
                    tasks.complete(work, context.getBean(TaskRunner.class).run(work));
                    checkpoint("after-child-result");
                }
                case "resume" -> {
                    var dispatcher = context.getBean(WorkflowDispatcher.class);
                    for (int i = 0; i < 30; i++) {
                        dispatcher.dispatchOne();
                        var instance = workflows.getInstance(ALICE, UUID.fromString(
                                environment.getRequiredProperty("p6.workflow.workspace-id")), instanceId);
                        // 取消/超时目标也是恢复终态，不能让新进程把已结束流程误报为恢复超时。
                        if (Set.of("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT").contains(instance.status())) {
                            System.out.println("P6_WORKFLOW_RESULT=" + instance.status());
                            return;
                        }
                        Thread.sleep(250);
                    }
                    throw new IllegalStateException("Workflow 恢复未在时限内进入终态");
                }
                default -> throw new IllegalArgumentException("未知 Workflow 崩溃检查动作");
            }
        }
    }

    private static void createChild(org.springframework.context.ConfigurableApplicationContext context,
                                    WorkflowService workflows, UUID instanceId) {
        var workspaceId = UUID.fromString(context.getEnvironment().getRequiredProperty("p6.workflow.workspace-id"));
        var instance = workflows.getInstance(ALICE, workspaceId, instanceId);
        var steps = context.getBean(JdbcWorkflowService.class);
        var step = steps.stepRuntime(instanceId, "analyze");
        var capabilities = context.getBean(CapabilityService.class);
        var capability = capabilities.requirePublished(ALICE, workspaceId,
                UUID.fromString("54000000-0000-4000-8000-000000000001"), "1.0.0");
        var binding = new TaskAssetBinding(capability.id(), capability.version(), capability.contentHash(),
                capability.skillId(), capability.skillVersion(), capability.skillContentHash());
        // 崩溃后重放必须重建 Dispatcher 原样的 provenance，幂等请求哈希才会与已存意图一致。
        var command = new CreateWorkflowTaskCommand(ALICE, workspaceId, instance.rootBudgetScopeId(),
                instance.rootTaskId(), step.dispatchKey(), capability.agentId(), capability.agentVersion(),
                step.inputJson(), instance.source(), binding, null, null, null,
                "workflow:" + instance.id() + "@" + instance.workflowVersion() + ":analyze", instance.qualityRunId(),
                new io.eaf.task.api.WorkflowTaskProvenance(instance.id(), instance.workflowId(),
                        instance.workflowVersion(), "analyze"));
        context.getBean(TaskService.class).createWorkflowTask(command);
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    private static void checkpoint(String name) throws InterruptedException {
        System.out.println("P6_WORKFLOW_CHECKPOINT=" + name);
        System.out.flush();
        new CountDownLatch(1).await();
    }
}
