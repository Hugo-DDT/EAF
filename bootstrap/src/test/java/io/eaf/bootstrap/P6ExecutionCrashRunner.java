package io.eaf.bootstrap;

import io.eaf.execution.api.ExecutionService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import io.eaf.task.api.TaskRunner;
import io.eaf.task.api.TaskService;
import java.util.Set;
import java.util.UUID;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** 子 JVM 只执行一个已持久化 Execution 动作，父测试可在外部副作用边界终止它。 */
public final class P6ExecutionCrashRunner {
    private P6ExecutionCrashRunner() { }

    public static void main(String[] args) {
        try (var context = new SpringApplicationBuilder(EafApplication.class).run(args)) {
            var environment = context.getEnvironment();
            var workspaceId = UUID.fromString(environment.getRequiredProperty("p6.execution.workspace-id"));
            if ("tool".equals(environment.getRequiredProperty("p6.execution.action"))) {
                var taskId = UUID.fromString(environment.getRequiredProperty("p6.task.id"));
                var tasks = context.getBean(TaskService.class);
                var runtime = context.getBean(TaskRunner.class);
                var work = tasks.claimOne().orElseThrow();
                if (!taskId.equals(work.id())) throw new IllegalStateException("远端 Task 未被预期 Worker 领取。");
                var outcome = runtime.run(work);
                tasks.complete(work, outcome);
                System.out.println("P6_TASK_RESULT=" + outcome.status());
                return;
            }
            var executionId = UUID.fromString(environment.getRequiredProperty("p6.execution.id"));
            var actor = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN,
                    Set.of("execution:read", "execution:verify", "approval:read"));
            var executions = context.getBean(ExecutionService.class);
            var result = "verify".equals(environment.getRequiredProperty("p6.execution.action"))
                    ? executions.verify(actor, workspaceId, executionId)
                    : executions.resume(actor, workspaceId, executionId);
            System.out.println("P6_EXECUTION_RESULT=" + result.status() + "; operationId=" + result.operationId());
        } catch (RuntimeException failed) {
            System.err.println("Execution 子进程终止：" + failed.getClass().getSimpleName());
            System.exit(23);
        }
    }
}
