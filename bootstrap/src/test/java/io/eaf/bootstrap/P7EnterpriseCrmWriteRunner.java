package io.eaf.bootstrap;

import io.eaf.execution.api.ExecutionService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import java.util.Set;
import java.util.UUID;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** 子进程只恢复已持久化的 Execution，不生成新的写入意图或 operationId。 */
public final class P7EnterpriseCrmWriteRunner {
    private P7EnterpriseCrmWriteRunner() { }

    public static void main(String[] args) {
        try (var context = new SpringApplicationBuilder(EafApplication.class).run(args)) {
            var environment = context.getEnvironment();
            var workspaceId = UUID.fromString(environment.getRequiredProperty("p7.crm.workspace-id"));
            var executionId = UUID.fromString(environment.getRequiredProperty("p7.crm.execution-id"));
            var actor = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN,
                    Set.of("approval:read", "execution:read", "execution:verify"));
            var executions = context.getBean(ExecutionService.class);
            var result = "verify".equals(environment.getRequiredProperty("p7.crm.action"))
                    ? executions.verify(actor, workspaceId, executionId)
                    : executions.resume(actor, workspaceId, executionId);
            System.out.println("P7_CRM_EXECUTION_RESULT=" + result.status() + "; operationId=" + result.operationId());
        } catch (RuntimeException failed) {
            System.err.println("CRM Execution 子进程失败：" + failed.getClass().getSimpleName());
            System.exit(23);
        }
    }
}
