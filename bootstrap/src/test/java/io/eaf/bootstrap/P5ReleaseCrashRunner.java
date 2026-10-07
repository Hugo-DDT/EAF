package io.eaf.bootstrap;

import io.eaf.learning.api.CandidateService;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.Ids;
import java.util.Set;
import java.util.UUID;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** 子 JVM 只执行一次候选发布；用于验证进程退出后从持久意图恢复。 */
public final class P5ReleaseCrashRunner {
    private P5ReleaseCrashRunner() { }

    public static void main(String[] args) {
        var exitCode = 0;
        try (var context = new SpringApplicationBuilder(EafApplication.class)
                .run(args)) {
            var environment = context.getEnvironment();
            var candidateId = UUID.fromString(environment.getRequiredProperty("p5.release.candidate-id"));
            var approvalId = UUID.fromString(environment.getRequiredProperty("p5.release.approval-id"));
            var workspaceId = UUID.fromString(environment.getRequiredProperty("p5.release.workspace-id"));
            var expectedVersion = environment.getRequiredProperty("p5.release.expected-version", Long.class);
            var actor = new ActorContext(Ids.ALICE, Ids.TENANT_A, ActorType.HUMAN, Set.of());
            var candidates = context.getBean(CandidateService.class);
            if ("withdraw".equals(environment.getProperty("p5.release.action"))) {
                candidates.withdrawRelease(new CandidateService.CandidateWithdrawalCommand(actor, workspaceId,
                        candidateId, expectedVersion, "test:eaf-p5-withdrawal"));
            } else {
                candidates.publish(new CandidateService.CandidateReleaseCommand(
                        actor, workspaceId, candidateId, expectedVersion, approvalId));
            }
        } catch (RuntimeException failed) {
            // 首次运行由测试安装的 PostgreSQL 故障点触发；非零退出表示进程已结束且未补记 Learning。
            System.err.println("候选发布子进程终止：" + failed.getClass().getSimpleName());
            exitCode = 23;
        }
        if (exitCode != 0) System.exit(exitCode);
    }
}
