package io.eaf.bootstrap;

import io.eaf.usage.api.ReserveSpendCommand;
import io.eaf.usage.api.UsageRecorder;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** 子 JVM 只按固定调用键尝试金额预留，用于验证数据库状态跨进程恢复。 */
public final class P7SpendBudgetRunner {
    private P7SpendBudgetRunner() { }

    public static void main(String[] args) {
        try (var context = new SpringApplicationBuilder(EafApplication.class).run(args)) {
            var environment = context.getEnvironment();
            var command = new ReserveSpendCommand(
                    UUID.fromString(environment.getProperty("p7.spend.tenant-id",
                            "70000000-0000-4000-8000-000000000001")),
                    UUID.fromString(environment.getProperty("p7.spend.workspace-id",
                            "10000000-0000-4000-8000-000000000001")),
                    "TASK",
                    UUID.fromString(environment.getRequiredProperty("p7.spend.scope-id")),
                    environment.getRequiredProperty("p7.spend.call-key"),
                    "fixture-provider",
                    environment.getRequiredProperty("p7.spend.model"),
                    "CHAT",
                    100,
                    new BigDecimal("0.10"),
                    "USD");
            var reservation = context.getBean(UsageRecorder.class).reserveSpend(command);
            System.out.println("P7_SPEND_RESERVATION=" +
                    (reservation.allowed() ? "ALLOWED" : reservation.code()));
            System.out.flush();
        } catch (RuntimeException failed) {
            // 子进程只输出安全错误类型，避免把数据库配置或连接信息传播到测试证据。
            System.err.println("Usage 子进程失败：" + failed.getClass().getSimpleName());
            System.exit(24);
        }
    }
}
