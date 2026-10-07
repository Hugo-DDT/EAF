package io.eaf.model.infrastructure;

import io.eaf.bootstrap.EafApplication;
import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** 配额子进程只用于占有/释放一个测试 HTTP 槽，不携带 Provider 凭据或发送外部请求。 */
public final class P20QuotaWorkerRunner {
    private P20QuotaWorkerRunner() { }

    public static void main(String[] args) throws Exception {
        try (var context = new SpringApplicationBuilder(EafApplication.class).run(args)) {
            var environment = context.getEnvironment();
            var quota = context.getBean(ProviderSharedQuota.class);
            if ("hold".equals(environment.getRequiredProperty("p20.quota.action"))) {
                var releaseFile = Path.of(environment.getRequiredProperty("p20.quota.release-file"));
                var lease = quota.tryAcquire();
                System.out.println("P20_QUOTA_HELD=" + lease.slotNo() + ";" + lease.leaseId());
                System.out.flush();
                var deadline = System.nanoTime() + java.time.Duration.ofMinutes(2).toNanos();
                while (!Files.exists(releaseFile) && System.nanoTime() < deadline) Thread.sleep(25);
                if (!Files.exists(releaseFile)) throw new IllegalStateException("配额测试释放闸门超时");
                quota.releaseAfterTransportClosed(lease);
                System.out.println("P20_QUOTA_RELEASED=true");
                return;
            }
            try {
                quota.tryAcquire();
                throw new IllegalStateException("第二个 JVM 越过了共享 Provider 槽上限");
            } catch (ProviderCapacityExceededException full) {
                System.out.println("P20_QUOTA_FULL=true");
            }
        }
    }
}
