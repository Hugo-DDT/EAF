package io.eaf.bootstrap;

import io.eaf.model.infrastructure.ProviderSharedQuota;
import io.eaf.task.infrastructure.TaskClusterCapacityMaintenance;
import java.util.UUID;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;

public final class P20MaintenanceRunner implements ApplicationRunner {
    private static final String OFFLINE_CONFIRMATION = "all-eaf-nodes-stopped-and-http-transports-closed";
    private final Environment environment;
    private final TaskClusterCapacityMaintenance taskCapacity;
    private final ProviderSharedQuota providerQuota;

    public P20MaintenanceRunner(Environment environment, TaskClusterCapacityMaintenance taskCapacity,
                                ProviderSharedQuota providerQuota) {
        this.environment = environment;
        this.taskCapacity = taskCapacity;
        this.providerQuota = providerQuota;
    }

    @Override
    public void run(ApplicationArguments args) {
        var command = environment.getRequiredProperty("eaf.p20.maintenance-command");
        switch (command) {
            case "task-config-show" -> printTaskConfig();
            case "task-config-set" -> updateTaskConfig();
            case "model-pool-set" -> updateModelPool();
            case "model-quarantine-list" -> printQuarantined();
            case "model-quarantine-clear" -> clearQuarantined();
            default -> throw new IllegalArgumentException("未知 maintenance command。");
        }
    }

    private void printTaskConfig() {
        var value = taskCapacity.read();
        System.out.printf("{\"queuedLimit\":%d,\"workspaceQueuedLimit\":%d,\"fairDispatchEnabled\":%s,\"revision\":%d}%n",
                value.queuedLimit(), value.workspaceQueuedLimit(), value.fairDispatchEnabled(), value.revision());
    }

    private void updateTaskConfig() {
        requireOfflineConfirmation();
        var queueLimit = integer("eaf.p20.queued-limit");
        var workspaceLimit = integer("eaf.p20.workspace-queued-limit");
        var fair = booleanValue("eaf.p20.fair-dispatch-enabled");
        if (taskCapacity.hasQueuedOverLimit(queueLimit, workspaceLimit))
            System.out.println("现有 QUEUED 数量高于新上限；新请求将在排空至额度内前被拒绝。");
        var updated = taskCapacity.update(longValue("eaf.p20.expected-revision"), queueLimit, workspaceLimit, fair);
        System.out.printf("{\"queuedLimit\":%d,\"workspaceQueuedLimit\":%d,\"fairDispatchEnabled\":%s,\"revision\":%d}%n",
                updated.queuedLimit(), updated.workspaceQueuedLimit(), updated.fairDispatchEnabled(), updated.revision());
    }

    private void printQuarantined() {
        for (var value : providerQuota.inspectQuarantined())
            System.out.printf("{\"slotNo\":%d,\"leaseId\":\"%s\",\"ownerIncarnation\":\"%s\",\"leaseUntil\":\"%s\"}%n",
                    value.slotNo(), value.leaseId(), value.ownerIncarnation(), value.leaseUntil());
    }

    private void updateModelPool() {
        requireOfflineConfirmation();
        var expected = integer("eaf.p20.expected-model-slots");
        var desired = environment.getRequiredProperty("eaf.model.shared-max-concurrent", Integer.class);
        var updated = providerQuota.resizePool(expected, desired);
        System.out.printf("{\"sharedProviderSlots\":%d}%n", updated);
    }

    private void clearQuarantined() {
        requireOfflineConfirmation();
        var slot = integer("eaf.p20.slot-no");
        var leaseId = UUID.fromString(environment.getRequiredProperty("eaf.p20.lease-id"));
        var operator = environment.getRequiredProperty("eaf.p20.operator");
        var reason = environment.getRequiredProperty("eaf.p20.reason");
        var cleared = providerQuota.clearQuarantined(slot, leaseId, operator,
                "离线确认：所有 EAF 节点已停止且 HTTP 传输已关闭。 " + reason);
        System.out.printf("{\"cleared\":%s,\"slotNo\":%d,\"leaseId\":\"%s\"}%n", cleared, slot, leaseId);
        if (!cleared) throw new IllegalStateException("隔离槽身份已变化或不存在；没有清理其它占用。");
    }

    private void requireOfflineConfirmation() {
        if (!OFFLINE_CONFIRMATION.equals(environment.getProperty("eaf.p20.offline-confirmation")))
            throw new IllegalArgumentException("维护操作需要显式确认所有节点已停止且旧 HTTP 传输已关闭。");
    }

    private int integer(String key) {
        try { return Integer.parseInt(environment.getRequiredProperty(key)); }
        catch (NumberFormatException invalid) { throw new IllegalArgumentException("维护参数必须为整数：" + key); }
    }

    private long longValue(String key) {
        try { return Long.parseLong(environment.getRequiredProperty(key)); }
        catch (NumberFormatException invalid) { throw new IllegalArgumentException("维护参数必须为整数：" + key); }
    }

    private boolean booleanValue(String key) {
        var value = environment.getRequiredProperty(key);
        if ("true".equalsIgnoreCase(value)) return true;
        if ("false".equalsIgnoreCase(value)) return false;
        throw new IllegalArgumentException("维护参数必须为 true 或 false：" + key);
    }
}
