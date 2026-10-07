package io.eaf.task.infrastructure;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Task Owner 的离线集群容量读取与 revision 条件维护，不暴露租户 REST 写入口。 */
@Component
public final class TaskClusterCapacityMaintenance {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final int configuredQueueLimit;
    private final int configuredWorkspaceLimit;
    private final boolean configuredFairDispatch;
    private final boolean maintenanceMode;

    public TaskClusterCapacityMaintenance(JdbcTemplate jdbc, PlatformTransactionManager transactionManager,
            @Value("${eaf.task.max-queued:256}") int queueLimit,
            @Value("${eaf.task.max-queued-per-workspace:${eaf.task.max-queued:256}}") int workspaceLimit,
            @Value("${eaf.task.fair-dispatch-enabled:true}") boolean fairDispatch,
            @Value("${eaf.p20.maintenance-command:}") String maintenanceCommand) {
        this.jdbc = jdbc;
        this.configuredQueueLimit = queueLimit;
        this.configuredWorkspaceLimit = workspaceLimit;
        this.configuredFairDispatch = fairDispatch;
        this.maintenanceMode = maintenanceCommand != null && !maintenanceCommand.isBlank();
        this.transactions = new TransactionTemplate(transactionManager);
        this.transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @PostConstruct
    void initialize() {
        if (configuredQueueLimit <= 0 || configuredWorkspaceLimit <= 0 || configuredWorkspaceLimit > configuredQueueLimit)
            throw new IllegalStateException("EAF Task 集群队列上限配置无效。");
        transactions.executeWithoutResult(status -> {
            jdbc.query("select pg_advisory_xact_lock(hashtext('eaf.task.cluster.capacity.v1'), 0)",
                    rs -> { if (rs.next()) rs.getObject(1); return null; });
            jdbc.update("insert into task.cluster_capacity_config(singleton_id, queued_limit, workspace_queued_limit, fair_dispatch_enabled) "
                            + "values (1, ?, ?, ?) on conflict (singleton_id) do nothing",
                    configuredQueueLimit, configuredWorkspaceLimit, configuredFairDispatch);
        });
        if (!maintenanceMode) requireRuntimeConfiguration();
    }

    public void requireRuntimeConfiguration() {
        var shared = read();
        if (shared == null || shared.queuedLimit() != configuredQueueLimit
                || shared.workspaceQueuedLimit() != configuredWorkspaceLimit
                || shared.fairDispatchEnabled() != configuredFairDispatch)
            throw new IllegalStateException("EAF Task 集群队列配置与数据库共享配置不一致；停止所有节点后使用维护命令调整。");
    }

    public CapacityConfig read() {
        return jdbc.query("select queued_limit, workspace_queued_limit, fair_dispatch_enabled, revision "
                        + "from task.cluster_capacity_config where singleton_id = 1",
                rs -> rs.next() ? new CapacityConfig(rs.getInt("queued_limit"), rs.getInt("workspace_queued_limit"),
                        rs.getBoolean("fair_dispatch_enabled"), rs.getLong("revision")) : null);
    }

    public CapacityConfig update(long expectedRevision, int queuedLimit, int workspaceQueuedLimit, boolean fairDispatchEnabled) {
        if (expectedRevision <= 0 || queuedLimit <= 0 || workspaceQueuedLimit <= 0 || workspaceQueuedLimit > queuedLimit)
            throw new IllegalArgumentException("集群队列上限必须为正数且工作区上限不大于总上限。");
        var changed = transactions.execute(status -> jdbc.update("update task.cluster_capacity_config set queued_limit = ?, "
                        + "workspace_queued_limit = ?, fair_dispatch_enabled = ?, revision = revision + 1, updated_at = now() "
                        + "where singleton_id = 1 and revision = ?",
                queuedLimit, workspaceQueuedLimit, fairDispatchEnabled, expectedRevision));
        if (changed == null || changed != 1) throw new IllegalStateException("集群容量 revision 已变化；重新读取后再操作。");
        return read();
    }

    public boolean hasQueuedOverLimit(int queuedLimit, int workspaceQueuedLimit) {
        var total = jdbc.queryForObject("select count(*) from task.task where status = 'QUEUED'", Long.class);
        var tooManyWorkspaces = jdbc.queryForObject("select exists(select 1 from task.task where status = 'QUEUED' "
                + "group by tenant_id, workspace_id having count(*) > ?)", Boolean.class, workspaceQueuedLimit);
        return total != null && total > queuedLimit || Boolean.TRUE.equals(tooManyWorkspaces);
    }

    public record CapacityConfig(int queuedLimit, int workspaceQueuedLimit, boolean fairDispatchEnabled, long revision) { }
}
