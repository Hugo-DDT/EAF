package io.eaf.workspace.infrastructure;

import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.eaf.workspace.api.WorkspaceGateReceipt;
import io.eaf.workspace.api.WorkspaceOperationalControl;
import java.time.Clock;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Workspace Owner 串行化新任务闸门，并在同一事务写入命令账本与审计事实。 */
@Service
public class JdbcWorkspaceOperationalControl implements WorkspaceOperationalControl {
    private static final String TASK_ADMISSION = "TASK_ADMISSION";
    private static final String BUSINESS_OUTBOUND = "BUSINESS_OUTBOUND";
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization authorization;
    private final AuditPort audit;
    private final Clock clock;
    private final boolean recoveryMode;

    public JdbcWorkspaceOperationalControl(JdbcTemplate jdbc, WorkspaceAuthorization authorization,
                                           AuditPort audit, Clock clock,
                                           @Value("${eaf.recovery.mode:false}") boolean recoveryMode) {
        this.jdbc = jdbc;
        this.authorization = authorization;
        this.audit = audit;
        this.clock = clock;
        this.recoveryMode = recoveryMode;
    }

    @Override
    @Transactional
    public WorkspaceGateReceipt stopTaskAdmission(ActorContext actor, UUID workspaceId,
                                                  String requestKey, boolean enabled, String reason) {
        return changeGate(actor, workspaceId, requestKey, enabled, reason, TASK_ADMISSION);
    }

    @Override
    @Transactional
    public WorkspaceGateReceipt stopBusinessOutbound(ActorContext actor, UUID workspaceId,
                                                    String requestKey, boolean enabled, String reason) {
        return changeGate(actor, workspaceId, requestKey, enabled, reason, BUSINESS_OUTBOUND);
    }

    private WorkspaceGateReceipt changeGate(ActorContext actor, UUID workspaceId, String requestKey,
                                            boolean enabled, String reason, String gate) {
        var normalizedReason = reason == null ? null : reason.strip();
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated())
            throw EafException.forbidden("Workspace 运维闸门只允许本人 HUMAN 操作者执行。");
        if (requestKey == null || requestKey.isBlank() || requestKey.length() > 200
                || normalizedReason == null || normalizedReason.isBlank() || normalizedReason.length() > 500)
            throw EafException.invalid("运维命令需要不超过 200 字符的请求键和 1 至 500 字符的原因。");
        // 恢复屏障只能通过 Owner 的 stop 命令持久化关闭；恢复核对期间拒绝任何重新开放请求。
        if (recoveryMode && enabled)
            throw EafException.conflict("RECOVERY_RECONCILIATION_REQUIRED", "恢复核对完成前不能重新开放 Workspace 运维闸门。");

        // 恢复入口单独授权，不能把停止权限隐式提升为重新开放权限。
        authorization.require(actor, workspaceId, enabled ? "workspace:operations:resume" : "workspace:operations:stop");
        lockWorkspace(actor.tenantId(), workspaceId);

        var keyHash = Hashing.sha256(String.join("\u001f", actor.tenantId().toString(), workspaceId.toString(), requestKey));
        var requestHash = Hashing.sha256(String.join("\u001f", actor.actorId().toString(), gate,
                Boolean.toString(enabled), normalizedReason));
        var commandId = UUID.randomUUID();
        var now = Instant.now(clock);
        var inserted = jdbc.update("insert into workspace.operations_command(command_id, tenant_id, workspace_id, actor_id, request_key_hash, request_hash, gate_name, enabled, reason, created_at) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on conflict (tenant_id, workspace_id, request_key_hash) do nothing",
                commandId, actor.tenantId(), workspaceId, actor.actorId(), keyHash, requestHash, gate,
                enabled, normalizedReason, Timestamp.from(now));
        if (inserted == 0) return replay(actor, workspaceId, keyHash, requestHash, gate);

        var version = jdbc.queryForObject("insert into workspace.operational_gate(tenant_id, workspace_id, gate_name, enabled, changed_by, changed_at, row_version, command_id) "
                        + "values (?, ?, ?, ?, ?, ?, 1, ?) on conflict (tenant_id, workspace_id, gate_name) do update set enabled = excluded.enabled, "
                        + "changed_by = excluded.changed_by, changed_at = excluded.changed_at, row_version = workspace.operational_gate.row_version + 1, command_id = excluded.command_id returning row_version",
                Long.class, actor.tenantId(), workspaceId, gate, enabled, actor.actorId(), Timestamp.from(now), commandId);
        jdbc.update("update workspace.operations_command set applied_version = ? where command_id = ?", version, commandId);
        audit.append(new AuditFact("workspace-ops:" + commandId, actor.tenantId(), workspaceId, actor.actorId(), null,
                "WORKSPACE_OPERATIONAL_GATE_CHANGED", enabled ? "OPEN" : "STOPPED",
                "{\"gate\":\"" + gate + "\",\"enabled\":" + enabled + ",\"version\":" + version
                        + ",\"reasonHash\":\"" + Hashing.sha256(normalizedReason) + "\"}", null));
        return new WorkspaceGateReceipt(commandId, enabled, version, now, false);
    }

    @Override
    @Transactional
    public boolean taskAdmissionOpen(UUID tenantId, UUID workspaceId) {
        return gateOpen(tenantId, workspaceId, TASK_ADMISSION);
    }

    @Override
    @Transactional
    public boolean businessOutboundOpen(UUID tenantId, UUID workspaceId) {
        return gateOpen(tenantId, workspaceId, BUSINESS_OUTBOUND);
    }

    private boolean gateOpen(UUID tenantId, UUID workspaceId, String gate) {
        if (tenantId == null || workspaceId == null) return false;
        // 恢复配置在进程内失败关闭，不改写备份中可能过期的身份、授权与运维事实。
        if (recoveryMode) return false;
        // 共享锁与停止命令对 Workspace 行的排他锁共同建立新任务的闸门线性化顺序。
        var workspaces = jdbc.queryForList("select id from workspace.workspace where tenant_id = ? and id = ? and status = 'ACTIVE' for share",
                UUID.class, tenantId, workspaceId);
        if (workspaces.isEmpty()) return false;
        var enabled = jdbc.query("select enabled from workspace.operational_gate where tenant_id = ? and workspace_id = ? and gate_name = ?",
                rs -> rs.next() ? rs.getBoolean(1) : null, tenantId, workspaceId, gate);
        return enabled == null || enabled;
    }

    private WorkspaceGateReceipt replay(ActorContext actor, UUID workspaceId, String keyHash, String requestHash, String gate) {
        var prior = jdbc.query("select command_id, actor_id, request_hash, gate_name, enabled, applied_version, created_at from workspace.operations_command "
                        + "where tenant_id = ? and workspace_id = ? and request_key_hash = ?",
                rs -> rs.next() ? new StoredCommand(rs.getObject("command_id", UUID.class), rs.getObject("actor_id", UUID.class),
                        rs.getString("request_hash"), rs.getString("gate_name"), rs.getBoolean("enabled"), rs.getLong("applied_version"),
                        rs.getTimestamp("created_at").toInstant()) : null,
                actor.tenantId(), workspaceId, keyHash);
        if (prior == null) throw EafException.conflict("OPERATIONS_COMMAND_CONFLICT", "运维请求键已被并发命令占用。");
        if (!actor.actorId().equals(prior.actorId()) || !requestHash.equals(prior.requestHash()) || !gate.equals(prior.gate()))
            throw EafException.conflict("OPERATIONS_COMMAND_CONFLICT", "相同运维请求键不能绑定不同操作者或参数。");
        return new WorkspaceGateReceipt(prior.commandId(), prior.enabled(), prior.version(), prior.createdAt(), true);
    }

    private void lockWorkspace(UUID tenantId, UUID workspaceId) {
        var workspaces = jdbc.queryForList("select id from workspace.workspace where tenant_id = ? and id = ? and status = 'ACTIVE' for update",
                UUID.class, tenantId, workspaceId);
        if (workspaces.isEmpty()) throw EafException.notFound();
    }

    private record StoredCommand(UUID commandId, UUID actorId, String requestHash, String gate,
                                 boolean enabled, long version, Instant createdAt) { }
}
