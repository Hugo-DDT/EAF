package io.eaf.workspace.infrastructure;

import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.organization.api.OrganizationDirectory;
import io.eaf.workspace.api.WorkspaceAccess;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JdbcWorkspaceAuthorization implements WorkspaceAuthorization {
    private final JdbcTemplate jdbc;
    private final OrganizationDirectory organizations;

    public JdbcWorkspaceAuthorization(JdbcTemplate jdbc, OrganizationDirectory organizations) {
        this.jdbc = jdbc;
        this.organizations = organizations;
    }

    @Override
    public WorkspaceAccess require(ActorContext actor, UUID workspaceId, String action) {
        if (actor == null || !isCurrentMember(actor.tenantId(), actor.actorId())
                || actor.delegated() && !isCurrentMember(actor.tenantId(), actor.principalId()))
            throw EafException.notFound();
        var workspace = jdbc.query("select id, tenant_id, name from workspace.workspace where id = ? and tenant_id = ? and status = 'ACTIVE'",
                rs -> rs.next() ? new WorkspaceAccess(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getString("name")) : null,
                workspaceId, actor.tenantId());
        if (workspace == null) throw EafException.notFound();
        if (isPersonalForAnotherActor(actor.tenantId(), workspaceId, actor.actorId())
                || isPersonal(workspaceId) && (actor.delegated() || actor.type() != io.eaf.shared.ActorType.HUMAN))
            throw EafException.notFound();
        if (actor.delegated()) {
            // 委托身份只在固定 Workspace 内可见，并同时满足 Owner、Agent 和不可变动作范围。
            if (!workspaceId.equals(actor.delegationWorkspaceId())) throw EafException.notFound();
            var visible = jdbc.queryForObject("select count(distinct actor_id) from workspace.\"grant\" where tenant_id = ? and workspace_id = ? and actor_id in (?, ?) and status = 'ACTIVE'",
                    Integer.class, actor.tenantId(), workspaceId, actor.principalId(), actor.actorId());
            if (visible == null || visible != 2) throw EafException.notFound();
            if (!actor.can(action) || !isAuthorized(actor.tenantId(), actor.principalId(), workspaceId, action)
                    || !isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, action))
                throw EafException.forbidden("当前委托未获准执行动作 " + action + "。");
            return workspace;
        }
        var visible = jdbc.queryForObject("select count(*) from workspace.\"grant\" where workspace_id = ? and actor_id = ? and status = 'ACTIVE'",
                Integer.class, workspaceId, actor.actorId());
        if (visible == null || visible == 0) throw EafException.notFound();
        var allowed = jdbc.queryForObject("select count(*) from workspace.\"grant\" where workspace_id = ? and actor_id = ? and action = ? and status = 'ACTIVE'",
                Integer.class, workspaceId, actor.actorId(), action);
        if (allowed == null || allowed == 0) throw EafException.forbidden("当前身份没有动作 " + action + " 的权限。");
        return workspace;
    }

    @Override
    public boolean isAuthorized(UUID tenantId, UUID actorId, UUID workspaceId, String action) {
        if (!isCurrentMember(tenantId, actorId)) return false;
        var count = jdbc.queryForObject("select count(*) from workspace.workspace w join workspace.\"grant\" g on g.workspace_id = w.id and g.tenant_id = w.tenant_id left join workspace.workspace_profile p on p.tenant_id = w.tenant_id and p.workspace_id = w.id where w.id = ? and w.tenant_id = ? and w.status = 'ACTIVE' and g.actor_id = ? and g.action = ? and g.status = 'ACTIVE' and (p.kind is null or p.kind <> 'PERSONAL' or p.owner_id = g.actor_id)",
                Integer.class, workspaceId, tenantId, actorId, action);
        return count != null && count > 0;
    }

    @Override
    public java.util.Set<String> actions(UUID tenantId, UUID actorId) {
        if (!isCurrentMember(tenantId, actorId)) return java.util.Set.of();
        return java.util.Set.copyOf(jdbc.queryForList("select distinct g.action from workspace.\"grant\" g join workspace.workspace w on w.tenant_id = g.tenant_id and w.id = g.workspace_id left join workspace.workspace_profile p on p.tenant_id = w.tenant_id and p.workspace_id = w.id where g.tenant_id = ? and g.actor_id = ? and g.status = 'ACTIVE' and w.status = 'ACTIVE' and (p.kind is null or p.kind <> 'PERSONAL' or p.owner_id = g.actor_id)",
                String.class, tenantId, actorId));
    }

    @Override
    public java.util.Set<String> actions(UUID tenantId, UUID actorId, UUID workspaceId) {
        if (!isCurrentMember(tenantId, actorId)) return java.util.Set.of();
        return java.util.Set.copyOf(jdbc.queryForList("select g.action from workspace.\"grant\" g join workspace.workspace w on w.tenant_id = g.tenant_id and w.id = g.workspace_id left join workspace.workspace_profile p on p.tenant_id = w.tenant_id and p.workspace_id = w.id where g.tenant_id = ? and g.actor_id = ? and g.workspace_id = ? and g.status = 'ACTIVE' and w.status = 'ACTIVE' and (p.kind is null or p.kind <> 'PERSONAL' or p.owner_id = g.actor_id)",
                String.class, tenantId, actorId, workspaceId));
    }

    @Override
    public List<WorkspaceAccess> list(ActorContext actor, String action) {
        if (actor == null || !isCurrentMember(actor.tenantId(), actor.actorId())) return List.of();
        if (actor.delegated()) {
            if (!isCurrentMember(actor.tenantId(), actor.principalId()) || !actor.can(action)
                    || !isAuthorized(actor.tenantId(), actor.principalId(), actor.delegationWorkspaceId(), action)
                    || !isAuthorized(actor.tenantId(), actor.actorId(), actor.delegationWorkspaceId(), action)) return List.of();
            return jdbc.query("select id, tenant_id, name from workspace.workspace where id = ? and tenant_id = ? and status = 'ACTIVE'",
                    (rs, row) -> new WorkspaceAccess(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getString("name")),
                    actor.delegationWorkspaceId(), actor.tenantId());
        }
        return jdbc.query("select distinct w.id, w.tenant_id, w.name from workspace.workspace w join workspace.\"grant\" g on g.workspace_id = w.id left join workspace.workspace_profile p on p.tenant_id = w.tenant_id and p.workspace_id = w.id where w.tenant_id = ? and g.actor_id = ? and g.action = ? and g.status = 'ACTIVE' and w.status = 'ACTIVE' and (p.kind is null or p.kind <> 'PERSONAL' or p.owner_id = g.actor_id) order by w.id",
                (rs, row) -> new WorkspaceAccess(rs.getObject("id", UUID.class), rs.getObject("tenant_id", UUID.class), rs.getString("name")),
                actor.tenantId(), actor.actorId(), action);
    }

    // ActorContext 是执行快照；成员状态从 Organization 实时读取，以便停用立即影响旧任务和会话。
    private boolean isCurrentMember(UUID tenantId, UUID actorId) {
        return tenantId != null && actorId != null && organizations.isActiveMember(tenantId, actorId);
    }

    private boolean isPersonal(UUID workspaceId) {
        var count = jdbc.queryForObject("select count(*) from workspace.workspace_profile where workspace_id = ? and kind = 'PERSONAL'",
                Integer.class, workspaceId);
        return count != null && count > 0;
    }

    private boolean isPersonalForAnotherActor(UUID tenantId, UUID workspaceId, UUID actorId) {
        var count = jdbc.queryForObject("select count(*) from workspace.workspace_profile where tenant_id = ? and workspace_id = ? and kind = 'PERSONAL' and owner_id <> ?",
                Integer.class, tenantId, workspaceId, actorId);
        return count != null && count > 0;
    }
}
// 本文件负责实现 EAF 的 JdbcWorkspaceAuthorization.java 相关代码。
