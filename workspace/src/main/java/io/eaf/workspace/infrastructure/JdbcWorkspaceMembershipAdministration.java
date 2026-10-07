package io.eaf.workspace.infrastructure;

import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.organization.api.OrganizationDirectory;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.eaf.workspace.api.WorkspaceMembershipAdministration;
import io.eaf.workspace.api.WorkspaceCatalog;
import io.eaf.workspace.api.WorkspaceKind;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcWorkspaceMembershipAdministration implements WorkspaceMembershipAdministration {
    private static final String MANAGE_ACTION = "workspace:members:manage";
    private static final Pattern ACTION = Pattern.compile("[a-z][a-z0-9:._-]{0,79}");
    private final JdbcTemplate jdbc;
    private final OrganizationDirectory organizations;
    private final WorkspaceAuthorization authorization;
    private final AuditPort audit;
    private final WorkspaceCatalog catalog;

    public JdbcWorkspaceMembershipAdministration(JdbcTemplate jdbc, OrganizationDirectory organizations,
                                                WorkspaceAuthorization authorization, AuditPort audit,
                                                WorkspaceCatalog catalog) {
        this.jdbc = jdbc;
        this.organizations = organizations;
        this.authorization = authorization;
        this.audit = audit;
        this.catalog = catalog;
    }

    @Override
    @Transactional
    public int grant(ActorContext administrator, UUID workspaceId, UUID subjectId, Set<String> actions) {
        requireWorkspaceManager(administrator, workspaceId, subjectId);
        organizations.lockActiveMembership(administrator.tenantId(), administrator.actorId());
        organizations.lockActiveMembership(administrator.tenantId(), subjectId);
        lockWorkspace(administrator.tenantId(), workspaceId);
        requireWorkspaceManager(administrator, workspaceId, subjectId);
        var requested = validateActions(actions);
        requirePersonalTransferRule(administrator, workspaceId, subjectId);
        var qualified = authorization.actions(administrator.tenantId(), administrator.actorId(), workspaceId);
        if (!qualified.containsAll(requested))
            throw EafException.forbidden("Workspace 管理员只能转授自己当前持有的动作权限。");
        var changed = 0;
        for (var action : requested) {
            changed += jdbc.update("insert into workspace.\"grant\" as current_grant(tenant_id, workspace_id, actor_id, action, status) "
                            + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) "
                            + "do update set status = 'ACTIVE' where current_grant.status <> 'ACTIVE'",
                    administrator.tenantId(), workspaceId, subjectId, action);
        }
        if (changed > 0) appendAudit(administrator, workspaceId, subjectId, "workspace.member.grant", changed);
        return changed;
    }

    @Override
    @Transactional
    public int revoke(ActorContext administrator, UUID workspaceId, UUID subjectId, Set<String> actions) {
        requireWorkspaceManager(administrator, workspaceId, subjectId);
        organizations.lockActiveMembership(administrator.tenantId(), administrator.actorId());
        organizations.lockActiveMembership(administrator.tenantId(), subjectId);
        lockWorkspace(administrator.tenantId(), workspaceId);
        requireWorkspaceManager(administrator, workspaceId, subjectId);
        var requested = validateActions(actions);
        requirePersonalTransferRule(administrator, workspaceId, subjectId);
        if (requested.contains(MANAGE_ACTION) && isCurrentManager(administrator.tenantId(), workspaceId, subjectId)) {
            if (administrator.actorId().equals(subjectId))
                throw EafException.conflict("WORKSPACE_ADMIN_SELF_REVOCATION", "Workspace 管理员不能通过普通授权 API 撤销自己的管理权限。");
            if (!hasOtherActiveManager(administrator.tenantId(), workspaceId, subjectId))
                throw EafException.conflict("LAST_WORKSPACE_ADMIN", "必须先为该 Workspace 登记另一位有效管理员。");
        }
        var changed = 0;
        for (var action : requested) {
            changed += jdbc.update("update workspace.\"grant\" set status = 'REVOKED' "
                            + "where tenant_id = ? and workspace_id = ? and actor_id = ? and action = ? and status = 'ACTIVE'",
                    administrator.tenantId(), workspaceId, subjectId, action);
        }
        if (changed > 0) appendAudit(administrator, workspaceId, subjectId, "workspace.member.revoke", changed);
        return changed;
    }

    @Override
    @Transactional
    public int revokeAllForTenantMember(ActorContext tenantAdministrator, UUID tenantId, UUID subjectId) {
        requireTenantAdministrator(tenantAdministrator, tenantId, subjectId);
        var managerWorkspaces = jdbc.queryForList("select workspace_id from workspace.\"grant\" "
                        + "where tenant_id = ? and actor_id = ? and action = ? and status = 'ACTIVE'",
                UUID.class, tenantId, subjectId, MANAGE_ACTION).stream().sorted().toList();
        for (var workspaceId : managerWorkspaces) {
            lockWorkspace(tenantId, workspaceId);
            if (!hasOtherActiveManager(tenantId, workspaceId, subjectId))
                throw EafException.conflict("LAST_WORKSPACE_ADMIN", "必须先为该 Workspace 登记另一位有效管理员。");
        }
        var changed = jdbc.update("update workspace.\"grant\" set status = 'REVOKED' "
                        + "where tenant_id = ? and actor_id = ? and status = 'ACTIVE'",
                tenantId, subjectId);
        if (changed > 0) appendAudit(tenantAdministrator, null, subjectId, "workspace.member.revoke_all", changed);
        return changed;
    }

    private void requireWorkspaceManager(ActorContext administrator, UUID workspaceId, UUID subjectId) {
        if (administrator == null || administrator.type() != ActorType.HUMAN || administrator.delegated()
                || workspaceId == null || subjectId == null)
            throw EafException.forbidden("只有本人操作的 HUMAN 管理员可以维护 Workspace 授权。");
        authorization.require(administrator, workspaceId, MANAGE_ACTION);
        if (!organizations.isActiveMember(administrator.tenantId(), subjectId)) throw EafException.notFound();
    }

    // 个人空间动作永远不转授；创建者本人仍可通过统一成员 API 做幂等维护。
    private void requirePersonalTransferRule(ActorContext administrator, UUID workspaceId, UUID subjectId) {
        var profile = catalog.profile(administrator.tenantId(), workspaceId);
        if (profile.kind() == WorkspaceKind.PERSONAL && !profile.ownerId().equals(subjectId))
            throw EafException.forbidden("个人空间权限不能授予其他主体。");
    }

    private void requireTenantAdministrator(ActorContext administrator, UUID tenantId, UUID subjectId) {
        if (administrator == null || administrator.type() != ActorType.HUMAN || administrator.delegated()
                || tenantId == null || subjectId == null || !tenantId.equals(administrator.tenantId())
                || !organizations.isTenantAdministrator(tenantId, administrator.actorId())
                || organizations.membership(tenantId, subjectId).isEmpty())
            throw EafException.forbidden("只有当前租户的已登记 HUMAN 管理员可以清理成员授权。");
    }

    private Set<String> validateActions(Set<String> actions) {
        if (actions == null || actions.isEmpty() || actions.size() > 100
                || actions.stream().anyMatch(action -> action == null || !ACTION.matcher(action).matches()))
            throw EafException.invalid("授权动作必须是 1 至 100 个格式有效的动作标识。");
        return Set.copyOf(actions);
    }

    // 备用管理员必须属于同一租户且当前仍为有效 Organization 成员。
    private boolean hasOtherActiveManager(UUID tenantId, UUID workspaceId, UUID excludedSubjectId) {
        List<UUID> candidates = jdbc.queryForList("select actor_id from workspace.\"grant\" where tenant_id = ? "
                        + "and workspace_id = ? and action = ? and status = 'ACTIVE' and actor_id <> ?",
                UUID.class, tenantId, workspaceId, MANAGE_ACTION, excludedSubjectId);
        return candidates.stream().anyMatch(subjectId -> organizations.isActiveMember(tenantId, subjectId));
    }

    private boolean isCurrentManager(UUID tenantId, UUID workspaceId, UUID subjectId) {
        var count = jdbc.queryForObject("select count(*) from workspace.\"grant\" where tenant_id = ? and workspace_id = ? "
                        + "and actor_id = ? and action = ? and status = 'ACTIVE'",
                Integer.class, tenantId, workspaceId, subjectId, MANAGE_ACTION);
        return count != null && count > 0;
    }

    // Workspace 行锁使并发的最后管理员撤销不能同时基于过期计数通过。
    private void lockWorkspace(UUID tenantId, UUID workspaceId) {
        var found = jdbc.query("select id from workspace.workspace where tenant_id = ? and id = ? and status = 'ACTIVE' for update",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null, tenantId, workspaceId);
        if (found == null) throw EafException.notFound();
    }

    // 审计保存变更主体与条数，不复制授权清单或业务资源数据。
    private void appendAudit(ActorContext administrator, UUID workspaceId, UUID subjectId, String action, int count) {
        audit.append(new AuditFact(UUID.randomUUID().toString(), administrator.tenantId(), workspaceId,
                administrator.actorId(), null, action, "CHANGED", "{\"subjectId\":\"" + subjectId
                + "\",\"grantCount\":" + count + "}", null));
    }
}
