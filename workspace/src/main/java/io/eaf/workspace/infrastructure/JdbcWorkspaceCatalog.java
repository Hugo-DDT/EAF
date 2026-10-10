package io.eaf.workspace.infrastructure;

import io.eaf.organization.api.OrganizationDirectory;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.workspace.api.ContextSourceSelection;
import io.eaf.workspace.api.ContextSourceSummary;
import io.eaf.workspace.api.ResolvedContextSources;
import io.eaf.workspace.api.WorkspaceAccess;
import io.eaf.workspace.api.WorkspaceAuthorization;
import io.eaf.workspace.api.WorkspaceCatalog;
import io.eaf.workspace.api.WorkspaceCreateResult;
import io.eaf.workspace.api.WorkspaceKind;
import io.eaf.workspace.api.WorkspacePage;
import io.eaf.workspace.api.WorkspaceProfile;
import io.eaf.workspace.api.WorkspaceSummary;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcWorkspaceCatalog implements WorkspaceCatalog {
    private static final Set<String> INITIAL_ACTIONS = Set.of("context:read", "knowledge:read", "knowledge:write",
            "knowledge:publish", "knowledge:source:manage", "workspace:members:manage", "prompt:read", "prompt:manage", "prompt:publish");
    private final JdbcTemplate jdbc;
    private final OrganizationDirectory organizations;
    private final WorkspaceAuthorization authorization;

    public JdbcWorkspaceCatalog(JdbcTemplate jdbc, OrganizationDirectory organizations,
                                WorkspaceAuthorization authorization) {
        this.jdbc = jdbc;
        this.organizations = organizations;
        this.authorization = authorization;
    }

    @Override
    @Transactional
    public WorkspaceCreateResult create(ActorContext actor, UUID workspaceId, String name, WorkspaceKind kind,
                                        UUID parentWorkspaceId) {
        requireHuman(actor);
        if (workspaceId == null || kind == null || kind == WorkspaceKind.GENERAL || name == null
                || name.isBlank() || name.trim().length() > 120)
            throw EafException.invalid("空间 ID、名称或类型无效。");
        var normalizedName = name.trim();
        validateParentShape(kind, parentWorkspaceId);
        organizations.lockActiveMembership(actor.tenantId(), actor.actorId());
        if (!organizations.isActiveMember(actor.tenantId(), actor.actorId())) throw EafException.notFound();
        if (kind != WorkspaceKind.PERSONAL && !organizations.isTenantAdministrator(actor.tenantId(), actor.actorId()))
            throw EafException.forbidden("只有当前租户已登记的 HUMAN 管理员可以创建共享空间。");
        var requestHash = Hashing.sha256(normalizedName + "|" + kind.name() + "|"
                + (parentWorkspaceId == null ? "" : parentWorkspaceId));
        var existing = jdbc.query("select w.id, w.name, w.status, p.kind, p.owner_id, p.parent_workspace_id, p.created_by, p.creation_hash "
                        + "from workspace.workspace w join workspace.workspace_profile p on p.tenant_id = w.tenant_id and p.workspace_id = w.id "
                        + "where w.tenant_id = ? and w.id = ?",
                rs -> rs.next() ? new ExistingWorkspace(rs.getString("name"), rs.getString("status"),
                        WorkspaceKind.valueOf(rs.getString("kind")), rs.getObject("owner_id", UUID.class),
                        rs.getObject("parent_workspace_id", UUID.class), rs.getObject("created_by", UUID.class),
                        rs.getString("creation_hash")) : null, actor.tenantId(), workspaceId);
        if (existing != null) {
            if (!existing.createdBy().equals(actor.actorId()) || !existing.creationHash().equals(requestHash))
                throw EafException.conflict("WORKSPACE_ID_CONFLICT", "该空间 ID 已用于不同的创建请求。");
            if (!"ACTIVE".equals(existing.status()))
                throw EafException.conflict("WORKSPACE_DISABLED", "该空间已停用，不能通过重放创建请求恢复。");
            // 幂等重放也须保有当前读取资格，避免撤权后仅凭旧创建者 ID 继续发现空间。
            authorization.require(actor, workspaceId, "context:read");
            return new WorkspaceCreateResult(summary(new WorkspaceAccess(workspaceId, actor.tenantId(), existing.name()),
                    new WorkspaceProfile(actor.tenantId(), workspaceId, existing.kind(), existing.ownerId(),
                            existing.parentWorkspaceId()), actor), false);
        }
        if (kind == WorkspaceKind.PERSONAL) {
            var duplicate = jdbc.query("select w.id from workspace.workspace_profile p join workspace.workspace w "
                            + "on w.tenant_id = p.tenant_id and w.id = p.workspace_id where p.tenant_id = ? "
                            + "and p.owner_id = ? and p.kind = 'PERSONAL'",
                    rs -> rs.next() ? rs.getObject(1, UUID.class) : null, actor.tenantId(), actor.actorId());
            if (duplicate != null)
                throw EafException.conflict("PERSONAL_WORKSPACE_EXISTS", "本人已有个人空间，不能使用新 ID 创建副本。");
        }
        validateParent(actor, kind, parentWorkspaceId);
        var now = Instant.now();
        try {
            jdbc.update("insert into workspace.workspace(id, tenant_id, name, status) values (?, ?, ?, 'ACTIVE')",
                    workspaceId, actor.tenantId(), normalizedName);
            jdbc.update("insert into workspace.workspace_profile(tenant_id, workspace_id, kind, owner_id, parent_workspace_id, created_by, creation_hash, created_at) "
                            + "values (?, ?, ?, ?, ?, ?, ?, ?)", actor.tenantId(), workspaceId, kind.name(),
                    kind == WorkspaceKind.PERSONAL ? actor.actorId() : null, parentWorkspaceId,
                    actor.actorId(), requestHash, Timestamp.from(now));
        } catch (DuplicateKeyException duplicate) {
            throw EafException.conflict("WORKSPACE_ID_CONFLICT", "该空间或个人空间创建请求已存在。");
        }
        for (var action : INITIAL_ACTIONS)
            jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) values (?, ?, ?, ?, 'ACTIVE')",
                    actor.tenantId(), workspaceId, actor.actorId(), action);
        return new WorkspaceCreateResult(new WorkspaceSummary(workspaceId, normalizedName, kind,
                kind == WorkspaceKind.PERSONAL ? actor.actorId() : null, parentWorkspaceId,
                readableParentName(actor, parentWorkspaceId)), true);
    }

    @Override
    public WorkspacePage list(ActorContext actor, WorkspaceKind kind, int limit, int offset) {
        requireHuman(actor);
        if (limit < 1 || limit > 50 || offset < 0) throw EafException.invalid("空间列表 limit 或 offset 无效。");
        var visible = new LinkedHashSet<WorkspaceAccess>();
        for (var action : authorization.actions(actor.tenantId(), actor.actorId()))
            visible.addAll(authorization.list(actor, action));
        var ordered = visible.stream().sorted(java.util.Comparator.comparing(item -> item.workspaceId().toString()))
                .map(access -> {
                    var profile = profile(actor.tenantId(), access.workspaceId());
                    return summary(access, profile, actor);
                }).filter(item -> kind == null || item.kind() == kind).toList();
        var start = Math.min((long) offset, ordered.size());
        var end = Math.min(start + limit, ordered.size());
        return new WorkspacePage(ordered.subList((int) start, (int) end), end < ordered.size() ? (int) end : null);
    }

    @Override
    public WorkspaceProfile profile(UUID tenantId, UUID workspaceId) {
        if (tenantId == null || workspaceId == null) throw EafException.notFound();
        return jdbc.query("select kind, owner_id, parent_workspace_id from workspace.workspace_profile where tenant_id = ? and workspace_id = ?",
                rs -> rs.next() ? new WorkspaceProfile(tenantId, workspaceId,
                        WorkspaceKind.valueOf(rs.getString("kind")), rs.getObject("owner_id", UUID.class),
                        rs.getObject("parent_workspace_id", UUID.class))
                        : new WorkspaceProfile(tenantId, workspaceId, WorkspaceKind.GENERAL, null, null),
                tenantId, workspaceId);
    }

    @Override
    public ContextSourceSelection listContextSources(ActorContext actor, UUID targetWorkspaceId, int limit, int offset) {
        requireHuman(actor);
        if (limit < 1 || limit > 50 || offset < 0) throw EafException.invalid("来源列表 limit 或 offset 无效。");
        authorization.require(actor, targetWorkspaceId, "context:read");
        var selected = preferenceIds(actor, targetWorkspaceId);
        var validSelected = new ArrayList<UUID>();
        var unavailable = 0;
        for (var id : selected) {
            if (canReadContext(actor, id)) validSelected.add(id); else unavailable++;
        }
        // 已保存来源按本人选择顺序排在可选但未选择的空间之前。
        var selectedPosition = new java.util.HashMap<UUID, Integer>();
        for (var index = 0; index < validSelected.size(); index++) selectedPosition.put(validSelected.get(index), index);
        var choices = authorization.list(actor, "context:read").stream().distinct()
                .map(access -> summary(access, profile(actor.tenantId(), access.workspaceId()), actor))
                .sorted(java.util.Comparator.comparing((WorkspaceSummary item) -> !item.workspaceId().equals(targetWorkspaceId))
                        .thenComparingInt(item -> selectedPosition.getOrDefault(item.workspaceId(), Integer.MAX_VALUE))
                        .thenComparing(item -> item.workspaceId().toString()))
                .map(item -> new ContextSourceSummary(item.workspaceId(), item.name(), item.kind(),
                        item.workspaceId().equals(targetWorkspaceId) || validSelected.contains(item.workspaceId())))
                .toList();
        var start = Math.min((long) offset, choices.size());
        var end = Math.min(start + limit, choices.size());
        return new ContextSourceSelection(choices.subList((int) start, (int) end), List.copyOf(validSelected), unavailable,
                end < choices.size() ? (int) end : null);
    }

    @Override
    @Transactional
    public ContextSourceSelection replaceContextSources(ActorContext actor, UUID targetWorkspaceId,
                                                         List<UUID> sourceWorkspaceIds) {
        requireHuman(actor);
        authorization.require(actor, targetWorkspaceId, "context:read");
        if (sourceWorkspaceIds == null || sourceWorkspaceIds.size() > 3
                || sourceWorkspaceIds.stream().anyMatch(java.util.Objects::isNull)
                || new LinkedHashSet<>(sourceWorkspaceIds).size() != sourceWorkspaceIds.size()
                || sourceWorkspaceIds.contains(targetWorkspaceId))
            throw EafException.invalid("附加来源必须是 0 至 3 个不重复且不包含目标空间的 UUID。");
        for (var sourceId : sourceWorkspaceIds) {
            if (!canReadContext(actor, sourceId)) throw EafException.notFound();
        }
        jdbc.update("delete from workspace.context_source_preference where tenant_id = ? and target_workspace_id = ? and actor_id = ?",
                actor.tenantId(), targetWorkspaceId, actor.actorId());
        for (var index = 0; index < sourceWorkspaceIds.size(); index++)
            jdbc.update("insert into workspace.context_source_preference(tenant_id, target_workspace_id, actor_id, source_workspace_id, position) values (?, ?, ?, ?, ?)",
                    actor.tenantId(), targetWorkspaceId, actor.actorId(), sourceWorkspaceIds.get(index), index + 1);
        return listContextSources(actor, targetWorkspaceId, 50, 0);
    }

    @Override
    public ResolvedContextSources resolveContextSources(ActorContext actor, UUID targetWorkspaceId,
                                                        List<UUID> explicitSourceWorkspaceIds) {
        requireHuman(actor);
        var target = authorization.require(actor, targetWorkspaceId, "context:read");
        var requested = explicitSourceWorkspaceIds == null ? preferenceIds(actor, targetWorkspaceId)
                : validateExplicit(targetWorkspaceId, explicitSourceWorkspaceIds);
        var unavailable = 0;
        var sources = new ArrayList<WorkspaceSummary>();
        sources.add(summary(target, profile(actor.tenantId(), targetWorkspaceId), actor));
        for (var sourceId : requested) {
            if (!canReadContext(actor, sourceId)) {
                if (explicitSourceWorkspaceIds != null) throw EafException.notFound();
                unavailable++;
                continue;
            }
            var access = authorization.require(actor, sourceId, "context:read");
            sources.add(summary(access, profile(actor.tenantId(), sourceId), actor));
        }
        return new ResolvedContextSources(List.copyOf(sources), unavailable);
    }

    @Override
    @Transactional
    public void deactivatePersonalSpaceForMember(UUID tenantId, UUID ownerId) {
        if (tenantId == null || ownerId == null) throw EafException.invalid("个人空间停用缺少租户或所有者。");
        var workspaceId = jdbc.query("select workspace_id from workspace.workspace_profile where tenant_id = ? and owner_id = ? and kind = 'PERSONAL'",
                rs -> rs.next() ? rs.getObject(1, UUID.class) : null, tenantId, ownerId);
        // 没有创建个人空间的成员不需要额外状态变更。
        if (workspaceId == null) return;
        jdbc.update("update workspace.workspace set status = 'DISABLED' where tenant_id = ? and id = ? and status = 'ACTIVE'",
                tenantId, workspaceId);
        jdbc.update("update workspace.\"grant\" set status = 'REVOKED' where tenant_id = ? and workspace_id = ? and status = 'ACTIVE'",
                tenantId, workspaceId);
        jdbc.update("delete from workspace.context_source_preference where tenant_id = ? and actor_id = ?",
                tenantId, ownerId);
    }

    private List<UUID> preferenceIds(ActorContext actor, UUID targetWorkspaceId) {
        return jdbc.queryForList("select source_workspace_id from workspace.context_source_preference where tenant_id = ? and target_workspace_id = ? and actor_id = ? order by position",
                UUID.class, actor.tenantId(), targetWorkspaceId, actor.actorId());
    }

    private List<UUID> validateExplicit(UUID targetWorkspaceId, List<UUID> ids) {
        if (ids.size() > 3 || ids.stream().anyMatch(java.util.Objects::isNull)
                || new LinkedHashSet<>(ids).size() != ids.size() || ids.contains(targetWorkspaceId))
            throw EafException.invalid("显式来源必须是 0 至 3 个不重复且不包含目标空间的 UUID。");
        return ids;
    }

    private boolean canReadContext(ActorContext actor, UUID workspaceId) {
        return workspaceId != null && authorization.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "context:read");
    }

    private void requireHuman(ActorContext actor) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated()
                || !organizations.isActiveMember(actor.tenantId(), actor.actorId()))
            throw EafException.forbidden("此 Workspace 管理入口仅供当前有效的本人 HUMAN 身份使用。");
    }

    private void validateParentShape(WorkspaceKind kind, UUID parentId) {
        if ((kind == WorkspaceKind.PERSONAL || kind == WorkspaceKind.ENTERPRISE) && parentId != null)
            throw EafException.invalid("该空间类型的父空间字段不符合层级规则。");
    }

    private void validateParent(ActorContext actor, WorkspaceKind kind, UUID parentId) {
        if (parentId == null) return;
        // 父关系只记录组织归属；创建者须能读和管理父空间，且本规则不产生权限继承。
        var parentAccess = authorization.require(actor, parentId, "context:read");
        authorization.require(actor, parentId, "workspace:members:manage");
        var parentKind = profile(actor.tenantId(), parentId).kind();
        var valid = kind == WorkspaceKind.PROJECT
                && (parentKind == WorkspaceKind.DEPARTMENT || parentKind == WorkspaceKind.ENTERPRISE)
                || kind == WorkspaceKind.DEPARTMENT && parentKind == WorkspaceKind.ENTERPRISE;
        if (!valid || !parentAccess.tenantId().equals(actor.tenantId()))
            throw EafException.invalid("父空间必须符合当前空间类型的同租户层级规则。");
    }

    private WorkspaceSummary summary(WorkspaceAccess access, WorkspaceProfile profile, ActorContext actor) {
        return new WorkspaceSummary(access.workspaceId(), access.name(), profile.kind(), profile.ownerId(),
                profile.parentWorkspaceId(), readableParentName(actor, profile.parentWorkspaceId()));
    }

    private String readableParentName(ActorContext actor, UUID parentId) {
        if (parentId == null || !canReadContext(actor, parentId)) return null;
        return jdbc.query("select name from workspace.workspace where tenant_id = ? and id = ? and status = 'ACTIVE'",
                rs -> rs.next() ? rs.getString(1) : null, actor.tenantId(), parentId);
    }

    private record ExistingWorkspace(String name, String status, WorkspaceKind kind, UUID ownerId,
                                     UUID parentWorkspaceId, UUID createdBy, String creationHash) { }
}
