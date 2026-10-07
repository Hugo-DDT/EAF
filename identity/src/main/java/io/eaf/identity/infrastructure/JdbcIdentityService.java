package io.eaf.identity.infrastructure;

import io.eaf.identity.api.CreateDelegationCommand;
import io.eaf.identity.api.DelegationResourceAuthorizer;
import io.eaf.identity.api.DelegationSnapshot;
import io.eaf.identity.api.IdentityService;
import io.eaf.organization.api.OrganizationDirectory;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import io.eaf.shared.Hashing;
import io.eaf.shared.Ids;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcIdentityService implements IdentityService {
    private static final Duration MAX_LIFETIME = Duration.ofMinutes(30);
    // 固定 reviewer 只提供只读建议；读取 Skill/Tool 元数据和远端复核仍受双方当前授权交集限制。
    private static final Set<String> DELEGABLE_ACTIONS = Set.of("task:create", "task:read", "agent:read", "agent:risk-review",
            "capability:read", "skill:read", "tool:read", "prompt:read", "context:read", "knowledge:read",
            "memory:read", "crm:customer:read");
    private final JdbcTemplate jdbc;
    private final OrganizationDirectory organizations;
    private final WorkspaceAuthorization workspaces;
    private final ObjectProvider<DelegationResourceAuthorizer> resourceAuthorizers;
    private final Clock clock;

    public JdbcIdentityService(JdbcTemplate jdbc, OrganizationDirectory organizations, WorkspaceAuthorization workspaces,
                               ObjectProvider<DelegationResourceAuthorizer> resourceAuthorizers, Clock clock) {
        this.jdbc = jdbc;
        this.organizations = organizations;
        this.workspaces = workspaces;
        this.resourceAuthorizers = resourceAuthorizers;
        this.clock = clock;
    }

    @Override
    public Optional<ActorContext> resolveToken(String token) {
        if (token == null || token.isBlank()) return Optional.empty();
        UUID subjectId = switch (token) {
            case "eaf-local-alice", "test-alice", "alice" -> Ids.ALICE;
            case "eaf-local-bob", "test-bob", "bob" -> Ids.BOB;
            case "eaf-local-carol", "test-carol", "carol" -> Ids.CAROL;
            case "eaf-local-risk-agent", "risk-agent" -> Ids.AGENT_RISK;
            default -> null;
        };
        if (subjectId == null) return Optional.empty();
        return activeSubject(subjectId, null).map(actor -> new ActorContext(actor.actorId(), actor.tenantId(), actor.type(), workspaces.actions(actor.tenantId(), actor.actorId())));
    }

    @Override
    public Optional<ActorContext> resolveExternalPrincipal(String issuer, String subject) {
        if (issuer == null || issuer.isBlank() || issuer.length() > 512
                || subject == null || subject.isBlank() || subject.length() > 512) return Optional.empty();
        // 外部 claims 只定位预先登记的主体；组织成员状态、主体类型和授权仍由本域数据决定。
        var subjectId = jdbc.query("select actor_id from \"identity\".external_identity "
                        + "where issuer = ? and subject = ? and status = 'ACTIVE'",
                rs -> rs.next() ? rs.getObject("actor_id", UUID.class) : null, issuer, subject);
        return subjectId == null ? Optional.empty() : activeSubject(subjectId, null);
    }

    @Override
    public Optional<ActorContext> resolveDelegatedToken(String token, UUID delegationId, String audience) {
        var delegate = resolveToken(token).filter(actor -> actor.type() == ActorType.AGENT);
        if (delegate.isEmpty() || delegationId == null) return Optional.empty();
        var ref = jdbc.query("select owner_id, workspace_id from identity.delegation where id = ? and tenant_id = ? and delegate_id = ? and audience = ? and status = 'ACTIVE' and expires_at > ?",
                rs -> rs.next() ? new UUID[]{rs.getObject("owner_id", UUID.class), rs.getObject("workspace_id", UUID.class)} : null,
                delegationId, delegate.get().tenantId(), delegate.get().actorId(), audience, Timestamp.from(Instant.now(clock)));
        if (ref == null) return Optional.empty();
        return resolveDelegation(delegate.get().tenantId(), ref[0], delegate.get().actorId(), delegationId, ref[1], audience);
    }

    @Override
    public Optional<ActorContext> resolveDelegation(UUID tenantId, UUID ownerId, UUID delegateId, UUID delegationId,
                                                   UUID workspaceId, String audience) {
        if (tenantId == null || ownerId == null || delegateId == null || delegationId == null || workspaceId == null || audience == null)
            return Optional.empty();
        var row = jdbc.query("select scope_hash from identity.delegation where id = ? and tenant_id = ? and workspace_id = ? and owner_id = ? and delegate_id = ? and audience = ? and status = 'ACTIVE' and expires_at > ?",
                rs -> rs.next() ? rs.getString("scope_hash") : null,
                delegationId, tenantId, workspaceId, ownerId, delegateId, audience, Timestamp.from(Instant.now(clock)));
        if (row == null || !activeSubject(ownerId, ActorType.HUMAN).filter(a -> a.tenantId().equals(tenantId)).isPresent()
                || !activeSubject(delegateId, ActorType.AGENT).filter(a -> a.tenantId().equals(tenantId)).isPresent()) return Optional.empty();
        var ownerActions = workspaces.actions(tenantId, ownerId, workspaceId);
        var delegateActions = workspaces.actions(tenantId, delegateId, workspaceId);
        var scopeActions = new HashSet<>(jdbc.queryForList("select action from identity.delegation_action where delegation_id = ?", String.class, delegationId));
        var allowedActions = new HashSet<>(scopeActions);
        allowedActions.retainAll(ownerActions);
        allowedActions.retainAll(delegateActions);
        // Owner 的 CRM 权限由客户资源授权表达；Agent 还必须持有 Workspace CRM 读取动作。
        if (scopeActions.contains("crm:customer:read") && ownerActions.contains("task:create")
                && delegateActions.contains("crm:customer:read") && currentCustomerScope(tenantId, ownerId, delegateId, workspaceId, delegationId))
            allowedActions.add("crm:customer:read");
        if (allowedActions.isEmpty()) return Optional.empty();
        var authorizationHash = authorizationHash(tenantId, ownerId, delegateId, workspaceId, delegationId, audience, row, allowedActions);
        return Optional.of(new ActorContext(delegateId, tenantId, ActorType.AGENT, allowedActions,
                ownerId, delegationId, workspaceId, authorizationHash));
    }

    @Override
    public boolean allowsDelegatedCustomerRead(ActorContext actor, String customerId) {
        if (actor == null || !actor.delegated() || actor.type() != ActorType.AGENT || customerId == null) return false;
        var count = jdbc.queryForObject("select count(*) from identity.delegation d join identity.delegation_customer c on c.delegation_id = d.id where d.id = ? and d.tenant_id = ? and d.workspace_id = ? and d.owner_id = ? and d.delegate_id = ? and d.audience = ? and d.status = 'ACTIVE' and d.expires_at > ? and c.customer_id = ?",
                Integer.class, actor.delegationId(), actor.tenantId(), actor.delegationWorkspaceId(), actor.principalId(), actor.actorId(), REST_AUDIENCE, Timestamp.from(Instant.now(clock)), customerId);
        return count != null && count > 0;
    }

    @Override
    @Transactional
    public DelegationSnapshot createDelegation(CreateDelegationCommand command) {
        if (command == null || command.owner() == null || command.workspaceId() == null || command.delegateId() == null)
            throw EafException.invalid("委托缺少 Owner、Workspace 或受托身份。");
        var owner = command.owner();
        if (owner.type() != ActorType.HUMAN || owner.delegated() || owner.actorId().equals(command.delegateId()))
            throw EafException.forbidden("只有当前 Workspace 内的 HUMAN Owner 可以签发一跳委托。");
        workspaces.require(owner, command.workspaceId(), "identity:delegation:manage");
        if (!activeSubject(owner.actorId(), ActorType.HUMAN).filter(a -> a.tenantId().equals(owner.tenantId())).isPresent()
                || !activeSubject(command.delegateId(), ActorType.AGENT).filter(a -> a.tenantId().equals(owner.tenantId())).isPresent())
            throw EafException.forbidden("受托 Agent 必须在同一租户内有效。");
        var actions = new HashSet<>(command.actions());
        var customers = new HashSet<>(command.customerIds());
        if (actions.isEmpty() || actions.size() > 10 || !DELEGABLE_ACTIONS.containsAll(actions))
            throw EafException.forbidden("委托动作不在只读业务白名单内。");
        if (actions.contains("crm:customer:read") != !customers.isEmpty() || customers.size() > 100
                || customers.stream().anyMatch(id -> id == null || id.isBlank() || id.length() > 160))
            throw EafException.invalid("客户读取委托必须绑定 1 至 100 个有效客户。");
        var ownerActions = workspaces.actions(owner.tenantId(), owner.actorId(), command.workspaceId());
        var delegateActions = workspaces.actions(owner.tenantId(), command.delegateId(), command.workspaceId());
        for (var action : actions) {
            var allowed = "crm:customer:read".equals(action)
                    ? ownerActions.contains("task:create") && delegateActions.contains(action) && !customers.isEmpty()
                    : ownerActions.contains(action) && delegateActions.contains(action);
            if (!allowed) throw EafException.forbidden("委托范围超出 Owner 或受托 Agent 当前授权。");
        }
        for (var customerId : customers) {
            if (resourceAuthorizers.orderedStream().noneMatch(validator -> validator.mayDelegateCustomerRead(
                    owner.tenantId(), owner.actorId(), command.delegateId(), command.workspaceId(), customerId)))
                throw EafException.forbidden("客户委托范围超出 Owner 或受托 Agent 当前资源授权。");
        }
        var now = Instant.now(clock);
        if (command.expiresAt() == null || !command.expiresAt().isAfter(now)
                || command.expiresAt().isAfter(now.plus(MAX_LIFETIME)))
            throw EafException.invalid("委托有效期必须在当前时间之后且不超过 30 分钟。");
        var id = UUID.randomUUID();
        var scopeHash = scopeHash(id, owner, command, actions, customers);
        jdbc.update("insert into identity.delegation(id, tenant_id, workspace_id, owner_id, delegate_id, audience, status, created_at, expires_at, scope_hash) values (?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?)",
                id, owner.tenantId(), command.workspaceId(), owner.actorId(), command.delegateId(), REST_AUDIENCE,
                Timestamp.from(now), Timestamp.from(command.expiresAt()), scopeHash);
        actions.forEach(action -> jdbc.update("insert into identity.delegation_action(delegation_id, action) values (?, ?)", id, action));
        customers.forEach(customer -> jdbc.update("insert into identity.delegation_customer(delegation_id, customer_id) values (?, ?)", id, customer));
        return snapshot(id, owner.tenantId(), owner.actorId());
    }

    @Override
    @Transactional
    public DelegationSnapshot revokeDelegation(ActorContext owner, UUID workspaceId, UUID delegationId) {
        if (owner == null || owner.type() != ActorType.HUMAN || owner.delegated())
            throw EafException.forbidden("只有 HUMAN Owner 可以撤销委托。");
        workspaces.require(owner, workspaceId, "identity:delegation:manage");
        jdbc.update("update identity.delegation set status = 'REVOKED', revoked_at = ? where id = ? and tenant_id = ? and workspace_id = ? and owner_id = ? and status = 'ACTIVE'",
                Timestamp.from(Instant.now(clock)), delegationId, owner.tenantId(), workspaceId, owner.actorId());
        return snapshot(delegationId, owner.tenantId(), owner.actorId());
    }

    private Optional<ActorContext> activeSubject(UUID subjectId, ActorType expectedType) {
        var row = jdbc.query("select type, status from \"identity\".subject where id = ?",
                rs -> rs.next() ? new String[]{rs.getString("type"), rs.getString("status")} : null, subjectId);
        if (row == null || !"ACTIVE".equals(row[1])) return Optional.empty();
        var type = ActorType.valueOf(row[0]);
        if (expectedType != null && type != expectedType) return Optional.empty();
        var membership = organizations.membership(subjectId).filter(m -> m.active());
        return membership.map(m -> new ActorContext(subjectId, m.tenantId(), type, workspaces.actions(m.tenantId(), subjectId)));
    }

    private DelegationSnapshot snapshot(UUID id, UUID tenantId, UUID ownerId) {
        var row = jdbc.query("select workspace_id, delegate_id, audience, created_at, expires_at, revoked_at, scope_hash from identity.delegation where id = ? and tenant_id = ? and owner_id = ?",
                rs -> rs.next() ? new Object[]{rs.getObject("workspace_id", UUID.class), rs.getObject("delegate_id", UUID.class), rs.getString("audience"), rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("expires_at").toInstant(), rs.getTimestamp("revoked_at") == null ? null : rs.getTimestamp("revoked_at").toInstant(), rs.getString("scope_hash")} : null,
                id, tenantId, ownerId);
        if (row == null) throw EafException.notFound();
        var actions = Set.copyOf(jdbc.queryForList("select action from identity.delegation_action where delegation_id = ?", String.class, id));
        var customers = Set.copyOf(jdbc.queryForList("select customer_id from identity.delegation_customer where delegation_id = ?", String.class, id));
        return new DelegationSnapshot(id, tenantId, (UUID) row[0], ownerId, (UUID) row[1], (String) row[2], actions, customers,
                (Instant) row[3], (Instant) row[4], (Instant) row[5], (String) row[6]);
    }

    private String scopeHash(UUID id, ActorContext owner, CreateDelegationCommand command, Set<String> actions, Set<String> customers) {
        return Hashing.sha256(String.join("\u001f", id.toString(), owner.tenantId().toString(), command.workspaceId().toString(),
                owner.actorId().toString(), command.delegateId().toString(), REST_AUDIENCE,
                String.join(",", new TreeSet<>(actions)), String.join(",", new TreeSet<>(customers)), command.expiresAt().toString()));
    }

    private String authorizationHash(UUID tenantId, UUID ownerId, UUID delegateId, UUID workspaceId, UUID delegationId,
                                     String audience, String scopeHash, Set<String> actions) {
        return Hashing.sha256(String.join("\u001f", tenantId.toString(), ownerId.toString(), delegateId.toString(),
                workspaceId.toString(), delegationId.toString(), audience, scopeHash, String.join(",", new TreeSet<>(actions))));
    }

    private boolean currentCustomerScope(UUID tenantId, UUID ownerId, UUID delegateId, UUID workspaceId, UUID delegationId) {
        var customers = jdbc.queryForList("select customer_id from identity.delegation_customer where delegation_id = ?", String.class, delegationId);
        return !customers.isEmpty() && customers.stream().allMatch(customer -> resourceAuthorizers.orderedStream()
                .anyMatch(authorizer -> authorizer.mayDelegateCustomerRead(tenantId, ownerId, delegateId, workspaceId, customer)));
    }
}
// Identity 在每次请求和异步继续前重新计算授权交集；Task 保存摘要只用于检测授权变化。
