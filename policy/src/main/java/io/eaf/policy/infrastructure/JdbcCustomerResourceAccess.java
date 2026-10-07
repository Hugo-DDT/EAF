package io.eaf.policy.infrastructure;

import io.eaf.organization.api.OrganizationDirectory;
import io.eaf.policy.api.CustomerResourceAccess;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.workspace.api.WorkspaceAuthorization;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JdbcCustomerResourceAccess implements CustomerResourceAccess {
    private final JdbcTemplate jdbc;
    private final WorkspaceAuthorization workspaces;
    private final OrganizationDirectory organizations;

    public JdbcCustomerResourceAccess(JdbcTemplate jdbc, WorkspaceAuthorization workspaces,
                                      OrganizationDirectory organizations) {
        this.jdbc = jdbc;
        this.workspaces = workspaces;
        this.organizations = organizations;
    }

    @Override
    public boolean canRead(ActorContext actor, UUID workspaceId, String customerId) {
        if (actor == null || actor.type() != ActorType.HUMAN || actor.delegated()
                || workspaceId == null || customerId == null || customerId.isBlank() || customerId.length() > 160
                || !organizations.isActiveMember(actor.tenantId(), actor.actorId())
                || !workspaces.isAuthorized(actor.tenantId(), actor.actorId(), workspaceId, "crm:customer:read"))
            return false;
        return hasGrant(actor.tenantId(), workspaceId, actor.actorId(), customerId);
    }

    @Override
    public List<UUID> activeReaders(UUID tenantId, UUID workspaceId, String customerId) {
        if (tenantId == null || workspaceId == null || customerId == null || customerId.isBlank()) return List.of();
        return jdbc.queryForList("select actor_id from policy.customer_grant where tenant_id = ? and workspace_id = ? "
                        + "and customer_id = ? and status = 'ACTIVE' order by actor_id", UUID.class,
                tenantId, workspaceId, customerId).stream()
                .filter(actorId -> organizations.isActiveMember(tenantId, actorId))
                .filter(actorId -> workspaces.isAuthorized(tenantId, actorId, workspaceId, "crm:customer:read"))
                .toList();
    }

    private boolean hasGrant(UUID tenantId, UUID workspaceId, UUID actorId, String customerId) {
        var count = jdbc.queryForObject("select count(*) from policy.customer_grant where tenant_id = ? "
                        + "and workspace_id = ? and actor_id = ? and customer_id = ? and status = 'ACTIVE'",
                Integer.class, tenantId, workspaceId, actorId, customerId);
        return count != null && count > 0;
    }
}
