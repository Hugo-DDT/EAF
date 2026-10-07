package io.eaf.provisioning.infrastructure;

import io.eaf.identity.api.ExternalIdentityBinding;
import io.eaf.identity.api.IdentityAdministration;
import io.eaf.organization.api.OrganizationMembershipAdministration;
import io.eaf.policy.api.PolicyCustomerGrantAdministration;
import io.eaf.provisioning.api.MembershipProvisioning;
import io.eaf.provisioning.api.ProvisionedHuman;
import io.eaf.shared.ActorContext;
import io.eaf.shared.EafException;
import io.eaf.workspace.api.WorkspaceMembershipAdministration;
import io.eaf.workspace.api.WorkspaceCatalog;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DefaultMembershipProvisioning implements MembershipProvisioning {
    private final IdentityAdministration identities;
    private final OrganizationMembershipAdministration organization;
    private final WorkspaceMembershipAdministration workspaces;
    private final WorkspaceCatalog workspaceCatalog;
    private final PolicyCustomerGrantAdministration policy;

    public DefaultMembershipProvisioning(IdentityAdministration identities,
                                         OrganizationMembershipAdministration organization,
                                         WorkspaceMembershipAdministration workspaces,
                                         PolicyCustomerGrantAdministration policy,
                                         WorkspaceCatalog workspaceCatalog) {
        this.identities = identities;
        this.organization = organization;
        this.workspaces = workspaces;
        this.workspaceCatalog = workspaceCatalog;
        this.policy = policy;
    }

    @Override
    @Transactional
    public ProvisionedHuman onboardHuman(ActorContext tenantAdministrator, UUID tenantId, String displayName,
                                         String issuer, String externalSubject,
                                         Map<UUID, Set<String>> workspaceActions,
                                         Map<UUID, Set<String>> customerIds) {
        if (workspaceActions == null || customerIds == null)
            throw EafException.invalid("Workspace 与客户授权清单必须显式提供。");
        organization.lockForProvisioning(tenantAdministrator, tenantId);
        var existing = identities.findExternalIdentity(tenantAdministrator, tenantId, issuer, externalSubject);
        var created = existing.isEmpty();
        var actorId = existing.map(ExternalIdentityBinding::actorId)
                .orElseGet(() -> identities.createHumanSubject(tenantAdministrator, tenantId, displayName));
        organization.addMember(tenantAdministrator, tenantId, actorId);
        identities.bindExternalIdentity(tenantAdministrator, tenantId, actorId, issuer, externalSubject);
        // 两类资源授权分别由所属域校验并落库；任一失败都会回滚整次开通事务。
        workspaceActions.forEach((workspaceId, actions) ->
                workspaces.grant(tenantAdministrator, workspaceId, actorId, actions));
        customerIds.forEach((workspaceId, customers) ->
                policy.grantCustomers(tenantAdministrator, workspaceId, actorId, customers));
        return new ProvisionedHuman(actorId, created);
    }

    @Override
    @Transactional
    public void offboardMember(ActorContext tenantAdministrator, UUID tenantId, UUID subjectId) {
        if (tenantAdministrator == null || subjectId == null || tenantId == null)
            throw EafException.invalid("成员停用请求缺少管理员、租户或主体。");
        if (tenantAdministrator.actorId().equals(subjectId))
            throw EafException.conflict("TENANT_ADMIN_SELF_DEACTIVATION", "租户管理员必须由另一位管理员停用。");
        // 先锁住成员生命周期，再依次写各域；授权方的同类写请求会等待此事务并复核成员状态。
        organization.lockForProvisioning(tenantAdministrator, tenantId);
        identities.revokeExternalIdentities(tenantAdministrator, tenantId, subjectId);
        policy.revokeAllForTenantMember(tenantAdministrator, tenantId, subjectId);
        workspaceCatalog.deactivatePersonalSpaceForMember(tenantId, subjectId);
        workspaces.revokeAllForTenantMember(tenantAdministrator, tenantId, subjectId);
        organization.removeTenantAdministrator(tenantAdministrator, tenantId, subjectId);
        organization.deactivateMember(tenantAdministrator, tenantId, subjectId);
    }
}
