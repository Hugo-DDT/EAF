package io.eaf.identity.infrastructure;

import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.identity.api.ExternalIdentityBinding;
import io.eaf.identity.api.IdentityAdministration;
import io.eaf.organization.api.OrganizationDirectory;
import io.eaf.organization.api.OrganizationMembershipAdministration;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JdbcIdentityAdministration implements IdentityAdministration {
    private final JdbcTemplate jdbc;
    private final OrganizationDirectory organizations;
    private final OrganizationMembershipAdministration membershipAdministration;
    private final AuditPort audit;
    private final String configuredIssuer;

    public JdbcIdentityAdministration(JdbcTemplate jdbc, OrganizationDirectory organizations,
                                      OrganizationMembershipAdministration membershipAdministration, AuditPort audit,
                                      @Value("${eaf.security.enterprise.issuer:}") String configuredIssuer) {
        this.jdbc = jdbc;
        this.organizations = organizations;
        this.membershipAdministration = membershipAdministration;
        this.audit = audit;
        this.configuredIssuer = configuredIssuer == null ? "" : configuredIssuer;
    }

    @Override
    @Transactional
    public UUID createHumanSubject(ActorContext tenantAdministrator, UUID tenantId, String displayName) {
        requireTenantAdministrator(tenantAdministrator, tenantId);
        if (displayName == null || displayName.isBlank() || displayName.trim().length() > 120
                || displayName.chars().anyMatch(Character::isISOControl))
            throw EafException.invalid("主体显示名称必须为 1 至 120 个有效字符。");
        var actorId = UUID.randomUUID();
        jdbc.update("insert into \"identity\".subject(id, type, display_name, status) values (?, 'HUMAN', ?, 'ACTIVE')",
                actorId, displayName.trim());
        appendAudit(tenantAdministrator, tenantId, actorId, "identity.human_subject.create", "ACTIVE");
        return actorId;
    }

    @Override
    public Optional<ExternalIdentityBinding> findExternalIdentity(ActorContext tenantAdministrator, UUID tenantId,
                                                                  String issuer, String externalSubject) {
        requireTenantAdministrator(tenantAdministrator, tenantId);
        validateExternalIdentity(issuer, externalSubject);
        var mapping = jdbc.query("select actor_id, status from \"identity\".external_identity where issuer = ? and subject = ?",
                rs -> rs.next() ? new ExternalIdentityBinding(rs.getObject("actor_id", UUID.class),
                        "ACTIVE".equals(rs.getString("status"))) : null, issuer, externalSubject);
        if (mapping == null) return Optional.empty();
        requireKnownHumanMember(tenantId, mapping.actorId());
        return Optional.of(mapping);
    }

    @Override
    @Transactional
    public ExternalIdentityBinding bindExternalIdentity(ActorContext tenantAdministrator, UUID tenantId, UUID actorId,
                                                        String issuer, String externalSubject) {
        requireTenantAdministrator(tenantAdministrator, tenantId);
        validateExternalIdentity(issuer, externalSubject);
        requireHumanMember(tenantId, actorId);
        var changed = jdbc.update("insert into \"identity\".external_identity as current_mapping(issuer, subject, actor_id, status) "
                        + "values (?, ?, ?, 'ACTIVE') on conflict (issuer, subject) do update set status = 'ACTIVE' "
                        + "where current_mapping.actor_id = excluded.actor_id and current_mapping.status <> 'ACTIVE'",
                issuer, externalSubject, actorId);
        var mapping = jdbc.query("select actor_id, status from \"identity\".external_identity where issuer = ? and subject = ?",
                rs -> rs.next() ? new ExternalIdentityBinding(rs.getObject("actor_id", UUID.class),
                        "ACTIVE".equals(rs.getString("status"))) : null, issuer, externalSubject);
        if (mapping == null || !mapping.actorId().equals(actorId))
            throw EafException.conflict("EXTERNAL_IDENTITY_ALREADY_BOUND", "该企业身份已绑定到其他内部主体，不能重新指派。");
        if (!mapping.active()) throw EafException.conflict("EXTERNAL_IDENTITY_DISABLED", "停用的企业身份必须经管理员明确重新绑定后才能启用。");
        if (changed > 0) appendAudit(tenantAdministrator, tenantId, actorId, "identity.external_identity.bind", "ACTIVE");
        return mapping;
    }

    @Override
    @Transactional
    public void appointTenantAdministrator(ActorContext tenantAdministrator, UUID tenantId, UUID actorId) {
        requireTenantAdministrator(tenantAdministrator, tenantId);
        requireHumanMember(tenantId, actorId);
        membershipAdministration.addTenantAdministrator(tenantAdministrator, tenantId, actorId);
    }

    @Override
    @Transactional
    public int revokeExternalIdentities(ActorContext tenantAdministrator, UUID tenantId, UUID actorId) {
        requireTenantAdministrator(tenantAdministrator, tenantId);
        requireExistingTenantMember(tenantId, actorId);
        var changed = jdbc.update("update \"identity\".external_identity set status = 'DISABLED' "
                        + "where actor_id = ? and status = 'ACTIVE'",
                actorId);
        if (changed > 0) appendAudit(tenantAdministrator, tenantId, actorId, "identity.external_identity.revoke", "DISABLED");
        return changed;
    }

    private void requireTenantAdministrator(ActorContext administrator, UUID tenantId) {
        if (administrator == null || administrator.type() != ActorType.HUMAN || administrator.delegated()
                || tenantId == null || !tenantId.equals(administrator.tenantId())
                || !organizations.isTenantAdministrator(tenantId, administrator.actorId()))
            throw EafException.forbidden("只有当前租户的已登记 HUMAN 管理员可以维护身份映射。");
    }

    private void requireHumanMember(UUID tenantId, UUID actorId) {
        if (actorId == null || !organizations.isActiveMember(tenantId, actorId)) throw EafException.notFound();
        requireHumanSubject(actorId);
    }

    private void requireKnownHumanMember(UUID tenantId, UUID actorId) {
        if (actorId == null || organizations.membership(tenantId, actorId).isEmpty()) throw EafException.notFound();
        requireHumanSubject(actorId);
    }

    private void requireHumanSubject(UUID actorId) {
        var type = jdbc.query("select type from \"identity\".subject where id = ? and status = 'ACTIVE'",
                rs -> rs.next() ? rs.getString("type") : null, actorId);
        if (!ActorType.HUMAN.name().equals(type))
            throw EafException.forbidden("企业 issuer/subject 只能绑定到当前租户的有效 HUMAN 主体。");
    }

    private void requireExistingTenantMember(UUID tenantId, UUID actorId) {
        if (actorId == null || organizations.membership(tenantId, actorId).isEmpty()) throw EafException.notFound();
    }

    private void validateExternalIdentity(String issuer, String externalSubject) {
        if (configuredIssuer.isBlank() || issuer == null || !configuredIssuer.equals(issuer)
                || externalSubject == null || externalSubject.isBlank() || externalSubject.length() > 512
                || externalSubject.chars().anyMatch(Character::isISOControl))
            throw EafException.forbidden("企业身份映射只能使用固定配置的 issuer 和有效 subject。");
    }

    // 审计不记录显示名或外部 subject，避免把身份目录资料复制到审计载荷。
    private void appendAudit(ActorContext administrator, UUID tenantId, UUID actorId, String action, String result) {
        audit.append(new AuditFact(UUID.randomUUID().toString(), tenantId, null, administrator.actorId(), null,
                action, result, "{\"actorId\":\"" + actorId + "\"}", null));
    }
}
