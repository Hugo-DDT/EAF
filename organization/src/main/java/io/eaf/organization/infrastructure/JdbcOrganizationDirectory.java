package io.eaf.organization.infrastructure;

import io.eaf.organization.api.Membership;
import io.eaf.organization.api.OrganizationDirectory;
import io.eaf.organization.api.OrganizationMembershipAdministration;
import io.eaf.audit.api.AuditFact;
import io.eaf.audit.api.AuditPort;
import io.eaf.shared.ActorContext;
import io.eaf.shared.ActorType;
import io.eaf.shared.EafException;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class JdbcOrganizationDirectory implements OrganizationDirectory, OrganizationMembershipAdministration {
    private final JdbcTemplate jdbc;
    private final AuditPort audit;

    public JdbcOrganizationDirectory(JdbcTemplate jdbc, AuditPort audit) {
        this.jdbc = jdbc;
        this.audit = audit;
    }

    @Override
    public java.util.Optional<Membership> membership(UUID subjectId) {
        return jdbc.query("select subject_id, tenant_id, status = 'ACTIVE' from organization.member where subject_id = ?",
                rs -> rs.next() ? java.util.Optional.of(new Membership(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class), rs.getBoolean(3))) : java.util.Optional.empty(),
                subjectId);
    }

    @Override
    public java.util.Optional<Membership> membership(UUID tenantId, UUID subjectId) {
        // 按两个主键字段限定查询，不能从其他租户的同一主体成员行推导授权。
        return jdbc.query("select subject_id, tenant_id, status = 'ACTIVE' from organization.member where tenant_id = ? and subject_id = ?",
                rs -> rs.next() ? java.util.Optional.of(new Membership(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class), rs.getBoolean(3))) : java.util.Optional.empty(),
                tenantId, subjectId);
    }

    @Override
    public void lockActiveMembership(UUID tenantId, UUID subjectId) {
        var tenant = jdbc.query("select id from organization.tenant where id = ? and status = 'ACTIVE' for update",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null, tenantId);
        if (tenant == null) throw EafException.notFound();
        var active = jdbc.query("select subject_id from organization.member where tenant_id = ? and subject_id = ? and status = 'ACTIVE' for update",
                rs -> rs.next() ? rs.getObject("subject_id", UUID.class) : null, tenantId, subjectId);
        if (active == null) throw EafException.notFound();
    }

    @Override
    public boolean isTenantAdministrator(UUID tenantId, UUID subjectId) {
        var count = jdbc.queryForObject("select count(*) from organization.tenant_admin a "
                        + "join organization.member m on m.tenant_id = a.tenant_id and m.subject_id = a.subject_id "
                        + "join organization.tenant t on t.id = a.tenant_id "
                        + "where a.tenant_id = ? and a.subject_id = ? and a.status = 'ACTIVE' "
                        + "and m.status = 'ACTIVE' and t.status = 'ACTIVE'",
                Integer.class, tenantId, subjectId);
        return count != null && count > 0;
    }

    @Override
    public boolean hasMembershipInOtherTenant(UUID tenantId, UUID subjectId) {
        var count = jdbc.queryForObject("select count(*) from organization.member where subject_id = ? and tenant_id <> ?",
                Integer.class, subjectId, tenantId);
        return count != null && count > 0;
    }

    @Override
    @Transactional
    public Membership addMember(ActorContext administrator, UUID tenantId, UUID subjectId) {
        requireAdministrator(administrator, tenantId);
        lockTenant(administrator, tenantId);
        if (subjectId == null) throw EafException.invalid("成员 Subject 必须提供。");
        if (hasMembershipInOtherTenant(tenantId, subjectId))
            throw EafException.conflict("TENANT_TRANSFER_FORBIDDEN", "已有 Organization 成员记录的 Subject 不能转移到其他租户。");
        var tenantActive = jdbc.queryForObject("select count(*) from organization.tenant where id = ? and status = 'ACTIVE'",
                Integer.class, tenantId);
        if (tenantActive == null || tenantActive == 0) throw EafException.notFound();
        var changed = jdbc.update("insert into organization.member as member(tenant_id, subject_id, status) values (?, ?, 'ACTIVE') "
                        + "on conflict (tenant_id, subject_id) do update set status = 'ACTIVE' where member.status <> 'ACTIVE'",
                tenantId, subjectId);
        if (changed > 0) appendAudit(administrator, tenantId, subjectId, "organization.member.add", "ACTIVE");
        return membership(tenantId, subjectId).orElseThrow(EafException::notFound);
    }

    @Override
    @Transactional
    public Membership deactivateMember(ActorContext administrator, UUID tenantId, UUID subjectId) {
        requireAdministrator(administrator, tenantId);
        lockTenant(administrator, tenantId);
        if (subjectId == null) throw EafException.invalid("成员 Subject 必须提供。");
        if (administrator.actorId().equals(subjectId))
            throw EafException.conflict("TENANT_ADMIN_SELF_DEACTIVATION", "租户管理员不能通过普通成员 API 停用自己。");
        if (isTenantAdministrator(tenantId, subjectId))
            throw EafException.conflict("TENANT_ADMIN_ROLE_ACTIVE", "停用租户管理员前，必须由另一位管理员先撤销其管理员资格。");
        var existing = membership(tenantId, subjectId).orElseThrow(EafException::notFound);
        if (!existing.active()) return existing;
        var changed = jdbc.update("update organization.member set status = 'DISABLED' where tenant_id = ? and subject_id = ? and status = 'ACTIVE'",
                tenantId, subjectId);
        if (changed > 0) appendAudit(administrator, tenantId, subjectId, "organization.member.deactivate", "DISABLED");
        return membership(tenantId, subjectId).orElseThrow(EafException::notFound);
    }

    @Override
    @Transactional
    public void addTenantAdministrator(ActorContext administrator, UUID tenantId, UUID subjectId) {
        requireAdministrator(administrator, tenantId);
        lockTenant(administrator, tenantId);
        if (subjectId == null || !isActiveMember(tenantId, subjectId)) throw EafException.notFound();
        var changed = jdbc.update("insert into organization.tenant_admin as current_admin(tenant_id, subject_id, status) "
                        + "values (?, ?, 'ACTIVE') on conflict (tenant_id, subject_id) do update set status = 'ACTIVE', updated_at = now() "
                        + "where current_admin.status <> 'ACTIVE'",
                tenantId, subjectId);
        if (changed > 0) appendAudit(administrator, tenantId, subjectId, "organization.tenant_admin.add", "ACTIVE");
    }

    @Override
    @Transactional
    public void lockForProvisioning(ActorContext administrator, UUID tenantId) {
        requireAdministrator(administrator, tenantId);
        lockTenant(administrator, tenantId);
    }

    @Override
    @Transactional
    public void removeTenantAdministrator(ActorContext administrator, UUID tenantId, UUID subjectId) {
        requireAdministrator(administrator, tenantId);
        lockTenant(administrator, tenantId);
        if (subjectId == null) throw EafException.invalid("管理员 Subject 必须提供。");
        if (administrator.actorId().equals(subjectId))
            throw EafException.conflict("TENANT_ADMIN_SELF_REMOVAL", "管理员不能通过普通成员 API 撤销自己的管理员资格。");
        var targetActive = jdbc.queryForObject("select count(*) from organization.tenant_admin where tenant_id = ? and subject_id = ? and status = 'ACTIVE'",
                Integer.class, tenantId, subjectId);
        if (targetActive == null || targetActive == 0) return;
        var remaining = jdbc.queryForObject("select count(*) from organization.tenant_admin a "
                        + "join organization.member m on m.tenant_id = a.tenant_id and m.subject_id = a.subject_id "
                        + "join organization.tenant t on t.id = a.tenant_id "
                        + "where a.tenant_id = ? and a.subject_id <> ? and a.status = 'ACTIVE' "
                        + "and m.status = 'ACTIVE' and t.status = 'ACTIVE'",
                Integer.class, tenantId, subjectId);
        if (remaining == null || remaining == 0)
            throw EafException.conflict("LAST_TENANT_ADMIN", "必须先登记另一位租户管理员，才能撤销最后一位管理员资格。");
        var changed = jdbc.update("update organization.tenant_admin set status = 'DISABLED', updated_at = now() "
                        + "where tenant_id = ? and subject_id = ? and status = 'ACTIVE'",
                tenantId, subjectId);
        if (changed > 0) appendAudit(administrator, tenantId, subjectId, "organization.tenant_admin.remove", "DISABLED");
    }

    // 管理资格从 Organization 持久记录读取；调用者类型和租户也必须与请求上下文一致。
    private void requireAdministrator(ActorContext administrator, UUID tenantId) {
        if (administrator == null || administrator.type() != ActorType.HUMAN || administrator.delegated()
                || tenantId == null || !tenantId.equals(administrator.tenantId())
                || !isTenantAdministrator(tenantId, administrator.actorId()))
            throw EafException.forbidden("只有当前租户的已登记 HUMAN 管理员可以维护成员。");
    }

    // 外层 coordinator 先锁租户，随后各域按固定顺序处理，避免管理员并发撤销和重入授权死锁。
    private void lockTenant(ActorContext administrator, UUID tenantId) {
        var tenant = jdbc.query("select id from organization.tenant where id = ? for update",
                rs -> rs.next() ? rs.getObject("id", UUID.class) : null, tenantId);
        if (tenant == null) throw EafException.notFound();
        requireAdministrator(administrator, tenantId);
    }

    // 审计事实只含内部主体标识和状态动作，不复制外部 issuer、subject 或认证凭证。
    private void appendAudit(ActorContext administrator, UUID tenantId, UUID subjectId, String action, String result) {
        audit.append(new AuditFact(UUID.randomUUID().toString(), tenantId, null, administrator.actorId(), null,
                action, result, "{\"subjectId\":\"" + subjectId + "\"}", null));
    }
}
// 本文件负责实现 EAF 的 JdbcOrganizationDirectory.java 相关代码。
