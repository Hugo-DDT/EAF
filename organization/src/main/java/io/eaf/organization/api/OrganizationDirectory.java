package io.eaf.organization.api;

import java.util.Optional;
import java.util.UUID;

public interface OrganizationDirectory {
    Optional<Membership> membership(UUID subjectId);

    // Workspace 在每次授权检查时按明确租户查询成员状态，避免旧 ActorContext 跨过停用操作。
    Optional<Membership> membership(UUID tenantId, UUID subjectId);

    default boolean isActiveMember(UUID tenantId, UUID subjectId) {
        return membership(tenantId, subjectId).map(Membership::active).orElse(false);
    }

    /** 在授权变更事务中锁定租户成员生命周期，防止撤权与新授权并发交错。 */
    void lockActiveMembership(UUID tenantId, UUID subjectId);

    // 初始租户管理员仅由受保护的部署开通流程登记，业务请求不能从 token claims 获取管理员身份。
    boolean isTenantAdministrator(UUID tenantId, UUID subjectId);

    // MVP 不允许同一内部 Subject 被迁入其他租户；已存在的历史成员行也必须显式审查。
    boolean hasMembershipInOtherTenant(UUID tenantId, UUID subjectId);
}
// 本文件负责实现 EAF 的 OrganizationDirectory.java 相关代码。
