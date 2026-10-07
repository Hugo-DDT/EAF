package io.eaf.organization.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

/** 由 Organization 自己维护租户成员，身份和 Workspace 模块只能通过此 API 改变成员状态。 */
public interface OrganizationMembershipAdministration {
    /** provisioning 在多域变更开始时调用，用租户行锁固定管理员与成员生命周期的串行顺序。 */
    void lockForProvisioning(ActorContext administrator, UUID tenantId);

    /** 创建成员或显式恢复同一租户的历史成员；跨租户转移由实现拒绝。 */
    Membership addMember(ActorContext administrator, UUID tenantId, UUID subjectId);

    /** 只改变 Organization 成员状态；生产入口须由 provisioning 协调下游映射和授权撤销。 */
    Membership deactivateMember(ActorContext administrator, UUID tenantId, UUID subjectId);

    /** 仅供 Identity 在验证目标为有效 HUMAN 后登记；首位管理员仍只由受保护部署建立。 */
    void addTenantAdministrator(ActorContext administrator, UUID tenantId, UUID subjectId);

    /** 撤销另一位管理员的资格，不能撤销调用者自身或最后一位有效管理员。 */
    void removeTenantAdministrator(ActorContext administrator, UUID tenantId, UUID subjectId);
}
// 成员变更只写 Organization 自有记录；成员登录映射和资源授权分别由各自域维护。
