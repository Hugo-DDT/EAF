package io.eaf.identity.api;

import io.eaf.shared.ActorContext;
import java.util.Optional;
import java.util.UUID;

/** Identity 的主体与企业映射管理 API；仅当前租户管理员可调用。 */
public interface IdentityAdministration {
    /** 创建不带默认授权的内部 HUMAN 主体，后续须由 Organization 与 Workspace 显式开通。 */
    UUID createHumanSubject(ActorContext tenantAdministrator, UUID tenantId, String displayName);

    /** 查询固定 issuer 下的映射；只允许读取本租户主体映射并保留停用映射以防止重绑。 */
    Optional<ExternalIdentityBinding> findExternalIdentity(ActorContext tenantAdministrator, UUID tenantId,
                                                           String issuer, String externalSubject);

    /** 只绑定部署固定 issuer，已绑定到另一内部主体的 issuer/subject 永不转移。 */
    ExternalIdentityBinding bindExternalIdentity(ActorContext tenantAdministrator, UUID tenantId, UUID actorId,
                                                 String issuer, String externalSubject);

    /** 只允许提名已加入本租户的有效 HUMAN；Organization 保存管理员资格。 */
    void appointTenantAdministrator(ActorContext tenantAdministrator, UUID tenantId, UUID actorId);

    /** 停用成员时使其所有外部映射失效，重入必须经管理员显式重新绑定。 */
    int revokeExternalIdentities(ActorContext tenantAdministrator, UUID tenantId, UUID actorId);
}
