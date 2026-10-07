package io.eaf.identity.api;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** 只返回页面展示所需的成员名称，不暴露身份凭据或外部映射。 */
public interface IdentityDirectory {
    Map<UUID, String> displayNames(UUID tenantId, Set<UUID> subjectIds);

    /** 返回有效同租户的 HUMAN 成员身份事实；不暴露身份凭据或外部映射。 */
    boolean isActiveHuman(UUID tenantId, UUID subjectId);
}
