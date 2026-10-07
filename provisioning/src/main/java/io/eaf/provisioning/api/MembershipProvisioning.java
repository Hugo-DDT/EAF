package io.eaf.provisioning.api;

import io.eaf.shared.ActorContext;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** 跨域成员生命周期入口；成员、身份和各类授权的真实状态仍分别归各自模块。 */
public interface MembershipProvisioning {
    ProvisionedHuman onboardHuman(ActorContext tenantAdministrator, UUID tenantId, String displayName,
                                  String issuer, String externalSubject,
                                  Map<UUID, Set<String>> workspaceActions,
                                  Map<UUID, Set<String>> customerIds);

    void offboardMember(ActorContext tenantAdministrator, UUID tenantId, UUID subjectId);
}
