package io.eaf.agent.api;

import java.util.List;
import java.util.UUID;

/** 登记单个远端 Agent 的稳定身份与本地授权上限，不包含端点或秘密。 */
public record RemoteAgentRegistration(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId,
                                      String agentKey, String agentVersion, UUID capabilityId,
                                      String capabilityVersion, UUID connectorId, String peerSkillId,
                                      List<String> allowedInputFields, List<String> allowedOutputFields,
                                      List<String> delegationActions, String resourceScope) {
    public RemoteAgentRegistration {
        allowedInputFields = List.copyOf(allowedInputFields);
        allowedOutputFields = List.copyOf(allowedOutputFields);
        delegationActions = List.copyOf(delegationActions);
    }
}
