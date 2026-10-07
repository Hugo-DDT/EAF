package io.eaf.policy.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

public record ServiceRequestPolicyRequest(ActorContext actor, UUID workspaceId, UUID agentId,
                                         String agentVersion, String toolName, String toolVersion,
                                         String effect, String taskSource) { }
