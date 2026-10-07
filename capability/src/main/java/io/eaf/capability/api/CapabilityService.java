package io.eaf.capability.api;

import io.eaf.shared.ActorContext;
import java.util.List;
import java.util.UUID;

// 跨域只公开经 Workspace 授权解析的 Capability Manifest。
public interface CapabilityService {
    CapabilityDefinition create(CreateCapabilityCommand command);
    CapabilityDefinition addVersion(ActorContext actor, UUID workspaceId, UUID capabilityId, CreateCapabilityVersionCommand command);
    List<CapabilityDefinition> list(ActorContext actor, UUID workspaceId);
    CapabilityDefinition get(ActorContext actor, UUID workspaceId, UUID capabilityId, String version);
    CapabilityDefinition requirePublished(ActorContext actor, UUID workspaceId, UUID capabilityId, String version);
    CapabilityDefinition publish(ActorContext actor, UUID workspaceId, UUID capabilityId, String version, long expectedVersion);
    CapabilityDefinition revoke(ActorContext actor, UUID workspaceId, UUID capabilityId, String version, long expectedVersion);
}
