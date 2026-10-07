package io.eaf.task.api;

import java.util.UUID;

// Capability 入口的不可变引用快照；空值表示旧版直接 Agent 请求。
public record TaskAssetBinding(UUID capabilityId, String capabilityVersion, String capabilityHash,
                               UUID skillId, String skillVersion, String skillHash) { }
