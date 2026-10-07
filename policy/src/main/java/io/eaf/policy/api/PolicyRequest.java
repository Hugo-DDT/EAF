package io.eaf.policy.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

public record PolicyRequest(ActorContext actor, UUID workspaceId, UUID agentId, String agentVersion,
                            String toolName, String toolVersion, String effect, String customerId,
                            String taskSource) { }
// 本文件负责实现 EAF 的 PolicyRequest.java 相关代码。
