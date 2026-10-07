package io.eaf.capability.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

public record CreateCapabilityCommand(ActorContext actor, UUID workspaceId, String name,
                                      String description, CreateCapabilityVersionCommand initialVersion) { }
// 本文件负责实现 CreateCapabilityCommand.java 相关代码。
