package io.eaf.skill.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

// Owner 取自可信 ActorContext，不允许请求体指定其他所有者。
public record CreateSkillCommand(ActorContext actor, UUID workspaceId, String name, String description,
                                 CreateSkillVersionCommand initialVersion) { }
