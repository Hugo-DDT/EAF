package io.eaf.skill.api;

import io.eaf.shared.ActorContext;
import java.util.List;
import java.util.UUID;

// 跨域调用只暴露已授权的 Skill 版本，不暴露持久化实体。
public interface SkillService {
    SkillDefinition create(CreateSkillCommand command);
    SkillDefinition addVersion(ActorContext actor, UUID workspaceId, UUID skillId, CreateSkillVersionCommand command);
    List<SkillDefinition> list(ActorContext actor, UUID workspaceId);
    SkillDefinition get(ActorContext actor, UUID workspaceId, UUID skillId, String version);
    SkillDefinition requirePublished(ActorContext actor, UUID workspaceId, UUID skillId, String version);
    SkillDefinition publish(ActorContext actor, UUID workspaceId, UUID skillId, String version, long expectedVersion);
    SkillDefinition revoke(ActorContext actor, UUID workspaceId, UUID skillId, String version, long expectedVersion);
}
