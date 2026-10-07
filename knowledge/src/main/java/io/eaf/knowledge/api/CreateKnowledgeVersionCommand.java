package io.eaf.knowledge.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

// 新知识内容以 CAS 和幂等键追加为版本，不原地覆盖已发布正文。
public record CreateKnowledgeVersionCommand(ActorContext actor, UUID workspaceId, UUID documentId,
                                           long expectedRowVersion, String content,
                                           String idempotencyKey) { }
