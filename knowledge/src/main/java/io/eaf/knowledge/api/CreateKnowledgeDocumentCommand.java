package io.eaf.knowledge.api;

import io.eaf.shared.ActorContext;
import java.util.Map;
import java.util.UUID;

/** 知识导入命令；owner、tenant 和状态不由请求体提供。 */
public record CreateKnowledgeDocumentCommand(ActorContext actor, UUID workspaceId, String title,
                                             String sourceRef, String content, Map<String, String> metadata,
                                             String idempotencyKey, String traceId) { }
// 本文件负责 EAF 的知识导入命令值类型。
