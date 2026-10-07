package io.eaf.knowledge.api;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** 对外暴露已通过文档级授权检查的不可变草稿版本详情。 */
public record KnowledgeDocument(UUID id, UUID tenantId, UUID workspaceId, UUID ownerId,
                                String title, String sourceRef, Map<String, String> metadata,
                                int version, String content, String contentHash, String status,
                                Instant createdAt, long rowVersion) { }
// 本文件负责 EAF 的知识文档公开结果值类型。
