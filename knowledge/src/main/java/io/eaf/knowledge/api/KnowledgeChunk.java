package io.eaf.knowledge.api;

import java.util.UUID;
import java.util.List;

/** 对外暴露可由原文位置和哈希校验的知识片段。 */
public record KnowledgeChunk(UUID id, UUID tenantId, UUID workspaceId, UUID documentId,
                             int documentVersion, String sourceRef, String chunkingVersion,
                             int chunkOrder, int startOffset, int endOffset, String offsetUnit,
                             String content, String contentHash, List<String> headingPath) {
    public KnowledgeChunk(UUID id, UUID tenantId, UUID workspaceId, UUID documentId,
                          int documentVersion, String sourceRef, String chunkingVersion,
                          int chunkOrder, int startOffset, int endOffset, String offsetUnit,
                          String content, String contentHash) {
        this(id, tenantId, workspaceId, documentId, documentVersion, sourceRef, chunkingVersion,
                chunkOrder, startOffset, endOffset, offsetUnit, content, contentHash, List.of());
    }
}
// 本类型只描述 knowledge 的不可变切块结果，不承载 Embedding 或检索状态。
