package io.eaf.knowledge.api;

import java.util.UUID;
import java.util.List;

/** 检索命中携带原文定位和配置事实，调用方无需接触原始向量。 */
public record KnowledgeSearchHit(UUID tenantId, UUID workspaceId, UUID documentId, int documentVersion,
                                 UUID chunkId, String sourceRef, String chunkingVersion, int chunkOrder,
                                 int startOffset, int endOffset, String offsetUnit, String content,
                                 String contentHash, UUID buildId, String configurationSignature,
                                 String distanceMetric, double distance, List<String> headingPath,
                                 List<String> channels, Integer vectorRank, Integer lexicalRank,
                                 Double rrfScore, String normalizationVersion) {
    public KnowledgeSearchHit(UUID tenantId, UUID workspaceId, UUID documentId, int documentVersion,
                              UUID chunkId, String sourceRef, String chunkingVersion, int chunkOrder,
                              int startOffset, int endOffset, String offsetUnit, String content,
                              String contentHash, UUID buildId, String configurationSignature,
                              String distanceMetric, double distance) {
        this(tenantId, workspaceId, documentId, documentVersion, chunkId, sourceRef, chunkingVersion,
                chunkOrder, startOffset, endOffset, offsetUnit, content, contentHash, buildId,
                configurationSignature, distanceMetric, distance, List.of(), List.of("VECTOR"), null,
                null, null, null);
    }

    public KnowledgeSearchHit(UUID tenantId, UUID workspaceId, UUID documentId, int documentVersion,
                              UUID chunkId, String sourceRef, String chunkingVersion, int chunkOrder,
                              int startOffset, int endOffset, String offsetUnit, String content,
                              String contentHash, UUID buildId, String configurationSignature,
                              String distanceMetric, double distance, List<String> headingPath) {
        this(tenantId, workspaceId, documentId, documentVersion, chunkId, sourceRef, chunkingVersion,
                chunkOrder, startOffset, endOffset, offsetUnit, content, contentHash, buildId,
                configurationSignature, distanceMetric, distance, headingPath, List.of("VECTOR"), null,
                null, null, null);
    }
}
// 命中只来自当前授权的正式发布；contentHash 与位置用于后续引用校验。
