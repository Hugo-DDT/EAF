package io.eaf.context.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** 实际送入模型的知识或记忆片段及可复核的来源版本。 */
public record ContextItem(String citationId, String sourceType, UUID documentId, int documentVersion, UUID chunkId,
                          UUID buildId, UUID memoryId, String memoryVersion, String sourceRef, String contentHash,
                          String content, Double distance, int estimatedTokens, String scope, List<String> evidenceRefs,
                          Instant expiresAt, String businessEntityType, String businessEntityId,
                          List<String> headingPath, Integer startOffset, Integer endOffset, String offsetUnit) {
    public ContextItem(String citationId, String sourceType, UUID documentId, int documentVersion, UUID chunkId,
                       UUID buildId, UUID memoryId, String memoryVersion, String sourceRef, String contentHash,
                       String content, Double distance, int estimatedTokens, String scope, List<String> evidenceRefs,
                       Instant expiresAt, String businessEntityType, String businessEntityId) {
        this(citationId, sourceType, documentId, documentVersion, chunkId, buildId, memoryId, memoryVersion,
                sourceRef, contentHash, content, distance, estimatedTokens, scope, evidenceRefs, expiresAt,
                businessEntityType, businessEntityId, List.of(), null, null, null);
    }

    public ContextItem(String citationId, String sourceType, UUID documentId, int documentVersion, UUID chunkId,
                       UUID buildId, UUID memoryId, String memoryVersion, String sourceRef, String contentHash,
                       String content, Double distance, int estimatedTokens, String scope, List<String> evidenceRefs,
                       Instant expiresAt, String businessEntityType, String businessEntityId, List<String> headingPath) {
        this(citationId, sourceType, documentId, documentVersion, chunkId, buildId, memoryId, memoryVersion,
                sourceRef, contentHash, content, distance, estimatedTokens, scope, evidenceRefs, expiresAt,
                businessEntityType, businessEntityId, headingPath, null, null, null);
    }
}
// citationId 只对本次结果有效；Knowledge 的旧历史缺少 sourceType 时按 KNOWLEDGE 读取。
