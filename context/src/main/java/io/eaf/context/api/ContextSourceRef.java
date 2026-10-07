package io.eaf.context.api;

import java.util.UUID;

/** 任务实际使用的上下文来源引用，不暴露正文内容。 */
public record ContextSourceRef(String citationId, String sourceType, UUID documentId, Integer documentVersion,
                               UUID chunkId, UUID buildId, UUID memoryId, String memoryVersion,
                               String contentHash) { }
