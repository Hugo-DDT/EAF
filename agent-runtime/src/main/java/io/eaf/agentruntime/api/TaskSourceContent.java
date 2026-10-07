package io.eaf.agentruntime.api;

import java.util.List;
import java.util.UUID;

/** 当前 attempt 最终上下文中经来源 currentness 复核的可读片段。 */
public record TaskSourceContent(String citationId, String sourceType, UUID documentId, Integer documentVersion,
                                UUID chunkId, UUID buildId, UUID memoryId, String memoryVersion,
                                String sourceRef, List<String> headingPath, Integer startOffset,
                                Integer endOffset, String offsetUnit, String content) { }
