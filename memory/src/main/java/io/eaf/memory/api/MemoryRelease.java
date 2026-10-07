package io.eaf.memory.api;

import java.time.Instant;
import java.util.UUID;

// 发布结果保留来源键和内容摘要；查询方仍需通过 Memory API 通过资源可见性检查。
public record MemoryRelease(UUID releaseId, UUID memoryId, String memoryVersion, String action,
                            String releaseOrigin, UUID candidateId, Integer candidateRevision,
                            String baseVersion, String contentHash, UUID actorId, Instant occurredAt) { }
