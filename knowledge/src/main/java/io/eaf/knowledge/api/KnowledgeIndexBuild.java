package io.eaf.knowledge.api;

import java.time.Instant;
import java.util.UUID;

/** 对外返回索引作业状态，不把 JDBC 行或原始向量暴露给调用方。 */
public record KnowledgeIndexBuild(UUID id, UUID tenantId, UUID workspaceId, UUID documentId,
                                  int assetVersion, String chunkingVersion, String provider,
                                  String model, String modelRevision, int dimension,
                                  String distanceMetric, String configurationSignature,
                                  String status, int totalChunks, int completedChunks,
                                  int nextChunkOrder, int inputTokens, int embeddingCalls,
                                  String failureCode, Instant createdAt, Instant startedAt,
                                  Instant completedAt, Instant updatedAt) { }
// 状态是派生索引的事实；原文和权限仍由 knowledge 文档/版本表负责。
