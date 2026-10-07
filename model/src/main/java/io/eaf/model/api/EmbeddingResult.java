package io.eaf.model.api;

import java.time.Instant;
import java.util.List;

/** Embedding 结果包含配置和可计量输入，不隐藏维度或模型来源。 */
public record EmbeddingResult(String provider, String model, int dimension, List<List<Float>> vectors,
                              int inputTokens, String usageStatus, Instant startedAt, Instant endedAt) { }
// 向量是派生结果；原文仍由 knowledge 保存，不能用本结果替代授权和版本信息。
