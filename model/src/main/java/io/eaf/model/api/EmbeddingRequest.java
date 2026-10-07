package io.eaf.model.api;

import java.time.Instant;
import java.util.List;

/** Embedding 请求固定期望 profile、用途、输入和调用归属；调用方不能声明授权结果。 */
public record EmbeddingRequest(String provider, String model, String revision, String usageSource, List<String> texts,
                               int dimension, int maxInputTokens, Instant deadline, EmbeddingCallScope scope) { }
// 数据外发权限由 Knowledge 在调用前核验，金额则由受信 Gateway 经 Usage 原子预留。
