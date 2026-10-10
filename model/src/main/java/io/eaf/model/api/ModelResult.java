package io.eaf.model.api;

import java.util.List;

public record ModelResult(String provider, String model, String publicOutput,
                          Integer inputTokens, Integer outputTokens, String usageStatus,
                          List<ModelToolCall> toolCalls, String finishReason,
                          String reportedResponseModel) {
    public ModelResult(String provider, String model, String publicOutput,
                       Integer inputTokens, Integer outputTokens, String usageStatus,
                       List<ModelToolCall> toolCalls, String finishReason) {
        this(provider, model, publicOutput, inputTokens, outputTokens, usageStatus, toolCalls, finishReason, null);
    }

    public ModelResult(String provider, String model, String publicOutput,
                       Integer inputTokens, Integer outputTokens, String usageStatus) {
        this(provider, model, publicOutput, inputTokens, outputTokens, usageStatus, List.of(), "STOP", null);
    }
}
// 本文件负责实现 EAF 的 ModelResult.java 相关代码。
