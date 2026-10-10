package io.eaf.model.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ModelRequest(UUID profileId, List<ModelMessage> messages, Instant deadline,
                           int tokenBudget, UUID taskId, String traceId,
                           List<ModelToolDefinition> tools, int callNo,
                           ModelProfileSnapshot profileSnapshot) {
    public ModelRequest(UUID profileId, List<ModelMessage> messages, Instant deadline,
                        int tokenBudget, UUID taskId, String traceId,
                        List<ModelToolDefinition> tools, int callNo) {
        this(profileId, messages, deadline, tokenBudget, taskId, traceId, tools, callNo, null);
    }

    public ModelRequest(UUID profileId, List<ModelMessage> messages, Instant deadline,
                        int tokenBudget, UUID taskId, String traceId) {
        this(profileId, messages, deadline, tokenBudget, taskId, traceId, List.of(), 1, null);
    }

    public int effectiveOutputTokenLimit() {
        if (profileSnapshot == null) return 0;
        var estimatedInput = messages == null ? 0 : messages.stream()
                .mapToInt(message -> message.content() == null ? 0 : message.content().length()).sum();
        return Math.max(0, Math.min(profileSnapshot.maxOutputTokens(), tokenBudget - estimatedInput));
    }
}
// 本文件负责实现 EAF 的 ModelRequest.java 相关代码。
