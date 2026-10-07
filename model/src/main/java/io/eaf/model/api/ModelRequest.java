package io.eaf.model.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ModelRequest(UUID profileId, List<ModelMessage> messages, Instant deadline,
                           int tokenBudget, UUID taskId, String traceId,
                           List<ModelToolDefinition> tools, int callNo) {
    public ModelRequest(UUID profileId, List<ModelMessage> messages, Instant deadline,
                        int tokenBudget, UUID taskId, String traceId) {
        this(profileId, messages, deadline, tokenBudget, taskId, traceId, List.of(), 1);
    }
}
// 本文件负责实现 EAF 的 ModelRequest.java 相关代码。
