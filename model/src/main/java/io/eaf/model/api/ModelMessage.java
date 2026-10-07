package io.eaf.model.api;

import java.util.List;

/** 模型消息保留工具调用批次和工具回复关联，供 Provider 往返及 Runtime 恢复使用。 */
public record ModelMessage(String role, String content, String toolCallId, String toolName,
                           List<ModelToolCall> toolCalls) {
    public ModelMessage {
        toolCalls = toolCalls == null ? List.of() : List.copyOf(toolCalls);
    }

    public ModelMessage(String role, String content) { this(role, content, null, null, List.of()); }
    public ModelMessage(String role, String content, String toolCallId, String toolName) {
        this(role, content, toolCallId, toolName, List.of());
    }
    public static ModelMessage assistant(List<ModelToolCall> calls) { return new ModelMessage("assistant", "", null, null, calls); }
    public static ModelMessage tool(String callId, String name, String content) {
        return new ModelMessage("tool", content, callId, name, List.of());
    }
}
// 本文件负责实现 EAF 的 ModelMessage.java 相关代码。
