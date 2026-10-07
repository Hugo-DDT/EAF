package io.eaf.task.api;

import java.time.Instant;
import java.util.UUID;

public record TaskStepView(int stepNo, String type, String role, String content,
                           String validation, Instant occurredAt, Integer callNo, UUID executionId) {
    public TaskStepView(int stepNo, String type, String role, String content,
                        String validation, Instant occurredAt) {
        this(stepNo, type, role, content, validation, occurredAt, null, null);
    }
}
// 本文件负责实现 EAF 的 TaskStepView.java 相关代码。
