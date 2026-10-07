package io.eaf.task.api;

import java.util.UUID;

// Task 域只返回本次已过期的 Task attempt 引用，Runtime 用它清理自己的运行记录。
public record TaskAttemptRecovery(UUID taskId, int attempt) { }
