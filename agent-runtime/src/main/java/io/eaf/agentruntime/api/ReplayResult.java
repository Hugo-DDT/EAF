package io.eaf.agentruntime.api;

import io.eaf.task.api.TaskStatus;
import java.util.UUID;

public record ReplayResult(UUID taskId, TaskStatus originalStatus, TaskStatus replayStatus,
                           boolean modelCalled, boolean matched, String resultJson, String errorCode) { }
// 本文件负责实现 EAF 的 ReplayResult.java 相关代码。
