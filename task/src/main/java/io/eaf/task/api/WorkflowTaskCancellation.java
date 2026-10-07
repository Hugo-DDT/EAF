package io.eaf.task.api;

import java.util.UUID;

// Task 域返回取消结果，Workflow 据此继续等待核验或记录已发生的业务效果。
public record WorkflowTaskCancellation(UUID taskId, TaskStatus status, String externalEffectStatus,
                                       boolean externalEffectPending) { }
