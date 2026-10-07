package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

// 子 Task 继承父任务的身份、来源、协议、资产和根预算，调用方只能给稳定创建键及工作输入。
public record CreateChildTaskCommand(ActorContext actor, UUID workspaceId, UUID parentTaskId,
                                     String creationKey, String input, String traceId) { }
