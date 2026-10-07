package io.eaf.agentruntime.api;

import io.eaf.shared.ActorContext;
import io.eaf.context.api.ContextSourceRef;
import io.eaf.task.api.TaskStepView;
import io.eaf.context.api.TeamExperienceUsage;
import java.util.List;
import java.util.UUID;

public interface RuntimeQuery {
    List<TaskStepView> steps(UUID tenantId, UUID taskId);
    List<ContextSourceRef> contextSources(ActorContext actor, UUID workspaceId, UUID taskId);
    TaskSources contextSourceContents(ActorContext actor, UUID workspaceId, UUID taskId);
    // 只有 Evaluation Owner 可读取当前样本的当次来源正文；通用来源接口对一律隐藏。
    TaskSources scenarioContextSourceContents(ActorContext actor, UUID workspaceId, UUID taskId);
    /** 当前 Task attempt 实际带入的 TEAM 卡精确版本；来源失效或不可读时拒绝返回。 */
    TeamExperienceUsage teamExperienceUsage(ActorContext actor, UUID workspaceId, UUID taskId);
    ReplayResult replay(UUID tenantId, UUID workspaceId, UUID taskId);

    // 面向协议的查询必须由实现校验当前 Actor、Workspace 与资产授权；不得回退到租户级历史读取。
    List<TaskStepView> steps(ActorContext actor, UUID workspaceId, UUID taskId);
    ReplayResult replay(ActorContext actor, UUID workspaceId, UUID taskId);
    boolean canExposeResult(ActorContext actor, UUID workspaceId, UUID taskId);
}
// 本文件负责实现 EAF 的 RuntimeQuery.java 相关代码。
