package io.eaf.task.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

// 调用方提供稳定幂等键与请求摘要，预算限额始终由 Task 域决定。
public record CreateBudgetScopeCommand(ActorContext actor, UUID workspaceId, String idempotencyKey,
                                       String requestHash) { }
