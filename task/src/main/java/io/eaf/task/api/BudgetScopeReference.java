package io.eaf.task.api;

import java.util.UUID;

// 预算范围只暴露稳定 ID，不泄漏 Task 域内部账本实体。
public record BudgetScopeReference(UUID id) { }
