package io.eaf.model.api;

import java.util.UUID;

/** 每次 Embedding 的可信服务端归属，用于 Usage 预算与故障审计。 */
public record EmbeddingCallScope(UUID tenantId, UUID workspaceId, UUID taskId, UUID runId,
                                 String source, String scopeType, UUID scopeId, int callNo,
                                 String callKey, String callType) { }
// Task/Workflow/Evaluation 复用其根 scope；独立索引或 REST 查询使用服务端生成的 JOB scope。
