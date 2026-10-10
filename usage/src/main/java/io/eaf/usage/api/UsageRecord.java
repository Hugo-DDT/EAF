package io.eaf.usage.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record UsageRecord(UUID tenantId, UUID workspaceId, UUID taskId, UUID runId, String source,
                          String provider, String model, Integer inputTokens, Integer outputTokens,
                          String usageStatus, BigDecimal estimatedCost, String costCurrency,
                          String costStatus, String costSource, int reservedTokens, String status, String errorCode,
                          Instant startedAt, Instant endedAt, int callNo, String callKey, String callType,
                          String scopeType, UUID scopeId, String priceVersion, String priceSourceVersion,
                          Instant priceEffectiveAt, String billingUnit, BigDecimal inputPricePerMillion,
                          BigDecimal outputPricePerMillion, BigDecimal requestPrice, BigDecimal actualCost,
                          String actualCostCurrency, String billingSource, UUID modelProfileId,
                          String modelProfileVersion, String modelConfigurationHash, String requestedModel,
                          Integer effectiveOutputTokenLimit, String reportedResponseModel, String finishReason,
                          Long modelOperationMillis) {
    public UsageRecord(UUID tenantId, UUID workspaceId, UUID taskId, UUID runId, String source,
                       String provider, String model, Integer inputTokens, Integer outputTokens,
                       String usageStatus, BigDecimal estimatedCost, String costCurrency,
                       String costStatus, String costSource, int reservedTokens, String status, String errorCode,
                       Instant startedAt, Instant endedAt, int callNo, String callKey, String callType,
                       String scopeType, UUID scopeId, String priceVersion, String priceSourceVersion,
                       Instant priceEffectiveAt, String billingUnit, BigDecimal inputPricePerMillion,
                       BigDecimal outputPricePerMillion, BigDecimal requestPrice, BigDecimal actualCost,
                       String actualCostCurrency, String billingSource) {
        this(tenantId, workspaceId, taskId, runId, source, provider, model, inputTokens, outputTokens, usageStatus,
                estimatedCost, costCurrency, costStatus, costSource, reservedTokens, status, errorCode, startedAt,
                endedAt, callNo, callKey, callType, scopeType, scopeId, priceVersion, priceSourceVersion,
                priceEffectiveAt, billingUnit, inputPricePerMillion, outputPricePerMillion, requestPrice,
                actualCost, actualCostCurrency, billingSource, null, null, null, null, null, null, null, null);
    }
    // 旧调用方按 Task/Run/调用序号生成稳定键，统一视作聊天模型调用。
    public UsageRecord(UUID tenantId, UUID workspaceId, UUID taskId, UUID runId, String source,
                       String provider, String model, Integer inputTokens, Integer outputTokens,
                       String usageStatus, BigDecimal estimatedCost, String costCurrency,
                       String costStatus, String costSource, int reservedTokens, String status, String errorCode,
                       Instant startedAt, Instant endedAt, int callNo) {
        this(tenantId, workspaceId, taskId, runId, source, provider, model, inputTokens, outputTokens, usageStatus,
                estimatedCost, costCurrency, costStatus, costSource, reservedTokens, status, errorCode, startedAt,
                endedAt, callNo, legacyCallKey(taskId, runId, callNo), "CHAT",
                "EVALUATION".equals(source) ? "EVALUATION" : "TASK", taskId,
                null, null, null, null, null, null, null, null, null, null);
    }

    // 非 Task 的 Embedding/peer 作业也通过同一账本记录稳定作用域，不伪造 Task 或 Runtime ID。
    public UsageRecord(UUID tenantId, UUID workspaceId, UUID taskId, UUID runId, String source,
                       String provider, String model, Integer inputTokens, Integer outputTokens,
                       String usageStatus, int reservedTokens, String status, String errorCode,
                       Instant startedAt, Instant endedAt, int callNo, String callKey, String callType,
                       String scopeType, UUID scopeId) {
        this(tenantId, workspaceId, taskId, runId, source, provider, model, inputTokens, outputTokens, usageStatus,
                null, null, "UNKNOWN_PRICE", null, reservedTokens, status, errorCode, startedAt, endedAt, callNo,
                callKey, callType, scopeType, scopeId, null, null, null, null, null, null, null, null, null, null);
    }

    public UsageRecord(UUID tenantId, UUID workspaceId, UUID taskId, UUID runId, String source,
                       String provider, String model, Integer inputTokens, Integer outputTokens,
                       String usageStatus, BigDecimal estimatedCost, String costCurrency,
                       String costStatus, String costSource, int reservedTokens, String status, String errorCode,
                       Instant startedAt, Instant endedAt) {
        this(tenantId, workspaceId, taskId, runId, source, provider, model, inputTokens, outputTokens, usageStatus,
                estimatedCost, costCurrency, costStatus, costSource, reservedTokens, status, errorCode,
                startedAt, endedAt, 1);
    }

    private static String legacyCallKey(UUID taskId, UUID runId, int callNo) {
        return taskId == null || runId == null ? null : "task:" + taskId + ":run:" + runId + ":call:" + callNo;
    }
}
