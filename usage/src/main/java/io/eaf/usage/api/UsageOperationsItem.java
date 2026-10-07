package io.eaf.usage.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Usage Owner 暴露的计量和核账摘要，不返回稳定调用键或供应商请求正文。 */
public record UsageOperationsItem(UUID usageId, UUID taskId, UUID runId, String source, String provider,
                                  String model, String callType, String status, String usageStatus,
                                  String costStatus, Integer inputTokens, Integer outputTokens,
                                  BigDecimal estimatedCost, BigDecimal actualCost, String currency,
                                  String errorCode, Instant startedAt, Instant endedAt) { }
