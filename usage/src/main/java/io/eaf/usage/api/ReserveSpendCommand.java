package io.eaf.usage.api;

import java.math.BigDecimal;
import java.util.UUID;

// Scope 预算只能由受信服务器调用方给定，Provider 身份来自 Model Gateway。
public record ReserveSpendCommand(UUID tenantId, UUID workspaceId, String scopeType, UUID scopeId,
                                 String callKey, String provider, String model, String callType,
                                 long maxTokens, BigDecimal limitAmount, String limitCurrency) { }
