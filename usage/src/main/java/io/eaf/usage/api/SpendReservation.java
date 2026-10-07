package io.eaf.usage.api;

import java.math.BigDecimal;

// 不允许出站时返回固定原因；保留金额与币种供调用方记录安全诊断。
public record SpendReservation(boolean allowed, String code, BigDecimal amount, String currency) { }
