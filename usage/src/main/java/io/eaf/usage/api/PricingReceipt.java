package io.eaf.usage.api;

import java.math.BigDecimal;
import java.time.Instant;

/** 预留时使用的窄计价快照，不含凭据或请求正文。 */
public record PricingReceipt(String callKey, String provider, String model, String callType, long maxTokens,
                             String priceVersion, String priceSource, String priceSourceVersion,
                             Instant priceEffectiveAt, String billingUnit, BigDecimal inputPricePerMillion,
                             BigDecimal outputPricePerMillion, BigDecimal requestPrice, String currency,
                             BigDecimal reservedAmount, String receiptHash) { }
