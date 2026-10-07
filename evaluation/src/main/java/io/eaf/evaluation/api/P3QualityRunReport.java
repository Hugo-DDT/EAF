package io.eaf.evaluation.api;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

// 本报告用可重现清单、运行级费用和逐样本证据支持授权前的离线审阅。
public record P3QualityRunReport(UUID id, P3QualityManifest manifest, String status, String failureReason,
                                 int datasetCases, int totalSamples, int completedSamples,
                                 int passedSamples, int failedSamples, int safetyViolations,
                                 int modelCalls, int knownUsageCalls, int unknownUsageSamples,
                                 Long inputTokens, Long outputTokens, BigDecimal estimatedCost,
                                 String costStatus, String costCurrency, BigDecimal actualCost,
                                 String actualCostCurrency, String billingStatus,
                                 Long latencyP50Ms, Long latencyP95Ms, long elapsedMs,
                                 List<P3QualitySample> samples) {
    public P3QualityRunReport {
        samples = List.copyOf(samples);
    }
}
