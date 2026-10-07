package io.eaf.evaluation.api;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

// 报告绑定候选快照与完整配置摘要；样本列表没有评测输入和保留集答案。
public record CandidateEvaluationReport(UUID id, UUID snapshotId, UUID candidateId, int candidateRevision,
                                        String targetType, UUID targetId, String baseVersion,
                                        String candidateContentHash, String baselineContextHash,
                                        String candidateContextHash, String datasetVersion, String datasetHash,
                                        UUID agentId, String agentVersion, UUID promptId, String promptVersion,
                                        UUID modelProfileId, String scoringVersion, String configurationHash,
                                        String status, int datasetCases, int totalSamples, int completedSamples,
                                        int baselineCorrect, int candidateCorrect, int targetSamples,
                                        int targetBaselineCorrect, int targetCandidateCorrect,
                                        int targetImprovedCases, int nonTargetRegressionCases, int safetyViolations,
                                        int citationSupportedCalls, int citationScoredCalls, int modelCalls,
                                        int knownUsageCalls, Long inputTokens, Long outputTokens,
                                        BigDecimal estimatedCost, String costStatus, long elapsedMs,
                                        String costCurrency, BigDecimal actualCost, String actualCostCurrency,
                                        String billingStatus, String failureReason, String purpose, String source,
                                        UUID spendScopeId, int failedSamples, int unknownUsageSamples,
                                        BigDecimal candidateAccuracyVariance, Long latencyP50Ms, Long latencyP95Ms,
                                        CandidateEvaluationManifest manifest, List<CandidateEvaluationSample> samples) {
    public CandidateEvaluationReport {
        samples = List.copyOf(samples);
    }
}
