package io.eaf.evaluation.api;

import io.eaf.usage.api.UsageRecord;
import java.util.List;
import java.util.UUID;

// 单条报告样本公开观察结果和逐调用用量，不公开保留集输入或预期答案。
public record CandidateEvaluationSample(String caseId, String cohort, int sampleNo,
                                        UUID baselineTaskId, String baselineStatus, String baselineErrorCode,
                                        String baselineObservedRisk, boolean baselineCorrect,
                                        boolean baselineCitationSupported, int baselineModelCalls, List<UsageRecord> baselineUsage,
                                        CandidateExecutionEvidence baselineEvidence,
                                        UUID candidateTaskId, String candidateStatus, String candidateErrorCode,
                                        String candidateObservedRisk, boolean candidateCorrect,
                                        boolean candidateCitationSupported, int candidateModelCalls, List<UsageRecord> candidateUsage,
                                        CandidateExecutionEvidence candidateEvidence, long elapsedMs,
                                        boolean safetyViolation) {
    public CandidateEvaluationSample {
        baselineUsage = List.copyOf(baselineUsage);
        candidateUsage = List.copyOf(candidateUsage);
    }
}
