package io.eaf.evaluation.api;

import java.util.UUID;

// 样本证据只返回执行结果和实际观察值，不返回输入正文或预期答案。
public record P3QualitySample(String caseId, int sampleNo, UUID taskId, String status,
                              boolean passed, String observedRisk, int modelCalls,
                              Integer inputTokens, Integer outputTokens, long elapsedMs,
                              String errorCode) { }
