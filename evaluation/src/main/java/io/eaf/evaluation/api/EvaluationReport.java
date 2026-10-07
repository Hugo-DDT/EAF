package io.eaf.evaluation.api;

import java.util.UUID;
import java.util.List;

// 评测报告区分任务通过率、引用合法性和授权违规；没有独立标注集的指标必须保持 null。
public record EvaluationReport(UUID id, String datasetVersion, int passed, int total, String status,
                               int sampleCount, int modelCalls, int knownUsageSamples,
                               long elapsedMs, List<String> failedCases, Double recallAtK,
                               Double citationValidityRate, Double supportedConclusionRate,
                               int authorizationViolations) { }
// 本文件负责实现 EAF 的 EvaluationReport.java 相关代码。
