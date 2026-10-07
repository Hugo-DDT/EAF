package io.eaf.evaluation.api;

import java.util.UUID;

/** 固定候选评测使用的资产、上下文、数据集与评分版本，不包含保留集输入或答案。 */
public record CandidateEvaluationManifest(UUID candidateId, int candidateRevision, String targetType,
                                          UUID targetId, String baseVersion, String candidateContentHash,
                                          String baselineContextHash, String candidateContextHash,
                                          String datasetVersion, String datasetHash, String datasetRole,
                                          String developmentDatasetVersion, UUID agentId, String agentVersion,
                                          UUID promptId, String promptVersion, UUID modelProfileId,
                                          String scoringVersion, String purpose, String source) { }
