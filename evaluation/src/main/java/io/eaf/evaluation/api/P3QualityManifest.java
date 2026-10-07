package io.eaf.evaluation.api;

import java.util.UUID;

// 公开清单只标识数据与配置版本，不包含合成输入或保留集答案。
public record P3QualityManifest(String datasetVersion, String datasetHash, UUID agentId, String agentVersion,
                                UUID promptId, String promptVersion, UUID modelProfileId,
                                String scoringVersion, String configurationHash, int datasetCases,
                                int samplesPerCase) { }
