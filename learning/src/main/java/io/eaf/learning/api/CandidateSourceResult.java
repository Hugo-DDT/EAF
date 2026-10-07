package io.eaf.learning.api;

import java.util.UUID;

/** 规则草拟逐个来源返回创建或跳过原因，便于审阅者发现证据缺口。 */
public record CandidateSourceResult(String sourceType, UUID targetId, String outcome,
                                    String reasonCode, UUID candidateId) { }
