package io.eaf.evaluation.api;

import io.eaf.shared.ActorContext;
import java.util.UUID;

/** 事实与引用语义复核由独立人员提交，决定不改写运行冻结的自动评分结果。 */
public record CandidateSampleReviewCommand(ActorContext actor, UUID workspaceId, UUID reportId,
                                           String caseId, int sampleNo, String factualDecision,
                                           String citationSemanticSupport, String rationale) { }
