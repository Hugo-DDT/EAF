package io.eaf.evaluation.api;

import java.util.List;
import java.util.UUID;

/** Evaluation 持有的协作评测范围；只公开固定 Workflow 版本和步骤，不公开保留集答案。 */
public record CollaborationEvaluationDefinition(UUID id, String manifestVersion, String purpose, String source,
                                                UUID baselineWorkflowId, String baselineWorkflowVersion,
                                                List<String> baselineStepIds, UUID reviewerWorkflowId,
                                                String reviewerWorkflowVersion, List<String> reviewerStepIds,
                                                String status) {
    public CollaborationEvaluationDefinition {
        baselineStepIds = baselineStepIds == null ? List.of() : List.copyOf(baselineStepIds);
        reviewerStepIds = reviewerStepIds == null ? List.of() : List.copyOf(reviewerStepIds);
    }
}
