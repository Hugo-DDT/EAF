package io.eaf.evaluation.api;

import java.util.List;

/** 评测只汇报 Task/Runtime 可见的工具和效果事实；引用 ID 校验不冒充语义蕴含判断。 */
public record CandidateExecutionEvidence(List<String> selectedTools, List<String> permissionOutcomes,
                                         int successfulToolExecutions, String externalEffectStatus,
                                         boolean externalEffectPending, String citationIdStatus,
                                         String contextCurrentness, String citationSemanticSupport) {
    public CandidateExecutionEvidence {
        selectedTools = List.copyOf(selectedTools);
        permissionOutcomes = List.copyOf(permissionOutcomes);
    }
}
