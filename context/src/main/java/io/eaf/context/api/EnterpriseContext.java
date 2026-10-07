package io.eaf.context.api;

import java.util.List;

/** 有界 EnterpriseContext；状态区分无证据、预算裁剪和正常命中。 */
public record EnterpriseContext(String status, int topK, int tokenBudget, int usedTokens,
                                int omittedCount, String truncationReason, List<ContextItem> items,
                                ExperienceUsage experienceUsage, TeamExperienceUsage teamExperienceUsage) {
    public EnterpriseContext(String status, int topK, int tokenBudget, int usedTokens, int omittedCount,
                             String truncationReason, List<ContextItem> items) {
        this(status, topK, tokenBudget, usedTokens, omittedCount, truncationReason, items, null, null);
    }
    public EnterpriseContext(String status, int topK, int tokenBudget, int usedTokens, int omittedCount,
                             String truncationReason, List<ContextItem> items, ExperienceUsage experienceUsage) {
        this(status, topK, tokenBudget, usedTokens, omittedCount, truncationReason, items, experienceUsage, null);
    }
}
// 这是本次调用的派生 DTO，不持久化，也不替代 knowledge 的正式事实。
