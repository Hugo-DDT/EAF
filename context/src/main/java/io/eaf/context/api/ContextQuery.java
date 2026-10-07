package io.eaf.context.api;

/** 上下文查询参数；服务端限制 topK 和 tokenBudget，避免绕过任务预算。 */
public record ContextQuery(String query, Integer topK, Integer tokenBudget, String businessEntityType,
                            String businessEntityId, String retrievalMode, String memoryPolicy) {
    public ContextQuery(String query, Integer topK, Integer tokenBudget, String businessEntityType,
                        String businessEntityId, String retrievalMode) {
        this(query, topK, tokenBudget, businessEntityType, businessEntityId, retrievalMode, "LEGACY");
    }
    public ContextQuery(String query, Integer topK, Integer tokenBudget) {
        this(query, topK, tokenBudget, null, null, "VECTOR", "LEGACY");
    }
    public ContextQuery(String query, Integer topK, Integer tokenBudget, String businessEntityType,
                        String businessEntityId) {
        this(query, topK, tokenBudget, businessEntityType, businessEntityId, "VECTOR", "LEGACY");
    }
}
// query 由调用者提供，权限和 Workspace 始终来自认证上下文而不是请求体。
