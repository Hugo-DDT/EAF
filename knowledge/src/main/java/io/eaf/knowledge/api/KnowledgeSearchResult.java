package io.eaf.knowledge.api;

import java.util.List;

/** 知识检索结果；空 hits 是合法的无证据结果，不代表越权资料不存在。 */
public record KnowledgeSearchResult(int topK, List<KnowledgeSearchHit> hits, String mode) {
    public KnowledgeSearchResult(int topK, List<KnowledgeSearchHit> hits) { this(topK, hits, "VECTOR"); }
}
// 结果不回显查询向量，也不把不可见文档数量或元数据泄露给调用者。
