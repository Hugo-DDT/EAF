package io.eaf.knowledge.api;

/** 受限的直接检索请求；topK 上限由服务端固定，避免成为无限 Embedding 通道。 */
public record KnowledgeSearchQuery(String query, Integer topK) { }
// query 只作为待嵌入文本，不能携带权限、tenant 或 document 过滤条件。
