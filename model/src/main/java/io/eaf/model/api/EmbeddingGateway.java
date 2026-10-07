package io.eaf.model.api;

public interface EmbeddingGateway {
    EmbeddingProfile profile();
    EmbeddingResult embed(EmbeddingRequest request);
    int callCount();
}
// Embedding 与 Chat 使用独立端口，knowledge 不得绕过 model 直接连接 Provider SDK。
