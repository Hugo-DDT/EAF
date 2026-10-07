package io.eaf.model.api;

/** 服务端登记的 Embedding 模型身份与单次输入上限。 */
public record EmbeddingProfile(String provider, String model, String revision, int dimension,
                                int maxBatchSize, int maxInputTokens, int maxTextCodePoints) {
    public EmbeddingProfile {
        if (provider == null || provider.isBlank() || model == null || model.isBlank()
                || revision == null || revision.isBlank() || dimension <= 0
                || maxBatchSize <= 0 || maxInputTokens <= 0 || maxTextCodePoints <= 0)
            throw new IllegalArgumentException("Embedding profile 必须包含固定身份和正数限制。");
    }
}
// Profile 是服务端选定的计量与向量空间事实，不接受业务请求覆盖。
