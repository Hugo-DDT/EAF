package io.eaf.model.infrastructure;

import io.eaf.model.api.EmbeddingFailure;
import io.eaf.model.api.EmbeddingProfile;
import io.eaf.model.api.EmbeddingGateway;
import io.eaf.model.api.EmbeddingRequest;
import io.eaf.model.api.EmbeddingResult;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** 确定性替身只验证批次、维度、预算和落库协议，不证明自然语言语义质量。 */
public final class DeterministicEmbeddingGateway implements EmbeddingGateway {
    public static final String PROVIDER = "deterministic";
    public static final String MODEL = "p3-test-embedding-8";
    public static final int DIMENSION = 8;
    private static final int MAX_BATCH_SIZE = 32;
    private static final int MAX_INPUT_TOKENS = 8_192;
    private static final int MAX_TEXT_CODE_POINTS = 8_192;
    private final AtomicInteger calls = new AtomicInteger();

    @Override
    public EmbeddingProfile profile() {
        return new EmbeddingProfile(PROVIDER, MODEL, "SHA256-NORMALIZED-V1", DIMENSION,
                MAX_BATCH_SIZE, MAX_INPUT_TOKENS, MAX_TEXT_CODE_POINTS);
    }

    @Override
    public EmbeddingResult embed(EmbeddingRequest request) {
        var started = Instant.now();
        validate(request);
        calls.incrementAndGet();
        var vectors = new ArrayList<List<Float>>();
        var inputTokens = 0;
        for (var text : request.texts()) {
            inputTokens += estimatedTokens(text);
            vectors.add(vector(text));
        }
        return new EmbeddingResult(PROVIDER, MODEL, DIMENSION, List.copyOf(vectors), inputTokens,
                "KNOWN", started, Instant.now());
    }

    @Override
    public int callCount() { return calls.get(); }

    private void validate(EmbeddingRequest request) {
        if (request == null || request.texts() == null || request.texts().isEmpty())
            throw failure("EMPTY_INPUT", "Embedding 文本批次不能为空。", false);
        if (!PROVIDER.equals(request.provider()) || !MODEL.equals(request.model())
                || !profile().revision().equals(request.revision()) || request.dimension() != DIMENSION)
            throw failure("UNKNOWN_MODEL", "Embedding 模型或维度未被确定性替身锁定。", false);
        if (!List.of("EMBEDDING_BATCH", "EMBEDDING_QUERY").contains(request.usageSource()))
            throw failure("INVALID_USAGE_SOURCE", "Embedding 必须声明索引或查询计量归属。", false);
        if (request.texts().size() > MAX_BATCH_SIZE)
            throw failure("BATCH_LIMIT_EXCEEDED", "Embedding 批次超过上限。", false);
        if (request.maxInputTokens() <= 0 || request.maxInputTokens() > MAX_INPUT_TOKENS)
            throw failure("BUDGET_EXCEEDED", "Embedding 输入预算未提供或超过 Profile 上限。", false);
        if (!validScope(request.scope(), request.usageSource()))
            throw failure("INVALID_USAGE_SCOPE", "Embedding 缺少有效的服务端计量作用域。", false);
        if (request.deadline() == null || !request.deadline().isAfter(Instant.now()))
            throw failure("UPSTREAM_TIMEOUT", "Embedding 截止时间已到。", true);
        var tokens = request.texts().stream().mapToInt(this::estimatedTokens).sum();
        if (request.texts().stream().anyMatch(text -> text == null || text.isBlank() || text.codePointCount(0, text.length()) > MAX_TEXT_CODE_POINTS))
            throw failure("INPUT_LIMIT_EXCEEDED", "Embedding 文本为空或超过单项限制。", false);
        if (tokens > request.maxInputTokens()) throw failure("BUDGET_EXCEEDED", "Embedding 输入预算不足。", false);
    }

    private boolean validScope(io.eaf.model.api.EmbeddingCallScope scope, String callType) {
        return scope != null && scope.tenantId() != null && scope.workspaceId() != null
                && scope.scopeType() != null && scope.scopeId() != null && scope.callKey() != null
                && !scope.callKey().isBlank() && scope.callType() != null && scope.callType().equals(callType)
                && scope.source() != null
                && List.of("USER", "EVALUATION").contains(scope.source());
    }

    private List<Float> vector(String text) {
        try {
            var digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            var values = new float[DIMENSION];
            var length = 0d;
            for (var i = 0; i < DIMENSION; i++) {
                values[i] = (digest[i] / 127.5f) - 1f;
                length += values[i] * values[i];
            }
            var norm = (float) Math.sqrt(length);
            var result = new ArrayList<Float>(DIMENSION);
            for (var value : values) result.add(value / norm);
            return List.copyOf(result);
        } catch (NoSuchAlgorithmException e) {
            throw new EmbeddingFailure("DEPENDENCY_UNAVAILABLE", "JDK 缺少 SHA-256。", false, false);
        }
    }

    private int estimatedTokens(String text) { return text == null ? Integer.MAX_VALUE : Math.max(1, (text.codePointCount(0, text.length()) + 3) / 4); }

    private EmbeddingFailure failure(String code, String detail, boolean retryable) { return new EmbeddingFailure(code, detail, retryable, false); }
}
// 本替身固定 8 维 SHA-256 派生向量；它只验证机制，不应被当作真实语义召回模型。
