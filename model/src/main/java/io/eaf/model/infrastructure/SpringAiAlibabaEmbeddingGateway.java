package io.eaf.model.infrastructure;

import io.eaf.model.api.EmbeddingCallScope;
import io.eaf.model.api.EmbeddingFailure;
import io.eaf.model.api.EmbeddingGateway;
import io.eaf.model.api.EmbeddingProfile;
import io.eaf.model.api.EmbeddingRequest;
import io.eaf.model.api.EmbeddingResult;
import io.eaf.usage.api.ReserveSpendCommand;
import io.eaf.usage.api.UsageRecord;
import io.eaf.usage.api.UsageRecorder;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Spring AI Alibaba DashScope Embedding 适配器；费用预留和协议校验留在 EAF Gateway。 */
public final class SpringAiAlibabaEmbeddingGateway implements EmbeddingGateway {
    private final Object embeddingModel;
    private final EmbeddingProfile profile;
    private final UsageRecorder usage;
    private final BigDecimal feeCap;
    private final String feeCurrency;
    private final AtomicInteger calls = new AtomicInteger();

    public SpringAiAlibabaEmbeddingGateway(Object embeddingModel, EmbeddingProfile profile, UsageRecorder usage,
                                           String feeCap, String feeCurrency) {
        this(embeddingModel, profile, usage, feeCap, feeCurrency, true);
    }

    SpringAiAlibabaEmbeddingGateway(Object embeddingModel, EmbeddingProfile profile, UsageRecorder usage,
                                   String feeCap, String feeCurrency, boolean configureDashScopeNoRetry) {
        this.embeddingModel = configureDashScopeNoRetry ? noRetryModel(embeddingModel) : embeddingModel;
        this.profile = profile;
        this.usage = usage;
        this.feeCap = new BigDecimal(feeCap);
        this.feeCurrency = feeCurrency;
    }

    @Override public EmbeddingProfile profile() { return profile; }

    @Override
    public EmbeddingResult embed(EmbeddingRequest request) {
        var started = Instant.now();
        validate(request);
        var scope = request.scope();
        io.eaf.usage.api.SpendReservation reservation;
        try {
            reservation = usage.reserveSpend(new ReserveSpendCommand(scope.tenantId(), scope.workspaceId(),
                    scope.scopeType(), scope.scopeId(), scope.callKey(), profile.provider(), profile.model(),
                    request.usageSource(), request.maxInputTokens(), feeCap, feeCurrency));
        } catch (RuntimeException failure) {
            throw failure("UNKNOWN_COST", "Embedding 金额预算预留失败，未向 Provider 出站。", false, false);
        }
        if (reservation == null || !reservation.allowed())
            throw failure(reservation == null ? "UNKNOWN_COST" : reservation.code(), "Embedding 金额预算或价格上界不允许出站。", false, false);

        Object raw;
        try {
            raw = invoke(request);
        } catch (ProviderNotCalled beforeOutbound) {
            releaseReservation(scope.callKey());
            throw failure("UPSTREAM_TIMEOUT", "Embedding 调用过期，已在出站前释放金额预留。", true, false);
        } catch (ProviderInterruptedCall interrupted) {
            try { recordFailure(scope, request, started, "UPSTREAM_TIMEOUT"); }
            finally { Thread.currentThread().interrupt(); }
            throw failure("UPSTREAM_TIMEOUT", "Spring AI Alibaba Embedding 调用被中断。", true, true);
        } catch (EmbeddingFailure e) {
            if (e.called()) recordFailure(scope, request, started, e.code());
            throw e;
        } catch (Exception e) {
            var cause = unwrap(e);
            var failure = providerFailure(cause);
            if (ProviderFailureSupport.capacityExceeded(cause) || ProviderFailureSupport.quotaUnavailable(cause)) {
                releaseReservation(scope.callKey());
                throw failure;
            }
            recordFailure(scope, request, started, failure.code());
            throw failure;
        }
        EmbeddingResult result;
        try {
            result = validateResponse(raw, request, started);
        } catch (EmbeddingFailure failure) {
            recordFailure(scope, request, started, failure.code());
            throw failure;
        }
        record(scope, request.maxInputTokens(), result.inputTokens(), "KNOWN", "SUCCEEDED", null, started);
        return result;
    }

    @Override public int callCount() { return calls.get(); }

    private void validate(EmbeddingRequest request) {
        // Profile、输入限制和 Usage 归属先于金额预留与 Provider 出站逐项校验。
        if (request == null || request.texts() == null || request.texts().isEmpty())
            throw failure("EMPTY_INPUT", "Embedding 文本批次不能为空。", false, false);
        if (!profile.provider().equals(request.provider()) || !profile.model().equals(request.model())
                || !profile.revision().equals(request.revision()) || profile.dimension() != request.dimension())
            throw failure("UNKNOWN_MODEL", "Embedding 请求 profile 与 Gateway 登记版本不一致。", false, false);
        if (!List.of("EMBEDDING_QUERY", "EMBEDDING_BATCH").contains(request.usageSource())
                || request.scope() == null || !request.usageSource().equals(request.scope().callType()))
            throw failure("INVALID_USAGE_SCOPE", "Embedding 缺少有效的服务端计量用途。", false, false);
        if (request.texts().size() > profile.maxBatchSize() || request.maxInputTokens() <= 0
                || request.maxInputTokens() > profile.maxInputTokens())
            throw failure("INPUT_LIMIT_EXCEEDED", "Embedding 批次或 Token 上限超过登记 Profile。", false, false);
        var scope = request.scope();
        if (scope.tenantId() == null || scope.workspaceId() == null || scope.scopeId() == null
                || scope.scopeType() == null || scope.callKey() == null || scope.callKey().isBlank()
                || !List.of("USER", "EVALUATION").contains(scope.source()))
            throw failure("INVALID_USAGE_SCOPE", "Embedding 缺少可信租户、Workspace 或费用作用域。", false, false);
        if (request.texts().stream().anyMatch(text -> text == null || text.isBlank()
                || text.codePointCount(0, text.length()) > profile.maxTextCodePoints()))
            throw failure("INPUT_LIMIT_EXCEEDED", "Embedding 文本为空或超过单项字符上限。", false, false);
        var estimatedTokens = request.texts().stream().mapToInt(text -> Math.max(1, (text.codePointCount(0, text.length()) + 3) / 4)).sum();
        if (estimatedTokens > request.maxInputTokens())
            throw failure("BUDGET_EXCEEDED", "Embedding 输入 Token 预留不足。", false, false);
        if (request.deadline() == null || !request.deadline().isAfter(Instant.now()))
            throw failure("UPSTREAM_TIMEOUT", "Embedding 截止时间已到。", true, false);
    }

    private Object invoke(EmbeddingRequest request) throws Exception {
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            var future = executor.submit(() -> callProvider(request));
            try {
                var timeoutMs = Math.max(1, java.time.Duration.between(Instant.now(), request.deadline()).toMillis());
                return future.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.TimeoutException e) {
                future.cancel(true);
                throw failure("UPSTREAM_TIMEOUT", "Spring AI Alibaba Embedding 调用超时。", true, true);
            } catch (InterruptedException e) {
                future.cancel(true);
                throw new ProviderInterruptedCall();
            } catch (java.util.concurrent.ExecutionException e) {
                if (e.getCause() instanceof ProviderNotCalled beforeOutbound) throw beforeOutbound;
                throw new IllegalStateException(e.getCause());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private Object callProvider(EmbeddingRequest request) throws Exception {
        var optionsType = Class.forName("com.alibaba.cloud.ai.dashscope.embedding.DashScopeEmbeddingOptions");
        var optionsBuilder = optionsType.getMethod("builder").invoke(null);
        optionsBuilder = optionsBuilder.getClass().getMethod("model", String.class).invoke(optionsBuilder, profile.model());
        optionsBuilder = optionsBuilder.getClass().getMethod("dimensions", Integer.class).invoke(optionsBuilder, profile.dimension());
        var textType = "EMBEDDING_QUERY".equals(request.usageSource()) ? "query" : "document";
        optionsBuilder = optionsBuilder.getClass().getMethod("textType", String.class).invoke(optionsBuilder, textType);
        var options = optionsBuilder.getClass().getMethod("build").invoke(optionsBuilder);
        var inputType = Class.forName("org.springframework.ai.embedding.EmbeddingRequest");
        var input = inputType.getConstructor(List.class, Class.forName("org.springframework.ai.embedding.EmbeddingOptions"))
                .newInstance(request.texts(), options);
        if (!request.deadline().isAfter(Instant.now())) throw new ProviderNotCalled();
        calls.incrementAndGet();
        return embeddingModel.getClass().getMethod("call", inputType).invoke(embeddingModel, input);
    }

    private EmbeddingResult validateResponse(Object response, EmbeddingRequest request, Instant started) {
        try {
            if (response == null) throw failure("MODEL_PROTOCOL_ERROR", "Embedding Provider 返回空响应。", false, true);
            var metadata = response.getClass().getMethod("getMetadata").invoke(response);
            var returnedModel = metadata == null ? null : metadata.getClass().getMethod("getModel").invoke(metadata);
            if (!profile.model().equals(returnedModel))
                throw failure("MODEL_PROTOCOL_ERROR", "Embedding Provider 返回模型标识与登记 Profile 不一致。", false, true);
            var rawResults = response.getClass().getMethod("getResults").invoke(response);
            if (!(rawResults instanceof List<?> results) || results.size() != request.texts().size())
                throw failure("MODEL_PROTOCOL_ERROR", "Embedding Provider 返回数量与请求批次不一致。", false, true);
            var vectors = new ArrayList<List<Float>>(results.size());
            for (var index = 0; index < results.size(); index++) {
                var embedding = results.get(index);
                var resultIndex = ((Number) embedding.getClass().getMethod("getIndex").invoke(embedding)).intValue();
                var rawVector = (float[]) embedding.getClass().getMethod("getOutput").invoke(embedding);
                if (resultIndex != index || rawVector == null || rawVector.length != profile.dimension())
                    throw failure("MODEL_PROTOCOL_ERROR", "Embedding Provider 返回顺序或维度不一致。", false, true);
                var vector = new ArrayList<Float>(rawVector.length);
                for (float value : rawVector) {
                    if (!Float.isFinite(value)) throw failure("MODEL_PROTOCOL_ERROR", "Embedding Provider 返回非有限数值。", false, true);
                    vector.add(value);
                }
                vectors.add(List.copyOf(vector));
            }
            var usageMetadata = metadata == null ? null : metadata.getClass().getMethod("getUsage").invoke(metadata);
            var tokenValue = usageMetadata == null ? null : usageMetadata.getClass().getMethod("getPromptTokens").invoke(usageMetadata);
            if (!(tokenValue instanceof Number count) || count.intValue() < 0 || count.intValue() > request.maxInputTokens())
                throw failure("MODEL_PROTOCOL_ERROR", "Embedding Provider 缺少可信且在限额内的输入用量。", false, true);
            return new EmbeddingResult(profile.provider(), profile.model(), profile.dimension(), List.copyOf(vectors),
                    count.intValue(), "KNOWN", started, Instant.now());
        } catch (EmbeddingFailure e) {
            throw e;
        } catch (Exception e) {
            throw failure("MODEL_PROTOCOL_ERROR", "Embedding Provider 返回缺少必需的模型、用量或向量字段。", false, true);
        }
    }

    private Object noRetryModel(Object model) {
        // Alibaba SDK 的隐式重试会把一次 EAF 请求变成多次收费出站，因此重试由 EAF 外层控制。
        try {
            var dashScopeModelType = Class.forName("com.alibaba.cloud.ai.dashscope.embedding.DashScopeEmbeddingModel");
            if (model == null || !dashScopeModelType.isInstance(model))
                throw new IllegalStateException();
            var retryType = Class.forName("org.springframework.retry.support.RetryTemplate");
            var retryTemplate = retryType.getConstructor().newInstance();
            var policyType = Class.forName("org.springframework.retry.RetryPolicy");
            var noRetryPolicy = Class.forName("org.springframework.retry.policy.NeverRetryPolicy").getConstructor().newInstance();
            retryType.getMethod("setRetryPolicy", policyType).invoke(retryTemplate, noRetryPolicy);
            var builder = model.getClass().getMethod("mutate").invoke(model);
            builder = builder.getClass().getMethod("retryTemplate", retryType).invoke(builder, retryTemplate);
            return builder.getClass().getMethod("build").invoke(builder);
        } catch (Exception e) {
            throw new IllegalStateException("无法关闭 DashScope Embedding Provider 的隐式重试。", e);
        }
    }

    private void recordFailure(EmbeddingCallScope scope, EmbeddingRequest request, Instant started, String code) {
        record(scope, request.maxInputTokens(), null, "UNKNOWN", "UPSTREAM_TIMEOUT".equals(code) ? "TIMED_OUT" : "FAILED", code, started);
    }

    private void record(EmbeddingCallScope scope, int reservedTokens, Integer inputTokens,
                        String usageStatus, String status, String errorCode, Instant started) {
        try {
            usage.record(new UsageRecord(scope.tenantId(), scope.workspaceId(), scope.taskId(), scope.runId(),
                    scope.source(), profile.provider(), profile.model(), inputTokens, null, usageStatus,
                    reservedTokens, status, errorCode, started, Instant.now(), scope.callNo(),
                    scope.callKey(), scope.callType(), scope.scopeType(), scope.scopeId()));
        } catch (RuntimeException failure) {
            throw new EmbeddingFailure("USAGE_RECORDING_FAILED", "Embedding 用量无法持久化，保留原金额预留且停止重试。", false, true);
        }
    }

    private void releaseReservation(String callKey) {
        try { usage.releaseSpend(callKey); }
        catch (RuntimeException failure) {
            throw new EmbeddingFailure("SPEND_RELEASE_FAILED", "已知未出站，但金额预留未能释放；Scope 保持封闭。", false, false);
        }
    }

    private EmbeddingFailure providerFailure(Throwable failure) {
        if (ProviderFailureSupport.capacityExceeded(failure))
            return failure("MODEL_CAPACITY_EXCEEDED", "Embedding 出站并发已满，请稍后重试。", false, false);
        if (ProviderFailureSupport.quotaUnavailable(failure))
            return failure("MODEL_QUOTA_UNAVAILABLE", "Embedding 共享并发协调暂不可用，未发送请求。", false, false);
        var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        var text = new StringBuilder();
        for (var cause = failure; cause != null && seen.add(cause); cause = cause.getCause())
            text.append(cause.getClass().getName()).append(' ').append(cause.getMessage()).append(' ');
        var normalized = text.toString().toLowerCase(Locale.ROOT);
        if (normalized.contains("timeout") || normalized.contains("timed out"))
            return failure("UPSTREAM_TIMEOUT", "Embedding Provider 调用超时。", true, true);
        if (normalized.contains("401") || normalized.contains("403") || normalized.contains("unauthorized"))
            return failure("UPSTREAM_AUTH", "Embedding Provider 认证失败。", false, true);
        if (normalized.contains("429") || normalized.contains("rate limit"))
            return failure("UPSTREAM_RATE_LIMITED", "Embedding Provider 限流。", false, true);
        return failure("UPSTREAM_FAILURE", "Embedding Provider 调用失败。", false, true);
    }

    private Throwable unwrap(Throwable failure) {
        var current = failure;
        while (current instanceof InvocationTargetException invocation && invocation.getCause() != null)
            current = invocation.getCause();
        return current;
    }

    private EmbeddingFailure failure(String code, String detail, boolean retryable, boolean called) {
        return new EmbeddingFailure(code, detail, retryable, called);
    }

    private static final class ProviderInterruptedCall extends RuntimeException { }
    private static final class ProviderNotCalled extends Exception { }
}
// Gateway 不输出原文、凭证或上游响应体到日志；未知成本以全额预留结算。
