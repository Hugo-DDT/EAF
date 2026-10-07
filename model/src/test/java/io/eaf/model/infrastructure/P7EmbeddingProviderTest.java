package io.eaf.model.infrastructure;

import io.eaf.model.api.EmbeddingCallScope;
import io.eaf.model.api.EmbeddingFailure;
import io.eaf.model.api.EmbeddingProfile;
import io.eaf.model.api.EmbeddingRequest;
import io.eaf.model.api.EmbeddingGateway;
import io.eaf.usage.api.ReserveSpendCommand;
import io.eaf.usage.api.SpendReservation;
import io.eaf.usage.api.UsageRecord;
import io.eaf.usage.api.UsageRecorder;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.metadata.DefaultUsage;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingResponse;
import org.springframework.ai.embedding.EmbeddingResponseMetadata;
import org.springframework.context.ApplicationContext;
import com.alibaba.cloud.ai.dashscope.embedding.DashScopeEmbeddingOptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 本地 Provider fixture 验证 pinned Spring AI Alibaba 协议与 Usage 闭环，不向外发送请求。 */
class P7EmbeddingProviderTest {
    private final EmbeddingProfile profile = new EmbeddingProfile("dashscope", "text-embedding-v3",
            "dashscope-2026-09", 3, 8, 100, 512);

    @Test
    void mapsQueryProfileAndReservesBeforeRecordingTrustedUsage() {
        var usage = usage(true);
        var providerRequest = new AtomicReference<org.springframework.ai.embedding.EmbeddingRequest>();
        var model = fakeModel(request -> {
            providerRequest.set(request);
            return response("text-embedding-v3", new float[]{0.1f, 0.2f, 0.3f}, 7);
        });
        var gateway = gateway(model, usage);
        var scope = scope("EMBEDDING_QUERY");

        var result = gateway.embed(request("EMBEDDING_QUERY", scope));

        assertThat(result.provider()).isEqualTo("dashscope");
        assertThat(result.model()).isEqualTo("text-embedding-v3");
        assertThat(result.inputTokens()).isEqualTo(7);
        var options = (DashScopeEmbeddingOptions) providerRequest.get().getOptions();
        assertThat(options.getModel()).isEqualTo("text-embedding-v3");
        assertThat(options.getDimensions()).isEqualTo(3);
        assertThat(options.getTextType()).isEqualTo("query");
        verify(usage).reserveSpend(argThat(command -> command.provider().equals("dashscope")
                && command.model().equals("text-embedding-v3") && command.callType().equals("EMBEDDING_QUERY")
                && command.scopeId().equals(scope.scopeId()) && command.maxTokens() == 100));
        verify(usage).record(argThat(record -> record.callKey().equals(scope.callKey())
                && record.provider().equals("dashscope") && record.model().equals("text-embedding-v3")
                && record.inputTokens() == 7 && "KNOWN".equals(record.usageStatus())
                && "SUCCEEDED".equals(record.status())));
    }

    @Test
    void recordsProtocolFailureAsUnknownCostAndNeverReturnsInvalidVector() {
        var usage = usage(true);
        var model = fakeModel(ignored -> response("text-embedding-v3", new float[]{0.1f, Float.NaN, 0.3f}, 7));
        var gateway = gateway(model, usage);
        var scope = scope("EMBEDDING_BATCH");

        assertThatThrownBy(() -> gateway.embed(request("EMBEDDING_BATCH", scope)))
                .isInstanceOf(EmbeddingFailure.class)
                .satisfies(error -> assertThat(((EmbeddingFailure) error).code()).isEqualTo("MODEL_PROTOCOL_ERROR"));
        verify(usage).record(argThat(record -> record.callKey().equals(scope.callKey())
                && record.inputTokens() == null && "UNKNOWN".equals(record.usageStatus())
                && "FAILED".equals(record.status()) && "MODEL_PROTOCOL_ERROR".equals(record.errorCode())));
    }

    @Test
    void deniedSpendDoesNotInvokeProvider() {
        var usage = usage(false);
        var providerCalls = new AtomicReference<>(0);
        var model = fakeModel(ignored -> {
            providerCalls.set(providerCalls.get() + 1);
            return response("text-embedding-v3", new float[]{0.1f, 0.2f, 0.3f}, 7);
        });
        var gateway = gateway(model, usage);

        assertThatThrownBy(() -> gateway.embed(request("EMBEDDING_QUERY", scope("EMBEDDING_QUERY"))))
                .isInstanceOf(EmbeddingFailure.class)
                .satisfies(error -> assertThat(((EmbeddingFailure) error).called()).isFalse());
        assertThat(providerCalls.get()).isZero();
        assertThat(gateway.callCount()).isZero();
        verify(usage, never()).record(any(UsageRecord.class));
    }

    @Test
    void timeoutSettlesTheReservedUpperBoundAsUnknownUsage() {
        var usage = usage(true);
        var model = fakeModel(ignored -> {
            try { Thread.sleep(2_000); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            return response("text-embedding-v3", new float[]{0.1f, 0.2f, 0.3f}, 7);
        });
        var gateway = gateway(model, usage);
        var base = request("EMBEDDING_QUERY", scope("EMBEDDING_QUERY"));
        var timeoutRequest = new EmbeddingRequest(base.provider(), base.model(), base.revision(), base.usageSource(),
                base.texts(), base.dimension(), base.maxInputTokens(), Instant.now().plusMillis(30), base.scope());

        assertThatThrownBy(() -> gateway.embed(timeoutRequest))
                .isInstanceOf(EmbeddingFailure.class)
                .satisfies(error -> assertThat(((EmbeddingFailure) error).code()).isEqualTo("UPSTREAM_TIMEOUT"));
        verify(usage).record(argThat(record -> "TIMED_OUT".equals(record.status())
                && "UNKNOWN".equals(record.usageStatus()) && record.inputTokens() == null));
    }

    @Test
    void liveEmbeddingWithoutProviderBeanFailsClosed() {
        var context = mock(ApplicationContext.class);
        // Profile 完整后允许查找 Provider；容器没有真实 Bean 时仍不得创建 Embedding 出站调用。
        when(context.getBeansOfType(any(Class.class))).thenReturn(java.util.Map.of());
        var usage = mock(UsageRecorder.class);
        EmbeddingGateway gateway = new ModelConfiguration().embeddingGateway("live-model", "text-embedding-v3",
                "dashscope-2026-09", "64", "10", "8192", "8192", "1.00", "USD", context, usage);

        assertThatThrownBy(gateway::profile)
                .isInstanceOf(EmbeddingFailure.class)
                .satisfies(error -> {
                    assertThat(((EmbeddingFailure) error).code()).isEqualTo("DEPENDENCY_UNAVAILABLE");
                    assertThat(((EmbeddingFailure) error).called()).isFalse();
                });
        assertThat(gateway.callCount()).isZero();
        verify(context).getBeansOfType(any(Class.class));
    }

    @Test
    void liveEmbeddingWithoutProfileFailsBeforeProviderLookup() {
        var context = mock(ApplicationContext.class);
        var usage = mock(UsageRecorder.class);
        EmbeddingGateway gateway = new ModelConfiguration().embeddingGateway("live-model", "", "", "", "", "", "",
                "1.00", "USD", context, usage);

        assertThatThrownBy(gateway::profile)
                .isInstanceOf(EmbeddingFailure.class)
                .satisfies(error -> {
                    assertThat(((EmbeddingFailure) error).code()).isEqualTo("DEPENDENCY_UNAVAILABLE");
                    assertThat(((EmbeddingFailure) error).called()).isFalse();
                });
        verify(context, never()).getBeansOfType(any(Class.class));
    }

    private SpringAiAlibabaEmbeddingGateway gateway(EmbeddingModel model, UsageRecorder usage) {
        return new SpringAiAlibabaEmbeddingGateway(model, profile, usage, "1.00", "USD", false);
    }

    private UsageRecorder usage(boolean allowed) {
        var usage = mock(UsageRecorder.class);
        when(usage.reserveSpend(any(ReserveSpendCommand.class)))
                .thenReturn(new SpendReservation(allowed, allowed ? null : "SPEND_CAP_REACHED", null, "USD"));
        return usage;
    }

    private EmbeddingRequest request(String callType, EmbeddingCallScope scope) {
        return new EmbeddingRequest(profile.provider(), profile.model(), profile.revision(), callType,
                List.of("authorized sample"), profile.dimension(), profile.maxInputTokens(),
                Instant.now().plusSeconds(2), scope);
    }

    private EmbeddingCallScope scope(String callType) {
        var tenant = UUID.randomUUID();
        var workspace = UUID.randomUUID();
        var id = UUID.randomUUID();
        return new EmbeddingCallScope(tenant, workspace, null, null, "USER", "JOB", id, 1,
                "job:embedding:" + id, callType);
    }

    private EmbeddingModel fakeModel(java.util.function.Function<org.springframework.ai.embedding.EmbeddingRequest, EmbeddingResponse> call) {
        return (EmbeddingModel) Proxy.newProxyInstance(EmbeddingModel.class.getClassLoader(),
                new Class<?>[]{EmbeddingModel.class}, (proxy, method, arguments) -> {
                    if ("call".equals(method.getName()) && arguments != null && arguments.length == 1
                            && arguments[0] instanceof org.springframework.ai.embedding.EmbeddingRequest request) return call.apply(request);
                    if ("embed".equals(method.getName())) return new float[]{0.1f, 0.2f, 0.3f};
                    if ("toString".equals(method.getName())) return "local-embedding-fixture";
                    if ("hashCode".equals(method.getName())) return System.identityHashCode(proxy);
                    if ("equals".equals(method.getName())) return proxy == arguments[0];
                    return null;
                });
    }

    private EmbeddingResponse response(String model, float[] vector, int tokens) {
        return new EmbeddingResponse(List.of(new Embedding(vector, 0)),
                new EmbeddingResponseMetadata(model, new DefaultUsage(tokens, 0, tokens)));
    }
}
// 本测试不调用真实网络；实际 Provider 质量与外部计费仍需独立授权 smoke 验证。
