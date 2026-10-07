package io.eaf.model.infrastructure;

import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.EmbeddingGateway;
import io.eaf.model.api.EmbeddingProfile;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
public class ModelConfiguration {
    @Bean
    ModelGateway modelGateway(@Value("${eaf.model.mode:deterministic}") String mode,
                              @Value("${eaf.model.scenario:SUCCESS}") String scenario,
                              @Value("${eaf.model.live.provider:${EAF_MODEL_LIVE_PROVIDER:dashscope}}") String provider,
                              @Value("${eaf.model.live.model:${EAF_MODEL_LIVE_MODEL:}}") String liveModel,
                              @Value("${eaf.model.live.base-url:${EAF_MODEL_LIVE_BASE_URL:}}") String baseUrl,
                              @Value("${eaf.model.live.credential-ref:${EAF_MODEL_LIVE_CREDENTIAL_REF:model-dashscope}}") String credentialRef,
                              @Value("${eaf.model.live.data-authorized:${EAF_MODEL_LIVE_DATA_AUTHORIZED:false}}") boolean dataAuthorized,
                              @Value("${eaf.model.live.outbound-data-scope:${EAF_MODEL_LIVE_OUTBOUND_DATA_SCOPE:}}") String outboundDataScope,
                              @Value("${eaf.model.live.fee-cap:${EAF_MODEL_LIVE_FEE_CAP:}}") String feeCap,
                              @Value("${eaf.model.live.fee-currency:${EAF_MODEL_LIVE_FEE_CURRENCY:}}") String feeCurrency,
                              @Value("${eaf.model.live.stop-condition:${EAF_MODEL_LIVE_STOP_CONDITION:}}") String stopCondition,
                              @Value("${eaf.model.live.run-owner:${EAF_MODEL_LIVE_RUN_OWNER:}}") String runOwner,
                              @Value("${eaf.model.live.authorization-id:${EAF_MODEL_LIVE_AUTHORIZATION_ID:}}") String authorizationId,
                              @Value("${eaf.model.live.authorization-expiry:${EAF_MODEL_LIVE_AUTHORIZATION_EXPIRY:}}") String authorizationExpiry,
                              org.springframework.context.ApplicationContext context) {
        return createModelGateway(mode, scenario, provider, liveModel, baseUrl, credentialRef, dataAuthorized,
                outboundDataScope, feeCap, feeCurrency, stopCondition, runOwner, authorizationId, authorizationExpiry, context);
    }

    @Bean
    io.eaf.model.api.TypedDecisionGateway typedDecisionGateway(
            @Value("${eaf.model.jev.mode:${EAF_MODEL_JEV_MODE:disabled}}") String mode,
            @Value("${eaf.model.jev.endpoint:${EAF_MODEL_JEV_ENDPOINT:https://api.typesafe.ai/v1/systemone}}") String endpoint,
            @Value("${eaf.model.jev.model:${EAF_MODEL_JEV_MODEL:jev-1.13.0}}") String model,
            @Value("${eaf.model.jev.data-authorized:${EAF_MODEL_JEV_DATA_AUTHORIZED:false}}") boolean dataAuthorized,
            @Value("${eaf.model.jev.outbound-data-scope:${EAF_MODEL_JEV_OUTBOUND_DATA_SCOPE:}}") String dataScope,
            @Value("${eaf.model.jev.run-owner:${EAF_MODEL_JEV_RUN_OWNER:}}") String runOwner,
            @Value("${eaf.model.jev.authorization-id:${EAF_MODEL_JEV_AUTHORIZATION_ID:}}") String authorizationId,
            @Value("${eaf.model.jev.authorization-expiry:${EAF_MODEL_JEV_AUTHORIZATION_EXPIRY:}}") String authorizationExpiry,
            @Value("${eaf.model.live.fee-cap:${EAF_MODEL_LIVE_FEE_CAP:}}") String feeCap,
            @Value("${eaf.model.live.fee-currency:${EAF_MODEL_LIVE_FEE_CURRENCY:}}") String feeCurrency,
            @Value("${eaf.model.live.stop-condition:${EAF_MODEL_LIVE_STOP_CONDITION:}}") String stopCondition,
            ProviderCredentialRequestInterceptor providerCredentials) {
        if ("disabled".equalsIgnoreCase(mode)) return new DisabledTypedDecisionGateway();
        if ("deterministic".equalsIgnoreCase(mode)) return new DeterministicTypedDecisionGateway();
        if (!"live".equalsIgnoreCase(mode)) return new MissingTypedDecisionGateway("不支持的 Jev 模式；不会回退到确定性替身。");
        var invalid = typedDecisionConfigurationError(endpoint, model, dataAuthorized, dataScope, runOwner,
                authorizationId, authorizationExpiry, feeCap, feeCurrency, stopCondition);
        if (invalid != null) return new MissingTypedDecisionGateway(invalid);
        var builder = RestClient.builder();
        providerCredentials.customize(builder);
        var client = builder.defaultHeader("Authorization", "Bearer eaf-dynamic-credential-placeholder").build();
        return new TypesafeJevDecisionGateway(client, endpoint, model, Instant.parse(authorizationExpiry), dataScope);
    }

    @Bean
    io.eaf.model.api.EvidenceAssessmentGateway evidenceAssessmentGateway(
            @Value("${eaf.model.jev.evidence-mode:${EAF_MODEL_JEV_EVIDENCE_MODE:disabled}}") String mode,
            @Value("${eaf.model.jev.endpoint:${EAF_MODEL_JEV_ENDPOINT:https://api.typesafe.ai/v1/systemone}}") String endpoint,
            @Value("${eaf.model.jev.model:${EAF_MODEL_JEV_MODEL:jev-1.13.0}}") String model,
            @Value("${eaf.model.jev.data-authorized:${EAF_MODEL_JEV_DATA_AUTHORIZED:false}}") boolean dataAuthorized,
            @Value("${eaf.model.jev.outbound-data-scope:${EAF_MODEL_JEV_OUTBOUND_DATA_SCOPE:}}") String dataScope,
            @Value("${eaf.model.jev.run-owner:${EAF_MODEL_JEV_RUN_OWNER:}}") String runOwner,
            @Value("${eaf.model.jev.authorization-id:${EAF_MODEL_JEV_AUTHORIZATION_ID:}}") String authorizationId,
            @Value("${eaf.model.jev.authorization-expiry:${EAF_MODEL_JEV_AUTHORIZATION_EXPIRY:}}") String authorizationExpiry,
            @Value("${eaf.model.live.fee-cap:${EAF_MODEL_LIVE_FEE_CAP:}}") String feeCap,
            @Value("${eaf.model.live.fee-currency:${EAF_MODEL_LIVE_FEE_CURRENCY:}}") String feeCurrency,
            @Value("${eaf.model.live.stop-condition:${EAF_MODEL_LIVE_STOP_CONDITION:}}") String stopCondition,
            ProviderCredentialRequestInterceptor providerCredentials) {
        if ("disabled".equalsIgnoreCase(mode)) return new DisabledEvidenceAssessmentGateway();
        if ("deterministic".equalsIgnoreCase(mode)) return new DeterministicEvidenceAssessmentGateway();
        if (!"live".equalsIgnoreCase(mode)) return new MissingEvidenceAssessmentGateway("不支持的 Jev 证据判断模式；不会回退到确定性替身。");
        var invalid = evidenceConfigurationError(endpoint, model, dataAuthorized, dataScope, runOwner,
                authorizationId, authorizationExpiry, feeCap, feeCurrency, stopCondition);
        if (invalid != null) return new MissingEvidenceAssessmentGateway(invalid);
        var builder = RestClient.builder();
        providerCredentials.customize(builder);
        var client = builder.defaultHeader("Authorization", "Bearer eaf-dynamic-credential-placeholder").build();
        return new TypesafeJevEvidenceAssessmentGateway(client, endpoint, model, Instant.parse(authorizationExpiry), dataScope);
    }

    private String evidenceConfigurationError(String endpoint, String model, boolean dataAuthorized,
                                              String dataScope, String runOwner, String authorizationId,
                                              String authorizationExpiry, String feeCap, String feeCurrency,
                                              String stopCondition) {
        if (!validTypesafeEndpoint(endpoint)) return "Jev endpoint 必须是官方 HTTPS API 地址。";
        if (model == null || !model.matches("[A-Za-z0-9._-]{1,150}")) return "Jev model ID 缺失或格式无效。";
        if (!dataAuthorized || !hasEvidenceDataScope(dataScope))
            return "Jev 证据判断只允许明确授权的问题与合成片段外发。";
        if (runOwner == null || runOwner.isBlank() || authorizationId == null || authorizationId.isBlank()
                || !validAuthorizationExpiry(authorizationExpiry))
            return "Jev 本轮授权 Owner、授权 ID 或未过期截止时间缺失。";
        if (!positiveFeeCap(feeCap) || !"USD".equals(feeCurrency) || !finiteStopConditions(stopCondition))
            return "Jev 证据判断必须复用有效 USD 费用上限与全部停止条件。";
        return null;
    }

    private String typedDecisionConfigurationError(String endpoint, String model, boolean dataAuthorized,
                                                    String dataScope, String runOwner, String authorizationId,
                                                    String authorizationExpiry, String feeCap, String feeCurrency,
                                                    String stopCondition) {
        if (!validTypesafeEndpoint(endpoint)) return "Jev endpoint 必须是官方 HTTPS API 地址。";
        if (model == null || !model.matches("[A-Za-z0-9._-]{1,150}")) return "Jev model ID 缺失或格式无效。";
        if (!dataAuthorized || !hasCustomerDataScope(dataScope))
            return "Jev 仅允许明确授权的合成客户摘要外发。";
        if (runOwner == null || runOwner.isBlank() || authorizationId == null || authorizationId.isBlank()
                || !validAuthorizationExpiry(authorizationExpiry))
            return "Jev 本轮授权 Owner、授权 ID 或未过期截止时间缺失。";
        if (!positiveFeeCap(feeCap) || !"USD".equals(feeCurrency) || !finiteStopConditions(stopCondition))
            return "Jev 必须复用有效 USD 费用上限与全部停止条件。";
        return null;
    }

    private boolean hasCustomerDataScope(String scope) {
        return "synthetic-customer-summary-only".equals(scope)
                || "synthetic-customer-summary-and-passages-only".equals(scope)
                || "synthetic-conversation-summary-and-passages-only".equals(scope)
                || "synthetic-conversation-with-experience-and-passages-only".equals(scope);
    }

    private boolean hasEvidenceDataScope(String scope) {
        return "synthetic-question-and-passages-only".equals(scope)
                || "synthetic-customer-summary-and-passages-only".equals(scope)
                || "synthetic-conversation-summary-and-passages-only".equals(scope)
                || "synthetic-conversation-with-experience-and-passages-only".equals(scope);
    }

    private boolean validTypesafeEndpoint(String value) {
        try {
            var uri = URI.create(value);
            return "https".equals(uri.getScheme()) && "api.typesafe.ai".equalsIgnoreCase(uri.getHost())
                    && (uri.getPort() == -1 || uri.getPort() == 443) && "/v1/systemone".equals(uri.getRawPath())
                    && uri.getRawUserInfo() == null && uri.getRawQuery() == null && uri.getRawFragment() == null;
        } catch (RuntimeException invalid) { return false; }
    }

    // 兼容已有确定性测试入口；真实 Provider 必须使用带运行授权元数据的 Spring 配置入口。
    ModelGateway modelGateway(String mode, String scenario, String liveModel, boolean dataAuthorized,
                              String feeCap, String feeCurrency, String stopCondition,
                              org.springframework.context.ApplicationContext context) {
        // 旧测试只验证 Provider 协议；固定为合成 fixture 元数据，不把它当作真实运行授权。
        return createModelGateway(mode, scenario, "dashscope", liveModel, "", "model-dashscope", dataAuthorized,
                "synthetic-evaluation-only", feeCap, feeCurrency, stopCondition, "legacy-fixture-owner",
                "legacy-fixture-authorization", "2099-01-01T00:00:00Z", context);
    }

    private ModelGateway createModelGateway(String mode, String scenario, String provider, String liveModel, String baseUrl,
                                      String credentialRef, boolean dataAuthorized, String outboundDataScope,
                                      String feeCap, String feeCurrency, String stopCondition, String runOwner,
                                      String authorizationId, String authorizationExpiry,
                                      org.springframework.context.ApplicationContext context) {
        if ("live-model".equals(mode)) {
            if (liveModel.isBlank()) return new MissingLiveModelGateway("live-model 未配置明确的 model ID。");
            if (provider == null || provider.isBlank()) return new MissingLiveModelGateway("live-model 未配置 Provider profile。");
            if (!credentialReference(credentialRef, provider))
                return new MissingLiveModelGateway("live-model 必须引用该 Provider 的已登记凭据；不得把明文凭据放入配置。");
            if (!dataAuthorized) return new MissingLiveModelGateway("live-model 未确认使用合成或获授权数据。");
            // 会话合成资料与评测保留集分开授权；其他范围继续关闭 Provider。
            if (!List.of("synthetic-evaluation-only", "synthetic-conversation-with-knowledge-only",
                    "synthetic-conversation-with-knowledge-and-experience-only").contains(outboundDataScope))
                return new MissingLiveModelGateway("live-model 只允许固定合成评测或合成会话外发范围。");
            if (!positiveFeeCap(feeCap)) return new MissingLiveModelGateway("live-model 未配置正数费用上限。");
            if (feeCurrency == null || !feeCurrency.matches("[A-Z]{3}")) return new MissingLiveModelGateway("live-model 未配置大写 ISO 币种。");
            if (!finiteStopConditions(stopCondition)) return new MissingLiveModelGateway("live-model 必须启用金额上限、未知费用、安全拒绝和人工停止四类停止条件。");
            if (runOwner == null || runOwner.isBlank() || authorizationId == null || authorizationId.isBlank()
                    || !validAuthorizationExpiry(authorizationExpiry))
                return new MissingLiveModelGateway("live-model 的运行 Owner 或本轮授权缺失、格式错误或已过期。");
            if ("deepseek".equalsIgnoreCase(provider)) {
                // 本地的人民币/其他币种或更高额度不受支持，避免配置扩大既定费用硬上限。
                if (!"USD".equals(feeCurrency) || new BigDecimal(feeCap).compareTo(BigDecimal.ONE) > 0)
                    return new MissingLiveModelGateway("DeepSeek 本地质量运行的费用硬上限为 1.00 USD。");
                if (!"https://api.deepseek.com".equals(baseUrl))
                    return new MissingLiveModelGateway("DeepSeek 官方 profile 必须使用 https://api.deepseek.com。");
                Object chatModel = findBean(context, "org.springframework.ai.openai.OpenAiChatModel");
                if (chatModel == null) return new MissingLiveModelGateway("DeepSeek profile 未配置 Spring AI OpenAI ChatModel 或本地凭据。");
                return new SpringAiModelGateway(chatModel, liveModel, "deepseek", Instant.parse(authorizationExpiry), outboundDataScope);
            }
            if (!"dashscope".equalsIgnoreCase(provider)) return new MissingLiveModelGateway("不支持的 live Provider profile：" + provider);
            Object chatModel = findBean(context, "com.alibaba.cloud.ai.dashscope.chat.DashScopeChatModel");
            if (chatModel == null) return new MissingLiveModelGateway("live-model 未配置 Spring AI Alibaba ChatModel 或凭证。");
            return new SpringAiAlibabaModelGateway(chatModel, liveModel, Instant.parse(authorizationExpiry), outboundDataScope);
        }
        if ("deterministic".equals(mode)) return new DeterministicModelGateway(scenario);
        return new MissingLiveModelGateway("不支持的模型模式，不会回退到确定性替身：" + mode);
    }

    private Object findBean(org.springframework.context.ApplicationContext context, String typeName) {
        try {
            var type = Class.forName(typeName);
            var candidates = context.getBeansOfType((Class<?>) type);
            return candidates.isEmpty() ? null : candidates.values().iterator().next();
        } catch (ClassNotFoundException ignored) {
            return null;
        }
    }

    // Provider 与凭据引用固定绑定，实际秘密仅由出站拦截器经 Credential API 按请求解析。
    private boolean credentialReference(String value, String provider) {
        var expected = "deepseek".equalsIgnoreCase(provider) ? "model-deepseek" : "model-dashscope";
        return expected.equals(value);
    }

    @Bean
    EmbeddingGateway embeddingGateway(@Value("${eaf.model.mode:deterministic}") String mode,
                                      @Value("${eaf.model.embedding.live.model:${EAF_EMBEDDING_MODEL:}}") String liveModel,
                                      @Value("${eaf.model.embedding.live.revision:${EAF_EMBEDDING_REVISION:}}") String revision,
                                      @Value("${eaf.model.embedding.live.dimension:${EAF_EMBEDDING_DIMENSION:}}") String dimension,
                                      @Value("${eaf.model.embedding.live.max-batch-size:${EAF_EMBEDDING_MAX_BATCH_SIZE:}}") String maxBatchSize,
                                      @Value("${eaf.model.embedding.live.max-input-tokens:${EAF_EMBEDDING_MAX_INPUT_TOKENS:}}") String maxInputTokens,
                                      @Value("${eaf.model.embedding.live.max-text-code-points:${EAF_EMBEDDING_MAX_TEXT_CODE_POINTS:}}") String maxTextCodePoints,
                                      @Value("${eaf.model.live.fee-cap:${EAF_MODEL_LIVE_FEE_CAP:}}") String feeCap,
                                      @Value("${eaf.model.live.fee-currency:${EAF_MODEL_LIVE_FEE_CURRENCY:}}") String feeCurrency,
                                      org.springframework.context.ApplicationContext context,
                                      io.eaf.usage.api.UsageRecorder usage) {
        if ("deterministic".equals(mode)) return new DeterministicEmbeddingGateway();
        if ("live-model".equals(mode)) {
            // Profile、凭证、限额和 Provider Bean 任一缺失时返回 fail-closed Gateway，不装配测试替身。
            if (!positiveInt(dimension) || !positiveInt(maxBatchSize) || !positiveInt(maxInputTokens)
                    || !positiveInt(maxTextCodePoints) || liveModel == null || liveModel.isBlank()
                    || revision == null || revision.isBlank())
                return new MissingEmbeddingGateway("live-model 未登记完整 Embedding 模型版本、维度与输入上限。");
            if (!supportedDashScopeProfile(liveModel, Integer.parseInt(dimension), Integer.parseInt(maxBatchSize),
                    Integer.parseInt(maxInputTokens)))
                return new MissingEmbeddingGateway("当前 DashScope Profile 的模型、维度、批次或 Token 上限不受支持。");
            if (!positiveFeeCap(feeCap) || feeCurrency == null || !feeCurrency.matches("[A-Z]{3}"))
                return new MissingEmbeddingGateway("live-model 未配置 Embedding 金额上限或币种。");
            Object embeddingModel = null;
            try {
                var embeddingModelType = Class.forName("com.alibaba.cloud.ai.dashscope.embedding.DashScopeEmbeddingModel");
                var candidates = context.getBeansOfType((Class<?>) embeddingModelType);
                if (!candidates.isEmpty()) embeddingModel = candidates.values().iterator().next();
            } catch (ClassNotFoundException ignored) { }
            if (embeddingModel == null)
                return new MissingEmbeddingGateway("live-model 未配置 Spring AI Alibaba EmbeddingModel。");
            var profile = new EmbeddingProfile("dashscope", liveModel, revision, Integer.parseInt(dimension),
                    Integer.parseInt(maxBatchSize), Integer.parseInt(maxInputTokens), Integer.parseInt(maxTextCodePoints));
            try {
                return new SpringAiAlibabaEmbeddingGateway(embeddingModel, profile, usage, feeCap, feeCurrency);
            } catch (RuntimeException unavailable) {
                return new MissingEmbeddingGateway("无法确认 DashScope Embedding 的重试与计量配置。");
            }
        }
        return new MissingEmbeddingGateway("当前未配置已验证的真实 Embedding Provider，不会回退到确定性替身。");
    }

    private boolean positiveInt(String value) {
        try { return value != null && Integer.parseInt(value) > 0; }
        catch (NumberFormatException ignored) { return false; }
    }

    private boolean supportedDashScopeProfile(String model, int dimension, int maxBatchSize, int maxInputTokens) {
        var supportedDimension = switch (model) {
            case "text-embedding-v3" -> Set.of(64, 128, 256, 512, 768, 1_024).contains(dimension);
            case "text-embedding-v4" -> Set.of(64, 128, 256, 512, 768, 1_024, 1_536, 2_048).contains(dimension);
            default -> false;
        };
        //  首期只开放已核对的文本模型；批次和每条输入不得超过 Provider 当前契约上限。
        return supportedDimension && maxBatchSize <= 10 && maxInputTokens <= 8_192;
    }

    private boolean positiveFeeCap(String value) {
        try {
            return value != null && !value.isBlank() && new BigDecimal(value).signum() > 0;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    // 授权时间必须是尚未过期的 UTC Instant；无法解析时一律关闭真实 Provider。
    private boolean validAuthorizationExpiry(String value) {
        try {
            return value != null && Instant.parse(value).isAfter(Instant.now());
        } catch (java.time.DateTimeException ignored) {
            return false;
        }
    }

    // 真实模型调用固定启用完整停止集合，不接受装饰性或不完整的停止配置。
    private boolean finiteStopConditions(String value) {
        if (value == null || value.isBlank()) return false;
        var supplied = Arrays.stream(value.split(",")).map(String::trim).filter(item -> !item.isEmpty()).collect(java.util.stream.Collectors.toSet());
        return supplied.equals(Set.of("CAP_REACHED", "UNKNOWN_COST", "SAFETY_VIOLATION", "HUMAN_STOP"));
    }

    private static final class MissingLiveModelGateway implements ModelGateway {
        private final String detail;

        private MissingLiveModelGateway(String detail) { this.detail = detail; }

        @Override public io.eaf.model.api.ModelResult call(io.eaf.model.api.ModelRequest request) {
            throw new io.eaf.model.api.ModelFailure("DEPENDENCY_UNAVAILABLE", detail, false, false);
        }
        @Override public int callCount() { return 0; }
    }

    private static final class MissingEmbeddingGateway implements EmbeddingGateway {
        private final String detail;

        private MissingEmbeddingGateway(String detail) { this.detail = detail; }

        @Override public io.eaf.model.api.EmbeddingProfile profile() {
            throw new io.eaf.model.api.EmbeddingFailure("DEPENDENCY_UNAVAILABLE", detail, false, false);
        }

        @Override public io.eaf.model.api.EmbeddingResult embed(io.eaf.model.api.EmbeddingRequest request) {
            throw new io.eaf.model.api.EmbeddingFailure("DEPENDENCY_UNAVAILABLE", detail, false, false);
        }

        @Override public int callCount() { return 0; }
    }
}
// 本文件负责实现 EAF 的 ModelConfiguration.java 相关代码。
