package io.eaf.model.infrastructure;

import io.eaf.model.api.ModelBillingProfile;
import io.eaf.model.api.ModelFailure;
import io.eaf.model.api.TypedDecisionGateway;
import io.eaf.model.api.TypedDecisionRequest;
import io.eaf.model.api.TypedDecisionResult;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

/** TypeSafe Jev 的非 Chat Choice 适配；EAF 只接受固定的风险类别和完整用量。 */
public final class TypesafeJevDecisionGateway implements TypedDecisionGateway {
    private static final String QUESTION_ID = "risk_level";
    private static final List<String> CHOICES = List.of("LOW", "MEDIUM", "HIGH", "UNKNOWN");
    private static final Set<String> CHOICE_SET = Set.copyOf(CHOICES);
    private static final ParameterizedTypeReference<Map<String, Object>> JSON_MAP = new ParameterizedTypeReference<>() { };
    private final RestClient client;
    private final String endpoint;
    private final String configuredModel;
    private final Instant authorizationExpiry;
    private final String outboundDataScope;
    private final AtomicInteger calls = new AtomicInteger();

    public TypesafeJevDecisionGateway(RestClient client, String endpoint, String configuredModel,
                                      Instant authorizationExpiry) {
        this(client, endpoint, configuredModel, authorizationExpiry, null);
    }

    public TypesafeJevDecisionGateway(RestClient client, String endpoint, String configuredModel,
                                      Instant authorizationExpiry, String outboundDataScope) {
        this.client = client;
        this.endpoint = endpoint;
        this.configuredModel = configuredModel;
        this.authorizationExpiry = authorizationExpiry;
        this.outboundDataScope = outboundDataScope;
    }

    @Override public boolean enabled() { return true; }
    @Override public boolean external() { return true; }
    @Override public ModelBillingProfile billingProfile() {
        return new ModelBillingProfile("typesafe", configuredModel, "DECISION");
    }
    @Override public String outboundDataScope() { return outboundDataScope; }

    @Override
    public TypedDecisionResult decide(TypedDecisionRequest request) {
        if (request.state() == null || request.state().isBlank() || request.state().codePointCount(0, request.state().length()) > 100_000)
            throw new ModelFailure("DECISION_INPUT_INVALID", "分类摘要为空或超出请求上限。", false, false);
        if (authorizationExpiry == null || !authorizationExpiry.isAfter(Instant.now()))
            throw new ModelFailure("DECISION_AUTHORIZATION_EXPIRED", "TypeSafe Jev 本轮授权缺失或已过期。", false, false);
        if (request.deadline() == null || !request.deadline().isAfter(Instant.now()))
            throw new ModelFailure("UPSTREAM_TIMEOUT", "Jev 分类截止时间已到。", true, false);
        var payload = requestBody(request.state());
        calls.incrementAndGet();
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var future = executor.submit(() -> send(payload));
        try {
            long remainingMs = Math.max(1L, java.time.Duration.between(Instant.now(), request.deadline()).toMillis());
            var response = future.get(remainingMs, TimeUnit.MILLISECONDS);
            return parse(response, request.tokenBudget());
        } catch (ModelFailure failure) {
            throw failure;
        } catch (TimeoutException timeout) {
            future.cancel(true);
            throw new ModelFailure("UPSTREAM_TIMEOUT", "TypeSafe Jev 请求超时；结果可能未知，不会自动重试。", true, true);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new ModelFailure("UPSTREAM_TIMEOUT", "TypeSafe Jev 请求被中断；结果可能未知，不会自动重试。", true, true);
        } catch (ExecutionException failed) {
            if (ProviderFailureSupport.capacityExceeded(failed.getCause()))
                throw new ModelFailure("MODEL_CAPACITY_EXCEEDED", "Jev 出站并发已满，请稍后重试。", false, false);
            if (ProviderFailureSupport.quotaUnavailable(failed.getCause()))
                throw new ModelFailure("MODEL_QUOTA_UNAVAILABLE", "Jev 共享并发协调暂不可用，未发送请求。", false, false);
            if (causedByCredentialFailure(failed.getCause()))
                throw new ModelFailure("DEPENDENCY_UNAVAILABLE", "TypeSafe Jev 凭据不可用，未发送请求。", false, false);
            var httpStatus = causedByHttpStatus(failed.getCause());
            if (httpStatus != null) throw httpFailure(httpStatus);
            throw new ModelFailure("UPSTREAM_FAILURE", "TypeSafe Jev 调用失败；响应详情已脱敏，不会自动重试。", false, true);
        } catch (RuntimeException invalid) {
            throw new ModelFailure("UPSTREAM_INVALID_RESPONSE", "TypeSafe Jev 响应无效；响应详情已脱敏。", false, true);
        } finally {
            executor.shutdownNow();
        }
    }

    @Override public int callCount() { return calls.get(); }

    private Map<String, Object> requestBody(String state) {
        var criteria = new LinkedHashMap<String, Object>();
        criteria.put("LOW", "材料明确呈现续约稳定、近期满意或持续合作信号。");
        criteria.put("MEDIUM", "存在需要关注的信号，但尚不足以支持明确高风险或低风险类别。");
        criteria.put("HIGH", "材料明确呈现续约中断、重大投诉或其他严重流失风险信号。");
        criteria.put("UNKNOWN", "信息不足、相互冲突，或缺少判断风险所需的关键材料。");
        var question = Map.<String, Object>of("type", "choice",
                "instructions", "仅根据这段合成客户摘要选择当前材料支持的流失风险类别。缺少关键证据或材料冲突时选 UNKNOWN。类别是建议，不是流失概率或外部事实。",
                "criteria", criteria);
        return Map.of("state", state, "model", configuredModel, "questions", Map.of(QUESTION_ID, question));
    }

    private Map<String, Object> send(Map<String, Object> payload) {
        return client.post().uri(endpoint).contentType(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                .body(payload).retrieve()
                .onStatus(status -> !status.is2xxSuccessful(), (request, response) -> {
                    throw new ProviderHttpStatusException(response.getStatusCode().value());
                })
                .body(JSON_MAP);
    }

    private TypedDecisionResult parse(Map<String, Object> response, int tokenBudget) {
        if (response == null) throw invalidResponse();
        var returnedModel = string(response.get("model"));
        var answers = object(response.get("answers"));
        var answer = object(answers.get(QUESTION_ID));
        if (returnedModel == null || returnedModel.isBlank() || returnedModel.length() > 150
                || !"choice".equals(answer.get("type"))) throw invalidResponse();
        var choice = string(answer.get("choice"));
        if (choice == null) throw invalidResponse();
        choice = choice.toUpperCase(Locale.ROOT);
        if (!CHOICE_SET.contains(choice)) throw invalidResponse();
        var probabilitiesNode = object(answer.get("probabilities"));
        if (!probabilitiesNode.keySet().equals(CHOICE_SET)) throw invalidResponse();
        var probabilities = new LinkedHashMap<String, BigDecimal>();
        var sum = BigDecimal.ZERO;
        for (var candidate : CHOICES) {
            var probability = decimal(probabilitiesNode.get(candidate));
            if (probability == null || probability.signum() < 0 || probability.compareTo(BigDecimal.ONE) > 0)
                throw invalidResponse();
            probabilities.put(candidate, probability);
            sum = sum.add(probability);
        }
        if (sum.subtract(BigDecimal.ONE).abs().compareTo(new BigDecimal("0.01")) > 0)
            throw invalidResponse();
        var maxProbability = probabilities.values().stream().max(BigDecimal::compareTo).orElseThrow();
        if (probabilities.get(choice).compareTo(maxProbability) != 0) throw invalidResponse();
        var confidence = decimal(answer.get("confidence"));
        if (confidence == null || confidence.signum() < 0 || confidence.compareTo(BigDecimal.ONE) > 0)
            throw invalidResponse();
        var usage = object(response.get("usage"));
        var inputTokens = integer(usage.get("input_tokens"));
        var outputTokens = integer(usage.get("output_tokens"));
        if (inputTokens == null || outputTokens == null || inputTokens < 0 || outputTokens < 0
                || (long) inputTokens + outputTokens > tokenBudget)
            throw invalidResponse();
        return new TypedDecisionResult("typesafe", returnedModel, choice, probabilities,
                inputTokens, outputTokens, "KNOWN");
    }

    private ModelFailure httpFailure(int status) {
        String code = status == 401 ? "DEPENDENCY_UNAVAILABLE"
                : status == 429 || status == 529 ? "UPSTREAM_UNAVAILABLE" : "UPSTREAM_FAILURE";
        String detail = status == 401 ? "TypeSafe Jev 未接受当前凭据。"
                : status == 429 || status == 529 ? "TypeSafe Jev 当前不可用；不会自动重试。"
                : "TypeSafe Jev 返回 HTTP " + status + "；响应详情已脱敏。";
        return new ModelFailure(code, detail, false, true);
    }

    private boolean causedByCredentialFailure(Throwable cause) {
        for (var current = cause; current != null; current = current.getCause())
            if (current instanceof ProviderCredentialUnavailableException) return true;
        return false;
    }

    private Integer causedByHttpStatus(Throwable cause) {
        for (var current = cause; current != null; current = current.getCause())
            if (current instanceof ProviderHttpStatusException status) return status.status();
        return null;
    }

    private ModelFailure invalidResponse() {
        return new ModelFailure("UPSTREAM_INVALID_RESPONSE", "TypeSafe Jev 缺少有效的 Choice、分布或 Usage；响应详情已脱敏。", false, true);
    }

    private Map<?, ?> object(Object value) {
        return value instanceof Map<?, ?> map ? map : Map.of();
    }

    private String string(Object value) { return value instanceof String text ? text : null; }

    private BigDecimal decimal(Object value) {
        if (value instanceof BigDecimal number) return number;
        if (value instanceof Number number && Double.isFinite(number.doubleValue()))
            return BigDecimal.valueOf(number.doubleValue());
        return null;
    }

    private Integer integer(Object value) {
        if (!(value instanceof Number number)) return null;
        try {
            var decimal = value instanceof BigDecimal bigDecimal ? bigDecimal : new BigDecimal(number.toString());
            return decimal.intValueExact();
        } catch (ArithmeticException | NumberFormatException invalid) { return null; }
    }

    private static final class ProviderHttpStatusException extends RuntimeException {
        private final int status;
        private ProviderHttpStatusException(int status) { this.status = status; }
        int status() { return status; }
    }
}
