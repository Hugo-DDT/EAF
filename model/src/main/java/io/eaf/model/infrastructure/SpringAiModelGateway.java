package io.eaf.model.infrastructure;

import io.eaf.model.api.ModelFailure;
import io.eaf.model.api.ModelBillingProfile;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelMessage;
import io.eaf.model.api.ModelRequest;
import io.eaf.model.api.ModelResult;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public final class SpringAiModelGateway implements ModelGateway {
    private final Object chatModel;
    private final String modelName;
    private final String providerName;
    private final Instant authorizationExpiry;
    private final String outboundDataScope;
    private final AtomicInteger calls = new AtomicInteger();
    private final Object configurationLock = new Object();
    private volatile boolean configured;

    public SpringAiModelGateway(Object chatModel, String modelName, String providerName, Instant authorizationExpiry) {
        this(chatModel, modelName, providerName, authorizationExpiry, "synthetic-evaluation-only");
    }

    public SpringAiModelGateway(Object chatModel, String modelName, String providerName, Instant authorizationExpiry,
                                String outboundDataScope) {
        this.chatModel = chatModel;
        this.modelName = modelName;
        this.providerName = providerName;
        this.authorizationExpiry = authorizationExpiry;
        this.outboundDataScope = outboundDataScope;
    }

    // Runtime 在出站前使用服务端登记的 Provider/model 作为 Usage 计价身份。
    @Override
    public ModelBillingProfile billingProfile() {
        return new ModelBillingProfile(providerName, modelName);
    }

    @Override
    public String outboundDataScope() { return outboundDataScope; }

    @Override
    public ModelResult call(ModelRequest request) {
        requireCurrentAuthorization();
        if (request.deadline().isBefore(Instant.now())) throw new ModelFailure("UPSTREAM_TIMEOUT", "模型截止时间已到。", true, false);
        try {
            var messages = request.messages().stream().map(this::message).toList();
            var options = configureProvider(request);
            var promptType = Class.forName("org.springframework.ai.chat.prompt.Prompt");
            var chatOptionsType = Class.forName("org.springframework.ai.chat.prompt.ChatOptions");
            var prompt = promptType.getConstructor(List.class, chatOptionsType).newInstance(messages, options);
            requireCurrentAuthorization();
            if (request.deadline().isBefore(Instant.now()))
                throw new ModelFailure("UPSTREAM_TIMEOUT", "模型截止时间已到。", true, false);
            calls.incrementAndGet();
            return callProvider(promptType, prompt, request.deadline());
        } catch (ModelFailure e) {
            throw e;
        } catch (Exception e) {
            throw new ModelFailure("UPSTREAM_FAILURE", "真实模型调用失败。", false, true);
        }
    }

    private ModelResult callProvider(Class<?> promptType, Object prompt, Instant deadline) throws Exception {
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var future = executor.submit(() -> {
            requireCurrentAuthorization();
            return readResult(promptType, prompt);
        });
        try {
            var remainingMs = Math.max(1L, java.time.Duration.between(Instant.now(), deadline).toMillis());
            return future.get(remainingMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new ModelFailure("UPSTREAM_TIMEOUT", "真实模型调用超时。", true, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new ModelFailure("UPSTREAM_TIMEOUT", "真实模型调用被中断。", true, true);
        } catch (ExecutionException e) {
            var cause = e.getCause();
            if (cause instanceof ModelFailure failure) throw failure;
            if (cause instanceof InvocationTargetException invocation)
                throw providerFailure(invocation.getCause());
            throw providerFailure(cause);
        } finally {
            future.cancel(true);
            executor.shutdownNow();
        }
    }

    private ModelResult readResult(Class<?> promptType, Object prompt) throws Exception {
        var response = chatModel.getClass().getMethod("call", promptType).invoke(chatModel, prompt);
        var generation = response.getClass().getMethod("getResult").invoke(response);
        var output = generation.getClass().getMethod("getOutput").invoke(generation);
        var text = (String) output.getClass().getMethod("getText").invoke(output);
        var metadata = response.getClass().getMethod("getMetadata").invoke(response);
        var model = metadata == null ? null : (String) metadata.getClass().getMethod("getModel").invoke(metadata);
        var usage = metadata == null ? null : metadata.getClass().getMethod("getUsage").invoke(metadata);
        Integer inputTokens = usage == null ? null : token(usage, "getPromptTokens");
        Integer outputTokens = usage == null ? null : token(usage, "getCompletionTokens");
        if (Integer.valueOf(0).equals(inputTokens) && Integer.valueOf(0).equals(outputTokens)) {
            inputTokens = null;
            outputTokens = null;
        }
        var resolvedModel = model == null || model.isBlank() ? modelName : model;
        var usageStatus = inputTokens != null && outputTokens != null ? "KNOWN" : "UNKNOWN";
        return new ModelResult("spring-ai-openai-compatible", resolvedModel, text, inputTokens, outputTokens, usageStatus);
    }

    private Integer token(Object usage, String method) throws Exception {
        var value = usage.getClass().getMethod(method).invoke(usage);
        return value instanceof Number number ? number.intValue() : null;
    }

    private Object message(ModelMessage message) {
        try {
            var type = switch (message.role()) {
                case "system" -> "org.springframework.ai.chat.messages.SystemMessage";
                case "user" -> "org.springframework.ai.chat.messages.UserMessage";
                case "assistant" -> "org.springframework.ai.chat.messages.AssistantMessage";
                default -> throw new ModelFailure("TOOL_CALL_NOT_ALLOWED", "不允许工具消息。", false, false);
            };
            return Class.forName(type).getConstructor(String.class).newInstance(message.content());
        } catch (ModelFailure e) {
            throw e;
        } catch (Exception e) {
            throw new ModelFailure("DEPENDENCY_UNAVAILABLE", "Spring AI 消息类型不可用。", false, false);
        }
    }

    // 每次真实出站前复核本轮授权，避免进程跨过授权截止时间后继续调用 Provider。
    private void requireCurrentAuthorization() {
        if (!authorizationExpiry.isAfter(Instant.now()))
            throw new ModelFailure("AUTHORIZATION_EXPIRED", "本次模型运行授权已过期，未向 Provider 出站。", false, false);
    }

    private Object configureProvider(ModelRequest request) {
        try {
            var defaults = chatModel.getClass().getMethod("getDefaultOptions").invoke(chatModel);
            if (!configured) {
                synchronized (configurationLock) {
                    if (!configured) {
                        var retryField = chatModel.getClass().getDeclaredField("retryTemplate");
                        retryField.trySetAccessible();
                        var retryTemplate = retryField.get(chatModel);
                        var retryPolicy = Class.forName("org.springframework.retry.policy.NeverRetryPolicy").getConstructor().newInstance();
                        retryTemplate.getClass().getMethod("setRetryPolicy", Class.forName("org.springframework.retry.RetryPolicy")).invoke(retryTemplate, retryPolicy);
                        configured = true;
                    }
                }
            }
            // 每次请求复制 options；禁止工具执行与 JSON 格式只作用于本次 Prompt。
            var options = defaults.getClass().getMethod("copy").invoke(defaults);
            options.getClass().getMethod("setToolCallbacks", List.class).invoke(options, List.of());
            options.getClass().getMethod("setToolNames", Set.class).invoke(options, Set.of());
            options.getClass().getMethod("setInternalToolExecutionEnabled", Boolean.class).invoke(options, false);
            options.getClass().getMethod("setTemperature", Double.class).invoke(options, 0.0d);
            var responseFormat = Class.forName("org.springframework.ai.openai.api.ResponseFormat").getConstructor().newInstance();
            var responseFormatType = Class.forName("org.springframework.ai.openai.api.ResponseFormat$Type");
            var jsonObject = java.util.Arrays.stream(responseFormatType.getEnumConstants())
                    .filter(value -> ((Enum<?>) value).name().equals("JSON_OBJECT")).findFirst().orElseThrow();
            responseFormat.getClass().getMethod("setType", responseFormatType).invoke(responseFormat, jsonObject);
            options.getClass().getMethod("setResponseFormat", responseFormat.getClass()).invoke(options, responseFormat);
            var estimatedInputTokens = request.messages().stream().mapToInt(m -> m.content().length()).sum();
            var outputTokenBudget = request.tokenBudget() - estimatedInputTokens;
            if (outputTokenBudget <= 0) throw new ModelFailure("BUDGET_EXCEEDED", "模型输入预算不足。", false, false);
            options.getClass().getMethod("setModel", String.class).invoke(options, modelName);
            options.getClass().getMethod("setMaxTokens", Integer.class).invoke(options, outputTokenBudget);
            return options;
        } catch (ModelFailure e) {
            throw e;
        } catch (Exception e) {
            throw new ModelFailure("DEPENDENCY_UNAVAILABLE", "无法确认 Provider 的重试与工具执行配置。", false, false);
        }
    }

    ModelFailure providerFailure(Throwable failure) {
        if (ProviderFailureSupport.capacityExceeded(failure))
            return new ModelFailure("MODEL_CAPACITY_EXCEEDED", "模型出站并发已满，请稍后重试。", false, false);
        if (ProviderFailureSupport.quotaUnavailable(failure))
            return new ModelFailure("MODEL_QUOTA_UNAVAILABLE", "模型共享并发协调暂不可用，未发送请求。", false, false);
        var description = "";
        for (var cause = failure; cause != null; cause = cause.getCause()) {
            description += " " + cause.getClass().getName() + " " + String.valueOf(cause.getMessage());
        }
        var text = description.toLowerCase(Locale.ROOT);
        if (contains(text, "timeout", "timed out", "read timed"))
            return new ModelFailure("UPSTREAM_TIMEOUT", "真实模型调用超时。", true, true);
        if (contains(text, "401", "403", "unauthorized", "forbidden", "invalid api key", "authentication"))
            return new ModelFailure("UPSTREAM_AUTH", "真实模型认证失败。", false, true);
        if (contains(text, "429", "too many requests", "rate limit", "ratelimit"))
            return new ModelFailure("UPSTREAM_RATE_LIMITED", "真实模型被限流。", false, true);
        return new ModelFailure("UPSTREAM_FAILURE", "真实模型调用失败。", false, true);
    }

    private boolean contains(String text, String... values) {
        for (var value : values) if (text.contains(value)) return true;
        return false;
    }

    @Override public int callCount() { return calls.get(); }
}
// 本文件负责实现 EAF 的 SpringAiModelGateway.java 相关代码。
