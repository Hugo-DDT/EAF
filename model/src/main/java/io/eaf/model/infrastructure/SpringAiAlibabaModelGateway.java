package io.eaf.model.infrastructure;

import io.eaf.model.api.ModelFailure;
import io.eaf.model.api.ModelBillingProfile;
import io.eaf.model.api.ModelGateway;
import io.eaf.model.api.ModelMessage;
import io.eaf.model.api.ModelRequest;
import io.eaf.model.api.ModelResult;
import io.eaf.model.api.ModelToolCall;
import io.eaf.model.api.ModelToolDefinition;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** Spring AI Alibaba 适配 Provider 协议；EAF 负责模型调用治理与业务工具执行边界。 */
public final class SpringAiAlibabaModelGateway implements ModelGateway {
    private final Object chatModel;
    private final String modelName;
    private final Instant authorizationExpiry;
    private final String outboundDataScope;
    private final AtomicInteger calls = new AtomicInteger();
    private final Object configurationLock = new Object();
    private volatile boolean configured;

    public SpringAiAlibabaModelGateway(Object chatModel, String modelName, Instant authorizationExpiry) {
        this(chatModel, modelName, authorizationExpiry, "synthetic-evaluation-only");
    }

    public SpringAiAlibabaModelGateway(Object chatModel, String modelName, Instant authorizationExpiry,
                                       String outboundDataScope) {
        this.chatModel = chatModel;
        this.modelName = modelName;
        this.authorizationExpiry = authorizationExpiry;
        this.outboundDataScope = outboundDataScope;
    }

    @Override
    public ModelBillingProfile billingProfile() { return new ModelBillingProfile("dashscope", modelName); }

    @Override
    public String outboundDataScope() { return outboundDataScope; }

    @Override
    public ModelResult call(ModelRequest request) {
        requireCurrentAuthorization();
        if (request.deadline().isBefore(Instant.now())) throw new ModelFailure("UPSTREAM_TIMEOUT", "模型截止时间已到。", true, false);
        try {
            var options = configureProvider(request);
            var promptType = Class.forName("org.springframework.ai.chat.prompt.Prompt");
            var messages = request.messages().stream().map(this::message).toList();
            var prompt = promptType.getConstructor(List.class, Class.forName("org.springframework.ai.chat.prompt.ChatOptions"))
                    .newInstance(messages, options);
            requireCurrentAuthorization();
            calls.incrementAndGet();
            return invoke(promptType, prompt, request.deadline());
        } catch (ModelFailure e) {
            throw e;
        } catch (Exception e) {
            throw new ModelFailure("UPSTREAM_FAILURE", "Spring AI Alibaba 模型调用失败。", false, true);
        }
    }

    // 与 DeepSeek 一致，模型在异步出站前仍须持有当前有效的单轮调用授权。
    private void requireCurrentAuthorization() {
        if (authorizationExpiry == null || !authorizationExpiry.isAfter(Instant.now()))
            throw new ModelFailure("AUTHORIZATION_EXPIRED", "本次模型运行授权已过期，未向 Provider 出站。", false, false);
    }

    private ModelResult invoke(Class<?> promptType, Object prompt, Instant deadline) throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var future = executor.submit(() -> {
                requireCurrentAuthorization();
                return read(promptType, prompt);
            });
            try {
                return future.get(Math.max(1, java.time.Duration.between(Instant.now(), deadline).toMillis()), TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.TimeoutException e) {
                future.cancel(true);
                throw new ModelFailure("UPSTREAM_TIMEOUT", "Spring AI Alibaba 模型调用超时。", true, true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                future.cancel(true);
                throw new ModelFailure("UPSTREAM_TIMEOUT", "Spring AI Alibaba 模型调用被中断。", true, true);
            } catch (java.util.concurrent.ExecutionException e) {
                var cause = e.getCause();
                if (cause instanceof ModelFailure failure) throw failure;
                throw providerFailure(cause);
            }
        }
    }

    private ModelResult read(Class<?> promptType, Object prompt) throws Exception {
        var response = chatModel.getClass().getMethod("call", promptType).invoke(chatModel, prompt);
        var generation = response.getClass().getMethod("getResult").invoke(response);
        var output = generation == null ? null : generation.getClass().getMethod("getOutput").invoke(generation);
        var text = output == null ? null : (String) output.getClass().getMethod("getText").invoke(output);
        var calls = new ArrayList<ModelToolCall>();
        if (output != null) {
            try {
                var raw = output.getClass().getMethod("getToolCalls").invoke(output);
                if (raw instanceof Iterable<?> iterable) for (var call : iterable) calls.add(new ModelToolCall(
                        String.valueOf(call.getClass().getMethod("id").invoke(call)),
                        String.valueOf(call.getClass().getMethod("name").invoke(call)),
                        String.valueOf(call.getClass().getMethod("arguments").invoke(call))));
            } catch (NoSuchMethodException ignored) { }
        }
        var metadata = response.getClass().getMethod("getMetadata").invoke(response);
        var usage = metadata == null ? null : metadata.getClass().getMethod("getUsage").invoke(metadata);
        Integer input = usage == null ? null : token(usage, "getPromptTokens");
        Integer outputTokens = usage == null ? null : token(usage, "getCompletionTokens");
        if (Integer.valueOf(0).equals(input) && Integer.valueOf(0).equals(outputTokens)) { input = null; outputTokens = null; }
        return new ModelResult("spring-ai-alibaba", modelName, text, input, outputTokens,
                input != null && outputTokens != null ? "KNOWN" : "UNKNOWN", List.copyOf(calls), calls.isEmpty() ? "STOP" : "TOOL_CALLS");
    }

    private Object message(ModelMessage message) {
        try {
            if ("tool".equals(message.role())) {
                if (message.toolCallId() == null || message.toolCallId().isBlank()
                        || message.toolName() == null || message.toolName().isBlank())
                    throw new ModelFailure("MODEL_PROTOCOL_ERROR", "工具结果缺少调用 ID 或工具名。", false, false);
                var responseType = Class.forName("org.springframework.ai.chat.messages.ToolResponseMessage$ToolResponse");
                var response = responseType.getConstructor(String.class, String.class, String.class)
                        .newInstance(message.toolCallId(), message.toolName(), message.content());
                var responseMessage = Class.forName("org.springframework.ai.chat.messages.ToolResponseMessage");
                var builder = responseMessage.getMethod("builder").invoke(null);
                builder = builder.getClass().getMethod("responses", List.class).invoke(builder, List.of(response));
                return builder.getClass().getMethod("build").invoke(builder);
            }
            if ("assistant".equals(message.role()) && !message.toolCalls().isEmpty()) {
                var assistant = Class.forName("org.springframework.ai.chat.messages.AssistantMessage");
                var toolCallType = Class.forName("org.springframework.ai.chat.messages.AssistantMessage$ToolCall");
                var calls = new ArrayList<>();
                for (var call : message.toolCalls()) calls.add(toolCallType.getConstructor(String.class, String.class, String.class, String.class)
                        .newInstance(call.callId(), "function", call.name(), call.argumentsJson()));
                var builder = assistant.getMethod("builder").invoke(null);
                builder = builder.getClass().getMethod("content", String.class).invoke(builder, message.content() == null ? "" : message.content());
                builder = builder.getClass().getMethod("toolCalls", List.class).invoke(builder, calls);
                return builder.getClass().getMethod("build").invoke(builder);
            }
            var type = switch (message.role()) {
                case "system" -> "org.springframework.ai.chat.messages.SystemMessage";
                case "user" -> "org.springframework.ai.chat.messages.UserMessage";
                case "assistant" -> "org.springframework.ai.chat.messages.AssistantMessage";
                default -> throw new ModelFailure("MODEL_PROTOCOL_ERROR", "未知模型消息角色。", false, false);
            };
            return Class.forName(type).getConstructor(String.class).newInstance(message.content());
        } catch (ModelFailure e) { throw e; }
        catch (Exception e) { throw new ModelFailure("DEPENDENCY_UNAVAILABLE", "Spring AI Alibaba 消息类型不可用。", false, false); }
    }

    private Object configureProvider(ModelRequest request) {
        try {
            var defaults = chatModel.getClass().getMethod("getDefaultOptions").invoke(chatModel);
            if (!configured) {
                synchronized (configurationLock) {
                    if (!configured) {
                        // Provider SDK 的默认重试会让一次 EAF 调用产生多次收费请求，因此由 EAF 持有重试与恢复。
                        var retryTemplate = chatModel.getClass().getField("retryTemplate").get(chatModel);
                        var retryPolicyType = Class.forName("org.springframework.retry.RetryPolicy");
                        var noRetry = Class.forName("org.springframework.retry.policy.NeverRetryPolicy").getConstructor().newInstance();
                        retryTemplate.getClass().getMethod("setRetryPolicy", retryPolicyType).invoke(retryTemplate, noRetry);
                        configured = true;
                    }
                }
            }
            // 每次请求复制 options，避免请求级工具、模型名和预算修改共享默认对象。
            var options = defaults.getClass().getMethod("copy").invoke(defaults);
            invokeIfPresent(options, "setModel", modelName);
            var estimate = request.messages().stream().mapToInt(m -> m.content() == null ? 0 : m.content().length()).sum();
            var outputBudget = request.tokenBudget() - estimate;
            if (outputBudget <= 0) throw new ModelFailure("BUDGET_EXCEEDED", "模型输入预算不足。", false, false);
            invokeIfPresent(options, "setMaxTokens", outputBudget);
            var callbacks = new ArrayList<>();
            for (var tool : request.tools()) callbacks.add(toolCallback(tool));
            options.getClass().getMethod("setToolCallbacks", List.class).invoke(options, callbacks);
            options.getClass().getMethod("setToolNames", Set.class).invoke(options,
                    request.tools().stream().map(ModelToolDefinition::name).collect(java.util.stream.Collectors.toUnmodifiableSet()));
            options.getClass().getMethod("setInternalToolExecutionEnabled", Boolean.class).invoke(options, false);
            invokeIfPresent(options, "setTemperature", 0.0d);
            return options;
        } catch (ModelFailure e) { throw e; }
        catch (Exception e) { throw new ModelFailure("DEPENDENCY_UNAVAILABLE", "无法确认 Spring AI Alibaba 工具执行配置。", false, false); }
    }

    private Object toolCallback(ModelToolDefinition tool) {
        try {
            var callbackType = Class.forName("org.springframework.ai.tool.function.FunctionToolCallback");
            Function<String, String> neverExecute = ignored -> {
                throw new IllegalStateException("EAF tool execution must go through ExecutionService");
            };
            var builder = callbackType.getMethod("builder", String.class, Function.class).invoke(null, tool.name(), neverExecute);
            builder = builder.getClass().getMethod("description", String.class).invoke(builder, tool.description());
            builder = builder.getClass().getMethod("inputType", java.lang.reflect.Type.class).invoke(builder, String.class);
            builder = builder.getClass().getMethod("inputSchema", String.class).invoke(builder, tool.inputSchema());
            return builder.getClass().getMethod("build").invoke(builder);
        } catch (Exception e) {
            throw new ModelFailure("DEPENDENCY_UNAVAILABLE", "无法构造 Spring AI Alibaba 工具定义。", false, false);
        }
    }

    private void invokeIfPresent(Object target, String name, Object value) throws Exception {
        for (var method : target.getClass().getMethods()) if (method.getName().equals(name) && method.getParameterCount() == 1) {
            var type = method.getParameterTypes()[0];
            if (value == null || type.isAssignableFrom(value.getClass()) || type == int.class || type == Integer.class || type == double.class || type == Double.class || type == boolean.class || type == Boolean.class) {
                method.invoke(target, value); return;
            }
        }
    }

    private Integer token(Object usage, String method) throws Exception {
        var value = usage.getClass().getMethod(method).invoke(usage);
        return value instanceof Number number ? number.intValue() : null;
    }

    private ModelFailure providerFailure(Throwable failure) {
        if (ProviderFailureSupport.capacityExceeded(failure))
            return new ModelFailure("MODEL_CAPACITY_EXCEEDED", "模型出站并发已满，请稍后重试。", false, false);
        if (ProviderFailureSupport.quotaUnavailable(failure))
            return new ModelFailure("MODEL_QUOTA_UNAVAILABLE", "模型共享并发协调暂不可用，未发送请求。", false, false);
        // HTTP 客户端会包装超时异常；沿 cause 链分类，同时避免循环 cause 造成死循环。
        var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        var text = new StringBuilder();
        for (var cause = failure; cause != null && seen.add(cause); cause = cause.getCause()) {
            text.append(cause.getClass().getName()).append(' ').append(cause.getMessage()).append(' ');
        }
        var normalized = text.toString().toLowerCase(Locale.ROOT);
        if (normalized.contains("timeout") || normalized.contains("timed out")) return new ModelFailure("UPSTREAM_TIMEOUT", "模型调用超时。", true, true);
        if (normalized.contains("401") || normalized.contains("403") || normalized.contains("unauthorized")) return new ModelFailure("UPSTREAM_AUTH", "模型认证失败。", false, true);
        if (normalized.contains("429") || normalized.contains("rate limit")) return new ModelFailure("UPSTREAM_RATE_LIMITED", "模型被限流。", false, true);
        return new ModelFailure("UPSTREAM_FAILURE", "真实模型调用失败。", false, true);
    }

    @Override public int callCount() { return calls.get(); }
}
// 本文件负责实现 EAF 的 SpringAiAlibabaModelGateway.java 相关代码。
