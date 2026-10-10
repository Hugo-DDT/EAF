package io.eaf.model.infrastructure;

import com.sun.net.httpserver.HttpServer;
import io.eaf.model.api.ModelFailure;
import io.eaf.model.api.ModelMessage;
import io.eaf.model.api.ModelProfileSnapshot;
import io.eaf.model.api.ModelRequest;
import io.eaf.model.api.ModelToolCall;
import io.eaf.model.api.ModelToolDefinition;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 本地假 Provider 通过实际 DashScope ChatModel 验证契约，不访问外网或真实模型。 */
class P7ModelProviderContractTest {
    private static final String TOOL_NAME = "crm.customer.query";
    private static final String TOOL_SCHEMA = "{\"type\":\"object\",\"properties\":{\"customerId\":{\"type\":\"string\"}},\"required\":[\"customerId\"]}";
    private static final String TOOL_RESPONSE = "{\"request_id\":\"fixture\",\"output\":{\"choices\":[{\"finish_reason\":\"tool_calls\",\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"id\":\"call-fixture-7\",\"type\":\"function\",\"function\":{\"name\":\"crm.customer.query\",\"arguments\":\"{\\\"customerId\\\":\\\"C-1\\\"}\"}}]}}]},\"usage\":{\"input_tokens\":11,\"output_tokens\":4,\"total_tokens\":15}}";
    private static final String FINAL_RESPONSE = "{\"request_id\":\"fixture\",\"output\":{\"choices\":[{\"finish_reason\":\"stop\",\"message\":{\"role\":\"assistant\",\"content\":\"done\"}}]},\"usage\":{\"input_tokens\":11,\"output_tokens\":4,\"total_tokens\":15}}";

    @Test
    void actualDashScopeContractUsesLocalFixtureAndLeavesExecutionToEaf() throws Exception {
        // 仅加载 Provider API 的本地 Embedding 测试依赖并不启用 Chat 自动配置；完整契约只在 live-model profile 下运行。
        if (!present("com.alibaba.cloud.ai.autoconfigure.dashscope.DashScopeChatAutoConfiguration")) {
            Assumptions.abort("live-model profile not active");
        }

        var requestBodies = new CopyOnWriteArrayList<String>();
        var requestCount = new AtomicInteger();
        var callbackCount = new AtomicInteger();
        var current = new AtomicReference<>(new FixtureResponse(200, TOOL_RESPONSE, 0));
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requestCount.incrementAndGet();
            requestBodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            var fixture = current.get();
            try {
                if (fixture.delayMillis() > 0) Thread.sleep(fixture.delayMillis());
                var body = fixture.body().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(fixture.status(), body.length);
                exchange.getResponseBody().write(body);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (java.io.IOException clientTimedOut) {
                // 客户端读取超时后关闭连接，属于本用例需要验证的传输结果。
            } finally {
                exchange.close();
            }
        });
        server.start();

        try (ConfigurableApplicationContext context = providerContext(server.getAddress().getPort())) {
            var chatModelType = Class.forName("org.springframework.ai.chat.model.ChatModel");
            // OpenAI starter 也在 live-model 测试类路径中，必须按 Provider 名称选择 DashScope Bean。
            var chatModel = context.getBean("dashScopeChatModel");
            var toolCallback = toolCallback(callbackCount);
            var options = toolOptions(toolCallback);
            var userMessage = Class.forName("org.springframework.ai.chat.messages.UserMessage").getConstructor(String.class)
                    .newInstance("查询合成客户 C-1");
            var promptType = Class.forName("org.springframework.ai.chat.prompt.Prompt");
            var prompt = promptType.getConstructor(List.class, Class.forName("org.springframework.ai.chat.prompt.ChatOptions"))
                    .newInstance(List.of(userMessage), options);
            var response = chatModelType.getMethod("call", promptType).invoke(chatModel, prompt);

            var generation = response.getClass().getMethod("getResult").invoke(response);
            var output = generation.getClass().getMethod("getOutput").invoke(generation);
            var toolCalls = (List<?>) output.getClass().getMethod("getToolCalls").invoke(output);
            assertThat(toolCalls).hasSize(1);
            assertThat(toolCalls.getFirst().getClass().getMethod("id").invoke(toolCalls.getFirst())).isEqualTo("call-fixture-7");
            assertThat(toolCalls.getFirst().getClass().getMethod("name").invoke(toolCalls.getFirst())).isEqualTo(TOOL_NAME);
            assertThat(toolCalls.getFirst().getClass().getMethod("arguments").invoke(toolCalls.getFirst())).asString().contains("C-1");
            var metadata = response.getClass().getMethod("getMetadata").invoke(response);
            var usage = metadata.getClass().getMethod("getUsage").invoke(metadata);
            assertThat(usage.getClass().getMethod("getPromptTokens").invoke(usage)).isEqualTo(11);
            assertThat(usage.getClass().getMethod("getCompletionTokens").invoke(usage)).isEqualTo(4);
            assertThat(callbackCount).hasValue(0);
            assertThat(requestBodies.getFirst()).contains("\"role\":\"user\"", "\"name\":\"" + TOOL_NAME + "\"", "\"customerId\"", "C-1");

            var gateway = new ModelConfiguration().modelGateway("live-model", "SUCCESS", "qwen-plus", true, "1", "USD",
                    "CAP_REACHED,UNKNOWN_COST,SAFETY_VIOLATION,HUMAN_STOP", context);
            var request = roundTripRequest();
            // 经 EAF Gateway 再走本地 Provider，确认结构化调用、关联结果与请求级 Schema 往返不丢失。
            var roundTrip = gateway.call(request);
            assertThat(roundTrip.toolCalls()).containsExactly(new ModelToolCall("call-fixture-7", TOOL_NAME, "{\"customerId\":\"C-1\"}"));
            var roundTripBody = requestBodies.getLast();
            assertThat(roundTripBody).contains("\"role\":\"assistant\"", "\"tool_calls\"", "call-request-7", "\"role\":\"tool\"",
                    "\"tool_call_id\":\"call-request-7\"", "\"name\":\"" + TOOL_NAME + "\"", "\"tools\"", TOOL_SCHEMA);

            current.set(new FixtureResponse(200, FINAL_RESPONSE, 0));
            var profile = new ModelProfileSnapshot(UUID.randomUUID(), "1.0.0", "a".repeat(64), 1, "bounded",
                    "LIVE", "dashscope", "qwen-plus", "spring-ai-alibaba", "1.1.2.2", "b".repeat(64),
                    0.0d, 8_000, 37, "JSON_OBJECT", false, "NONE", "dashscope", "qwen-plus", "CHAT", "CURRENT_AT_CALL");
            var selectedRequest = new ModelRequest(UUID.randomUUID(), List.of(new ModelMessage("user", "profile fixture")),
                    Instant.now().plusSeconds(5), 1_000, UUID.randomUUID(), "p32-profile-fixture", List.of(), 1, profile);
            gateway.call(selectedRequest);
            assertThat(requestBodies.getLast()).contains("\"max_tokens\":37", "\"model\":\"qwen-plus\"",
                    "\"response_format\":{\"type\":\"json_object\"}");

            current.set(new FixtureResponse(500, "{\"message\":\"fixture failure\"}", 0));
            var beforeFailure = requestCount.get();
            assertThatThrownBy(() -> gateway.call(request)).isInstanceOf(ModelFailure.class)
                    .satisfies(failure -> assertThat(((ModelFailure) failure).code()).isEqualTo("UPSTREAM_FAILURE"));
            assertThat(requestCount.get() - beforeFailure).isEqualTo(1);

            // 延迟最终文本响应只验证 HTTP 读超时，不让工具执行分支影响传输断言。
            current.set(new FixtureResponse(200, FINAL_RESPONSE, 1_000));
            var beforeTimeout = requestCount.get();
            var started = System.nanoTime();
            assertThatThrownBy(() -> gateway.call(request)).isInstanceOf(ModelFailure.class)
                    .satisfies(failure -> assertThat(((ModelFailure) failure).code()).isEqualTo("UPSTREAM_TIMEOUT"));
            var elapsed = Duration.ofNanos(System.nanoTime() - started);
            assertThat(requestCount.get() - beforeTimeout).isEqualTo(1);
            assertThat(elapsed).isLessThan(Duration.ofSeconds(2));
        } finally {
            server.stop(0);
        }
    }

    private static ConfigurableApplicationContext providerContext(int port) {
        return new SpringApplicationBuilder(ProviderFixtureApplication.class)
                .web(WebApplicationType.NONE)
                .logStartupInfo(false)
                .properties(Map.of(
                        "spring.main.banner-mode", "off",
                        // live-model 同时加载 OpenAI 兼容 starter；该值只满足本地 fixture 的 Bean 装配，不会访问外网。
                        "spring.ai.openai.api-key", "local-openai-fixture-only",
                        "spring.ai.dashscope.api-key", "local-fixture-only",
                        "spring.ai.dashscope.base-url", "http://127.0.0.1:" + port,
                        "spring.ai.dashscope.chat.options.model", "qwen-plus",
                        "spring.ai.retry.max-attempts", "3",
                        "spring.http.clients.connect-timeout", "200ms",
                        "spring.http.clients.read-timeout", "250ms"))
                // Application YAML 高于 SpringApplication 默认属性；命令行值确保 fixture 超时覆盖真实默认值。
                .run("--spring.http.clients.connect-timeout=200ms", "--spring.http.clients.read-timeout=250ms");
    }

    private static Object toolCallback(AtomicInteger calls) throws Exception {
        var callbackType = Class.forName("org.springframework.ai.tool.function.FunctionToolCallback");
        var builder = callbackType.getMethod("builder", String.class, Function.class)
                .invoke(null, TOOL_NAME, (Function<String, String>) input -> {
                    calls.incrementAndGet();
                    return "must not execute inside the provider SDK";
                });
        builder = builder.getClass().getMethod("description", String.class).invoke(builder, "Query synthetic customer data");
        builder = builder.getClass().getMethod("inputType", Type.class).invoke(builder, String.class);
        builder = builder.getClass().getMethod("inputSchema", String.class).invoke(builder, TOOL_SCHEMA);
        return builder.getClass().getMethod("build").invoke(builder);
    }

    private static Object toolOptions(Object callback) throws Exception {
        var optionsType = Class.forName("com.alibaba.cloud.ai.dashscope.chat.DashScopeChatOptions");
        var builder = optionsType.getMethod("builder").invoke(null);
        builder = builder.getClass().getMethod("model", String.class).invoke(builder, "qwen-plus");
        builder = builder.getClass().getMethod("toolCallbacks", List.class).invoke(builder, List.of(callback));
        builder = builder.getClass().getMethod("internalToolExecutionEnabled", Boolean.class).invoke(builder, false);
        return builder.getClass().getMethod("build").invoke(builder);
    }

    private static ModelRequest roundTripRequest() {
        var messages = List.of(new ModelMessage("user", "fixture request"),
                ModelMessage.assistant(List.of(new ModelToolCall("call-request-7", TOOL_NAME, "{\"customerId\":\"C-1\"}"))),
                ModelMessage.tool("call-request-7", TOOL_NAME, "{\"customerId\":\"C-1\",\"status\":\"active\"}"));
        return new ModelRequest(UUID.randomUUID(), messages, Instant.now().plusSeconds(5), 1_000,
                UUID.randomUUID(), "p7-provider-fixture", List.of(new ModelToolDefinition(TOOL_NAME, "1", "Query customer", TOOL_SCHEMA)), 2);
    }

    private static boolean present(String name) {
        try {
            Class.forName(name);
            return true;
        } catch (ClassNotFoundException absent) {
            return false;
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
    static class ProviderFixtureApplication {
        @Bean
        RestClient.Builder restClientBuilder(
                @Value("${spring.http.clients.connect-timeout:2s}") Duration connectTimeout,
                @Value("${spring.http.clients.read-timeout:20s}") Duration readTimeout) {
            // Provider fixture 使用与生产相同的 HTTP 全局超时属性，避免真实网络调用。
            var factory = new SimpleClientHttpRequestFactory();
            factory.setConnectTimeout(Math.toIntExact(connectTimeout.toMillis()));
            factory.setReadTimeout(Math.toIntExact(readTimeout.toMillis()));
            return RestClient.builder().requestFactory(factory);
        }
    }

    private record FixtureResponse(int status, String body, long delayMillis) { }
}
