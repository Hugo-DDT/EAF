package io.eaf.model.infrastructure;

import com.sun.net.httpserver.HttpServer;
import io.eaf.model.api.ModelMessage;
import io.eaf.model.api.ModelRequest;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**：实际 Spring AI OpenAI 适配器通过 loopback 验证 DeepSeek 请求 options 隔离。 */
class P19ModelConcurrencyTest {
    private static final String STOP_CONDITIONS = "CAP_REACHED,UNKNOWN_COST,SAFETY_VIOLATION,HUMAN_STOP";
    private static final String RESPONSE = "{\"id\":\"chatcmpl-fixture\",\"object\":\"chat.completion\",\"created\":1,"
            + "\"model\":\"deepseek-flash\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"{}\"},\"finish_reason\":\"stop\"}],"
            + "\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}";

    @Test
    void concurrentDeepSeekRequestsKeepTheirOwnOptions() throws Exception {
        Assumptions.assumeTrue(present("org.springframework.ai.openai.OpenAiChatModel"), "Enable the live-model profile.");
        var bodies = new CopyOnWriteArrayList<String>();
        var requestsEntered = new CountDownLatch(2);
        var releaseResponses = new CountDownLatch(1);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        try (var httpWorkers = Executors.newVirtualThreadPerTaskExecutor()) {
            server.setExecutor(httpWorkers);
            server.createContext("/", exchange -> {
                bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                requestsEntered.countDown();
                try {
                    if (!releaseResponses.await(5, TimeUnit.SECONDS)) throw new java.io.IOException("fixture barrier timed out");
                    var bytes = RESPONSE.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, bytes.length);
                    exchange.getResponseBody().write(bytes);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } finally {
                    exchange.close();
                }
            });
            server.start();
            try (var context = providerContext(server.getAddress().getPort());
                 var callers = Executors.newFixedThreadPool(2)) {
                var gateway = new ModelConfiguration().modelGateway("live-model", "SUCCESS", "deepseek",
                        "deepseek-flash", "https://api.deepseek.com", "model-deepseek", true,
                        "synthetic-evaluation-only", "1.00", "USD", STOP_CONDITIONS, "local-fixture-owner",
                        "p19-loopback-authorization", "2099-01-01T00:00:00Z", context);
                var shortInput = "alpha";
                var longInput = "beta-request-with-more-input";
                var first = callers.submit(() -> gateway.call(request(shortInput, 105)));
                var second = callers.submit(() -> gateway.call(request(longInput, 205)));
                assertThat(requestsEntered.await(5, TimeUnit.SECONDS)).as("both HTTP calls overlap before either response").isTrue();
                assertThat(bodies).hasSize(2);
                assertThat(bodyFor(bodies, shortInput)).contains("\"max_tokens\":100");
                assertThat(bodyFor(bodies, longInput)).contains("\"max_tokens\":177");
                releaseResponses.countDown();
                assertThat(first.get(5, TimeUnit.SECONDS).publicOutput()).isEqualTo("{}");
                assertThat(second.get(5, TimeUnit.SECONDS).publicOutput()).isEqualTo("{}");
                assertThat(gateway.callCount()).isEqualTo(2);
            } finally {
                releaseResponses.countDown();
                server.stop(0);
            }
        }
    }

    private static String bodyFor(List<String> bodies, String content) {
        return bodies.stream().filter(body -> body.contains(content)).findFirst().orElseThrow();
    }

    private static ModelRequest request(String content, int tokenBudget) {
        return new ModelRequest(java.util.UUID.randomUUID(), List.of(new ModelMessage("user", content)),
                Instant.now().plusSeconds(10), tokenBudget, java.util.UUID.randomUUID(), "p19-loopback", List.of(), 2);
    }

    private static ConfigurableApplicationContext providerContext(int port) {
        return new SpringApplicationBuilder(ProviderFixtureApplication.class)
                .web(WebApplicationType.NONE)
                .logStartupInfo(false)
                .properties(Map.of(
                        "spring.main.banner-mode", "off",
                        "spring.ai.openai.api-key", "local-openai-fixture-only",
                        "spring.ai.openai.base-url", "http://127.0.0.1:" + port,
                        "spring.ai.openai.chat.options.model", "deepseek-flash",
                        "spring.ai.dashscope.api-key", "local-dashscope-fixture-only",
                        "spring.ai.retry.max-attempts", "1",
                        "spring.http.clients.connect-timeout", "200ms",
                        "spring.http.clients.read-timeout", "5s"))
                .run("--spring.http.clients.connect-timeout=200ms", "--spring.http.clients.read-timeout=5s");
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
    static class ProviderFixtureApplication { }
}
