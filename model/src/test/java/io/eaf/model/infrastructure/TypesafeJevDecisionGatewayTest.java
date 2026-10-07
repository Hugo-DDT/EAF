package io.eaf.model.infrastructure;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.eaf.model.api.ModelFailure;
import io.eaf.model.api.TypedDecisionRequest;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TypesafeJevDecisionGatewayTest {
    private static final String SUCCESS = """
            {"model":"jev-1.13.0","answers":{"risk_level":{"type":"choice","choice":"HIGH",
            "probabilities":{"LOW":0.03,"MEDIUM":0.12,"HIGH":0.80,"UNKNOWN":0.05},"confidence":0.72}},
            "usage":{"input_tokens":20,"output_tokens":3}}
            """;

    @Test
    void sendsChoiceRequestToLocalHttpFixtureAndMapsDistributionAndUsage() throws Exception {
        var observedBody = new AtomicReference<String>();
        var observedAuthorization = new AtomicReference<String>();
        var server = server(exchange -> {
            try { observedBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)); }
            catch (java.io.IOException failure) { throw new RuntimeException(failure); }
            observedAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            respond(exchange, 200, SUCCESS);
        });
        try {
            var gateway = gateway(server);
            var result = gateway.decide(request(1_000, Instant.now().plusSeconds(3)));

            assertThat(result.provider()).isEqualTo("typesafe");
            assertThat(result.model()).isEqualTo("jev-1.13.0");
            assertThat(result.choice()).isEqualTo("HIGH");
            assertThat(result.probabilities()).containsEntry("HIGH", new java.math.BigDecimal("0.8"));
            assertThat(result.inputTokens()).isEqualTo(20);
            assertThat(result.outputTokens()).isEqualTo(3);
            assertThat(result.usageStatus()).isEqualTo("KNOWN");
            assertThat(observedAuthorization.get()).isEqualTo("Bearer fixture-key");
            assertThat(observedBody.get()).contains("合成客户摘要", "jev-1.13.0", "\"type\":\"choice\"", "\"UNKNOWN\"");
            assertThat(gateway.billingProfile().callType()).isEqualTo("DECISION");
        } finally { server.stop(0); }
    }

    @Test
    void rejectsIncompleteChoiceOrUsageWithoutExposingResponseBody() throws Exception {
        var server = server(exchange -> respond(exchange, 200,
                "{\"model\":\"jev-1.13.0\",\"answers\":{\"risk_level\":{\"type\":\"choice\",\"choice\":\"HIGH\","
                        + "\"probabilities\":{\"LOW\":0,\"MEDIUM\":0,\"HIGH\":1,\"UNKNOWN\":0},\"confidence\":1}},"
                        + "\"private_detail\":\"must-not-escape\"}"));
        try {
            assertThatThrownBy(() -> gateway(server).decide(request(1_000, Instant.now().plusSeconds(3))))
                    .isInstanceOf(ModelFailure.class)
                    .satisfies(error -> {
                        var failure = (ModelFailure) error;
                        assertThat(failure.code()).isEqualTo("UPSTREAM_INVALID_RESPONSE");
                        assertThat(failure.called()).isTrue();
                        assertThat(failure.getMessage()).doesNotContain("must-not-escape");
                    });
        } finally { server.stop(0); }
    }

    @Test
    void redactsHttpErrorsAndStopsAtTaskDeadlineWithoutRetry() throws Exception {
        var server = server(exchange -> respond(exchange, 503, "{\"detail\":\"secret-provider-response\"}"));
        try {
            assertThatThrownBy(() -> gateway(server).decide(request(1_000, Instant.now().plusSeconds(3))))
                    .isInstanceOf(ModelFailure.class)
                    .satisfies(error -> {
                        var failure = (ModelFailure) error;
                        assertThat(failure.code()).isEqualTo("UPSTREAM_FAILURE");
                        assertThat(failure.called()).isTrue();
                        assertThat(failure.getMessage()).doesNotContain("secret-provider-response");
                    });
        } finally { server.stop(0); }

        var slow = server(exchange -> {
            try { Thread.sleep(250); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            respond(exchange, 200, SUCCESS);
        });
        try {
            assertThatThrownBy(() -> gateway(slow).decide(request(1_000, Instant.now().plusMillis(25))))
                    .isInstanceOf(ModelFailure.class)
                    .satisfies(error -> {
                        var failure = (ModelFailure) error;
                        assertThat(failure.code()).isEqualTo("UPSTREAM_TIMEOUT");
                        assertThat(failure.called()).isTrue();
                        assertThat(failure.getMessage()).doesNotContain("Bearer");
                    });
        } finally { slow.stop(0); }
    }

    @Test
    void deterministicFixtureReturnsUnknownForMissingOrConflictingEvidence() {
        var gateway = new DeterministicTypedDecisionGateway();
        var result = gateway.decide(request(0, Instant.now().plusSeconds(3), "客户资料缺失，记录冲突"));

        assertThat(result.choice()).isEqualTo("UNKNOWN");
        assertThat(result.probabilities()).containsEntry("UNKNOWN", java.math.BigDecimal.ONE);
        assertThat(gateway.callCount()).isEqualTo(1);
    }

    private TypesafeJevDecisionGateway gateway(HttpServer server) {
        var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1))
                .followRedirects(HttpClient.Redirect.NEVER).build();
        var factory = new JdkClientHttpRequestFactory(client);
        factory.setReadTimeout(Duration.ofSeconds(1));
        var rest = RestClient.builder().requestFactory(factory)
                .requestInterceptor((request, body, execution) -> {
                    request.getHeaders().setBearerAuth("fixture-key");
                    return execution.execute(request, body);
                }).build();
        return new TypesafeJevDecisionGateway(rest,
                "http://127.0.0.1:" + server.getAddress().getPort() + "/v1/systemone",
                "jev-1.13.0", Instant.now().plusSeconds(30));
    }

    private TypedDecisionRequest request(int tokenBudget, Instant deadline) {
        return request(tokenBudget, deadline, "合成客户摘要：续约已中断并出现重大投诉。");
    }

    private TypedDecisionRequest request(int tokenBudget, Instant deadline, String state) {
        return new TypedDecisionRequest(state, deadline, tokenBudget, UUID.randomUUID(), "test-trace", 1);
    }

    private HttpServer server(java.util.function.Consumer<HttpExchange> handler) throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/systemone", exchange -> handler.accept(exchange));
        server.start();
        return server;
    }

    private static void respond(HttpExchange exchange, int status, String body) {
        try {
            var bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        } catch (java.io.IOException ignored) {
            // 截止时间测试会主动中断本地 fixture 的客户端连接。
        } finally { exchange.close(); }
    }
}
