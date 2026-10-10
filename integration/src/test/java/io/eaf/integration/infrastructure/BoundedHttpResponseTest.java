package io.eaf.integration.infrastructure;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundedHttpResponseTest {
    @Test
    void readsNormalResponsesAndRetainsOnlyLimitPlusOneByte() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/normal", exchange -> respond(exchange, "hello"));
        server.createContext("/large", exchange -> respond(exchange, "0123456789x"));
        server.start();
        try {
            var client = HttpClient.newHttpClient();
            var normal = BoundedHttpResponse.send(client, request(server, "/normal"), 10,
                    Instant.now().plusSeconds(2), Duration.ofSeconds(2));
            var oversized = BoundedHttpResponse.send(client, request(server, "/large"), 10,
                    Instant.now().plusSeconds(2), Duration.ofSeconds(2));

            assertThat(normal.statusCode()).isEqualTo(200);
            assertThat(normal.bodyText()).isEqualTo("hello");
            assertThat(normal.oversized()).isFalse();
            assertThat(oversized.oversized()).isTrue();
            assertThat(oversized.body()).hasSize(11);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void deadlineCoversBodyAfterHeadersHaveArrived() throws Exception {
        var headersSent = new CountDownLatch(1);
        var executor = Executors.newCachedThreadPool();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/slow", exchange -> {
            try {
                exchange.sendResponseHeaders(200, 0);
                exchange.getResponseBody().write('x');
                exchange.getResponseBody().flush();
                headersSent.countDown();
                Thread.sleep(1_800);
                exchange.getResponseBody().write('y');
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (java.io.IOException clientClosed) {
                // 期限到达后读取器必须取消尚未完成的响应体订阅。
            } finally {
                exchange.close();
            }
        });
        server.start();
        try {
            var started = System.nanoTime();
            assertThatThrownBy(() -> BoundedHttpResponse.send(HttpClient.newHttpClient(), request(server, "/slow"),
                    1_024, Instant.now().plusMillis(500), Duration.ofSeconds(5)))
                    .isInstanceOf(HttpTimeoutException.class);
            assertThat(headersSent.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(1_500));
        } finally {
            server.stop(0);
            executor.shutdownNow();
        }
    }

    private static HttpRequest request(HttpServer server, String path) {
        return HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path))
                .timeout(Duration.ofSeconds(3)).GET().build();
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, String body) {
        try {
            var bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        } catch (java.io.IOException ignored) {
        } finally {
            exchange.close();
        }
    }
}
