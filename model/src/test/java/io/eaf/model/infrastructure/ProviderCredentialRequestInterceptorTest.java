package io.eaf.model.infrastructure;

import io.eaf.credential.api.CredentialResolutionPort;
import io.eaf.credential.api.ResolvedCredential;
import java.net.URI;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.mock.http.client.MockClientHttpResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//  验证凭据/目标/边界； 用慢传输和超限响应测量许可竞争与失败释放。
class ProviderCredentialRequestInterceptorTest {
    @Test
    void replacesPlaceholderWithFreshProviderCredentialForEveryRequest() throws Exception {
        var secrets = new AtomicReference<>("provider-key-v1");
        var credentials = mock(CredentialResolutionPort.class);
        when(credentials.resolveSystem("model-deepseek", "deepseek", "model.chat", "provider:deepseek:invoke"))
                .thenAnswer(invocation -> new ResolvedCredential("model-deepseek", secrets.get().endsWith("v1") ? 1 : 2, secrets.get()));
        var interceptor = new ProviderCredentialRequestInterceptor(credentials);
        var observed = new ArrayList<String>();

        send(interceptor, observed);
        secrets.set("provider-key-v2");
        send(interceptor, observed);

        assertThat(observed).containsExactly("Bearer provider-key-v1", "Bearer provider-key-v2");
        verify(credentials, org.mockito.Mockito.times(2))
                .resolveSystem("model-deepseek", "deepseek", "model.chat", "provider:deepseek:invoke");
    }

    @Test
    void typesafeTargetResolvesItsDedicatedDecisionCredential() throws Exception {
        var credentials = mock(CredentialResolutionPort.class);
        when(credentials.resolveSystem("model-typesafe", "typesafe", "model.decision", "provider:typesafe:invoke"))
                .thenReturn(new ResolvedCredential("model-typesafe", 1, "synthetic-typesafe-key"));
        var interceptor = new ProviderCredentialRequestInterceptor(credentials);
        var request = new MockClientHttpRequest(HttpMethod.POST, URI.create("https://api.typesafe.ai/v1/systemone"));
        request.getHeaders().setBearerAuth("eaf-dynamic-credential-placeholder");
        var observed = new AtomicReference<String>();

        var response = interceptor.intercept(request, "synthetic state".getBytes(), (actual, body) -> {
            observed.set(actual.getHeaders().getFirst("Authorization"));
            return new MockClientHttpResponse(new byte[0], HttpStatus.OK);
        });
        response.close();

        assertThat(observed.get()).isEqualTo("Bearer synthetic-typesafe-key");
        verify(credentials).resolveSystem("model-typesafe", "typesafe", "model.decision", "provider:typesafe:invoke");
    }

    @Test
    void typesafeMissingCredentialStopsBeforeHttpExecution() {
        var credentials = mock(CredentialResolutionPort.class);
        when(credentials.resolveSystem("model-typesafe", "typesafe", "model.decision", "provider:typesafe:invoke"))
                .thenThrow(new IllegalStateException("secret-value-must-not-escape"));
        var interceptor = new ProviderCredentialRequestInterceptor(credentials);
        var request = new MockClientHttpRequest(HttpMethod.POST, URI.create("https://api.typesafe.ai/v1/systemone"));
        request.getHeaders().setBearerAuth("eaf-dynamic-credential-placeholder");

        assertThatThrownBy(() -> interceptor.intercept(request, new byte[0], (actual, body) -> {
            throw new AssertionError("TypeSafe 凭据缺失时不得执行 HTTP 请求。");
        })).isInstanceOf(java.io.IOException.class)
                .hasMessage("Provider 凭据不可用，未发送请求。")
                .hasMessageNotContaining("secret-value-must-not-escape");
        verify(credentials).resolveSystem("model-typesafe", "typesafe", "model.decision", "provider:typesafe:invoke");
    }

    @Test
    void unavailableCredentialStopsBeforeHttpExecutionAndDoesNotExposeSecret() {
        var credentials = mock(CredentialResolutionPort.class);
        when(credentials.resolveSystem("model-deepseek", "deepseek", "model.chat", "provider:deepseek:invoke"))
                .thenThrow(new IllegalStateException("secret-value-must-not-escape"));
        var interceptor = new ProviderCredentialRequestInterceptor(credentials);
        var request = request();
        request.getHeaders().setBearerAuth("eaf-dynamic-credential-placeholder");

        assertThatThrownBy(() -> interceptor.intercept(request, new byte[0], (actual, body) -> {
            throw new AssertionError("凭据解析失败时不得执行 HTTP 请求。");
        })).isInstanceOf(java.io.IOException.class)
                .hasMessage("Provider 凭据不可用，未发送请求。")
                .hasMessageNotContaining("secret-value-must-not-escape");
        verify(credentials, never()).resolveSystem("model-dashscope", "dashscope", "model.chat", "provider:dashscope:invoke");
    }

    @Test
    void providerPortAndPathMustMatchRegisteredHttpsTargetBeforeCredentialResolution() {
        var credentials = mock(CredentialResolutionPort.class);
        var interceptor = new ProviderCredentialRequestInterceptor(credentials);
        var request = new MockClientHttpRequest(HttpMethod.POST, URI.create("https://api.deepseek.com:8443/chat/completions"));
        request.getHeaders().setBearerAuth("eaf-dynamic-credential-placeholder");

        assertThatThrownBy(() -> interceptor.intercept(request, new byte[0], (actual, body) -> {
            throw new AssertionError("错误 Provider 目标不得执行 HTTP 请求。");
        })).isInstanceOf(java.io.IOException.class).hasMessage("Provider 出站目标未登记。");
        verify(credentials, never()).resolveSystem("model-deepseek", "deepseek", "model.chat", "provider:deepseek:invoke");
    }

    @Test
    void enterpriseProviderRequiresDeploymentEgressConfirmationBeforeCredentialResolution() {
        var credentials = mock(CredentialResolutionPort.class);
        var interceptor = new ProviderCredentialRequestInterceptor(credentials, Duration.ofSeconds(1),
                Duration.ofSeconds(2), "enterprise", false, 16, null);
        var request = request();
        request.getHeaders().setBearerAuth("eaf-dynamic-credential-placeholder");

        assertThatThrownBy(() -> interceptor.intercept(request, new byte[0], (actual, body) -> {
            throw new AssertionError("未确认 enterprise egress 策略时不得执行 HTTP 请求。");
        })).isInstanceOf(java.io.IOException.class)
                .hasMessage("Enterprise Provider egress policy is not confirmed.");
        verify(credentials, never()).resolveSystem("model-deepseek", "deepseek", "model.chat", "provider:deepseek:invoke");
    }

    @Test
    void unknownHostCannotReceiveProviderPlaceholderOrRequestBody() {
        var credentials = mock(CredentialResolutionPort.class);
        var interceptor = new ProviderCredentialRequestInterceptor(credentials);
        var request = new MockClientHttpRequest(HttpMethod.POST, URI.create("https://attacker.example/chat/completions"));
        request.getHeaders().set("X-DashScope-Api-Key", "eaf-dynamic-credential-placeholder");

        assertThatThrownBy(() -> interceptor.intercept(request, "synthetic prompt".getBytes(), (actual, body) -> {
            throw new AssertionError("未知 Provider 目标不得执行 HTTP 请求。");
        })).isInstanceOf(java.io.IOException.class).hasMessage("Provider 出站目标未登记。");
        verify(credentials, never()).resolveSystem("model-dashscope", "dashscope", "model.chat", "provider:dashscope:invoke");
    }

    @Test
    void oversizedProviderRequestStopsBeforeCredentialResolutionAndHttpExecution() {
        var credentials = mock(CredentialResolutionPort.class);
        var interceptor = new ProviderCredentialRequestInterceptor(credentials);

        assertThatThrownBy(() -> interceptor.intercept(request(), new byte[1_048_577], (actual, body) -> {
            throw new AssertionError("超限模型请求不得执行 HTTP 请求。");
        })).isInstanceOf(java.io.IOException.class).hasMessage("Provider request exceeds configured size limit.");
        verify(credentials, never()).resolveSystem("model-deepseek", "deepseek", "model.chat", "provider:deepseek:invoke");
    }

    @Test
    void oversizedProviderResponseClosesTheResponseAndFailsDuringStreamingRead() throws Exception {
        var credentials = mock(CredentialResolutionPort.class);
        when(credentials.resolveSystem("model-deepseek", "deepseek", "model.chat", "provider:deepseek:invoke"))
                .thenReturn(new ResolvedCredential("model-deepseek", 1, "synthetic-provider-key"));
        var interceptor = new ProviderCredentialRequestInterceptor(credentials);
        var oversized = new byte[1_048_577];
        var response = interceptor.intercept(request(), new byte[0],
                (actual, body) -> new MockClientHttpResponse(oversized, HttpStatus.OK));

        assertThatThrownBy(() -> response.getBody().readAllBytes()).isInstanceOf(java.io.IOException.class)
                .hasMessage("Provider response exceeds configured size limit.");
        response.close();
    }

    @Test
    void slowProviderFixtureSaturatesAndReleasesAllSixteenOutboundPermits() throws Exception {
        var credentials = mock(CredentialResolutionPort.class);
        when(credentials.resolveSystem("model-deepseek", "deepseek", "model.chat", "provider:deepseek:invoke"))
                .thenReturn(new ResolvedCredential("model-deepseek", 1, "synthetic-provider-key"));
        var interceptor = new ProviderCredentialRequestInterceptor(credentials);
        var enteredTransport = new CountDownLatch(16);
        var releaseTransport = new CountDownLatch(1);
        var fixtureCalls = new AtomicInteger();
        var heldResponses = new CopyOnWriteArrayList<org.springframework.http.client.ClientHttpResponse>();
        var saturationStarted = System.nanoTime();

        try (var workers = Executors.newFixedThreadPool(16)) {
            // 模拟慢 Provider：假传输在响应返回前挂起，期间每个请求都占用一份生产并发许可。
            var requests = java.util.stream.IntStream.range(0, 16).mapToObj(index -> CompletableFuture.runAsync(() -> {
                try {
                    var response = interceptor.intercept(request(), new byte[0], (actual, body) -> {
                        fixtureCalls.incrementAndGet();
                        enteredTransport.countDown();
                        try {
                            if (!releaseTransport.await(5, TimeUnit.SECONDS))
                                throw new java.io.IOException("slow Provider fixture timed out");
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new java.io.IOException("slow Provider fixture interrupted", interrupted);
                        }
                        return new MockClientHttpResponse(new byte[0], HttpStatus.OK);
                    });
                    heldResponses.add(response);
                } catch (Exception failure) {
                    throw new java.util.concurrent.CompletionException(failure);
                }
            }, workers)).toList();

            assertThat(enteredTransport.await(5, TimeUnit.SECONDS)).isTrue();
            var saturationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - saturationStarted);
            assertThatThrownBy(() -> interceptor.intercept(request(), new byte[0], (actual, body) -> {
                fixtureCalls.incrementAndGet();
                throw new AssertionError("满载时第 17 个请求不得进入 Provider 传输。");
            })).isInstanceOf(ProviderCapacityExceededException.class).hasMessage("Provider 出站并发已满。");
            assertThat(fixtureCalls).hasValue(16);
            verify(credentials, org.mockito.Mockito.times(16))
                    .resolveSystem("model-deepseek", "deepseek", "model.chat", "provider:deepseek:invoke");

            releaseTransport.countDown();
            CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new)).join();
            heldResponses.forEach(org.springframework.http.client.ClientHttpResponse::close);
            var retry = interceptor.intercept(request(), new byte[0],
                    (actual, body) -> new MockClientHttpResponse(new byte[0], HttpStatus.OK));
            retry.close();
            System.out.printf(java.util.Locale.ROOT,
                    "P7_LOAD_PROVIDER op=slow-response-fixture concurrent_limit=16 entered=16 rejected_while_full=1 retry_after_close=1 saturation_ms=%d fixture_requests=%d real_provider_calls=0 external_writes=0%n",
                    saturationMillis, fixtureCalls.get() + 1);
        } finally {
            releaseTransport.countDown();
            heldResponses.forEach(org.springframework.http.client.ClientHttpResponse::close);
        }
    }

    @Test
    void concurrentOversizedProviderResponsesAreStoppedAtTheOneMiBLimit() throws Exception {
        var credentials = mock(CredentialResolutionPort.class);
        when(credentials.resolveSystem("model-deepseek", "deepseek", "model.chat", "provider:deepseek:invoke"))
                .thenReturn(new ResolvedCredential("model-deepseek", 1, "synthetic-provider-key"));
        var interceptor = new ProviderCredentialRequestInterceptor(credentials);
        var oversized = new byte[1_048_577];
        var rejected = new AtomicInteger();
        var latencies = new CopyOnWriteArrayList<Long>();
        try (var workers = Executors.newFixedThreadPool(16)) {
            var requests = java.util.stream.IntStream.range(0, 16).mapToObj(index -> CompletableFuture.runAsync(() -> {
                try {
                    var started = System.nanoTime();
                    var response = interceptor.intercept(request(), new byte[0],
                            (actual, body) -> new MockClientHttpResponse(oversized, HttpStatus.OK));
                    assertThatThrownBy(() -> response.getBody().readAllBytes()).isInstanceOf(java.io.IOException.class)
                            .hasMessage("Provider response exceeds configured size limit.");
                    response.close();
                    latencies.add(System.nanoTime() - started);
                    rejected.incrementAndGet();
                } catch (Exception failure) {
                    throw new java.util.concurrent.CompletionException(failure);
                }
            }, workers)).toList();
            CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new)).join();
        }
        assertThat(rejected).hasValue(16);
        assertThat(latencies).hasSize(16);
        verify(credentials, org.mockito.Mockito.times(16))
                .resolveSystem("model-deepseek", "deepseek", "model.chat", "provider:deepseek:invoke");
        var retry = interceptor.intercept(request(), new byte[0],
                (actual, body) -> new MockClientHttpResponse(new byte[0], HttpStatus.OK));
        retry.close();
        var sorted = latencies.stream().sorted().toList();
        System.out.printf(java.util.Locale.ROOT,
                "P7_LOAD_PROVIDER op=oversized-response-fixture requested=16 payload_bytes=1048577 max_response_bytes=1048576 rejected=16 retry_after_rejection=1 p50_ms=%.2f p95_ms=%.2f real_provider_calls=0 external_writes=0%n",
                sorted.get(7) / 1_000_000.0, sorted.get(15) / 1_000_000.0);
    }

    @Test
    void customizedRestClientReturnsRedirectInsteadOfFollowingIt() throws Exception {
        var targetCalls = new AtomicInteger();
        var target = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        target.createContext("/capture", exchange -> {
            targetCalls.incrementAndGet();
            exchange.close();
        });
        target.start();
        var redirect = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        redirect.createContext("/source", exchange -> {
            exchange.getResponseHeaders().add("Location", "http://127.0.0.1:" + target.getAddress().getPort() + "/capture");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        redirect.start();
        try {
            var interceptor = new ProviderCredentialRequestInterceptor(mock(CredentialResolutionPort.class),
                    Duration.ofSeconds(1), Duration.ofSeconds(2));
            var builder = org.springframework.web.client.RestClient.builder();
            interceptor.customize(builder);
            var response = builder.build().get()
                    .uri("http://127.0.0.1:" + redirect.getAddress().getPort() + "/source")
                    .retrieve().toBodilessEntity();
            assertThat(response.getStatusCode().value()).isEqualTo(302);
            assertThat(targetCalls).hasValue(0);
        } finally {
            redirect.stop(0);
            target.stop(0);
        }
    }

    @Test
    void customizedRestClientRejectsUntrustedCertificateBeforeHttpHandlerRuns() throws Exception {
        var work = Files.createTempDirectory("eaf-p7-tls-fixture");
        var keyStorePath = work.resolve("fixture.p12");
        var keytool = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("win") ? "keytool.exe" : "keytool");
        var generated = new ProcessBuilder(keytool.toString(), "-genkeypair", "-alias", "fixture", "-keyalg", "RSA",
                "-keysize", "2048", "-validity", "1", "-storetype", "PKCS12", "-keystore", keyStorePath.toString(),
                "-storepass", "changeit", "-keypass", "changeit", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost")
                .redirectErrorStream(true).start();
        var keytoolOutput = new String(generated.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        assertThat(generated.waitFor()).as(keytoolOutput).isZero();

        var keys = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(keyStorePath)) { keys.load(input, "changeit".toCharArray()); }
        var keyManagers = javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(keys, "changeit".toCharArray());
        var serverTls = javax.net.ssl.SSLContext.getInstance("TLS");
        serverTls.init(keyManagers.getKeyManagers(), null, null);
        var handlerCalls = new AtomicInteger();
        var server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(serverTls));
        server.createContext("/source", exchange -> {
            handlerCalls.incrementAndGet();
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        try {
            var interceptor = new ProviderCredentialRequestInterceptor(mock(CredentialResolutionPort.class),
                    Duration.ofSeconds(1), Duration.ofSeconds(2));
            var builder = org.springframework.web.client.RestClient.builder();
            interceptor.customize(builder);
            assertThatThrownBy(() -> builder.build().get()
                    .uri("https://localhost:" + server.getAddress().getPort() + "/source")
                    .retrieve().toBodilessEntity()).isInstanceOf(org.springframework.web.client.RestClientException.class);
            assertThat(handlerCalls).hasValue(0);
        } finally {
            server.stop(0);
            Files.deleteIfExists(keyStorePath);
            Files.deleteIfExists(work);
        }
    }

    private void send(ProviderCredentialRequestInterceptor interceptor, java.util.List<String> observed) throws Exception {
        var request = request();
        request.getHeaders().setBearerAuth("eaf-dynamic-credential-placeholder");
        var response = interceptor.intercept(request, new byte[0], (actual, body) -> {
            observed.add(actual.getHeaders().getFirst("Authorization"));
            return new MockClientHttpResponse(new byte[0], HttpStatus.OK);
        });
        response.close();
    }

    private MockClientHttpRequest request() {
        return new MockClientHttpRequest(HttpMethod.POST, URI.create("https://api.deepseek.com/chat/completions"));
    }
}
