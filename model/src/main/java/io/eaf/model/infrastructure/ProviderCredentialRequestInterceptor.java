package io.eaf.model.infrastructure;

import io.eaf.credential.api.CredentialResolutionPort;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestClientCustomizer;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Component;

/** Spring AI 的固定 Provider 目标在真正发送 HTTP 时解析当前凭据版本，不沿用启动时密钥。 */
@Component
public final class ProviderCredentialRequestInterceptor implements ClientHttpRequestInterceptor, RestClientCustomizer, MeterBinder {
    private static final String PLACEHOLDER = "eaf-dynamic-credential-placeholder";
    // 限制模型 Provider 单响应体和并发；额度持续占用到响应被消费/关闭。
    private static final int MAX_REQUEST_BYTES = 1_048_576;
    private static final int MAX_RESPONSE_BYTES = 1_048_576;
    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 16;
    private final CredentialResolutionPort credentials;
    private final Duration connectTimeout;
    private final Duration readTimeout;
    private final boolean enterprise;
    private final boolean egressPolicyConfirmed;
    private final Semaphore outboundPermits;
    private final ProviderSharedQuota sharedQuota;
    private final AtomicInteger outboundActive = new AtomicInteger();
    private volatile Counter outboundRejected;

    public ProviderCredentialRequestInterceptor(CredentialResolutionPort credentials) {
        this(credentials, Duration.ofSeconds(2), Duration.ofSeconds(20), false, false, DEFAULT_MAX_CONCURRENT_REQUESTS, null);
    }

    public ProviderCredentialRequestInterceptor(CredentialResolutionPort credentials,
                                                Duration connectTimeout, Duration readTimeout) {
        this(credentials, connectTimeout, readTimeout, false, false, DEFAULT_MAX_CONCURRENT_REQUESTS, null);
    }

    @Autowired
    public ProviderCredentialRequestInterceptor(CredentialResolutionPort credentials,
                                                @Value("${spring.http.clients.connect-timeout:2s}") Duration connectTimeout,
                                                @Value("${spring.http.clients.read-timeout:20s}") Duration readTimeout,
                                                @Value("${eaf.security.mode:disabled}") String securityMode,
                                                @Value("${eaf.outbound.enterprise.egress-policy-confirmed:false}") boolean egressPolicyConfirmed,
                                                @Value("${eaf.model.max-concurrent-requests:16}") int maxConcurrentRequests,
                                                ProviderSharedQuota sharedQuota) {
        this(credentials, connectTimeout, readTimeout, "enterprise".equalsIgnoreCase(securityMode),
                egressPolicyConfirmed, maxConcurrentRequests, sharedQuota);
    }

    private ProviderCredentialRequestInterceptor(CredentialResolutionPort credentials, Duration connectTimeout,
                                                 Duration readTimeout, boolean enterprise,
                                                 boolean egressPolicyConfirmed, int maxConcurrentRequests,
                                                 ProviderSharedQuota sharedQuota) {
        if (maxConcurrentRequests <= 0) throw new IllegalArgumentException("eaf.model.max-concurrent-requests must be positive.");
        this.credentials = credentials;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.enterprise = enterprise;
        this.egressPolicyConfirmed = egressPolicyConfirmed;
        this.outboundPermits = new Semaphore(maxConcurrentRequests);
        this.sharedQuota = sharedQuota;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        registry.gauge("eaf.model.outbound.active", outboundActive);
        outboundRejected = Counter.builder("eaf.model.outbound.rejected").register(registry);
    }

    @Override
    public void customize(org.springframework.web.client.RestClient.Builder builder) {
        // Provider 使用禁止自动跟随重定向的 JDK 客户端；框架仅提供 HTTP 传输，EAF 校验目标和凭据。
        var httpClient = java.net.http.HttpClient.newBuilder().connectTimeout(connectTimeout)
                .followRedirects(java.net.http.HttpClient.Redirect.NEVER).build();
        var requestFactory = new org.springframework.http.client.JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(readTimeout);
        builder.requestFactory(requestFactory).requestInterceptor(this);
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
            throws java.io.IOException {
        var uri = request.getURI();
        var host = uri.getHost() == null ? "" : uri.getHost().toLowerCase(java.util.Locale.ROOT);
        String ref;
        String audience;
        String permission;
        String use;
        if ("api.deepseek.com".equals(host)) {
            if (!validProviderUri(uri, "deepseek")) throw new IOException("Provider 出站目标未登记。");
            ref = "model-deepseek";
            audience = "deepseek";
            permission = "provider:deepseek:invoke";
            use = "model.chat";
        } else if ("dashscope.aliyuncs.com".equals(host)) {
            if (!validProviderUri(uri, "dashscope")) throw new IOException("Provider 出站目标未登记。");
            ref = "model-dashscope";
            audience = "dashscope";
            permission = "provider:dashscope:invoke";
            use = uri.getPath().toLowerCase(java.util.Locale.ROOT).contains("embedding")
                    ? "model.embedding" : "model.chat";
        } else if ("api.typesafe.ai".equals(host)) {
            if (!validProviderUri(uri, "typesafe")) throw new IOException("Provider 出站目标未登记。");
            ref = "model-typesafe";
            audience = "typesafe";
            permission = "provider:typesafe:invoke";
            use = "model.decision";
        } else {
            // 保留无 EAF Provider 凭据的本地 fixture 与其它普通 HTTP 客户端；占位密钥不得流向任意主机。
            if (hasPlaceholder(request)) throw new IOException("Provider 出站目标未登记。");
            return execution.execute(request, body);
        }
        if (enterprise && !egressPolicyConfirmed)
            throw new IOException("Enterprise Provider egress policy is not confirmed.");
        if (body.length > MAX_REQUEST_BYTES) throw new IOException("Provider request exceeds configured size limit.");
        if (!outboundPermits.tryAcquire()) {
            var counter = outboundRejected;
            if (counter != null) counter.increment();
            throw new ProviderCapacityExceededException();
        }
        outboundActive.incrementAndGet();
        boolean responseOwnsPermit = false;
        ProviderSharedQuota.SharedLease sharedLease = null;
        ScheduledFuture<?> heartbeat = null;
        boolean transportStarted = false;
        try {
            io.eaf.credential.api.ResolvedCredential resolved;
            try {
                resolved = credentials.resolveSystem(ref, audience, use, permission);
            } catch (RuntimeException unavailable) {
                throw new ProviderCredentialUnavailableException("Provider 凭据不可用，未发送请求。");
            }
            if (resolved == null || resolved.valueForOutboundRequest() == null || resolved.valueForOutboundRequest().isBlank())
                throw new ProviderCredentialUnavailableException("Provider 凭据不可用，未发送请求。");
            // 清除框架可能由本地占位配置生成的 Authorization，再放入当前凭据版本。
            request.getHeaders().setBearerAuth(resolved.valueForOutboundRequest());
            // 共享槽在发送前取得；传输是否结束以响应 close 为准，外层超时不能提前归还配额。
            if (sharedQuota != null) sharedLease = sharedQuota.tryAcquire();
            var lost = new AtomicBoolean();
            var transportResponse = new AtomicReference<ClientHttpResponse>();
            var leaseForHeartbeat = sharedLease;
            if (sharedLease != null) heartbeat = sharedQuota.heartbeat(sharedLease, () -> {
                lost.set(true);
                var activeResponse = transportResponse.get();
                if (activeResponse != null) try { activeResponse.close(); } catch (RuntimeException ignored) { }
            });
            transportStarted = true;
            var response = execution.execute(request, body);
            var closeHeartbeat = heartbeat;
            var bounded = new BoundedClientHttpResponse(response, MAX_RESPONSE_BYTES, () -> {
                if (closeHeartbeat != null) closeHeartbeat.cancel(false);
                if (leaseForHeartbeat != null) sharedQuota.releaseAfterTransportClosed(leaseForHeartbeat);
                releasePermit();
            });
            responseOwnsPermit = true;
            transportResponse.set(bounded);
            if (lost.get()) {
                bounded.close();
                throw new IOException("Provider 共享并发租约失效，响应已关闭。");
            }
            return bounded;
        } finally {
            if (!responseOwnsPermit) {
                if (heartbeat != null) heartbeat.cancel(false);
                if (sharedLease != null && sharedQuota != null) {
                    if (transportStarted) sharedQuota.quarantine(sharedLease);
                    else sharedQuota.releaseAfterTransportClosed(sharedLease);
                }
                releasePermit();
            }
        }
    }

    private void releasePermit() {
        if (outboundActive.decrementAndGet() < 0) throw new IllegalStateException("Provider outbound permit released more than once.");
        outboundPermits.release();
    }

    private boolean validProviderUri(URI uri, String provider) {
        if (!"https".equals(uri.getScheme()) || uri.getPort() != -1 && uri.getPort() != 443
                || uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null) return false;
        var path = uri.getRawPath();
        if ("deepseek".equals(provider)) return "/chat/completions".equals(path);
        if ("typesafe".equals(provider)) return "/v1/systemone".equals(path);
        return "/compatible-mode/v1/chat/completions".equals(path)
                || "/compatible-mode/v1/embeddings".equals(path)
                || "/api/v1/services/aigc/text-generation/generation".equals(path)
                || "/api/v1/services/embeddings/text-embedding/text-embedding".equals(path);
    }

    private boolean hasPlaceholder(HttpRequest request) {
        return request.getHeaders().values().stream().flatMap(java.util.Collection::stream)
                .anyMatch(value -> PLACEHOLDER.equals(value) || ("Bearer " + PLACEHOLDER).equals(value));
    }

    /** Provider 响应超过 1 MiB 时立即关闭流和并发许可，不把无界内容交给模型解析器。 */
    private static final class BoundedClientHttpResponse implements ClientHttpResponse {
        private final ClientHttpResponse delegate;
        private final int maxBytes;
        private final Runnable releasePermit;
        private final AtomicBoolean closed = new AtomicBoolean();
        private InputStream body;

        private BoundedClientHttpResponse(ClientHttpResponse delegate, int maxBytes, Runnable releasePermit) {
            this.delegate = delegate;
            this.maxBytes = maxBytes;
            this.releasePermit = releasePermit;
        }

        @Override public org.springframework.http.HttpStatusCode getStatusCode() throws IOException { return delegate.getStatusCode(); }
        @Override public String getStatusText() throws IOException { return delegate.getStatusText(); }
        @Override public org.springframework.http.HttpHeaders getHeaders() { return delegate.getHeaders(); }

        @Override
        public synchronized InputStream getBody() throws IOException {
            if (body == null) body = new FilterInputStream(delegate.getBody()) {
                private int readBytes;

                @Override public int read() throws IOException {
                    if (readBytes == maxBytes) {
                        int extra = super.read();
                        if (extra < 0) return -1;
                        throw tooLarge();
                    }
                    int value = super.read();
                    if (value >= 0) readBytes++;
                    return value;
                }

                @Override public int read(byte[] bytes, int offset, int length) throws IOException {
                    if (length == 0) return 0;
                    if (readBytes == maxBytes) {
                        int extra = super.read();
                        if (extra < 0) return -1;
                        throw tooLarge();
                    }
                    int allowed = Math.min(length, maxBytes - readBytes);
                    int count = super.read(bytes, offset, allowed);
                    if (count > 0) readBytes += count;
                    return count;
                }

                @Override public long skip(long amount) throws IOException {
                    var buffer = new byte[(int) Math.min(8_192, Math.max(1, amount))];
                    long skipped = 0;
                    while (skipped < amount) {
                        int count = read(buffer, 0, (int) Math.min(buffer.length, amount - skipped));
                        if (count < 0) break;
                        skipped += count;
                    }
                    return skipped;
                }

                @Override public void close() throws IOException { BoundedClientHttpResponse.this.closeChecked(); }

                private IOException tooLarge() throws IOException {
                    BoundedClientHttpResponse.this.closeChecked();
                    return new IOException("Provider response exceeds configured size limit.");
                }
            };
            return body;
        }

        @Override
        public void close() {
            try { closeChecked(); }
            catch (IOException failure) { throw new java.io.UncheckedIOException(failure); }
        }

        private void closeChecked() throws IOException {
            if (closed.compareAndSet(false, true)) {
                try {
                    var stream = delegate.getBody();
                    if (stream != null) stream.close();
                    delegate.close();
                } catch (IOException failure) {
                    closed.set(false);
                    throw failure;
                } catch (RuntimeException failure) {
                    closed.set(false);
                    throw failure;
                }
                releasePermit.run();
            }
        }
    }
}
// 拦截器只接受 EAF 登记的固定 Provider 主机；未知主机不带 EAF 凭据出站。
