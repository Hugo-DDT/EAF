package io.eaf.integration.infrastructure;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

// 响应体只保留上限加一个判定字节；同一绝对截止时间覆盖响应头和完整正文。
record BoundedHttpResponse(int statusCode, java.net.http.HttpHeaders headers, byte[] body, int maxBytes) {
    static BoundedHttpResponse send(HttpClient client, HttpRequest request, int maxBytes, Instant deadline,
                                    Duration adapterTimeout) throws IOException, InterruptedException {
        var now = Instant.now();
        var remaining = Math.max(0, Duration.between(now, deadline).toNanos());
        var requestTimeout = request.timeout().map(Duration::toNanos).orElse(Long.MAX_VALUE);
        var timeoutNanos = Math.min(Math.min(remaining, adapterTimeout.toNanos()), requestTimeout);
        if (maxBytes < 0 || timeoutNanos <= 0) throw new HttpTimeoutException("HTTP response deadline elapsed");

        var cancelled = new AtomicBoolean();
        var body = new AtomicReference<LimitedBodySubscriber>();
        var requestFuture = client.sendAsync(request, ignored -> {
            var subscriber = new LimitedBodySubscriber(maxBytes);
            body.set(subscriber);
            if (cancelled.get()) subscriber.cancel();
            return subscriber;
        });
        var endNanos = System.nanoTime() + timeoutNanos;
        try {
            var waitNanos = endNanos - System.nanoTime();
            if (waitNanos <= 0) throw new TimeoutException();
            var response = requestFuture.get(waitNanos, TimeUnit.NANOSECONDS);
            return new BoundedHttpResponse(response.statusCode(), response.headers(), response.body(), maxBytes);
        } catch (TimeoutException timeout) {
            cancel(requestFuture, body, cancelled);
            throw new HttpTimeoutException("HTTP response deadline elapsed");
        } catch (InterruptedException interrupted) {
            cancel(requestFuture, body, cancelled);
            throw interrupted;
        } catch (ExecutionException failed) {
            var cause = failed.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof RuntimeException runtime) throw runtime;
            if (cause instanceof Error error) throw error;
            throw new IOException("HTTP response failed", cause);
        }
    }

    boolean oversized() { return body.length > maxBytes; }

    String bodyText() {
        return new String(body, 0, Math.min(body.length, maxBytes), StandardCharsets.UTF_8);
    }

    private static void cancel(CompletableFuture<?> request, AtomicReference<LimitedBodySubscriber> body,
                               AtomicBoolean cancelled) {
        cancelled.set(true);
        request.cancel(true);
        var subscriber = body.get();
        if (subscriber != null) subscriber.cancel();
    }

    private static final class LimitedBodySubscriber implements HttpResponse.BodySubscriber<byte[]> {
        private final int maxBytes;
        private final ByteArrayOutputStream received;
        private final CompletableFuture<byte[]> body = new CompletableFuture<>();
        private volatile Flow.Subscription subscription;
        private volatile boolean cancelled;

        private LimitedBodySubscriber(int maxBytes) {
            this.maxBytes = maxBytes;
            this.received = new ByteArrayOutputStream(Math.min(maxBytes + 1, 16_384));
        }

        @Override public CompletableFuture<byte[]> getBody() { return body; }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            if (cancelled) subscription.cancel(); else subscription.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            if (cancelled || body.isDone()) return;
            for (var buffer : buffers) {
                while (buffer.hasRemaining() && received.size() <= maxBytes) received.write(buffer.get());
                if (received.size() > maxBytes) {
                    cancelled = true;
                    subscription.cancel();
                    body.complete(received.toByteArray());
                    return;
                }
            }
            subscription.request(1);
        }

        @Override public void onError(Throwable failure) { body.completeExceptionally(failure); }
        @Override public void onComplete() { body.complete(received.toByteArray()); }

        private void cancel() {
            cancelled = true;
            var current = subscription;
            if (current != null) current.cancel();
            body.completeExceptionally(new CancellationException());
        }
    }
}
