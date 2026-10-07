package io.eaf.integration.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.eaf.connector.api.A2aIntegrationPort;
import io.eaf.connector.api.ConnectorDefinition;
import io.eaf.connector.api.RemoteA2aOutcome;
import io.eaf.connector.api.RemoteA2aResult;
import io.eaf.credential.api.CredentialRequest;
import io.eaf.credential.api.CredentialResolutionPort;
import io.eaf.shared.EafException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Semaphore;
import org.springframework.stereotype.Service;

/** 固定 reviewer 的 A2A 1.0 HTTP 适配；凭据只在本边界解析，重定向不携带秘密。 */
@Service
public class A2aHttpIntegration implements A2aIntegrationPort {
    // 固定单实例出站预算；超额请求快速失败，适配器不自动重试含副作用的 SendMessage。
    private static final int MAX_REQUEST_BYTES = 32_768;
    private static final int MAX_RESPONSE_BYTES = 32_768;
    private static final int MAX_CONCURRENT_REQUESTS = 16;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
            .followRedirects(HttpClient.Redirect.NEVER).build();
    private final ObjectMapper json;
    private final CredentialResolutionPort credentials;
    private final OutboundTargetPolicy targets;
    private final Semaphore outboundPermits = new Semaphore(MAX_CONCURRENT_REQUESTS);

    public A2aHttpIntegration(ObjectMapper json, CredentialResolutionPort credentials, OutboundTargetPolicy targets) {
        this.json = json;
        this.credentials = credentials;
        this.targets = targets;
    }

    @Override
    public RemoteA2aResult sendTask(ConnectorDefinition connector, String rpcId, String skillId,
                                    String messageId, String text, Instant deadline) {
        var profile = profile(connector);
        if (profile == null || !profile.skillId().equals(skillId) || messageId == null || messageId.isBlank()
                || messageId.length() > 128 || text == null || text.isBlank() || text.length() > 4_000)
            return rejected("A2A_REQUEST_INVALID");
        var body = json.createObjectNode();
        body.put("jsonrpc", "2.0");
        body.put("id", rpcId);
        body.put("method", "SendMessage");
        var params = body.putObject("params");
        var message = params.putObject("message");
        message.put("messageId", messageId);
        message.put("role", "ROLE_USER");
        message.putArray("parts").addObject().put("text", text);
        params.putObject("metadata").put("skillId", skillId);
        return post(connector, "a2a.send", rpcId, body, deadline, true);
    }

    @Override
    public RemoteA2aResult getTask(ConnectorDefinition connector, String rpcId, String taskId, Instant deadline) {
        if (taskId == null || taskId.isBlank() || taskId.length() > 200) return rejected("A2A_TASK_ID_INVALID");
        var body = json.createObjectNode();
        body.put("jsonrpc", "2.0");
        body.put("id", rpcId);
        body.put("method", "GetTask");
        body.putObject("params").put("id", taskId);
        return post(connector, "a2a.get", rpcId, body, deadline, false);
    }

    @Override
    public RemoteA2aResult cancelTask(ConnectorDefinition connector, String rpcId, String taskId, Instant deadline) {
        if (taskId == null || taskId.isBlank() || taskId.length() > 200) return rejected("A2A_TASK_ID_INVALID");
        var body = json.createObjectNode();
        body.put("jsonrpc", "2.0");
        body.put("id", rpcId);
        body.put("method", "CancelTask");
        body.putObject("params").put("id", taskId);
        return post(connector, "a2a.cancel", rpcId, body, deadline, false);
    }

    private RemoteA2aResult post(ConnectorDefinition connector, String use, String rpcId, ObjectNode body,
                                 Instant deadline, boolean send) {
        // 拒绝超限并发，避免远端阻塞耗尽 Runtime worker 或连接资源；适配器不自动重试。
        if (!outboundPermits.tryAcquire()) return unknown("A2A_OUTBOUND_CAPACITY");
        try {
            var profile = profile(connector);
            if (profile == null || !"ACTIVE".equals(connector.status()) || !connector.allowedUses().contains(use)
                    || !connector.permissions().equals(java.util.Set.of(profile.permission()))
                    || !profile.audience().equals(connector.audience())) return rejected("CONNECTOR_UNAVAILABLE");
            // 目标策略先于 Credential 解析，拒绝目标不会触碰凭据边界或网络。
            var endpoint = targets.a2aEndpoint(connector);
            var requestBody = json.writeValueAsBytes(body);
            if (requestBody.length > MAX_REQUEST_BYTES) return rejected("A2A_REQUEST_TOO_LARGE");
            var token = credentials.resolve(new CredentialRequest(connector.tenantId(), connector.workspaceId(),
                    connector.credentialRef(), profile.audience(), use, profile.permission())).valueForOutboundRequest();
            var remaining = Math.max(1, Duration.between(Instant.now(), deadline).toMillis());
            var request = HttpRequest.newBuilder(endpoint).timeout(Duration.ofMillis(Math.min(5_000, remaining)))
                    .header("Content-Type", "application/a2a+json")
                    .header("Accept", "application/a2a+json, application/json")
                    .header("A2A-Version", "1.0")
                    .header("Authorization", "Bearer " + token)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(requestBody)).build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            try (InputStream stream = response.body()) {
                var bytes = stream.readNBytes(MAX_RESPONSE_BYTES + 1);
                if (bytes.length > MAX_RESPONSE_BYTES) return unknown("A2A_RESPONSE_TOO_LARGE");
                if (response.statusCode() >= 500 || response.statusCode() == 408 || response.statusCode() == 429)
                    return unknown("A2A_UPSTREAM_UNAVAILABLE");
                if (response.statusCode() < 200 || response.statusCode() >= 300)
                    return rejected("A2A_HTTP_" + response.statusCode());
                // 仅解析 JSON 协议响应；HTML/代理错误页不能被当成 A2A Task。
                var contentType = response.headers().firstValue("Content-Type").orElse("").toLowerCase(java.util.Locale.ROOT);
                if (!contentType.startsWith("application/a2a+json") && !contentType.startsWith("application/json"))
                    return unknown("A2A_CONTENT_TYPE_INVALID");
                var root = json.readTree(bytes);
                if (!root.path("jsonrpc").asText().equals("2.0") || !root.path("id").asText().equals(rpcId))
                    return unknown("A2A_RESPONSE_MISMATCH");
                if (root.has("error")) return rejected("A2A_REMOTE_ERROR");
                JsonNode result = root.path("result");
                // SendMessage 的 1.0 Task 响应在 result.task；GetTask 直接在 result 返回 Task。
                JsonNode task = send ? result.path("task") : result;
                if (!task.isObject()) return unknown("A2A_TASK_RESPONSE_MISSING");
                var id = task.path("id").asText(null);
                var contextId = task.path("contextId").asText(null);
                var state = task.path("status").path("state").asText(null);
                return new RemoteA2aResult(RemoteA2aOutcome.TASK, id, contextId, state,
                        json.writeValueAsString(task), null);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return unknown("A2A_INTERRUPTED");
        } catch (java.net.http.HttpTimeoutException timeout) {
            return unknown("A2A_TIMEOUT");
        } catch (io.eaf.shared.EafException denied) {
            // Connector/凭据边界的显式拒绝没有发送请求，可以安全地作为失败事实返回。
            return rejected(denied.code());
        } catch (Exception failure) {
            // 响应丢失或格式损坏时，SendMessage 可能已被接收；调用方不得盲目重发。
            return unknown("A2A_RESPONSE_UNKNOWN");
        } finally {
            outboundPermits.release();
        }
    }

    private PeerProfile profile(ConnectorDefinition connector) {
        if (connector == null) return null;
        return switch (connector.provider()) {
            case "A2A_REVIEW_PEER" -> new PeerProfile("agent.risk.review", "eaf:a2a:peer", "agent:risk-review");
            case "A2A_EVALUATION_REVIEW_PEER" -> new PeerProfile("agent.risk.review.evaluation",
                    "eaf:evaluation-peer", "agent:risk-review-evaluation");
            default -> null;
        };
    }

    private static RemoteA2aResult rejected(String code) {
        return new RemoteA2aResult(RemoteA2aOutcome.REJECTED, null, null, null, null, code);
    }

    private static RemoteA2aResult unknown(String code) {
        return new RemoteA2aResult(RemoteA2aOutcome.UNKNOWN, null, null, null, null, code);
    }

    private record PeerProfile(String skillId, String audience, String permission) { }
}
// 本文件负责实现固定 A2A reviewer 的 JSON-RPC HTTP 适配。
