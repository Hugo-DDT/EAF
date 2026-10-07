package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.eaf.shared.Ids;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.ProtocolVersions;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 用固定本地 JWKS 和独立 HTTP 客户端核对 REST、MCP、A2A 的企业身份与实时授权一致性。 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "eaf.security.mode=enterprise",
        "eaf.security.enterprise.allow-loopback-http=true",
        "eaf.security.enterprise.audience=eaf:p7-protocol-test",
        "eaf.security.enterprise.token-type=at+jwt",
        "eaf.security.enterprise.jws-algorithm=RS256",
        "eaf.task.dispatcher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false",
        "eaf.execution.remote-poller-enabled=false"
})
class P7ProtocolEnterpriseAuthTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String AUDIENCE = "eaf:p7-protocol-test";
    private static final UUID WORKSPACE = Ids.WORKSPACE_A;
    private static final UUID CAPABILITY = UUID.fromString("54000000-0000-4000-8000-000000000001");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final KeyPair SIGNING_KEY = rsaKeyPair();
    private static final LocalJwkIssuer IDP = new LocalJwkIssuer(SIGNING_KEY);
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final List<String> BOB_ACTIONS = List.of("agent:read", "task:create", "task:read", "task:cancel",
            "audit:read", "capability:read", "skill:read", "prompt:read", "tool:read");

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("postgres"));

    @DynamicPropertySource
    static void configureDatabaseAndIssuer(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
        registry.add("eaf.security.enterprise.issuer", IDP::issuer);
        registry.add("eaf.security.enterprise.jwks-uri", IDP::jwksUri);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private ObjectMapper json;

    @BeforeEach
    void restoreMembersMappingsAndFixturePermissions() {
        IDP.setUnavailable(false);
        jdbc.update("update \"identity\".subject set status = 'ACTIVE' where id in (?, ?)", Ids.ALICE, Ids.BOB);
        for (var actorId : List.of(Ids.ALICE, Ids.BOB)) {
            jdbc.update("insert into organization.member(tenant_id, subject_id, status) values (?, ?, 'ACTIVE') "
                            + "on conflict (tenant_id, subject_id) do update set status = 'ACTIVE'",
                    Ids.TENANT_A, actorId);
        }
        for (var action : BOB_ACTIONS) {
            jdbc.update("insert into workspace.\"grant\"(tenant_id, workspace_id, actor_id, action, status) "
                            + "values (?, ?, ?, ?, 'ACTIVE') on conflict (workspace_id, actor_id, action) "
                            + "do update set tenant_id = excluded.tenant_id, status = 'ACTIVE'",
                    Ids.TENANT_A, WORKSPACE, Ids.BOB, action);
        }
        map("external-alice", Ids.ALICE);
        map("external-bob", Ids.BOB);
    }

    @AfterAll
    static void stopLocalIssuer() {
        IDP.close();
    }

    @Test
    void allProtocolsResolveMappedActorsIgnoreClaimsAndRefreshSessionAndResourceAuthorization() throws Exception {
        var forgedClaims = Map.of("tenant_id", Ids.TENANT_B.toString(), "actor_type", "AGENT",
                "roles", List.of("system:admin", "approval:decide"), "scope", "task:create");
        var aliceToken = issueToken("external-alice", forgedClaims);
        var bobToken = issueToken("external-bob", forgedClaims);

        // REST 入口以已验签 issuer+subject 映射为准，忽略 token 自报租户和角色。
        var restCreated = postRestTask(aliceToken, "p7-15-rest-alice");
        assertThat(restCreated.statusCode()).isEqualTo(202);
        var restTaskId = UUID.fromString(json.readTree(restCreated.body()).path("id").asText());
        assertTaskOwner(restTaskId, Ids.ALICE, "REST");

        var selectedToken = new AtomicReference<>(aliceToken);
        var transport = HttpClientStreamableHttpTransport.builder(baseUrl())
                .endpoint("/mcp")
                .supportedProtocolVersions(List.of(ProtocolVersions.MCP_2025_11_25))
                .httpRequestCustomizer((builder, method, uri, sessionId, context) -> builder
                        .header("Authorization", "Bearer " + selectedToken.get())
                        .header("Origin", "http://localhost:3000"))
                .build();
        try (McpSyncClient mcp = McpClient.sync(transport).build()) {
            assertThat(mcp.initialize().protocolVersion()).isEqualTo(ProtocolVersions.MCP_2025_11_25);
            var aliceMcpTask = structured(mcp.callTool(createMcpTask("p7-15-mcp-alice")));
            var aliceMcpTaskId = UUID.fromString((String) aliceMcpTask.get("id"));
            assertTaskOwner(aliceMcpTaskId, Ids.ALICE, "MCP");

            // 在同一 MCP session 换用另一有效 Bearer；每个 HTTP 请求必须重算身份，不能沿用 initialize 的 Alice。
            selectedToken.set(bobToken);
            var bobMcpTask = structured(mcp.callTool(createMcpTask("p7-15-mcp-bob")));
            var bobMcpTaskId = UUID.fromString((String) bobMcpTask.get("id"));
            assertTaskOwner(bobMcpTaskId, Ids.BOB, "MCP");

            var a2aCreated = postA2aTask(bobToken, "p7-15-a2a-bob");
            assertThat(a2aCreated.statusCode()).isEqualTo(200);
            var a2aTaskId = UUID.fromString(json.readTree(a2aCreated.body()).path("result").path("task").path("id").asText());
            assertTaskOwner(a2aTaskId, Ids.BOB, "A2A");

            // 撤销 Bob 的当前动作后，REST、MCP、A2A 都拒绝创建；各入口只映射为自己的协议错误格式。
            setGrant(Ids.BOB, "task:create", "REVOKED");
            var bobTasksBeforeDenial = taskCount(Ids.BOB);
            assertThat(postRestTask(bobToken, "p7-15-rest-denied").statusCode()).isEqualTo(403);
            var mcpDenied = mcp.callTool(createMcpTask("p7-15-mcp-denied"));
            assertThat(mcpDenied.isError()).isTrue();
            assertThat(mcpDenied.content().toString()).contains("POLICY_DENIED");
            var a2aDenied = postA2aTask(bobToken, "p7-15-a2a-denied");
            assertThat(json.readTree(a2aDenied.body()).path("error").path("code").asInt()).isEqualTo(-32000);
            assertThat(taskCount(Ids.BOB)).isEqualTo(bobTasksBeforeDenial);

            // 同一 MCP session 的下一次请求也必须立即看到外部映射停用，而不是保留旧主体快照。
            jdbc.update("update \"identity\".external_identity set status = 'DISABLED' where issuer = ? and subject = ?",
                    IDP.issuer(), "external-bob");
            assertThat(restGet("/api/v1/me", bobToken).statusCode()).isEqualTo(401);
            assertThatThrownBy(() -> mcp.callTool(createMcpTask("p7-15-mcp-after-revocation")))
                    .as("MCP 对新请求重新验证已停用的外部映射").isInstanceOf(RuntimeException.class);
            assertThat(postA2aList(bobToken).statusCode()).isEqualTo(401);
            assertThat(taskCount(Ids.BOB)).isEqualTo(bobTasksBeforeDenial);
        }
    }

    private CallToolRequest createMcpTask(String idempotencyKey) {
        return new CallToolRequest("eaf.tasks.create", Map.of("workspaceId", WORKSPACE.toString(),
                "capabilityId", CAPABILITY.toString(), "capabilityVersion", "1.0.0",
                "input", "验证协议认证一致性。", "idempotencyKey", idempotencyKey));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> structured(CallToolResult result) {
        assertThat(result.isError()).as("MCP 工具错误：%s", result.content()).isNotEqualTo(Boolean.TRUE);
        return (Map<String, Object>) result.structuredContent();
    }

    private HttpResponse<String> postRestTask(String token, String idempotencyKey) throws Exception {
        var body = JSON.writeValueAsString(Map.of("capabilityId", CAPABILITY.toString(),
                "capabilityVersion", "1.0.0", "input", "验证协议认证一致性。"));
        var request = HttpRequest.newBuilder(URI.create(baseUrl() + "/api/v1/workspaces/" + WORKSPACE + "/tasks"))
                .header("Authorization", "Bearer " + token).header("Idempotency-Key", idempotencyKey)
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postA2aTask(String token, String messageId) throws Exception {
        var requestBody = JSON.writeValueAsString(Map.of("jsonrpc", "2.0", "id", messageId,
                "method", "SendMessage", "params", Map.of("message", Map.of("messageId", messageId,
                        "role", "ROLE_USER", "parts", List.of(Map.of("text", "验证协议认证一致性。"))),
                        "metadata", Map.of("skillId", "customer-risk-followup@1.0.0"))));
        var request = HttpRequest.newBuilder(URI.create(baseUrl() + "/a2a/" + WORKSPACE))
                .header("Authorization", "Bearer " + token).header("A2A-Version", "1.0")
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(requestBody)).build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postA2aList(String token) throws Exception {
        var requestBody = JSON.writeValueAsString(Map.of("jsonrpc", "2.0", "id", "list-after-revocation",
                "method", "ListTasks", "params", Map.of()));
        var request = HttpRequest.newBuilder(URI.create(baseUrl() + "/a2a/" + WORKSPACE))
                .header("Authorization", "Bearer " + token).header("A2A-Version", "1.0")
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(requestBody)).build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> restGet(String path, String token) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(baseUrl() + path)).header("Authorization", "Bearer " + token);
        return HTTP.send(builder.GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private void assertTaskOwner(UUID taskId, UUID expectedActor, String protocol) {
        var row = jdbc.queryForMap("select actor_id, tenant_id, entry_protocol from task.task where id = ?", taskId);
        assertThat(row.get("actor_id")).isEqualTo(expectedActor);
        assertThat(row.get("tenant_id")).isEqualTo(Ids.TENANT_A);
        assertThat(row.get("entry_protocol")).isEqualTo(protocol);
    }

    private int taskCount(UUID actorId) {
        return jdbc.queryForObject("select count(*) from task.task where tenant_id = ? and workspace_id = ? and actor_id = ?",
                Integer.class, Ids.TENANT_A, WORKSPACE, actorId);
    }

    private void setGrant(UUID actorId, String action, String status) {
        jdbc.update("update workspace.\"grant\" set status = ? where tenant_id = ? and workspace_id = ? and actor_id = ? and action = ?",
                status, Ids.TENANT_A, WORKSPACE, actorId, action);
    }

    private void map(String externalSubject, UUID actorId) {
        jdbc.update("insert into \"identity\".external_identity(issuer, subject, actor_id, status) values (?, ?, ?, 'ACTIVE') "
                        + "on conflict (issuer, subject) do update set actor_id = excluded.actor_id, status = 'ACTIVE'",
                IDP.issuer(), externalSubject, actorId);
    }

    private String issueToken(String externalSubject, Map<String, ?> claims) throws Exception {
        var now = Instant.now();
        var header = JSON.createObjectNode().put("alg", "RS256").put("kid", "p7-protocol-key").put("typ", "at+jwt");
        var payload = JSON.createObjectNode().put("iss", IDP.issuer()).put("sub", externalSubject)
                .put("iat", now.getEpochSecond()).put("exp", now.plusSeconds(300).getEpochSecond())
                .put("nbf", now.minusSeconds(1).getEpochSecond());
        payload.putArray("aud").add(AUDIENCE);
        claims.forEach((name, value) -> {
            if (value instanceof String text) payload.put(name, text);
            else if (value instanceof List<?> values) {
                var array = payload.putArray(name);
                values.forEach(item -> array.add(item.toString()));
            }
        });
        var signingInput = encode(JSON.writeValueAsBytes(header)) + "." + encode(JSON.writeValueAsBytes(payload));
        var signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(SIGNING_KEY.getPrivate());
        signature.update(signingInput.getBytes(StandardCharsets.US_ASCII));
        return signingInput + "." + encode(signature.sign());
    }

    private String baseUrl() {
        return "http://127.0.0.1:" + port;
    }

    private static KeyPair rsaKeyPair() {
        try {
            var generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair();
        } catch (Exception failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    private static String encode(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static String jwks(KeyPair pair) {
        var key = (RSAPublicKey) pair.getPublic();
        return "{\"keys\":[{\"kty\":\"RSA\",\"use\":\"sig\",\"alg\":\"RS256\","
                + "\"kid\":\"p7-protocol-key\",\"n\":\"" + encode(unsignedBytes(key.getModulus().toByteArray()))
                + "\",\"e\":\"" + encode(unsignedBytes(key.getPublicExponent().toByteArray())) + "\"}]}";
    }

    private static byte[] unsignedBytes(byte[] value) {
        return value.length > 1 && value[0] == 0 ? java.util.Arrays.copyOfRange(value, 1, value.length) : value;
    }

    /** 本地只读 JWKS 服务固定一把测试公钥，所有测试请求都不连接真实企业 IdP。 */
    private static final class LocalJwkIssuer implements AutoCloseable {
        private final HttpServer server;
        private final AtomicReference<String> body = new AtomicReference<>();
        private volatile boolean unavailable;

        private LocalJwkIssuer(KeyPair keyPair) {
            try {
                body.set(jwks(keyPair));
                server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
                server.createContext("/jwks", exchange -> {
                    if (unavailable) {
                        exchange.sendResponseHeaders(503, -1);
                        exchange.close();
                        return;
                    }
                    var response = body.get().getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/jwk-set+json");
                    exchange.getResponseHeaders().add("Cache-Control", "public, max-age=300");
                    exchange.sendResponseHeaders(200, response.length);
                    try (var output = exchange.getResponseBody()) { output.write(response); }
                });
                server.start();
            } catch (Exception failure) {
                throw new ExceptionInInitializerError(failure);
            }
        }

        private String issuer() { return "http://127.0.0.1:" + server.getAddress().getPort() + "/issuer"; }
        private String jwksUri() { return "http://127.0.0.1:" + server.getAddress().getPort() + "/jwks"; }
        private void setUnavailable(boolean value) { unavailable = value; }
        @Override public void close() { server.stop(0); }
    }
}
