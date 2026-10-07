package io.eaf.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import io.eaf.identity.api.IdentityService;
import io.eaf.shared.Ids;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 使用本地 RSA/JWKS fixture 验证企业 Bearer 身份边界，不访问真实 IdP。 */
@Testcontainers
@SpringBootTest(properties = {
        "eaf.security.mode=enterprise",
        "eaf.security.enterprise.allow-loopback-http=true",
        "eaf.security.enterprise.audience=eaf:test-resource",
        "eaf.security.enterprise.token-type=at+jwt",
        "eaf.security.enterprise.jws-algorithm=RS256",
        "eaf.task.dispatcher-enabled=false",
        "eaf.workflow.dispatcher-enabled=false",
        "eaf.execution.outbox-publisher-enabled=false",
        "eaf.execution.remote-poller-enabled=false"
})
@AutoConfigureMockMvc
class P7EnterpriseIdentityTest {
    private static final String IMAGE = "pgvector/pgvector:pg17@sha256:dca0d688bbb31d3f851502ffcb9c7791387b4fcc544ae434dab41761e5ece317";
    private static final String EXTERNAL_AUDIENCE = "eaf:test-resource";
    private static final String TOKEN_TYPE = "at+jwt";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final KeyPair KEY_A = rsaKeyPair();
    private static final KeyPair KEY_B = rsaKeyPair();
    private static final LocalJwkIssuer IDP = new LocalJwkIssuer(KEY_A, "key-a");
    private static final java.util.UUID SERVICE_ID = java.util.UUID.fromString("88000000-0000-4000-8000-000000000001");

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse(IMAGE).asCompatibleSubstituteFor("postgres"));

    @Autowired private org.springframework.test.web.servlet.MockMvc mvc;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private IdentityService identities;
    @Autowired private RestTemplateBuilder restTemplateBuilder;

    @DynamicPropertySource
    static void databaseAndIssuerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/test-migration,classpath:db/migration");
        registry.add("eaf.security.enterprise.issuer", IDP::issuer);
        registry.add("eaf.security.enterprise.jwks-uri", IDP::jwksUri);
    }

    @BeforeEach
    void restoreFixtureMappingsAndKeySet() {
        IDP.publish(KEY_A, "key-a");
        IDP.setUnavailable(false);
        jdbc.update("update \"identity\".subject set status = 'ACTIVE' where id in (?, ?, ?, ?)",
                Ids.ALICE, Ids.BOB, Ids.AGENT_RISK, SERVICE_ID);
        jdbc.update("insert into \"identity\".subject(id, type, display_name, status) values (?, 'SERVICE', ' test service', 'ACTIVE') "
                        + "on conflict (id) do update set type = 'SERVICE', status = 'ACTIVE'", SERVICE_ID);
        jdbc.update("insert into organization.member(tenant_id, subject_id, status) values (?, ?, 'ACTIVE') "
                        + "on conflict (tenant_id, subject_id) do update set status = 'ACTIVE'", Ids.TENANT_A, Ids.BOB);
        jdbc.update("insert into organization.member(tenant_id, subject_id, status) values (?, ?, 'ACTIVE') "
                        + "on conflict (tenant_id, subject_id) do update set status = 'ACTIVE'", Ids.TENANT_A, Ids.AGENT_RISK);
        jdbc.update("insert into organization.member(tenant_id, subject_id, status) values (?, ?, 'ACTIVE') "
                        + "on conflict (tenant_id, subject_id) do update set status = 'ACTIVE'", Ids.TENANT_A, SERVICE_ID);
        map("external-alice", Ids.ALICE);
        map("external-bob", Ids.BOB);
        map("external-risk-agent", Ids.AGENT_RISK);
        map("external-service", SERVICE_ID);
    }

    @AfterAll
    static void stopLocalIssuer() {
        IDP.close();
    }

    @Test
    void signedIssuerSubjectMapsToServerIdentityAndIgnoresTenantAndRoleClaims() throws Exception {
        var token = token("external-bob", KEY_A, "key-a", IDP.issuer(), EXTERNAL_AUDIENCE,
                TOKEN_TYPE, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().minusSeconds(1),
                Map.of("tenant_id", Ids.TENANT_B.toString(), "actor_type", "AGENT", "roles", new String[]{"system:admin"},
                        "scope", "approval:decide"));

        mvc.perform(get("/api/v1/me").header("Authorization", "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.actorId").value(Ids.BOB.toString()))
                .andExpect(jsonPath("$.tenantId").value(Ids.TENANT_A.toString()))
                .andExpect(jsonPath("$.type").value("HUMAN"))
                .andExpect(jsonPath("$.actions").value(org.hamcrest.Matchers.not(org.hamcrest.Matchers.hasItem("system:admin"))));
        mvc.perform(get("/api/v1/me").header("Authorization", "bEaReR " + token)).andExpect(status().isOk());

        assertUnauthorized(token("unmapped-principal", KEY_A, "key-a", IDP.issuer(), EXTERNAL_AUDIENCE,
                TOKEN_TYPE, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().minusSeconds(1), Map.of()));
        // 企业模式不得回退到 local alias，也不得在多个 Authorization 头之间任选身份。
        assertUnauthorized("eaf-local-alice");
        mvc.perform(get("/api/v1/me").header("Authorization", "Bearer " + token, "Bearer " + token))
                .andExpect(status().isUnauthorized());
        assertThat(identities.resolveExternalPrincipal(IDP.issuer(), "external-bob").orElseThrow().actorId()).isEqualTo(Ids.BOB);
    }

    @Test
    void invalidIssuerAudienceTimeWindowTokenTypeAndSignatureAreRejected() throws Exception {
        assertUnauthorized(token("external-alice", KEY_A, "key-a", "https://attacker.example.test", EXTERNAL_AUDIENCE,
                TOKEN_TYPE, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().minusSeconds(1), Map.of()));
        assertUnauthorized(token("external-alice", KEY_A, "key-a", IDP.issuer(), "another-resource",
                TOKEN_TYPE, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().minusSeconds(1), Map.of()));
        assertUnauthorized(token("external-alice", KEY_A, "key-a", IDP.issuer(), EXTERNAL_AUDIENCE,
                "JWT", Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().minusSeconds(1), Map.of()));
        assertUnauthorized(token("external-alice", KEY_A, "key-a", IDP.issuer(), EXTERNAL_AUDIENCE,
                TOKEN_TYPE, Instant.now().minusSeconds(600), Instant.now().minusSeconds(300), Instant.now().minusSeconds(600), Map.of()));
        assertUnauthorized(token("external-alice", KEY_A, "key-a", IDP.issuer(), EXTERNAL_AUDIENCE,
                TOKEN_TYPE, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().plusSeconds(300), Map.of()));
        assertUnauthorized(tokenMissingTimeClaim("external-alice", "exp"));
        assertUnauthorized(tokenMissingTimeClaim("external-alice", "nbf"));

        var valid = token("external-alice", KEY_A, "key-a", IDP.issuer(), EXTERNAL_AUDIENCE,
                TOKEN_TYPE, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().minusSeconds(1), Map.of());
        var altered = valid.substring(0, valid.length() - 1) + (valid.endsWith("A") ? "B" : "A");
        assertUnauthorized(altered);
        assertUnauthorized(hmacToken("external-alice"));
        assertUnauthorized(unsignedToken("external-alice"));
    }

    @Test
    void disabledExternalMappingAndInactiveMembershipAreRejectedImmediately() throws Exception {
        var token = token("external-alice", KEY_A, "key-a", IDP.issuer(), EXTERNAL_AUDIENCE,
                TOKEN_TYPE, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().minusSeconds(1), Map.of());
        jdbc.update("update \"identity\".external_identity set status = 'DISABLED' where issuer = ? and subject = ?",
                IDP.issuer(), "external-alice");
        assertUnauthorized(token);

        jdbc.update("update \"identity\".external_identity set status = 'ACTIVE' where issuer = ? and subject = ?",
                IDP.issuer(), "external-alice");
        jdbc.update("update organization.member set status = 'DISABLED' where tenant_id = ? and subject_id = ?",
                Ids.TENANT_A, Ids.ALICE);
        assertUnauthorized(token);
    }

    @Test
    void agentAndServiceTokensCannotSelfAssignHumanTypeOrApproveBusinessWrites() throws Exception {
        var hostileClaims = Map.<String, Object>of("tenant_id", Ids.TENANT_B.toString(), "actor_type", "HUMAN",
                "roles", new String[]{"system:admin", "approval:decide"}, "scope", "approval:decide");
        var agentToken = token("external-risk-agent", KEY_A, "key-a", IDP.issuer(), EXTERNAL_AUDIENCE,
                TOKEN_TYPE, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().minusSeconds(1), hostileClaims);
        mvc.perform(get("/api/v1/me").header("Authorization", "Bearer " + agentToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.actorId").value(Ids.AGENT_RISK.toString()))
                .andExpect(jsonPath("$.tenantId").value(Ids.TENANT_A.toString()))
                .andExpect(jsonPath("$.type").value("AGENT"))
                .andExpect(jsonPath("$.actions").value(org.hamcrest.Matchers.not(org.hamcrest.Matchers.hasItem("approval:decide"))));
        assertApprovalDenied(agentToken);

        var serviceToken = token("external-service", KEY_A, "key-a", IDP.issuer(), EXTERNAL_AUDIENCE,
                TOKEN_TYPE, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().minusSeconds(1), hostileClaims);
        mvc.perform(get("/api/v1/me").header("Authorization", "Bearer " + serviceToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.actorId").value(SERVICE_ID.toString()))
                .andExpect(jsonPath("$.type").value("SERVICE"));
        assertApprovalDenied(serviceToken);
    }

    @Test
    void unknownKeyFetchRefreshesTheJwksSetAndUnavailableKeysFailClosed() throws Exception {
        var oldToken = token("external-alice", KEY_A, "key-a", IDP.issuer(), EXTERNAL_AUDIENCE,
                TOKEN_TYPE, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().minusSeconds(1), Map.of());
        mvc.perform(get("/api/v1/me").header("Authorization", "Bearer " + oldToken)).andExpect(status().isOk());

        IDP.publish(KEY_B, "key-b");
        var rotatedToken = token("external-alice", KEY_B, "key-b", IDP.issuer(), EXTERNAL_AUDIENCE,
                TOKEN_TYPE, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().minusSeconds(1), Map.of());
        mvc.perform(get("/api/v1/me").header("Authorization", "Bearer " + rotatedToken)).andExpect(status().isOk());

        IDP.setUnavailable(true);
        var missingKeyToken = token("external-alice", KEY_A, "missing-key", IDP.issuer(), EXTERNAL_AUDIENCE,
                TOKEN_TYPE, Instant.now().minusSeconds(1), Instant.now().plusSeconds(300), Instant.now().minusSeconds(1), Map.of());
        assertUnauthorized(missingKeyToken);
    }

    @Test
    void enterpriseConfigurationRejectsRemoteHttpJwksAndNonAllowlistedSignatureAlgorithms() {
        assertThatThrownBy(() -> new io.eaf.identity.infrastructure.EnterpriseBearerVerifier(identities,
                restTemplateBuilder, "https://issuer.example.test", "http://idp.example.test/jwks",
                EXTERNAL_AUDIENCE, TOKEN_TYPE, "RS256", false)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new io.eaf.identity.infrastructure.EnterpriseBearerVerifier(identities,
                restTemplateBuilder, "https://issuer.example.test", "https://issuer.example.test/jwks",
                EXTERNAL_AUDIENCE, TOKEN_TYPE, "HS256", false)).isInstanceOf(IllegalArgumentException.class);
    }

    private void map(String externalSubject, java.util.UUID actorId) {
        jdbc.update("insert into \"identity\".external_identity(issuer, subject, actor_id, status) values (?, ?, ?, 'ACTIVE') "
                        + "on conflict (issuer, subject) do update set actor_id = excluded.actor_id, status = 'ACTIVE'",
                IDP.issuer(), externalSubject, actorId);
    }

    private void assertUnauthorized(String token) throws Exception {
        mvc.perform(get("/api/v1/me").header("Authorization", "Bearer " + token))
                .andExpect(status().isUnauthorized())
                .andExpect(result -> assertThat(result.getResponse().getContentAsString()).doesNotContain(token));
    }

    private void assertApprovalDenied(String token) throws Exception {
        // 身份验证成功后仍要由 Approval 域核对主体类型，AGENT/SERVICE 永远不能批准写入。
        mvc.perform(post("/api/v1/workspaces/{workspaceId}/approvals/{approvalId}/decisions",
                        Ids.WORKSPACE_A, java.util.UUID.randomUUID())
                        .header("Authorization", "Bearer " + token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"decision\":\"APPROVED\",\"expectedVersion\":0}"))
                .andExpect(status().isForbidden());
    }

    private static String token(String subject, KeyPair keyPair, String keyId, String issuer, String audience,
                                String tokenType, Instant issuedAt, Instant expiresAt, Instant notBefore,
                                Map<String, ?> extraClaims) throws Exception {
        var header = JSON.createObjectNode().put("alg", "RS256").put("kid", keyId);
        if (tokenType != null) header.put("typ", tokenType);
        var payload = JSON.createObjectNode().put("iss", issuer).put("sub", subject)
                .put("exp", expiresAt.getEpochSecond()).put("iat", issuedAt.getEpochSecond())
                .put("nbf", notBefore.getEpochSecond());
        payload.putArray("aud").add(audience);
        extraClaims.forEach((name, value) -> putClaim(payload, name, value));
        var signingInput = encode(JSON.writeValueAsBytes(header)) + "." + encode(JSON.writeValueAsBytes(payload));
        var signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(keyPair.getPrivate());
        signature.update(signingInput.getBytes(StandardCharsets.US_ASCII));
        return signingInput + "." + encode(signature.sign());
    }

    private static String tokenMissingTimeClaim(String subject, String omittedClaim) throws Exception {
        var header = JSON.createObjectNode().put("alg", "RS256").put("kid", "key-a").put("typ", TOKEN_TYPE);
        var now = Instant.now();
        var payload = JSON.createObjectNode().put("iss", IDP.issuer()).put("sub", subject)
                .put("iat", now.getEpochSecond());
        payload.putArray("aud").add(EXTERNAL_AUDIENCE);
        if (!"exp".equals(omittedClaim)) payload.put("exp", now.plusSeconds(300).getEpochSecond());
        if (!"nbf".equals(omittedClaim)) payload.put("nbf", now.minusSeconds(1).getEpochSecond());
        var signingInput = encode(JSON.writeValueAsBytes(header)) + "." + encode(JSON.writeValueAsBytes(payload));
        var signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(KEY_A.getPrivate());
        signature.update(signingInput.getBytes(StandardCharsets.US_ASCII));
        return signingInput + "." + encode(signature.sign());
    }

    private static String hmacToken(String subject) throws Exception {
        var header = JSON.createObjectNode().put("alg", "HS256").put("kid", "key-a").put("typ", TOKEN_TYPE);
        var now = Instant.now();
        var payload = JSON.createObjectNode().put("iss", IDP.issuer()).put("sub", subject)
                .put("exp", now.plusSeconds(300).getEpochSecond()).put("iat", now.getEpochSecond())
                .put("nbf", now.minusSeconds(1).getEpochSecond()).put("aud", EXTERNAL_AUDIENCE);
        var signingInput = encode(JSON.writeValueAsBytes(header)) + "." + encode(JSON.writeValueAsBytes(payload));
        var mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec("not-an-idp-secret".getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return signingInput + "." + encode(mac.doFinal(signingInput.getBytes(StandardCharsets.US_ASCII)));
    }

    private static String unsignedToken(String subject) throws Exception {
        var header = JSON.createObjectNode().put("alg", "none").put("typ", TOKEN_TYPE);
        var now = Instant.now();
        var payload = JSON.createObjectNode().put("iss", IDP.issuer()).put("sub", subject)
                .put("exp", now.plusSeconds(300).getEpochSecond()).put("iat", now.getEpochSecond())
                .put("nbf", now.minusSeconds(1).getEpochSecond()).put("aud", EXTERNAL_AUDIENCE);
        return encode(JSON.writeValueAsBytes(header)) + "." + encode(JSON.writeValueAsBytes(payload)) + ".";
    }

    private static void putClaim(ObjectNode target, String name, Object value) {
        if (value instanceof String text) target.put(name, text);
        else if (value instanceof String[] values) {
            ArrayNode array = target.putArray(name);
            for (var entry : values) array.add(entry);
        } else throw new IllegalArgumentException("测试 fixture 不支持该 JWT claim 类型。");
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

    private static String jwk(String keyId, KeyPair pair) {
        var publicKey = (RSAPublicKey) pair.getPublic();
        var modulus = encode(unsignedBytes(publicKey.getModulus().toByteArray()));
        var exponent = encode(unsignedBytes(publicKey.getPublicExponent().toByteArray()));
        return "{\"keys\":[{\"kty\":\"RSA\",\"use\":\"sig\",\"alg\":\"RS256\",\"kid\":\""
                + keyId + "\",\"n\":\"" + modulus + "\",\"e\":\"" + exponent + "\"}]}";
    }

    private static byte[] unsignedBytes(byte[] value) {
        return value.length > 1 && value[0] == 0 ? java.util.Arrays.copyOfRange(value, 1, value.length) : value;
    }

    /** 本地只读 JWKS fixture 可切换公钥或模拟短暂不可用，不连接真实 IdP。 */
    private static final class LocalJwkIssuer implements AutoCloseable {
        private final HttpServer server;
        private final AtomicReference<String> jwks = new AtomicReference<>();
        private final AtomicBoolean unavailable = new AtomicBoolean();

        private LocalJwkIssuer(KeyPair initialKey, String initialKeyId) {
            try {
                server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
                server.createContext("/jwks", exchange -> {
                    if (unavailable.get()) {
                        var body = "fixture unavailable".getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(503, body.length);
                        try (var output = exchange.getResponseBody()) { output.write(body); }
                        return;
                    }
                    var body = jwks.get().getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/jwk-set+json");
                    exchange.getResponseHeaders().add("Cache-Control", "public, max-age=300");
                    exchange.sendResponseHeaders(200, body.length);
                    try (var output = exchange.getResponseBody()) { output.write(body); }
                });
                server.start();
                publish(initialKey, initialKeyId);
            } catch (Exception failure) {
                throw new ExceptionInInitializerError(failure);
            }
        }

        private String issuer() { return "http://127.0.0.1:" + server.getAddress().getPort() + "/issuer"; }
        private String jwksUri() { return "http://127.0.0.1:" + server.getAddress().getPort() + "/jwks"; }
        private void publish(KeyPair keyPair, String keyId) { jwks.set(jwk(keyId, keyPair)); }
        private void setUnavailable(boolean value) { unavailable.set(value); }
        @Override public void close() { server.stop(0); }
    }
}
